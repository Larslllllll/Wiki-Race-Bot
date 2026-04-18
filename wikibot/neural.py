"""Transformer-based link scorer for WikiRace.

Architecture
────────────
Cross-encoder:  "{current_title} | {candidate_title} | {destination_title}"
                      → DistilBERT → binary classifier (good hop / bad hop)

GPU support
───────────
  Priority:  torch-directml (Intel Arc on Windows)  →  CUDA  →  CPU
  Install:   pip install torch-directml transformers

Training data
─────────────
  Generated from the local crawl graph:
    positive label → link whose reverse-BFS distance < current node's distance
    negative label → link that doesn't improve distance
"""
from __future__ import annotations

import random
import time
from pathlib import Path
from typing import List, Optional

_MODEL_ID  = "distilbert-base-uncased"   # ~66 M params, fits easily in 8 GB
_MAX_LEN   = 64                           # title strings are short
_DEFAULT_MODEL_DIR = Path("models/neural_scorer")


# ── device selection ──────────────────────────────────────────────────────────

def get_device(gpu_index: int = 0):
    """Auto-detect the best available device.

    Order:  torch-directml (Intel Arc / AMD / any DX12)  →  CUDA  →  CPU
    """
    try:
        import torch_directml  # type: ignore
        dev = torch_directml.device(gpu_index)
        print(f"[device] Intel Arc GPU via DirectML (index {gpu_index})")
        return dev
    except (ImportError, Exception):
        pass
    try:
        import torch
        if torch.cuda.is_available():
            name = torch.cuda.get_device_name(gpu_index)
            dev  = torch.device(f"cuda:{gpu_index}")
            print(f"[device] CUDA GPU {gpu_index}: {name}")
            return dev
    except Exception:
        pass
    import torch
    print("[device] CPU (install torch-directml for Intel Arc GPU support)")
    return torch.device("cpu")


# ── scorer ────────────────────────────────────────────────────────────────────

class NeuralLinkScorer:
    """Loads a fine-tuned cross-encoder and scores link candidates at runtime."""

    def __init__(
        self,
        model_dir: Path = _DEFAULT_MODEL_DIR,
        device=None,
        batch_size: int = 128,
    ) -> None:
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer  # type: ignore

        self.device     = device if device is not None else get_device()
        self.batch_size = batch_size

        print(f"[neural] loading model from {model_dir} …", flush=True)
        self.tok   = AutoTokenizer.from_pretrained(str(model_dir))
        self.model = AutoModelForSequenceClassification.from_pretrained(
            str(model_dir), num_labels=2
        )
        self.model.to(self.device)
        self.model.eval()
        print("[neural] model ready")

    # ------------------------------------------------------------------

    def rank(
        self,
        current: str,
        candidates: List[str],
        destination: str,
    ) -> List[tuple[float, str]]:
        """Score all candidates and return [(score, title), …] sorted descending."""
        if not candidates:
            return []

        import torch

        texts = [f"{current} | {c} | {destination}" for c in candidates]
        all_probs: List[float] = []

        for i in range(0, len(texts), self.batch_size):
            chunk = texts[i : i + self.batch_size]
            enc   = self.tok(
                chunk,
                padding=True,
                truncation=True,
                max_length=_MAX_LEN,
                return_tensors="pt",
            )
            enc = {k: v.to(self.device) for k, v in enc.items()}
            with torch.no_grad():
                logits = self.model(**enc).logits
            probs = torch.softmax(logits, dim=-1)[:, 1].cpu().tolist()
            all_probs.extend(probs)

        scored = sorted(zip(all_probs, candidates), reverse=True)
        return scored

    # ------------------------------------------------------------------
    # Class-level training entry point
    # ------------------------------------------------------------------

    @classmethod
    def train(
        cls,
        examples: List[dict],
        output_dir: Path = _DEFAULT_MODEL_DIR,
        *,
        device=None,
        epochs: int = 3,
        batch_size: int = 128,
        lr: float = 2e-5,
        max_examples: Optional[int] = None,
        start_from: Optional[str] = None,
    ) -> "NeuralLinkScorer":
        """start_from: path to a saved model dir to continue from (instead of base distilbert)."""
        """Fine-tune DistilBERT on WikiRace hop-ranking examples.

        Each example must have keys:
            current      – current page title (str)
            candidate    – link title to score (str)
            destination  – destination title (str)
            label        – 1 = good hop, 0 = bad hop (int)
        """
        import torch
        from torch.utils.data import DataLoader, TensorDataset
        from transformers import (  # type: ignore
            AutoModelForSequenceClassification,
            AutoTokenizer,
            get_linear_schedule_with_warmup,
        )

        device = device if device is not None else get_device()

        # ── subsample if requested ────────────────────────────────────
        if max_examples and len(examples) > max_examples:
            random.shuffle(examples)
            examples = examples[:max_examples]

        n_pos = sum(1 for e in examples if e["label"] == 1)
        n_neg = len(examples) - n_pos
        print(f"[train] {len(examples):,} examples  ({n_pos:,} pos / {n_neg:,} neg)")

        # ── tokenise ──────────────────────────────────────────────────
        print(f"[train] tokenising with {_MODEL_ID} …", flush=True)
        tok   = AutoTokenizer.from_pretrained(_MODEL_ID)
        texts = [
            f"{e['current']} | {e['candidate']} | {e['destination']}"
            for e in examples
        ]
        enc = tok(
            texts,
            padding=True,
            truncation=True,
            max_length=_MAX_LEN,
            return_tensors="pt",
        )
        labels  = torch.tensor([e["label"] for e in examples], dtype=torch.long)
        dataset = TensorDataset(enc["input_ids"], enc["attention_mask"], labels)
        loader  = DataLoader(dataset, batch_size=batch_size, shuffle=True, pin_memory=False)

        # ── model ─────────────────────────────────────────────────────
        import gc
        load_from = start_from if start_from else _MODEL_ID
        print(f"[train] loading model from {load_from} …", flush=True)
        model = AutoModelForSequenceClassification.from_pretrained(load_from, num_labels=2)
        model.to(device)
        # Release any mmap handles the loader held on the model file so
        # later save_pretrained() can overwrite it without Windows error 1224.
        gc.collect()

        optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=0.01, foreach=False)
        total_steps = len(loader) * epochs
        scheduler   = get_linear_schedule_with_warmup(
            optimizer,
            num_warmup_steps=max(1, total_steps // 10),
            num_training_steps=total_steps,
        )

        # ── training loop ─────────────────────────────────────────────
        try:
            from tqdm import tqdm as _tqdm
        except ImportError:
            _tqdm = None

        import shutil

        def _atomic_save(label: str) -> None:
            """Save model to a temp dir, then atomically rename over output_dir."""
            nonlocal start_from
            tmp_dir = output_dir.parent / (output_dir.name + "_saving")
            try:
                if tmp_dir.exists():
                    shutil.rmtree(tmp_dir)
                tmp_dir.mkdir(parents=True)

                cpu_state = {k: v.detach().cpu() for k, v in model.state_dict().items()}
                cpu_model = type(model)(model.config)
                cpu_model.load_state_dict(cpu_state)
                cpu_model.save_pretrained(str(tmp_dir))
                tok.save_pretrained(str(tmp_dir))
                del cpu_model, cpu_state
                gc.collect()

                old_dir = output_dir.parent / (output_dir.name + "_old")
                if old_dir.exists():
                    shutil.rmtree(old_dir)
                if output_dir.exists():
                    output_dir.rename(old_dir)
                tmp_dir.rename(output_dir)
                if old_dir.exists():
                    shutil.rmtree(old_dir, ignore_errors=True)

                start_from = str(output_dir)
                print(f"\n[train] saved → {output_dir}  ({label})", flush=True)
            except Exception as _save_err:
                print(f"\n[train] save failed: {_save_err} — continuing without save", flush=True)
                shutil.rmtree(tmp_dir, ignore_errors=True)

        output_dir.mkdir(parents=True, exist_ok=True)
        epoch_label  = str(epochs) if epochs else "∞"
        save_every   = max(1, int(len(loader) * 0.05))   # checkpoint every 5 % of batches
        epoch = 0
        try:
            while True:
                epoch += 1
                if epochs and epoch > epochs:
                    break

                model.train()
                total_loss = 0.0
                correct    = 0

                bar = (
                    _tqdm(loader, desc=f"epoch {epoch}/{epoch_label}", unit="batch", leave=True)
                    if _tqdm else loader
                )

                skipped = 0
                for step, batch in enumerate(bar, 1):
                    try:
                        ids, mask, lbl = (t.to(device) for t in batch)
                        optimizer.zero_grad(set_to_none=True)
                        out = model(input_ids=ids, attention_mask=mask, labels=lbl)
                        out.loss.backward()
                        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
                        optimizer.step()
                        scheduler.step()

                        total_loss += out.loss.item()
                        correct    += (out.logits.argmax(dim=-1) == lbl).sum().item()
                    except Exception as _batch_err:
                        skipped += 1
                        optimizer.zero_grad(set_to_none=True)
                        if skipped <= 3:
                            print(f"\n[train] skipped batch {step} ({_batch_err})", flush=True)
                        continue

                    if _tqdm:
                        bar.set_postfix(loss=f"{total_loss/step:.4f}", acc=f"{correct/(step*batch_size)*100:.1f}%")
                    elif step % 50 == 0 or step == len(loader):
                        print(f"\r[train] epoch {epoch}/{epoch_label}  step {step}/{len(loader)}  loss={total_loss/step:.4f}  acc={correct/(step*batch_size)*100:.1f}%", end="", flush=True)

                    # Mid-epoch checkpoint every 5 % of batches
                    if step % save_every == 0:
                        model.eval()
                        _atomic_save(f"epoch {epoch} step {step}/{len(loader)}")
                        model.train()

                if not _tqdm:
                    print()

                _atomic_save(f"epoch {epoch}")

        except KeyboardInterrupt:
            print(f"\n[train] stopped by user after epoch {epoch}")
            _atomic_save(f"epoch {epoch} interrupted")
            raise

        # Return a ready-to-use scorer
        inst        = cls.__new__(cls)
        inst.device = device
        inst.tok    = tok
        inst.model  = model.eval()
        inst.batch_size = 128
        return inst


# ── training data generation ─────────────────────────────────────────────────

def generate_neural_examples(
    graph,                         # WikiGraph
    *,
    destination_count:     int = 300,
    max_pages_per_dest:    int = 50,
    negative_ratio:        int = 2,
    pages_path: Optional[Path]  = None,
    verbose:               bool = True,
) -> List[dict]:
    """Build text-format training examples from the crawl graph.

    Returns a list of dicts with keys: current, candidate, destination, label.
    """
    import random as _rnd
    from .model import _sample_destinations_from_file  # reuse existing helper

    rng = _rnd.Random()

    if pages_path and pages_path.exists():
        destinations = _sample_destinations_from_file(pages_path, destination_count, rng, graph)
    else:
        destinations = graph.sample_destinations(destination_count, rng)

    if verbose:
        print(f"[train] generating neural examples from {len(destinations)} destinations …")

    examples: List[dict] = []

    for dest_node in destinations:
        dest_title = dest_node[1]
        rdist = graph.reverse_distances(dest_node, max_depth=8, max_nodes=150_000)

        candidates = [
            n for n, d in rdist.items() if d > 0 and graph.neighbors(n)
        ]
        rng.shuffle(candidates)

        for current_node in candidates[:max_pages_per_dest]:
            cur_dist = rdist[current_node]
            cur_title = current_node[1]

            positives = [
                nb for nb in graph.neighbors(current_node)
                if rdist.get(nb, cur_dist + 99) < cur_dist
            ]
            negatives = [
                nb for nb in graph.neighbors(current_node)
                if rdist.get(nb, cur_dist + 99) >= cur_dist
            ]

            if not positives or not negatives:
                continue

            pos = rng.choice(positives)
            examples.append({
                "current":     cur_title,
                "candidate":   pos[1],
                "destination": dest_title,
                "label":       1,
            })

            rng.shuffle(negatives)
            for neg in negatives[:negative_ratio]:
                examples.append({
                    "current":     cur_title,
                    "candidate":   neg[1],
                    "destination": dest_title,
                    "label":       0,
                })

    if verbose:
        print(f"[train] generated {len(examples):,} neural training examples")
    return examples
