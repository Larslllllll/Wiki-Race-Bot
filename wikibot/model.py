from __future__ import annotations

import json
import math
import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from .graph import NodeKey, WikiGraph


TOKEN_RE = re.compile(r"\w+", re.UNICODE)


def tokenize(text: str) -> List[str]:
    return TOKEN_RE.findall(text.lower())


def overlap_ratio(left: List[str], right: List[str]) -> float:
    if not left or not right:
        return 0.0
    left_set = set(left)
    right_set = set(right)
    return len(left_set & right_set) / max(len(left_set | right_set), 1)


def sigmoid(value: float) -> float:
    if value >= 0:
        z = math.exp(-value)
        return 1.0 / (1.0 + z)
    z = math.exp(value)
    return z / (1.0 + z)


def build_feature_vector(
    graph: WikiGraph,
    current: NodeKey,
    candidate: NodeKey,
    destination: NodeKey,
    reverse_distances: Dict[NodeKey, int],
) -> Dict[str, float]:
    current_tokens = tokenize(current[1])
    candidate_tokens = tokenize(candidate[1])
    destination_tokens = tokenize(destination[1])
    candidate_distance = reverse_distances.get(candidate)
    current_distance = reverse_distances.get(current)

    features: Dict[str, float] = {
        "bias": 1.0,
        "candidate_lang_match_destination": 1.0 if candidate[0] == destination[0] else 0.0,
        "candidate_is_exact_destination": 1.0 if candidate == destination else 0.0,
        "candidate_overlap_destination": overlap_ratio(candidate_tokens, destination_tokens),
        "current_overlap_destination": overlap_ratio(current_tokens, destination_tokens),
        "candidate_in_degree": math.log1p(graph.in_degree.get(candidate, 0)),
        "candidate_out_degree": math.log1p(graph.out_degree.get(candidate, 0)),
        "candidate_title_length": float(len(candidate_tokens)),
        "destination_title_length": float(len(destination_tokens)),
    }

    if candidate_distance is not None:
        features["candidate_distance_known"] = 1.0
        features["candidate_distance_inverse"] = 1.0 / (1.0 + candidate_distance)
        features["candidate_distance_is_1"] = 1.0 if candidate_distance == 1 else 0.0
        features["candidate_distance_is_2"] = 1.0 if candidate_distance == 2 else 0.0
        features["candidate_distance_is_far"] = 1.0 if candidate_distance >= 4 else 0.0
    else:
        features["candidate_distance_unknown"] = 1.0

    if current_distance is not None:
        features["current_distance_known"] = 1.0

    for token in destination_tokens[:6]:
        if token in candidate_tokens:
            features[f"dest_token_hit::{token}"] = 1.0

    return features


@dataclass
class TrainingExample:
    label: int
    features: Dict[str, float]


class LinearLinkScorer:
    def __init__(self, weights: Dict[str, float] | None = None) -> None:
        self.weights = weights or {}

    def score_features(self, features: Dict[str, float]) -> float:
        return sum(self.weights.get(name, 0.0) * value for name, value in features.items())

    def score(
        self,
        graph: WikiGraph,
        current: NodeKey,
        candidate: NodeKey,
        destination: NodeKey,
        reverse_distances: Dict[NodeKey, int],
    ) -> float:
        features = build_feature_vector(graph, current, candidate, destination, reverse_distances)
        return self.score_features(features)

    def train(
        self,
        examples: Iterable[TrainingExample],
        epochs: Optional[int] = None,
        learning_rate: float = 0.08,
        l2: float = 0.0005,
        verbose: bool = False,
        patience: int = 3,
        min_delta: float = 0.001,
    ) -> None:
        rows = list(examples)
        if not rows:
            return

        n_pos = sum(1 for r in rows if r.label == 1)
        n_neg = len(rows) - n_pos
        epoch_label = f"up to {epochs}" if epochs is not None else "∞"
        if verbose:
            print(f"[train] {len(rows):,} examples ({n_pos:,} pos / {n_neg:,} neg), {epoch_label} epochs  (Ctrl+C to stop)")

        best_loss = float("inf")
        no_improve = 0
        epoch = 0

        try:
            while True:
                epoch += 1
                if epochs is not None and epoch > epochs:
                    break
                random.shuffle(rows)
                loss = 0.0
                correct = 0
                for row in rows:
                    score = self.score_features(row.features)
                    prediction = sigmoid(score)
                    error = row.label - prediction
                    p_clipped = max(min(prediction, 1 - 1e-9), 1e-9)
                    loss += -(row.label * math.log(p_clipped) + (1 - row.label) * math.log(1 - p_clipped))
                    correct += 1 if (prediction >= 0.5) == bool(row.label) else 0
                    for name, value in row.features.items():
                        current_weight = self.weights.get(name, 0.0)
                        self.weights[name] = current_weight + learning_rate * (
                            error * value - l2 * current_weight
                        )
                avg_loss = loss / len(rows)
                if verbose:
                    acc = correct / len(rows) * 100
                    label = str(epochs) if epochs is not None else "∞"
                    print(f"[train] epoch {epoch}/{label}  loss={avg_loss:.4f}  acc={acc:.1f}%")

                if best_loss - avg_loss > min_delta:
                    best_loss = avg_loss
                    no_improve = 0
                else:
                    no_improve += 1
                    if no_improve >= patience:
                        if epochs is None:
                            # infinite mode: keep going with reshuffled examples
                            no_improve = 0
                            best_loss = float("inf")
                            if verbose:
                                print(f"[train] plateau reached — resampling examples for next round")
                            break  # signal caller to regenerate examples
                        else:
                            if verbose:
                                print(f"[train] early stop at epoch {epoch} (no improvement for {patience} epochs)")
                            break
        except KeyboardInterrupt:
            if verbose:
                print(f"\n[train] stopped at epoch {epoch} by user")

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "weights": self.weights,
        }
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    @classmethod
    def load(cls, path: Path) -> "LinearLinkScorer":
        payload = json.loads(path.read_text(encoding="utf-8"))
        return cls(weights={key: float(value) for key, value in payload["weights"].items()})


def _sample_destinations_from_file(
    pages_path: Path,
    count: int,
    rng: random.Random,
    graph: "WikiGraph",
) -> List[NodeKey]:
    """Pick random destinations by random line index from pages.jsonl."""
    import os
    lines: List[int] = []  # byte offsets of valid lines
    with pages_path.open("rb") as fh:
        offset = 0
        for raw in fh:
            if raw.strip():
                lines.append(offset)
            offset += len(raw)

    rng.shuffle(lines)
    destinations: List[NodeKey] = []
    with pages_path.open("r", encoding="utf-8") as fh:
        for byte_offset in lines:
            if len(destinations) >= count:
                break
            fh.seek(byte_offset)
            try:
                row = json.loads(fh.readline())
                node: NodeKey = (row["lang"], row["title"])
                if node in graph.nodes:
                    destinations.append(node)
            except Exception:
                pass
    return destinations


def generate_training_examples(
    graph: WikiGraph,
    destination_count: int = 220,
    max_pages_per_destination: int = 40,
    negative_ratio: int = 2,
    verbose: bool = False,
    pages_path: Optional[Path] = None,
) -> List[TrainingExample]:
    rng = random.Random()  # seeded from system time → different each run
    examples: List[TrainingExample] = []

    source_nodes = len([n for n in graph.nodes if graph.neighbors(n)])
    if verbose:
        print(f"[train] graph: {len(graph.nodes):,} nodes, {source_nodes:,} with outlinks, sampling {destination_count} destinations ...")

    if pages_path and pages_path.exists():
        destinations = _sample_destinations_from_file(pages_path, destination_count, rng, graph)
        if verbose:
            print(f"[train] sampled {len(destinations)} destinations from {pages_path.name}")
    else:
        destinations = graph.sample_destinations(destination_count, rng)

    for destination in destinations:
        reverse_distances = graph.reverse_distances(destination, max_depth=6, max_nodes=60_000)
        candidates = [node for node, depth in reverse_distances.items() if depth > 0 and graph.neighbors(node)]
        rng.shuffle(candidates)

        for current in candidates[:max_pages_per_destination]:
            current_distance = reverse_distances[current]
            positive_neighbors = [
                neighbor
                for neighbor in graph.neighbors(current)
                if reverse_distances.get(neighbor, current_distance + 99) < current_distance
            ]
            negative_neighbors = [
                neighbor
                for neighbor in graph.neighbors(current)
                if reverse_distances.get(neighbor, current_distance + 99) >= current_distance
            ]
            if not positive_neighbors or not negative_neighbors:
                continue

            positive = rng.choice(positive_neighbors)
            examples.append(
                TrainingExample(
                    label=1,
                    features=build_feature_vector(graph, current, positive, destination, reverse_distances),
                )
            )

            rng.shuffle(negative_neighbors)
            for negative in negative_neighbors[:negative_ratio]:
                examples.append(
                    TrainingExample(
                        label=0,
                        features=build_feature_vector(graph, current, negative, destination, reverse_distances),
                    )
                )

    if verbose:
        print(f"[train] generated {len(examples):,} training examples")
    return examples


def train_model(
    graph: WikiGraph,
    destination_count: int = 220,
    max_pages_per_destination: int = 40,
    negative_ratio: int = 2,
    epochs: Optional[int] = None,
    pages_path: Optional[Path] = None,
    verbose: bool = True,
) -> LinearLinkScorer:
    """Single training round — used by ensure_model and tests."""
    examples = generate_training_examples(
        graph,
        verbose=verbose,
        destination_count=destination_count,
        max_pages_per_destination=max_pages_per_destination,
        negative_ratio=negative_ratio,
        pages_path=pages_path,
    )
    scorer = LinearLinkScorer()
    scorer.train(examples, epochs=epochs, verbose=verbose)
    return scorer
