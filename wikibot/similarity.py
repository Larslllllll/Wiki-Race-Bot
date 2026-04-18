"""Semantic link scorer — ranks Wikipedia link candidates by title similarity.

Works without any local graph or training data.

  • If ``sentence-transformers`` is installed → neural cosine similarity.
  • Otherwise → fast token-overlap heuristic (still surprisingly good).
"""
from __future__ import annotations

import re
from typing import List, Tuple

_TOKEN_RE = re.compile(r"\w+", re.UNICODE)


def _tokens(text: str) -> List[str]:
    return _TOKEN_RE.findall(text.lower())


def _jaccard(a: List[str], b: List[str]) -> float:
    if not a or not b:
        return 0.0
    sa, sb = set(a), set(b)
    return len(sa & sb) / len(sa | sb)


class SimilarityScorer:
    """Ranks link candidates by semantic proximity to the destination title."""

    def __init__(self) -> None:
        self._model = None
        self._tried = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def rank(
        self,
        candidates: List[str],
        destination: str,
    ) -> List[Tuple[float, str]]:
        """Return [(score, title), …] sorted descending."""
        if not candidates:
            return []
        model = self._load_model()
        if model is not None:
            return self._rank_neural(model, candidates, destination)
        return self._rank_heuristic(candidates, destination)

    # ------------------------------------------------------------------
    # Backends
    # ------------------------------------------------------------------

    def _load_model(self):
        if self._tried:
            return self._model
        self._tried = True
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore
            print("[scorer] loading sentence-transformers (all-MiniLM-L6-v2) …", flush=True)
            self._model = SentenceTransformer("all-MiniLM-L6-v2")
            print("[scorer] model ready", flush=True)
        except ImportError:
            print("[scorer] sentence-transformers not installed → token-overlap fallback")
        except Exception as exc:
            print(f"[scorer] model failed to load ({exc}) → token-overlap fallback")
        return self._model

    def _rank_neural(self, model, candidates: List[str], destination: str) -> List[Tuple[float, str]]:
        import numpy as np  # type: ignore

        texts = candidates + [destination]
        embs = model.encode(texts, batch_size=512, show_progress_bar=False, normalize_embeddings=True)
        dest_emb = embs[-1]
        cand_embs = embs[:-1]
        sims = (cand_embs @ dest_emb).tolist()
        scored = sorted(zip(sims, candidates), reverse=True)
        return scored

    def _rank_heuristic(self, candidates: List[str], destination: str) -> List[Tuple[float, str]]:
        dest_tok = _tokens(destination)
        scored: List[Tuple[float, str]] = []
        for title in candidates:
            cand_tok = _tokens(title)
            score = _jaccard(cand_tok, dest_tok)
            # Bonus: shares first word with destination
            if cand_tok and dest_tok and cand_tok[0] == dest_tok[0]:
                score += 0.25
            scored.append((score, title))
        scored.sort(reverse=True)
        return scored
