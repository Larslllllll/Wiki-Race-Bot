from __future__ import annotations

import json
import random
from collections import defaultdict, deque
from pathlib import Path
from typing import Deque, Dict, Iterable, List, Optional, Set, Tuple


NodeKey = Tuple[str, str]


class WikiGraph:
    def __init__(
        self,
        adjacency: Dict[NodeKey, List[NodeKey]],
        reverse_adjacency: Dict[NodeKey, List[NodeKey]],
        in_degree: Dict[NodeKey, int],
        out_degree: Dict[NodeKey, int],
        page_ids: Dict[NodeKey, int],
    ) -> None:
        self.adjacency = adjacency
        self.reverse_adjacency = reverse_adjacency
        self.in_degree = in_degree
        self.out_degree = out_degree
        self.page_ids = page_ids
        self.nodes = set(adjacency) | set(reverse_adjacency) | set(page_ids)

    @classmethod
    def load(
        cls,
        edges_path: Path,
        pages_path: Optional[Path] = None,
        max_edges: Optional[int] = None,
    ) -> "WikiGraph":
        """Load graph from JSONL files.

        max_edges: if set, read at most this many edges (the first N, which are
        the earliest-crawled and thus most heavily-linked hub pages — ideal for
        training).  Use e.g. max_edges=2_000_000 to cap RAM on large crawls.
        """
        # Use lists directly — no intermediate set, no sorted() conversion.
        # Dedup is done in-place after loading so we never hold two copies simultaneously.
        adjacency: Dict[NodeKey, List[NodeKey]] = defaultdict(list)
        reverse: Dict[NodeKey, List[NodeKey]] = defaultdict(list)
        in_degree: Dict[NodeKey, int] = defaultdict(int)
        out_degree: Dict[NodeKey, int] = defaultdict(int)
        page_ids: Dict[NodeKey, int] = {}

        if pages_path and pages_path.exists():
            with pages_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    page_id = row.get("page_id")
                    if page_id:
                        page_ids[(row["lang"], row["title"])] = int(page_id)

        edges_loaded = 0
        with edges_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                if max_edges is not None and edges_loaded >= max_edges:
                    break
                row = json.loads(line)
                src = (row["from_lang"], row["from_title"])
                dst = (row["to_lang"], row["to_title"])
                adjacency[src].append(dst)
                reverse[dst].append(src)
                out_degree[src] += 1
                in_degree[dst] += 1
                edges_loaded += 1

                from_page_id = row.get("from_page_id")
                to_page_id = row.get("to_page_id")
                if from_page_id:
                    page_ids[src] = int(from_page_id)
                if to_page_id:
                    page_ids[dst] = int(to_page_id)

        # Deduplicate neighbor lists in-place (no extra global seen-set needed).
        # dict.fromkeys preserves first-occurrence order and runs in O(n) per node.
        for node in adjacency:
            nbrs = adjacency[node]
            if len(nbrs) != len(set(nbrs)):
                adjacency[node] = list(dict.fromkeys(nbrs))
                # Fix degree count to match deduplicated list
                out_degree[node] = len(adjacency[node])
        for node in reverse:
            parents = reverse[node]
            if len(parents) != len(set(parents)):
                reverse[node] = list(dict.fromkeys(parents))
                in_degree[node] = len(reverse[node])

        return cls(dict(adjacency), dict(reverse), dict(in_degree), dict(out_degree), page_ids)

    def has_node(self, node: NodeKey) -> bool:
        return node in self.nodes

    def neighbors(self, node: NodeKey) -> List[NodeKey]:
        return self.adjacency.get(node, [])

    def reverse_neighbors(self, node: NodeKey) -> List[NodeKey]:
        return self.reverse_adjacency.get(node, [])

    def sample_destinations(
        self,
        count: int,
        rng: random.Random,
        preferred_langs: Iterable[str] = ("en", "de"),
    ) -> List[NodeKey]:
        preferred = [node for node in self.nodes if node[0] in preferred_langs]
        others = [node for node in self.nodes if node[0] not in preferred_langs]
        rng.shuffle(preferred)
        rng.shuffle(others)
        return (preferred + others)[:count]

    def shortest_path(
        self,
        start: NodeKey,
        destination: NodeKey,
        max_depth: int = 8,
        max_nodes: int = 100_000,
    ) -> Optional[List[NodeKey]]:
        if start == destination:
            return [start]
        if start not in self.nodes or destination not in self.nodes:
            return None

        queue: Deque[Tuple[NodeKey, int]] = deque([(start, 0)])
        parents: Dict[NodeKey, Optional[NodeKey]] = {start: None}

        while queue and len(parents) <= max_nodes:
            node, depth = queue.popleft()
            if depth >= max_depth:
                continue
            for neighbor in self.neighbors(node):
                if neighbor in parents:
                    continue
                parents[neighbor] = node
                if neighbor == destination:
                    path = [destination]
                    cursor: Optional[NodeKey] = destination
                    while cursor is not None:
                        cursor = parents[cursor]
                        if cursor is not None:
                            path.append(cursor)
                    path.reverse()
                    return path
                queue.append((neighbor, depth + 1))
        return None

    def reverse_distances(
        self,
        destination: NodeKey,
        max_depth: int = 8,
        max_nodes: int = 100_000,
    ) -> Dict[NodeKey, int]:
        if destination not in self.nodes:
            return {}

        distances: Dict[NodeKey, int] = {destination: 0}
        queue: Deque[NodeKey] = deque([destination])

        while queue and len(distances) <= max_nodes:
            node = queue.popleft()
            depth = distances[node]
            if depth >= max_depth:
                continue
            for parent in self.reverse_neighbors(node):
                if parent in distances:
                    continue
                distances[parent] = depth + 1
                queue.append(parent)
        return distances
