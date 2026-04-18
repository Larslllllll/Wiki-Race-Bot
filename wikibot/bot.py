"""WikiRace bot.

Navigation priority
───────────────────
1. Graph BFS (if a local graph is loaded)
   → Finds complete path instantly, zero Wikipedia API calls.

2. Semantic step-by-step (always available, no graph required)
   a. Pre-fetch backlinks of destination (pages that link TO it).
   b. Each hop:
        i.  Destination directly on current page?  → done.
       ii.  Any link on current page is a known backlink?  → go there.
      iii.  Otherwise rank all links by semantic similarity to destination.
"""
from __future__ import annotations

import random
import time
from typing import Dict, List, Optional, Set


from .client import WikiRaceClient
from .graph import NodeKey, WikiGraph
from .model import LinearLinkScorer, TrainingExample, build_feature_vector
from .similarity import SimilarityScorer
from .types import GameSettings, GameSnapshot, PageRef  # noqa: F401 (used in type hints)
from .wikipedia import WikipediaClient

# Optional — only imported when a neural model is actually loaded
try:
    from .neural import NeuralLinkScorer as _NeuralLinkScorer
except ImportError:
    _NeuralLinkScorer = None  # type: ignore


class WikiRaceBot:
    def __init__(
        self,
        wikipedia: WikipediaClient,
        *,
        graph: Optional[WikiGraph] = None,
        scorer: Optional[LinearLinkScorer] = None,
        neural_scorer=None,
        max_moves: int = 10_000,
        game_timeout: float = 300.0,
        human_delay: float = 0.0,   # seconds between moves (0 = instant)
    ) -> None:
        self.graph         = graph
        self.scorer        = scorer
        self.neural_scorer = neural_scorer
        self.wikipedia     = wikipedia
        self.max_moves     = max_moves
        self.game_timeout  = game_timeout
        self.human_delay   = human_delay
        self._similarity   = SimilarityScorer()
        self._rdist_cache: Dict[NodeKey, Dict[NodeKey, int]] = {}

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def play(
        self,
        client: WikiRaceClient,
        snapshot: GameSnapshot,
        *,
        verbose: bool = True,
        learn: bool = False,
    ) -> List[PageRef]:
        settings    = snapshot.settings
        lang        = settings.language
        start       = settings.start
        destination = settings.destination
        deadline    = time.time() + self.game_timeout

        if verbose:
            print(f"\n[bot] '{start.title}'  →  '{destination.title}'  (lang={lang})")

        # Signal presence immediately so we appear in the player list
        try:
            client.submit_finished_path(snapshot.id, snapshot.session, [start])
        except Exception:
            pass

        # ── 1. Graph BFS ──────────────────────────────────────────────
        if self.graph is not None:
            path = self._graph_path(start, destination, lang, verbose)
            if path is not None:
                if self.human_delay > 0 and len(path) > 1:
                    # Drip-feed the path one hop at a time with human-like timing
                    for i in range(2, len(path) + 1):
                        time.sleep(random.uniform(self.human_delay * 0.6, self.human_delay * 1.4))
                        try:
                            client.submit_finished_path(snapshot.id, snapshot.session, path[:i])
                        except Exception:
                            pass
                else:
                    client.submit_finished_path(snapshot.id, snapshot.session, path)
                if verbose:
                    print(f"[bot] submitted graph path: {len(path) - 1} hop(s)")
                return path

        # ── 2. Semantic step-by-step ──────────────────────────────────
        path = self._navigate(
            start, destination, lang, deadline, verbose,
            client=client, snapshot=snapshot,
        )
        if verbose:
            print(f"[bot] finished: {len(path) - 1} hop(s)")
        return path

    def visited_pages(self, path: List[PageRef]) -> List[PageRef]:
        """Return unique pages from path for frontier injection."""
        seen: Set[str] = set()
        result: List[PageRef] = []
        for p in path:
            if p.title.lower() not in seen:
                seen.add(p.title.lower())
                result.append(p)
        return result

    # ------------------------------------------------------------------
    # Strategy 1 — graph BFS
    # ------------------------------------------------------------------

    def _graph_path(
        self,
        start: PageRef,
        destination: PageRef,
        lang: str,
        verbose: bool,
    ) -> Optional[List[PageRef]]:
        assert self.graph is not None
        start_node: NodeKey = (lang, start.title)
        dest_node:  NodeKey = (lang, destination.title)

        t0 = time.time()

        # ── resolve start ─────────────────────────────────────────────
        if start_node not in self.graph.nodes:
            if verbose:
                print(f"[bot] '{start.title}' not in graph — fetching Wikipedia links as bridge …")
            bridge_ref, start_node = self._find_bridge_node(start, lang, verbose)
            if start_node is None:
                if verbose:
                    print(f"[bot] no bridge found — falling back")
                return None
            # bridge_ref is the first hop (from Wikipedia, not graph)
        else:
            bridge_ref = None

        # ── resolve destination ───────────────────────────────────────
        if dest_node not in self.graph.nodes:
            if verbose:
                print(f"[bot] '{destination.title}' not in graph — fetching backlinks as bridge …")
            dest_bridge_ref, dest_node = self._find_dest_bridge(destination, lang, verbose)
            if dest_node is None:
                if verbose:
                    print(f"[bot] no dest bridge found — falling back")
                return None
        else:
            dest_bridge_ref = None

        # ── BFS ───────────────────────────────────────────────────────
        nodes = self.graph.shortest_path(start_node, dest_node, max_depth=8, max_nodes=200_000, timeout=10.0)
        dt = time.time() - t0

        if not nodes:
            if verbose:
                print(f"[bot] no graph path ({dt:.2f}s) — falling back")
            return None

        path = self._nodes_to_refs(nodes, start if bridge_ref is None else bridge_ref,
                                   destination if dest_bridge_ref is None else dest_bridge_ref)

        # Prepend bridge hop if start wasn't in graph
        if bridge_ref is not None:
            path = [start] + path
        # Append dest bridge hop if destination wasn't in graph
        if dest_bridge_ref is not None:
            path = path + [destination]

        if verbose:
            route = " → ".join(p.title for p in path)
            print(f"[bot] graph path ({len(path) - 1} hops, {dt:.2f}s): {route}")
        return path

    def _find_bridge_node(
        self, page: PageRef, lang: str, verbose: bool
    ) -> tuple:
        """Fetch page's Wikipedia links, return (PageRef, NodeKey) for the first one in the graph."""
        try:
            links = self.wikipedia.fetch_page_links(lang, page.title, max_pages=2)
        except Exception:
            return None, None
        if not links:
            return None, None
        candidates = [(lang, lk.title) for lk in links]
        present = self.graph.nodes_present(candidates) if hasattr(self.graph, "nodes_present") else []
        if not present:
            return None, None
        node = present[0]
        ref = next((lk for lk in links if lk.title == node[1]), None)
        if ref is None:
            ref = PageRef(lang=lang, title=node[1])
        if verbose:
            print(f"[bot] bridge: '{page.title}' → '{node[1]}' (in graph)")
        return ref, node

    def _find_dest_bridge(
        self, page: PageRef, lang: str, verbose: bool
    ) -> tuple:
        """Fetch destination backlinks, return (PageRef, NodeKey) for the first one in the graph."""
        try:
            backlinks = self.wikipedia.fetch_backlinks(lang, page.title, limit=200)
        except Exception:
            return None, None
        candidates = [(lang, t) for t in backlinks]
        present = self.graph.nodes_present(candidates) if hasattr(self.graph, "nodes_present") else []
        if not present:
            return None, None
        node = present[0]
        ref = PageRef(lang=lang, title=node[1])
        if verbose:
            print(f"[bot] dest bridge: '{node[1]}' → '{page.title}' (backlink in graph)")
        return ref, node

    def _nodes_to_refs(
        self,
        nodes: List[NodeKey],
        start: PageRef,
        destination: PageRef,
    ) -> List[PageRef]:
        refs: List[PageRef] = []
        for i, (lang, title) in enumerate(nodes):
            if i == 0:
                refs.append(start)
            elif i == len(nodes) - 1:
                refs.append(destination)
            else:
                pageid = self.graph.page_ids.get((lang, title)) if self.graph else None
                refs.append(PageRef(lang=lang, title=title, pageid=pageid))
        return refs

    # ------------------------------------------------------------------
    # Strategy 2 — semantic step-by-step
    # ------------------------------------------------------------------

    def _navigate(
        self,
        start: PageRef,
        destination: PageRef,
        lang: str,
        deadline: float,
        verbose: bool,
        *,
        client: "WikiRaceClient",
        snapshot: "GameSnapshot",
    ) -> List[PageRef]:
        # Pre-fetch backlinks: pages that link TO destination.
        # These are "one hop from destination" — prioritise them.
        if verbose:
            print(f"[bot] fetching destination backlinks …", flush=True)
        try:
            backlinks: Set[str] = self.wikipedia.fetch_backlinks(lang, destination.title, limit=500)
            if verbose:
                print(f"[bot] {len(backlinks)} backlinks found")
        except Exception as exc:
            if verbose:
                print(f"[bot] backlinks failed ({exc}) — continuing without")
            backlinks = set()

        current  = start
        path: List[PageRef] = [current]
        visited: Set[str]   = {current.title.lower()}
        steps_without_progress = 0   # hops since last backlink hit (or start)
        RANDOM_WALK_THRESHOLD = 8    # switch to random after this many stuck hops

        for step in range(1, self.max_moves + 1):
            if self._is_dest(current, destination):
                break

            if time.time() > deadline:
                if verbose:
                    print(f"                  (timeout passed at step {step - 1} — continuing anyway)", flush=True)
                deadline = float("inf")  # never check again

            random_walk = steps_without_progress >= RANDOM_WALK_THRESHOLD
            next_page, hit_backlink = self._pick_next(
                current, destination, lang, backlinks, visited, verbose,
                force_random=random_walk,
            )

            if hit_backlink or random_walk:
                steps_without_progress = 0
            else:
                steps_without_progress += 1

            if self.human_delay > 0:
                time.sleep(random.uniform(self.human_delay * 0.6, self.human_delay * 1.4))

            if verbose:
                prefix = "[rnd] " if random_walk else ""
                print(f"[move {step:02d}] {prefix}'{current.title}'  →  '{next_page.title}'")

            path.append(next_page)
            visited.add(next_page.title.lower())
            current = next_page

            # Keep the server alive — intermediate submissions fail (path doesn't end
            # at destination yet) but the final one succeeds. Errors are expected & harmless.
            try:
                client.submit_finished_path(snapshot.id, snapshot.session, path)
            except Exception:
                pass  # expected until we reach the destination

        else:
            raise RuntimeError(
                f"Reached move limit ({self.max_moves}) without reaching destination"
            )

        return path

    def _pick_next(
        self,
        current: PageRef,
        destination: PageRef,
        lang: str,
        backlinks: Set[str],
        visited: Set[str],
        verbose: bool,
        *,
        force_random: bool = False,
    ) -> tuple:   # (PageRef, hit_backlink: bool)
        links = self.wikipedia.fetch_page_links(lang, current.title, max_pages=4)
        link_map: Dict[str, PageRef] = {l.title: l for l in links}
        candidates = [l for l in links if l.title.lower() not in visited]

        if not candidates:
            candidates = links
            if not candidates:
                raise RuntimeError(f"No links at all on '{current.title}' — dead end")
            if verbose:
                print(f"                  (all links visited — allowing revisit)", flush=True)

        titles = [l.title for l in candidates]

        # ── i. Direct link to destination? ──────────────────────────
        if destination.title in titles:
            ref = PageRef(lang=lang, title=destination.title, pageid=destination.pageid)
            return ref, True

        # ── ii. Any link is a known backlink? ────────────────────────
        backlink_titles = [t for t in titles if t in backlinks]

        if backlink_titles:
            ranked = self._rank(current.title, backlink_titles, destination.title)
            best_title = ranked[0][1]
            if verbose:
                print(f"                  (backlink hit → '{best_title}')", flush=True)
            ref = link_map.get(best_title) or PageRef(lang=lang, title=best_title)
            return ref, True

        # ── iii. Random walk (when stuck) ────────────────────────────
        if force_random:
            chosen = random.choice(candidates)
            return chosen, False

        # ── iv. Neural scorer or semantic similarity ─────────────────
        ranked = self._rank(current.title, titles, destination.title)
        best_title = ranked[0][1]
        ref = link_map.get(best_title) or PageRef(lang=lang, title=best_title)
        return ref, False

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _rank(self, current_title: str, candidates: List[str], dest_title: str):
        if self.neural_scorer is not None:
            return self.neural_scorer.rank(current_title, candidates, dest_title)
        return self._similarity.rank(candidates, dest_title)

    @staticmethod
    def _is_dest(current: PageRef, destination: PageRef) -> bool:
        if current.pageid is not None and destination.pageid is not None:
            return current.pageid == destination.pageid
        return current.title.lower() == destination.title.lower()

    def create_random_settings(self, language: str) -> GameSettings:
        start, destination = self.wikipedia.fetch_random_pages(language, 2)
        while destination.pageid == start.pageid:
            destination = self.wikipedia.fetch_random_pages(language, 1)[0]
        return GameSettings(language=language, start=start, destination=destination)
