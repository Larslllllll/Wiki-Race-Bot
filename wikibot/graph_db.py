"""SQLite-backed Wikipedia graph — on-demand BFS, zero RAM loading.

Build the database once:
    python wiki_race_bot.py build-index

Then play instantly:
    python wiki_race_bot.py play --graph --join CODE
    # opens DB in milliseconds, BFS finds path in 1-5 seconds
"""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

NodeKey = Tuple[str, str]

DEFAULT_DB = Path("crawl_output/graph.db")


# ── duck-type proxies so WikiGraphDB is drop-in for WikiGraph in bot.py ──────

class _NodeProxy:
    """Proxy for `graph.nodes` — supports `node in graph.nodes` via DB lookup."""
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def __contains__(self, node: object) -> bool:
        if not isinstance(node, tuple) or len(node) != 2:
            return False
        cur = self._conn.execute(
            "SELECT 1 FROM nodes WHERE lang=? AND title=? LIMIT 1", node
        )
        return cur.fetchone() is not None


class _PageIdProxy:
    """Proxy for `graph.page_ids` — supports `.get((lang, title))` via DB lookup."""
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def get(self, node: NodeKey, default: Optional[int] = None) -> Optional[int]:
        cur = self._conn.execute(
            "SELECT page_id FROM nodes WHERE lang=? AND title=?", node
        )
        row = cur.fetchone()
        return row[0] if row and row[0] is not None else default


# ── main class ────────────────────────────────────────────────────────────────

class WikiGraphDB:
    """On-demand BFS over a SQLite graph — no RAM loading, instant startup."""

    def __init__(self, db_path: Path = DEFAULT_DB) -> None:
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        # Tune SQLite for read-heavy random access
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA cache_size=-524288")    # 512 MB page cache
        self._conn.execute("PRAGMA temp_store=MEMORY")
        self._conn.execute("PRAGMA mmap_size=4294967296")  # up to 4 GB mmap

        # Duck-type compatibility with WikiGraph
        self.nodes    = _NodeProxy(self._conn)
        self.page_ids = _PageIdProxy(self._conn)

    # ------------------------------------------------------------------

    def nodes_present(self, nodes: List[NodeKey]) -> List[NodeKey]:
        """Return the subset of nodes that exist in the DB."""
        if not nodes:
            return []
        c = self._conn
        c.execute("CREATE TEMP TABLE IF NOT EXISTS _check_q (lang TEXT, title TEXT)")
        c.execute("DELETE FROM _check_q")
        c.executemany("INSERT INTO _check_q VALUES (?,?)", nodes)
        rows = c.execute(
            "SELECT n.lang, n.title FROM nodes n JOIN _check_q q ON n.lang=q.lang AND n.title=q.title"
        ).fetchall()
        return [(r[0], r[1]) for r in rows]

    def _neighbors_batch(self, nodes: List[NodeKey]) -> Dict[NodeKey, List[NodeKey]]:
        """Fetch outgoing neighbors for many nodes at once via a temp table."""
        result: Dict[NodeKey, List[NodeKey]] = {}
        if not nodes:
            return result

        c = self._conn
        c.execute("CREATE TEMP TABLE IF NOT EXISTS _bfs_q (lang TEXT, title TEXT)")
        # Index is required — without it SQLite scans all 700M+ edges instead of
        # using idx_from, causing the query to hang for hours.
        c.execute("CREATE INDEX IF NOT EXISTS _bfs_q_idx ON _bfs_q(lang, title)")
        c.execute("DELETE FROM _bfs_q")
        c.executemany("INSERT INTO _bfs_q VALUES (?,?)", nodes)
        rows = c.execute(
            """SELECT e.from_lang, e.from_title, e.to_lang, e.to_title
               FROM _bfs_q q
               JOIN edges e ON e.from_lang = q.lang AND e.from_title = q.title"""
        ).fetchall()
        for fl, ft, tl, tt in rows:
            result.setdefault((fl, ft), []).append((tl, tt))
        return result

    # ------------------------------------------------------------------

    def shortest_path(
        self,
        start: NodeKey,
        destination: NodeKey,
        max_depth: int = 8,
        max_nodes: int = 500_000,
        timeout: float = 10.0,
    ) -> Optional[List[NodeKey]]:
        if start == destination:
            return [start]

        parents: Dict[NodeKey, Optional[NodeKey]] = {start: None}
        frontier: List[NodeKey] = [start]
        deadline = time.time() + timeout

        for _ in range(max_depth):
            if not frontier or len(parents) > max_nodes or time.time() > deadline:
                break

            nbr_map = self._neighbors_batch(frontier)
            next_frontier: List[NodeKey] = []

            for node in frontier:
                for nbr in nbr_map.get(node, []):
                    if nbr in parents:
                        continue
                    parents[nbr] = node
                    if nbr == destination:
                        path: List[NodeKey] = []
                        cursor: Optional[NodeKey] = destination
                        while cursor is not None:
                            path.append(cursor)
                            cursor = parents[cursor]
                        path.reverse()
                        return path
                    next_frontier.append(nbr)

            frontier = next_frontier

        return None

    # ------------------------------------------------------------------

    @staticmethod
    def build(
        db_path: Path,
        edges_path: Path,
        pages_path: Optional[Path] = None,
    ) -> None:
        """Build or incrementally update the SQLite DB from JSONL crawl files.

        Byte offsets of the last-indexed position are stored in a `meta` table.
        On subsequent runs only new lines (appended since last run) are added —
        the B-tree index is maintained automatically by SQLite.
        """
        db_path.parent.mkdir(parents=True, exist_ok=True)
        incremental = db_path.exists()
        conn = sqlite3.connect(str(db_path))

        if incremental:
            # Fast incremental settings — WAL already set, index already built
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-1048576")
            conn.execute("PRAGMA temp_store=MEMORY")
            # Ensure meta table exists (may be missing if DB was built before this schema)
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS meta (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
            """)
        else:
            # Fastest bulk-insert settings for initial build
            conn.execute("PRAGMA journal_mode=OFF")
            conn.execute("PRAGMA synchronous=OFF")
            conn.execute("PRAGMA cache_size=-1048576")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.executescript("""
                CREATE TABLE nodes (
                    lang    TEXT NOT NULL,
                    title   TEXT NOT NULL,
                    page_id INTEGER,
                    PRIMARY KEY (lang, title)
                ) WITHOUT ROWID;
                CREATE TABLE edges (
                    from_lang  TEXT NOT NULL,
                    from_title TEXT NOT NULL,
                    to_lang    TEXT NOT NULL,
                    to_title   TEXT NOT NULL
                );
                CREATE TABLE meta (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
            """)

        # ── read stored offsets ───────────────────────────────────────
        def _get_offset(key: str) -> int:
            row = conn.execute(
                "SELECT value FROM meta WHERE key=?", (key,)
            ).fetchone()
            return int(row[0]) if row else 0

        def _set_offset(key: str, value: int) -> None:
            conn.execute(
                "INSERT OR REPLACE INTO meta VALUES (?,?)", (key, str(value))
            )

        t0 = time.time()

        # ── pages ─────────────────────────────────────────────────────
        if pages_path and pages_path.exists():
            pages_start = _get_offset("pages_offset")
            file_size   = pages_path.stat().st_size
            if pages_start < file_size:
                new_kb = (file_size - pages_start) // 1024
                label  = "updating pages" if incremental else "loading pages"
                print(f"[index] {label} (+{new_kb:,} KB) …", flush=True)
                buf: list = []
                with pages_path.open("rb") as fh:
                    fh.seek(pages_start)
                    for raw in fh:
                        line = raw.decode("utf-8").strip()
                        if not line:
                            continue
                        row = json.loads(line)
                        buf.append((row["lang"], row["title"], row.get("page_id")))
                        if len(buf) >= 100_000:
                            conn.executemany(
                                "INSERT OR IGNORE INTO nodes VALUES (?,?,?)", buf
                            )
                            buf.clear()
                    pages_end = fh.tell()
                if buf:
                    conn.executemany(
                        "INSERT OR IGNORE INTO nodes VALUES (?,?,?)", buf
                    )
                _set_offset("pages_offset", pages_end)
                conn.commit()
                print(f"[index] pages done  ({time.time()-t0:.0f}s)", flush=True)
            else:
                print("[index] pages: already up to date", flush=True)

        # ── edges ─────────────────────────────────────────────────────
        edges_start = _get_offset("edges_offset")
        file_size   = edges_path.stat().st_size
        if edges_start >= file_size:
            print("[index] edges: already up to date — nothing to do", flush=True)
            conn.close()
            return

        new_mb = (file_size - edges_start) // (1024 * 1024)
        label  = f"updating edges (+{new_mb:,} MB)" if incremental else "loading edges"
        print(f"[index] {label} — this takes a while …", flush=True)

        edge_buf: list = []
        node_buf: list = []
        count = 0

        with edges_path.open("rb") as fh:
            fh.seek(edges_start)
            for raw in fh:
                line = raw.decode("utf-8").strip()
                if not line:
                    continue
                row  = json.loads(line)
                fl, ft = row["from_lang"], row["from_title"]
                tl, tt = row["to_lang"],   row["to_title"]
                edge_buf.append((fl, ft, tl, tt))
                node_buf.append((fl, ft, None))
                node_buf.append((tl, tt, None))
                count += 1

                if len(edge_buf) >= 100_000:
                    conn.executemany("INSERT INTO edges VALUES (?,?,?,?)", edge_buf)
                    conn.executemany(
                        "INSERT OR IGNORE INTO nodes VALUES (?,?,?)", node_buf
                    )
                    edge_buf.clear()
                    node_buf.clear()

                if count % 5_000_000 == 0:
                    conn.commit()
                    elapsed = time.time() - t0
                    rate    = count / elapsed
                    print(
                        f"[index] {count/1_000_000:.0f}M edges  "
                        f"({elapsed/60:.1f} min, {rate/1000:.0f}k/s)",
                        flush=True,
                    )

            edges_end = fh.tell()

        if edge_buf:
            conn.executemany("INSERT INTO edges VALUES (?,?,?,?)", edge_buf)
            conn.executemany(
                "INSERT OR IGNORE INTO nodes VALUES (?,?,?)", node_buf
            )
        _set_offset("edges_offset", edges_end)
        conn.commit()

        # ── index (only on initial build — maintained automatically after) ──
        if not incremental:
            print("[index] building forward index …", flush=True)
            conn.execute("CREATE INDEX idx_from ON edges(from_lang, from_title)")
            conn.commit()
            conn.execute("PRAGMA journal_mode=WAL")
            conn.commit()

        conn.close()

        elapsed = time.time() - t0
        size_gb = db_path.stat().st_size / 1e9
        action  = "updated" if incremental else "done"
        print(
            f"[index] {action} → {db_path}  "
            f"({size_gb:.1f} GB, {elapsed/60:.1f} min)",
            flush=True,
        )
