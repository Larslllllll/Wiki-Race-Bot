"""Parse Wikipedia MySQL SQL dumps into graph.db.

Wikipedia changed the pagelinks table in 2022: pl_title is now NULL and
pl_target_id references a separate linktarget table.

Files needed per language from https://dumps.wikimedia.org/{lang}wiki/latest/:
  {lang}wiki-latest-page.sql.gz           page titles + IDs
  {lang}wiki-latest-pagelinks.sql.gz      links (new: uses pl_target_id)
  {lang}wiki-latest-linktarget.sql.gz     id → title mapping for new format
"""
from __future__ import annotations

import gzip
import sqlite3
import time
from pathlib import Path
from typing import Dict, Iterator, List, Optional


def parse_lang(
    lang: str,
    pages_path: Path,
    links_path: Path,
    db_path: Path,
    *,
    linktarget_path: Optional[Path] = None,
    verbose: bool = True,
) -> None:
    """Parse one language's Wikipedia SQL dumps into graph.db."""

    db_path.parent.mkdir(parents=True, exist_ok=True)
    incremental = db_path.exists()
    conn = sqlite3.connect(str(db_path))
    _init_db(conn, incremental)

    t0 = time.time()

    # ── Pass 1: page table → nodes + id→title map ─────────────────────────
    if verbose:
        sz = pages_path.stat().st_size // (1024 * 1024)
        print(f"[dump:{lang}] reading page table ({sz} MB) …", flush=True)

    id_to_title: Dict[int, str] = {}
    node_buf: list = []
    page_count = 0

    for row in _iter_rows(pages_path, "page"):
        if len(row) < 3:
            continue
        try:
            page_id   = int(row[0])
            namespace = int(row[1])
            title     = str(row[2]).replace("_", " ")
        except (ValueError, TypeError):
            continue
        if namespace != 0:
            continue
        id_to_title[page_id] = title
        node_buf.append((lang, title, page_id))
        page_count += 1
        if len(node_buf) >= 100_000:
            conn.executemany("INSERT OR IGNORE INTO nodes VALUES (?,?,?)", node_buf)
            node_buf.clear()

    if node_buf:
        conn.executemany("INSERT OR IGNORE INTO nodes VALUES (?,?,?)", node_buf)
    conn.commit()

    if verbose:
        print(f"[dump:{lang}] {page_count:,} articles  ({time.time()-t0:.0f}s)", flush=True)

    # ── Detect pagelinks format ────────────────────────────────────────────
    new_format = _detect_new_format(links_path)
    if verbose:
        fmt = "new (linktarget)" if new_format else "old (pl_title)"
        print(f"[dump:{lang}] pagelinks format: {fmt}", flush=True)

    # ── Pass 2a (new format): linktarget → lt_id→title map ───────────────
    lt_map: Dict[int, str] = {}
    if new_format:
        if linktarget_path is None or not linktarget_path.exists():
            print(
                f"[dump:{lang}] ERROR: new pagelinks format requires linktarget table.\n"
                f"  Download: https://dumps.wikimedia.org/{lang}wiki/latest/"
                f"{lang}wiki-latest-linktarget.sql.gz\n"
                f"  Then re-run with --linktarget-{lang} <path>",
                flush=True,
            )
            conn.close()
            return
        sz = linktarget_path.stat().st_size // (1024 * 1024)
        print(f"[dump:{lang}] reading linktarget ({sz} MB) …", flush=True)
        t1 = time.time()
        for row in _iter_rows(linktarget_path, "linktarget"):
            # (lt_id, lt_namespace, lt_title)
            if len(row) < 3:
                continue
            try:
                lt_id  = int(row[0])
                lt_ns  = int(row[1])
                lt_ttl = str(row[2]).replace("_", " ")
            except (ValueError, TypeError):
                continue
            if lt_ns == 0:
                lt_map[lt_id] = lt_ttl
        print(f"[dump:{lang}] {len(lt_map):,} link targets  ({time.time()-t1:.0f}s)", flush=True)

    # ── Pass 2b: pagelinks → edges ─────────────────────────────────────────
    sz = links_path.stat().st_size // (1024 * 1024)
    print(f"[dump:{lang}] reading pagelinks ({sz} MB) …", flush=True)

    edge_buf: list = []
    edge_count = 0
    t2 = time.time()

    for row in _iter_rows(links_path, "pagelinks"):
        try:
            if new_format:
                # New format: (pl_from, pl_target_id, pl_from_namespace)
                if len(row) < 3:
                    continue
                pl_from           = int(row[0])
                pl_target_id      = int(row[1])
                pl_from_namespace = int(row[2])
                if pl_from_namespace != 0:
                    continue
                to_title = lt_map.get(pl_target_id)
                if to_title is None:
                    continue
            else:
                # Old format: (pl_from, pl_namespace, pl_title, pl_from_namespace)
                if len(row) < 4:
                    continue
                pl_from           = int(row[0])
                pl_namespace      = int(row[1])
                pl_title          = row[2]
                pl_from_namespace = int(row[3])
                if pl_from_namespace != 0 or pl_namespace != 0:
                    continue
                if pl_title is None:
                    continue
                to_title = str(pl_title).replace("_", " ")

            from_title = id_to_title.get(pl_from)
            if from_title is None:
                continue

            edge_buf.append((lang, from_title, lang, to_title))
            edge_count += 1

            if len(edge_buf) >= 100_000:
                conn.executemany("INSERT INTO edges VALUES (?,?,?,?)", edge_buf)
                edge_buf.clear()

            if edge_count % 5_000_000 == 0:
                conn.commit()
                elapsed = time.time() - t2
                rate    = edge_count / elapsed
                if verbose:
                    print(
                        f"[dump:{lang}] {edge_count/1_000_000:.0f}M edges  "
                        f"({elapsed/60:.1f}min, {rate/1000:.0f}k/s)",
                        flush=True,
                    )
        except (ValueError, TypeError, IndexError):
            continue

    if edge_buf:
        conn.executemany("INSERT INTO edges VALUES (?,?,?,?)", edge_buf)
    conn.commit()

    # ── Build index on initial DB ──────────────────────────────────────────
    if not incremental:
        if verbose:
            print(f"[dump:{lang}] building forward index …", flush=True)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_from ON edges(from_lang, from_title)")
        conn.commit()
        conn.execute("PRAGMA journal_mode=WAL")
        conn.commit()

    conn.close()

    elapsed = time.time() - t0
    size_gb = db_path.stat().st_size / 1e9
    if verbose:
        print(
            f"[dump:{lang}] done  {edge_count/1_000_000:.1f}M edges  "
            f"{elapsed/60:.1f}min  DB={size_gb:.1f}GB",
            flush=True,
        )


# ── helpers ────────────────────────────────────────────────────────────────────

def _detect_new_format(links_path: Path) -> bool:
    """Peek at the first INSERT row to count columns. 3 cols = new format."""
    for row in _iter_rows(links_path, "pagelinks"):
        return len(row) == 3
    return False


def _init_db(conn: sqlite3.Connection, incremental: bool) -> None:
    if incremental:
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA journal_mode=WAL")
    else:
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA cache_size=-1048576")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS nodes (
            lang    TEXT NOT NULL,
            title   TEXT NOT NULL,
            page_id INTEGER,
            PRIMARY KEY (lang, title)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS edges (
            from_lang  TEXT NOT NULL,
            from_title TEXT NOT NULL,
            to_lang    TEXT NOT NULL,
            to_title   TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
    """)


def _iter_rows(path: Path, table: str) -> Iterator[List]:
    """Yield parsed rows from INSERT INTO `table` VALUES lines in a .sql.gz file."""
    prefix1 = f"INSERT INTO `{table}` VALUES "
    prefix2 = f"INSERT INTO {table} VALUES "
    opener  = gzip.open if str(path).endswith(".gz") else open

    with opener(str(path), "rb") as fh:
        for raw in fh:
            try:
                line = raw.decode("utf-8", errors="replace").rstrip()
            except Exception:
                continue
            if line.startswith(prefix1):
                yield from _parse_values(line[len(prefix1):])
            elif line.startswith(prefix2):
                yield from _parse_values(line[len(prefix2):])


def _parse_values(s: str) -> Iterator[List]:
    """Parse MySQL VALUES string: (v1,v2,...),(v1,...); → lists of Python values."""
    i = 0
    n = len(s)
    while i < n:
        if s[i] != "(":
            i += 1
            continue
        i += 1
        row: List = []
        while i < n:
            c = s[i]
            if c == ")":
                i += 1
                break
            elif c == "'":
                i += 1
                buf: List[str] = []
                while i < n:
                    ch = s[i]
                    if ch == "\\" and i + 1 < n:
                        nx = s[i + 1]
                        buf.append({"'": "'", "\\": "\\", "n": "\n", "r": "\r", "0": "\0"}.get(nx, nx))
                        i += 2
                    elif ch == "'":
                        i += 1
                        break
                    else:
                        buf.append(ch)
                        i += 1
                row.append("".join(buf))
                if i < n and s[i] == ",":
                    i += 1
            elif s[i:i+4] == "NULL":
                row.append(None)
                i += 4
                if i < n and s[i] == ",":
                    i += 1
            elif c == ",":
                i += 1
            else:
                j = i
                while i < n and s[i] not in (",", ")"):
                    i += 1
                row.append(s[j:i])
                if i < n and s[i] == ",":
                    i += 1
        yield row
        if i < n and s[i] == ",":
            i += 1
