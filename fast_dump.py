"""
fast_dump.py — Wikipedia SQL dumps → graph.db
Fast path: regex parsers (C-speed) + Python dict lookups + SQLite OFF/OFF
Expected: EN ~40-50min, DE ~15min
"""
from __future__ import annotations

import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Iterator, Optional

try:
    from tqdm import tqdm as _tqdm
    _HAS_TQDM = True
except ImportError:
    _HAS_TQDM = False

PRINT_INTERVAL = 30  # seconds between progress lines if no tqdm

# Approximate row counts per language for tqdm progress bars
_APPROX = {
    ("en", "page"):        19_000_000,
    ("en", "linktarget"):  35_000_000,
    ("en", "pagelinks"):  900_000_000,
    ("de", "page"):         5_100_000,
    ("de", "linktarget"):  12_100_000,
    ("de", "pagelinks"):  110_000_000,
}

def _pbar(lang: str, table: str, desc: str):
    if not _HAS_TQDM:
        return None
    total = _APPROX.get((lang, table))
    return _tqdm(total=total, desc=desc, unit=" rows",
                 unit_scale=True, dynamic_ncols=True, colour="cyan")

# ── 7-Zip ─────────────────────────────────────────────────────────────────────

def _find_7zip() -> Optional[str]:
    if env := os.environ.get("SEVENZIP"):
        return env
    for c in [r"C:\Program Files\7-Zip\7z.exe",
              r"C:\Program Files (x86)\7-Zip\7z.exe", "7z"]:
        try:
            subprocess.run([c, "i"], capture_output=True, timeout=3)
            return c
        except Exception:
            continue
    return None

SEVENZIP = _find_7zip()

def open_stream(path: Path):
    if SEVENZIP and str(path).endswith(".gz"):
        proc = subprocess.Popen(
            [SEVENZIP, "e", "-so", str(path)],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=1 << 23,
        )
        return proc.stdout
    import gzip
    return gzip.open(str(path), "rb")

# ── Fast regex parsers (C-speed) ──────────────────────────────────────────────

# Page:        (page_id, 0,  'title',  ...)   — namespace=0 only
_RE_PAGE   = re.compile(r"\((\d+),0,'((?:[^'\\]|\\.)*)'")
# Linktarget:  (lt_id,   0,  'title')          — namespace=0 only
_RE_LT     = re.compile(r"\((\d+),0,'((?:[^'\\]|\\.)*)'")
# Pagelinks new: (pl_from, 0,  pl_target_id)   — pl_from_namespace=0 only
_RE_PL_NEW = re.compile(r"\((\d+),0,(\d+)\)")
# Pagelinks old: (pl_from, 0,  'title',  0)    — both namespaces=0
_RE_PL_OLD = re.compile(r"\((\d+),0,'((?:[^'\\]|\\.)*)',0\)")

def _unescape(s: str) -> str:
    return (s.replace("\\'", "'").replace("\\\\", "\\")
             .replace("\\n", "\n").replace("\\r", "\r").replace("\\0", "\0"))

def _lines(path: Path, table: str):
    """Yield raw INSERT lines for the given table."""
    p1 = f"INSERT INTO `{table}` VALUES ".encode()
    p2 = f"INSERT INTO {table} VALUES ".encode()
    with open_stream(path) as fh:
        for raw in fh:
            if raw.startswith(p1) or raw.startswith(p2):
                yield raw.decode("utf-8", errors="replace")

# ── DB init ───────────────────────────────────────────────────────────────────

def _init_db(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA cache_size=-262144")   # 256 MB
    conn.execute("PRAGMA page_size=8192")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS nodes(
            lang TEXT NOT NULL, title TEXT NOT NULL, page_id INTEGER,
            PRIMARY KEY(lang, title)) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS edges(
            from_lang TEXT NOT NULL, from_title TEXT NOT NULL,
            to_lang   TEXT NOT NULL, to_title   TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
    """)

# ── Pass 1 ────────────────────────────────────────────────────────────────────

def _pass1_pages(conn: sqlite3.Connection, lang: str, pages: Path
                 ) -> Optional[Dict[int, str]]:
    t = time.time()
    print(f"[{lang}] Pass 1/3: page table …", flush=True)
    try:
        id_map: Dict[int, str] = {}
        buf: list = []
        count = 0
        last = t
        pbar = _pbar(lang, "page", f"  [{lang}] articles")
        for line in _lines(pages, "page"):
            for m in _RE_PAGE.finditer(line):
                pid   = int(m.group(1))
                title = _unescape(m.group(2)).replace("_", " ")
                id_map[pid] = title
                buf.append((lang, title, pid))
                count += 1
                if pbar is not None: pbar.update(1)
            if len(buf) >= 200_000:
                conn.executemany("INSERT OR IGNORE INTO nodes VALUES(?,?,?)", buf)
                buf.clear()
            now = time.time()
            if pbar is None and now - last >= PRINT_INTERVAL:
                last = now
                print(f"[{lang}]   {count:,} articles  ({now-t:.0f}s) …", flush=True)
            if count % 2_000_000 == 0 and count > 0:
                conn.commit()
        if pbar is not None: pbar.close()
        if buf:
            conn.executemany("INSERT OR IGNORE INTO nodes VALUES(?,?,?)", buf)
        conn.commit()
        print(f"[{lang}] {count:,} articles  ({time.time()-t:.0f}s)", flush=True)
        return id_map
    except MemoryError:
        print(f"[{lang}] MemoryError in Pass 1 — not enough RAM", flush=True)
        conn.commit()
        return None

# ── Pass 2 ────────────────────────────────────────────────────────────────────

def _pass2_lt(lang: str, lt: Path) -> Optional[Dict[int, str]]:
    t = time.time()
    print(f"[{lang}] Pass 2/3: linktarget …", flush=True)
    try:
        lt_map: Dict[int, str] = {}
        count = 0
        last = t
        pbar = _pbar(lang, "linktarget", f"  [{lang}] targets")
        for line in _lines(lt, "linktarget"):
            for m in _RE_LT.finditer(line):
                lt_map[int(m.group(1))] = _unescape(m.group(2)).replace("_", " ")
                count += 1
                if pbar is not None: pbar.update(1)
            now = time.time()
            if pbar is None and now - last >= PRINT_INTERVAL:
                last = now
                print(f"[{lang}]   {count:,} targets  ({now-t:.0f}s) …", flush=True)
        if pbar is not None: pbar.close()
        print(f"[{lang}] {count:,} targets  ({time.time()-t:.0f}s)", flush=True)
        return lt_map
    except MemoryError:
        print(f"[{lang}] MemoryError in Pass 2 — not enough RAM", flush=True)
        return None

# ── Detect format ─────────────────────────────────────────────────────────────

def _detect_fmt(links: Path) -> bool:
    """True = new format (3 int cols), False = old format (pl_title)."""
    for line in _lines(links, "pagelinks"):
        m3 = _RE_PL_NEW.search(line)
        m4 = _RE_PL_OLD.search(line)
        if m3: return True
        if m4: return False
    return False

# ── Pass 3 ────────────────────────────────────────────────────────────────────

def _pass3_edges(conn: sqlite3.Connection, lang: str, links: Path,
                 new_fmt: bool, id_map: Dict[int, str],
                 lt_map: Dict[int, str]) -> int:
    t = time.time()
    mode = "new" if new_fmt else "old"
    print(f"[{lang}] Pass 3/3: pagelinks → edges  ({mode} format, dict lookup) …", flush=True)

    ebuf: list = []
    edge_count = 0
    last = t
    pat = _RE_PL_NEW if new_fmt else _RE_PL_OLD
    pbar = _pbar(lang, "pagelinks", f"  [{lang}] edges")

    for line in _lines(links, "pagelinks"):
        for m in pat.finditer(line):
            from_title = id_map.get(int(m.group(1)))
            if from_title is None:
                continue
            if new_fmt:
                to_title = lt_map.get(int(m.group(2)))
            else:
                to_title = _unescape(m.group(2)).replace("_", " ")
            if to_title is None:
                continue
            ebuf.append((lang, from_title, lang, to_title))
            edge_count += 1
            if pbar is not None: pbar.update(1)

        if len(ebuf) >= 500_000:
            conn.executemany("INSERT INTO edges VALUES(?,?,?,?)", ebuf)
            ebuf.clear()
            conn.commit()
        now = time.time()
        if pbar is None and now - last >= PRINT_INTERVAL:
            last = now
            e = now - t
            rate = edge_count / e / 1000 if e > 0 else 0
            print(f"[{lang}]   {edge_count:,} edges  ({e/60:.1f}min, {rate:.0f}k/s) …", flush=True)

    if pbar is not None: pbar.close()
    if ebuf:
        conn.executemany("INSERT INTO edges VALUES(?,?,?,?)", ebuf)
    conn.commit()
    e = time.time() - t
    print(f"[{lang}] {edge_count:,} edges  ({e/60:.1f}min)", flush=True)
    return edge_count

# ── main ──────────────────────────────────────────────────────────────────────

def parse_lang(lang: str, pages: Path, links: Path,
               lt: Optional[Path], db: Path) -> None:
    conn = sqlite3.connect(str(db))
    _init_db(conn)
    t0 = time.time()

    new_fmt = _detect_fmt(links)
    print(f"[{lang}] format: {'new (linktarget)' if new_fmt else 'old (pl_title)'}", flush=True)

    has_lt = lt and lt.exists()
    if new_fmt and not has_lt:
        print(f"[{lang}] ERROR: --linktarget-{lang} required"); conn.close(); return

    id_map = _pass1_pages(conn, lang, pages)
    if id_map is None:
        print(f"[{lang}] FATAL: not enough RAM for page dict"); conn.close(); return

    lt_map: Dict[int, str] = {}
    if has_lt:
        lt_map = _pass2_lt(lang, lt)
        if lt_map is None:
            print(f"[{lang}] FATAL: not enough RAM for lt dict"); conn.close(); return

    edge_count = _pass3_edges(conn, lang, links, new_fmt, id_map, lt_map)
    del id_map, lt_map  # free RAM before next language

    if edge_count == 0:
        print(f"[{lang}] WARNING: 0 edges produced — check dump files"); conn.close(); return

    print(f"[{lang}] building index …", flush=True)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_from ON edges(from_lang, from_title)")
    conn.commit()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.commit()
    conn.close()

    size_gb = db.stat().st_size / 1e9
    print(f"[{lang}] DONE  {edge_count/1e6:.1f}M edges  "
          f"{(time.time()-t0)/60:.1f}min  DB={size_gb:.1f}GB\n", flush=True)


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--pages-en");      p.add_argument("--links-en")
    p.add_argument("--linktarget-en"); p.add_argument("--pages-de")
    p.add_argument("--links-de");      p.add_argument("--linktarget-de")
    p.add_argument("--db", default="crawl_output/graph.db")
    a = p.parse_args()

    print(f"[fast_dump] 7-Zip: {SEVENZIP or 'NOT FOUND'}", flush=True)
    free = shutil.disk_usage(Path(a.db).parent).free / 1e9
    print(f"[info] Disk free: {free:.1f} GB", flush=True)

    db = Path(a.db); db.parent.mkdir(parents=True, exist_ok=True)

    if a.pages_en and a.links_en:
        parse_lang("en", Path(a.pages_en), Path(a.links_en),
                   Path(a.linktarget_en) if a.linktarget_en else None, db)
    if a.pages_de and a.links_de:
        parse_lang("de", Path(a.pages_de), Path(a.links_de),
                   Path(a.linktarget_de) if a.linktarget_de else None, db)
    print("All done.")
