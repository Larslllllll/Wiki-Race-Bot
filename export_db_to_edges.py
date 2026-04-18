"""export_db_to_edges.py — export graph.db edges back to edges.jsonl (mit Resume)

Usage:
    python export_db_to_edges.py
    python export_db_to_edges.py --db crawl_output/graph.db --out crawl_output/edges.jsonl
"""
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import time
from pathlib import Path

from tqdm import tqdm


def _count_lines_fast(path: Path) -> int:
    """Zählt Zeilen schnell via Binär-Chunks — keine Dekodierung nötig."""
    count = 0
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8 * 1024 * 1024), b""):
            count += chunk.count(b"\n")
    return count


def export(db_path: Path, out_path: Path) -> None:
    if not db_path.exists():
        print(f"[error] DB nicht gefunden: {db_path}")
        return

    # ── Disk-Check ────────────────────────────────────────────────────
    free_gb  = shutil.disk_usage(out_path.parent).free / 1e9
    db_gb    = db_path.stat().st_size / 1e9
    need_gb  = db_gb * 0.7  # edges.jsonl ≈ 70% der DB-Größe
    print(f"[export] Disk frei: {free_gb:.1f} GB  (braucht ca. {need_gb:.1f} GB)", flush=True)
    if free_gb < need_gb * 1.05:
        print(f"[warn]  Möglicherweise zu wenig Platz! Fortfahren trotzdem …", flush=True)

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.execute("PRAGMA cache_size=-262144")

    total = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]

    # ── Resume: bestehende Zeilen zählen ─────────────────────────────
    offset = 0
    append_mode = "w"
    if out_path.exists() and out_path.stat().st_size > 0:
        print(f"[resume] Zähle bereits exportierte Zeilen in {out_path} …", flush=True)
        t_count = time.time()
        offset = _count_lines_fast(out_path)
        print(f"[resume] {offset:,} Zeilen bereits vorhanden ({time.time()-t_count:.1f}s) — überspringe …", flush=True)
        append_mode = "a"

    remaining = total - offset
    if remaining <= 0:
        print(f"[export] Bereits vollständig ({total:,} edges). Fertig.")
        conn.close()
        return

    print(f"[export] {total:,} edges total, {offset:,} bereits fertig → {remaining:,} verbleibend", flush=True)

    t0 = time.time()
    count = 0
    buf = []
    BUF_SIZE = 100_000

    query = f"SELECT from_lang, from_title, to_lang, to_title FROM edges ORDER BY rowid LIMIT -1 OFFSET {offset}"

    with out_path.open(append_mode, encoding="utf-8") as fh, \
         tqdm(total=remaining, initial=0, unit=" edges", unit_scale=True,
              dynamic_ncols=True, colour="cyan") as pbar:

        try:
            for fl, ft, tl, tt in conn.execute(query):
                buf.append(
                    json.dumps(
                        {"from_lang": fl, "from_title": ft,
                         "to_lang": tl, "to_title": tt, "edge_type": "article"},
                        ensure_ascii=False,
                    )
                )
                count += 1

                if len(buf) >= BUF_SIZE:
                    fh.write("\n".join(buf) + "\n")
                    buf.clear()
                    pbar.update(BUF_SIZE)
                    # Disk-Check alle 5M edges
                    if count % 5_000_000 == 0:
                        free = shutil.disk_usage(out_path.parent).free / 1e9
                        pbar.set_postfix_str(f"disk={free:.1f}GB frei")
                        if free < 2.0:
                            print(f"\n[error] Disk fast voll ({free:.1f} GB frei) — Abbruch!", flush=True)
                            if buf:
                                fh.write("\n".join(buf) + "\n")
                            conn.close()
                            return

            if buf:
                fh.write("\n".join(buf) + "\n")
                pbar.update(len(buf))

        except OSError as e:
            if buf:
                try:
                    fh.write("\n".join(buf) + "\n")
                except Exception:
                    pass
            free = shutil.disk_usage(out_path.parent).free / 1e9
            print(f"\n[error] Schreibfehler: {e}  (Disk frei: {free:.1f} GB)", flush=True)
            print(f"[info]  Beim nächsten Start wird automatisch an Zeile {offset + count} fortgesetzt.", flush=True)
            conn.close()
            return

    conn.close()
    size_gb = out_path.stat().st_size / 1e9
    elapsed = time.time() - t0
    print(f"[export] done — {offset + count:,} edges total  {size_gb:.1f} GB  {elapsed/60:.1f} min")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--db",  default="crawl_output/graph.db")
    p.add_argument("--out", default="crawl_output/edges.jsonl")
    a = p.parse_args()
    export(Path(a.db), Path(a.out))
