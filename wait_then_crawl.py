"""wait_then_crawl.py — wartet bis export_db_to_edges.py fertig ist, dann startet wiki.py"""
import subprocess
import time
from pathlib import Path

WATCH_SCRIPT = "export_db_to_edges.py"
WATCH_FILE   = Path("crawl_output/edges.jsonl")
CRAWL        = ["python", "wiki.py", "--tor", "--threads", "256"]
POLL         = 30


def is_running(script: str) -> bool:
    try:
        import psutil
        for p in psutil.process_iter(["cmdline"]):
            try:
                if any(script in arg for arg in (p.info["cmdline"] or [])):
                    return True
            except Exception:
                pass
        return False
    except ImportError:
        # Fallback: Datei wächst noch → Export läuft noch
        if not WATCH_FILE.exists():
            return True
        size_a = WATCH_FILE.stat().st_size
        time.sleep(3)
        size_b = WATCH_FILE.stat().st_size
        return size_b > size_a


print(f"[wait] Überwache ob '{WATCH_SCRIPT}' noch läuft …", flush=True)
print(f"[wait] (Falls psutil fehlt: pip install psutil)", flush=True)

while True:
    if not is_running(WATCH_SCRIPT):
        print(f"\n[wait] '{WATCH_SCRIPT}' ist fertig!", flush=True)
        break
    size_gb = WATCH_FILE.stat().st_size / 1e9 if WATCH_FILE.exists() else 0
    print(f"[wait] noch am laufen … edges.jsonl={size_gb:.1f} GB  (Check in {POLL}s)", flush=True)
    time.sleep(POLL)

print(f"[wait] 5 Sekunden warten …", flush=True)
time.sleep(5)

print(f"[start] Starte: {' '.join(CRAWL)}", flush=True)
subprocess.run(CRAWL)
