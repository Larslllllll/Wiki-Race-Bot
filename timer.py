"""
timer.py — waits for all Wikipedia dump files to finish downloading,
then runs parse-dump automatically in this console.
"""
import subprocess
import sys
import time
from pathlib import Path

DOWNLOADS = Path.home() / "Downloads"

DUMPS = {
    "pages-en":      DOWNLOADS / "enwiki-latest-page.sql.gz",
    "links-en":      DOWNLOADS / "enwiki-latest-pagelinks.sql.gz",
    "linktarget-en": DOWNLOADS / "enwiki-latest-linktarget.sql.gz",
    "pages-de":      DOWNLOADS / "dewiki-latest-page.sql.gz",
    "links-de":      DOWNLOADS / "dewiki-latest-pagelinks.sql.gz",
    "linktarget-de": DOWNLOADS / "dewiki-latest-linktarget.sql.gz",
}

CHECK_INTERVAL = 10
STABLE_CHECKS  = 3


def file_ready(path: Path, prev_sizes: dict) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False
    size = path.stat().st_size
    history = prev_sizes.setdefault(str(path), [])
    history.append(size)
    if len(history) > STABLE_CHECKS:
        history.pop(0)
    return len(history) == STABLE_CHECKS and len(set(history)) == 1


def crdownload_info() -> str:
    files = list(DOWNLOADS.glob("*.crdownload")) + list(DOWNLOADS.glob("*.part"))
    if not files:
        return ""
    parts = [f"{f.name[:40]}  {f.stat().st_size/1e9:.2f} GB" for f in files]
    return "  Downloading: " + " | ".join(parts)


def fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def main() -> None:
    print("=" * 65)
    print("  WikiRace dump watcher")
    print(f"  Watching: {DOWNLOADS}")
    print("=" * 65)
    for key, path in DUMPS.items():
        print(f"  {key:16s}  {path.name}")
    print("=" * 65)
    print()

    BLOCK = len(DUMPS) + 3
    prev_sizes: dict = {}
    ready: dict = {k: False for k in DUMPS}
    first = True

    while True:
        lines = [f"  Status @ {time.strftime('%H:%M:%S')}:"]

        all_ready = True
        for key, path in DUMPS.items():
            if not ready[key] and file_ready(path, prev_sizes):
                ready[key] = True
            if ready[key]:
                size = path.stat().st_size
                lines.append(f"  [✓] {key:16s}  {path.name}  ({fmt_size(size)})")
            elif path.exists():
                all_ready = False
                size = path.stat().st_size
                lines.append(f"  [ ] {key:16s}  {path.name}  {fmt_size(size)} …")
            else:
                all_ready = False
                lines.append(f"  [ ] {key:16s}  {path.name}  not found yet")

        dl = crdownload_info()
        lines.append(dl if dl else "")
        lines.append("")

        if not first:
            print(f"\033[{BLOCK}A", end="")
        first = False
        for line in lines:
            print(f"\r{line:<75}")

        if all_ready:
            print("  All files ready — starting parse-dump …")
            print("=" * 65)
            break

        time.sleep(CHECK_INTERVAL)

    # ── Run parse-dump ────────────────────────────────────────────────────
    script_dir = Path(__file__).parent
    cmd = [
        sys.executable, str(script_dir / "wiki_race_bot.py"), "parse-dump",
        "--pages-en",      str(DUMPS["pages-en"]),
        "--links-en",      str(DUMPS["links-en"]),
        "--linktarget-en", str(DUMPS["linktarget-en"]),
        "--pages-de",      str(DUMPS["pages-de"]),
        "--links-de",      str(DUMPS["links-de"]),
        "--linktarget-de", str(DUMPS["linktarget-de"]),
    ]

    print("  Running:", " ".join(cmd))
    print("=" * 65 + "\n")

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in proc.stdout:
        print(line, end="", flush=True)
    proc.wait()

    print("\n" + "=" * 65)
    if proc.returncode == 0:
        print("  parse-dump finished successfully!")
    else:
        print(f"  parse-dump exited with code {proc.returncode}")
    print("=" * 65)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[timer] stopped.")
