#!/usr/bin/env python3
"""Endless multi-language Wikipedia crawler for Wiki-Race datasets.

Run with:
    python wiki.py                          # direct connection, 16 threads
    python wiki.py --tor                    # auto-start Tor instances, 16 threads
    python wiki.py --tor --threads 32       # 32 threads → ~8 Tor instances (1 per 4 threads)
    python wiki.py --tor --tor-instances 4  # explicit instance count
    python wiki.py --tor --no-start-tor     # connect to pre-running instances on 9050/9051

Requirements for --tor:
    pip install stem requests[socks]
    tor.exe at C:\\tor\\tor\\tor.exe  (or pass --tor-exe PATH)
    Each instance uses 2 consecutive ports: instance i → SOCKS=(9050+2i), control=(9051+2i)
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional, Set, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


USER_AGENT = "WikiRaceCrawler/0.3 (+local research script)"
OUTPUT_DIR = Path("crawl_output")
PAGES_PATH = OUTPUT_DIR / "pages.jsonl"
EDGES_PATH = OUTPUT_DIR / "edges.jsonl"
STATS_PATH = OUTPUT_DIR / "stats.json"
FRONTIER_STATE_PATH = OUTPUT_DIR / "frontier_state.json"

PRIORITY_LANGS: Tuple[str, ...] = ("en", "de")
NON_PRIORITY_POP_INTERVAL = 40

MIN_REQUEST_DELAY_SECONDS = 0.08
INITIAL_REQUEST_DELAY_SECONDS = 0.18
MAX_REQUEST_DELAY_SECONDS = 12.0
SUCCESS_DELAY_DECAY = 0.97
MAX_RETRY_ATTEMPTS = 8

STATE_FLUSH_EVERY_SECONDS = 8.0
STATS_FLUSH_EVERY_SECONDS = 8.0

# Tor port scheme: instance i uses SOCKS=(BASE + i*STEP), control=(BASE + i*STEP + 1)
TOR_SOCKS_BASE_PORT = 9050
TOR_PORT_STEP = 2           # ports consumed per instance
TOR_NEWNYM_COOLDOWN = 10.0  # Tor enforces this minimum between NEWNYM signals
TOR_CIRCUIT_BUILD_WAIT = 3.0  # wait after NEWNYM for circuits to establish
TOR_ROTATE_AFTER = 5        # consecutive 429s *per router* before rotating that instance's IP
THREADS_PER_TOR_INSTANCE = 4

import shutil as _shutil
TOR_EXE_DEFAULT: str = (
    (_shutil.which("tor") or _shutil.which("tor.exe") or r"C:\tor\tor\tor.exe")
    if sys.platform == "win32"
    else (_shutil.which("tor") or "/usr/bin/tor")
)
TOR_DATA_BASE_DEFAULT: Path = (
    Path.home() / "tor_data" if sys.platform != "win32"
    else Path(r"C:\tor")
)

REQUEST_TIMEOUT = 12        # seconds

_FILE_WRITE_LOCK = threading.Lock()
_PRINT_LOCK = threading.Lock()
_thread_local = threading.local()


def _log(*args: Any, **kwargs: Any) -> None:
    with _PRINT_LOCK:
        print(*args, **kwargs)


# ---------------------------------------------------------------------------
# Tor
# ---------------------------------------------------------------------------

class TorRouter:
    """Manages one Tor instance: tracks per-instance 429 count, rotates circuit at threshold."""

    def __init__(self, socks_port: int, control_port: int) -> None:
        self.socks_port = socks_port
        self.control_port = control_port
        self._controller: Any = None
        self._last_newnym: float = 0.0
        self._consecutive_429: int = 0
        self._rotating: bool = False
        self._counter_lock = threading.Lock()

    def connect(self) -> None:
        try:
            from stem.control import Controller
        except ImportError:
            raise SystemExit("[tor] 'stem' not installed. Run: pip install stem requests[socks]")
        self._controller = Controller.from_port(port=self.control_port)
        self._controller.authenticate()
        _log(f"[tor:{self.socks_port}] connected — exit IP: {self._get_current_ip()}")

    def note_success(self) -> None:
        with self._counter_lock:
            self._consecutive_429 = 0

    def note_429(self) -> None:
        """Register a 429 for this router. Only rotates THIS instance's circuit.

        While a rotation is in progress, other threads stop counting (so they
        can't immediately re-trigger rotation the moment the first one finishes)
        and instead sleep briefly to let the new circuit establish.
        """
        rotate_now = False
        currently_rotating = False
        with self._counter_lock:
            if self._rotating:
                currently_rotating = True   # someone else is already on it
            else:
                self._consecutive_429 += 1
                count = self._consecutive_429
                if count >= TOR_ROTATE_AFTER:
                    self._rotating = True
                    self._consecutive_429 = 0
                    rotate_now = True
                elif count % 5 == 0:
                    _log(
                        f"[tor:{self.socks_port}] {count} consecutive 429s "
                        f"(rotate at {TOR_ROTATE_AFTER})",
                        file=sys.stderr,
                    )

        if rotate_now:
            self._do_rotate()
        elif currently_rotating:
            time.sleep(2.0)  # back off while the winning thread rotates

    def _do_rotate(self) -> None:
        # Guard against being called after close() during shutdown
        if self._controller is None:
            with self._counter_lock:
                self._rotating = False
                self._consecutive_429 = 0
            return

        try:
            from stem import Signal
            elapsed = time.time() - self._last_newnym
            remaining = TOR_NEWNYM_COOLDOWN - elapsed
            if remaining > 0:
                _log(f"[tor:{self.socks_port}] rotating IP — cooldown {remaining:.1f}s …", file=sys.stderr)
                time.sleep(remaining)
            else:
                _log(f"[tor:{self.socks_port}] rotating IP …", file=sys.stderr)
            if self._controller is None:
                return
            self._controller.signal(Signal.NEWNYM)
            self._last_newnym = time.time()
            time.sleep(TOR_CIRCUIT_BUILD_WAIT)
            _log(f"[tor:{self.socks_port}] new exit IP: {self._get_current_ip()}")
        except Exception as exc:
            _log(f"[tor:{self.socks_port}] rotation error: {exc}", file=sys.stderr)
        finally:
            # Always release the lock and reset counter — prevents perpetual rotation loop
            with self._counter_lock:
                self._rotating = False
                self._consecutive_429 = 0

    def _get_current_ip(self) -> str:
        try:
            return self._controller.get_info("address", None) or "unknown"
        except Exception:
            return "unknown"

    def close(self) -> None:
        if self._controller:
            try:
                self._controller.close()
            except Exception:
                pass
            self._controller = None


class TorInstanceManager:
    """Launches N Tor sub-processes and returns connected TorRouter objects.

    Port assignment:
        instance i → SocksPort = TOR_SOCKS_BASE_PORT + i*TOR_PORT_STEP
                      ControlPort = TOR_SOCKS_BASE_PORT + i*TOR_PORT_STEP + 1
    """

    def __init__(
        self,
        n_instances: int,
        tor_exe: str = TOR_EXE_DEFAULT,
        data_base: Optional[Path] = None,
    ) -> None:
        self.n_instances = n_instances
        self.tor_exe = tor_exe
        self.data_base: Path = data_base or TOR_DATA_BASE_DEFAULT
        self._processes: List[Any] = []
        self.routers: List[TorRouter] = []

    def start(self) -> List[TorRouter]:
        """Kill port occupants, clean stale locks, start all Tor processes."""
        self._cleanup_before_start()

        for i in range(self.n_instances):
            socks_port = TOR_SOCKS_BASE_PORT + i * TOR_PORT_STEP
            ctrl_port = socks_port + 1
            data_dir = self.data_base / f"data_{i}"
            torrc_path = self.data_base / f"torrc_{i}"

            data_dir.mkdir(parents=True, exist_ok=True)
            torrc_path.write_text(
                f"SocksPort {socks_port}\n"
                f"ControlPort {ctrl_port}\n"
                f"CookieAuthentication 1\n"
                f"DataDirectory {data_dir.as_posix()}\n",
                encoding="ascii",
            )
            extra_flags = {"creationflags": subprocess.CREATE_NO_WINDOW} if sys.platform == "win32" else {}
            proc = subprocess.Popen(
                [self.tor_exe, "-f", str(torrc_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,   # merge stderr into stdout so we can show it on failure
                **extra_flags,
            )
            self._processes.append(proc)
            self.routers.append(TorRouter(socks_port=socks_port, control_port=ctrl_port))
            _log(f"[tor] instance {i}: SOCKS={socks_port} control={ctrl_port} PID={proc.pid}")

        _log(f"[tor] waiting for {self.n_instances} instance(s) to bootstrap …")
        self._wait_for_socks_ports()

        for router in self.routers:
            router.connect()

        return self.routers

    def _cleanup_before_start(self) -> None:
        """Kill any process already on our ports and remove stale DataDir lock files."""
        import socket as _socket

        for i in range(self.n_instances):
            socks_port = TOR_SOCKS_BASE_PORT + i * TOR_PORT_STEP
            ctrl_port = socks_port + 1

            for port in (socks_port, ctrl_port):
                try:
                    conn = _socket.create_connection(("127.0.0.1", port), timeout=0.3)
                    conn.close()
                    _log(f"[tor] port {port} in use — evicting …", file=sys.stderr)
                    self._kill_process_on_port(port)
                    # Wait until the OS actually releases the port (up to 8 s)
                    for _ in range(16):
                        time.sleep(0.5)
                        try:
                            c = _socket.create_connection(("127.0.0.1", port), timeout=0.2)
                            c.close()
                        except OSError:
                            break  # port is free
                except OSError:
                    pass  # free already

            # Remove stale DataDir lock so Tor doesn't refuse to start
            lock = self.data_base / f"data_{i}" / "lock"
            if lock.exists():
                try:
                    lock.unlink()
                    _log(f"[tor] removed stale lock: {lock}", file=sys.stderr)
                except Exception:
                    pass

    @staticmethod
    def _kill_process_on_port(port: int) -> None:
        try:
            if sys.platform == "win32":
                result = subprocess.run(["netstat", "-ano"], capture_output=True, text=True)
                for line in result.stdout.splitlines():
                    parts = line.split()
                    # TCP  0.0.0.0:9050  0.0.0.0:0  LISTENING  <PID>
                    if len(parts) >= 5 and f":{port}" in parts[1] and "LISTENING" in parts[3]:
                        pid = parts[4]
                        subprocess.run(["taskkill", "/F", "/PID", pid], capture_output=True)
                        _log(f"[tor] killed PID {pid} (was on port {port})", file=sys.stderr)
                        return
            else:
                # fuser (Debian/Pi), then lsof (macOS/many Linux), then /proc/net/tcp (Android/Termux)
                if subprocess.run(["fuser", "-k", f"{port}/tcp"], capture_output=True).returncode == 0:
                    return
                r = subprocess.run(["lsof", "-ti", f"tcp:{port}"], capture_output=True, text=True)
                if r.returncode == 0 and r.stdout.strip():
                    for pid in r.stdout.strip().splitlines():
                        subprocess.run(["kill", "-9", pid.strip()], capture_output=True)
                    return
                # Last resort: parse /proc/net/tcp (Linux/Android, always present)
                TorInstanceManager._kill_via_proc_net(port)
        except Exception:
            pass

    @staticmethod
    def _kill_via_proc_net(port: int) -> None:
        """Kill processes listening on port by reading /proc/net/tcp[6]."""
        import os
        hex_port = format(port, "04X")
        for tcp_file in ("/proc/net/tcp", "/proc/net/tcp6"):
            try:
                with open(tcp_file) as f:
                    for line in f:
                        parts = line.split()
                        if len(parts) < 10:
                            continue
                        local = parts[1]           # e.g. 00000000:232A
                        state = parts[3]
                        inode = parts[9]
                        if state != "0A":          # 0A = LISTEN
                            continue
                        if not local.endswith(f":{hex_port}"):
                            continue
                        # Find PID owning this inode
                        for pid in os.listdir("/proc"):
                            if not pid.isdigit():
                                continue
                            try:
                                fd_dir = f"/proc/{pid}/fd"
                                for fd in os.listdir(fd_dir):
                                    link = os.readlink(f"{fd_dir}/{fd}")
                                    if f"socket:[{inode}]" in link:
                                        os.kill(int(pid), 9)
                                        _log(f"[tor] killed PID {pid} via /proc/net/tcp (port {port})", file=sys.stderr)
                                        return
                            except (PermissionError, FileNotFoundError, ProcessLookupError):
                                pass
            except FileNotFoundError:
                pass

    def _wait_for_socks_ports(self, timeout: int = 120) -> None:
        import socket as _socket
        deadline = time.time() + timeout
        pending = set(range(self.n_instances))
        while pending and time.time() < deadline:
            time.sleep(1)
            for i in list(pending):
                port = TOR_SOCKS_BASE_PORT + i * TOR_PORT_STEP
                proc = self._processes[i]
                proc.poll()
                if proc.returncode is not None:
                    out = b""
                    if proc.stdout:
                        try:
                            out = proc.stdout.read(4096)
                        except Exception:
                            pass
                    msg = out.decode(errors="replace").strip()
                    raise RuntimeError(
                        f"Tor instance {i} (SOCKS port {port}) exited with code {proc.returncode}"
                        + (f"\n  Tor says: {msg[-400:]}" if msg else "")
                    )
                try:
                    conn = _socket.create_connection(("127.0.0.1", port), timeout=0.5)
                    conn.close()
                    pending.discard(i)
                    _log(f"[tor] instance {i} (port {port}) ready")
                except OSError:
                    pass

        if pending:
            bad_ports = [TOR_SOCKS_BASE_PORT + i * TOR_PORT_STEP for i in pending]
            raise RuntimeError(
                f"Tor instances on SOCKS ports {bad_ports} did not bootstrap within {timeout}s"
            )

    def stop(self) -> None:
        for router in self.routers:
            router.close()
        for proc in self._processes:
            try:
                proc.terminate()
            except Exception:
                pass
        for proc in self._processes:
            try:
                proc.wait(timeout=5)
            except (subprocess.TimeoutExpired, KeyboardInterrupt, Exception):
                try:
                    proc.kill()
                except Exception:
                    pass
        self._processes.clear()
        self.routers.clear()


def _get_or_create_session(socks_port: int) -> Any:
    """Return a per-thread requests.Session routed through the given SOCKS port."""
    try:
        import requests as _requests
    except ImportError:
        raise SystemExit("[tor] 'requests' not installed. Run: pip install requests[socks]")
    if getattr(_thread_local, "socks_port", None) != socks_port:
        proxy = f"socks5h://127.0.0.1:{socks_port}"
        session = _requests.Session()
        session.proxies = {"http": proxy, "https": proxy}
        session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
        _thread_local.session = session
        _thread_local.socks_port = socks_port
    return _thread_local.session


# ---------------------------------------------------------------------------
# HTTP + throttle
# ---------------------------------------------------------------------------

class PageNotFoundError(RuntimeError):
    pass


class RequestThrottle:
    def __init__(self, current_delay: float = INITIAL_REQUEST_DELAY_SECONDS) -> None:
        self.current_delay = current_delay
        self.last_request_finished_at = 0.0
        self._lock = threading.Lock()

    def wait_before_request(self) -> None:
        with self._lock:
            remaining = self.current_delay - (time.time() - self.last_request_finished_at)
        if remaining > 0:
            time.sleep(remaining)

    def note_success(self) -> None:
        with self._lock:
            self.last_request_finished_at = time.time()
            self.current_delay = max(MIN_REQUEST_DELAY_SECONDS, self.current_delay * SUCCESS_DELAY_DECAY)

    def note_throttle(self, retry_after_seconds: Optional[float]) -> float:
        with self._lock:
            suggested = retry_after_seconds if retry_after_seconds is not None else max(1.0, self.current_delay * 2.0)
            self.current_delay = min(MAX_REQUEST_DELAY_SECONDS, max(self.current_delay * 1.6, suggested))
            self.last_request_finished_at = time.time()
            return self.current_delay

    def note_error(self) -> float:
        with self._lock:
            self.current_delay = min(MAX_REQUEST_DELAY_SECONDS, max(0.6, self.current_delay * 1.35))
            self.last_request_finished_at = time.time()
            return self.current_delay


def parse_retry_after(header_value: Optional[str]) -> Optional[float]:
    if not header_value:
        return None
    try:
        return max(float(int(header_value)), 0.0)
    except ValueError:
        pass
    try:
        target_time = parsedate_to_datetime(header_value)
    except (TypeError, ValueError, IndexError):
        return None
    if target_time.tzinfo is None:
        target_time = target_time.replace(tzinfo=timezone.utc)
    return max((target_time - datetime.now(timezone.utc)).total_seconds(), 0.0)


def api_get_json(url: str, throttle: RequestThrottle, router: Optional[TorRouter] = None) -> dict:
    """Fetch JSON. Uses a per-thread requests+SOCKS session when router is given, else urllib."""
    last_error: Optional[Exception] = None
    hard_attempts = 0

    while hard_attempts < MAX_RETRY_ATTEMPTS:
        try:
            if router is not None:
                # --- Tor path: requests with per-thread SOCKS session ---
                session = _get_or_create_session(router.socks_port)
                response = session.get(url, timeout=REQUEST_TIMEOUT)

                if response.status_code == 200:
                    router.note_success()
                    return response.json()

                if response.status_code == 429:
                    router.note_429()
                    time.sleep(1.5)  # brief pause before retry regardless of rotation
                    continue  # no hard_attempts++

                if 500 <= response.status_code < 600:
                    hard_attempts += 1
                    sleep_s = throttle.note_error()
                    _log(
                        f"[warn] server error {response.status_code}; "
                        f"sleeping {sleep_s:.2f}s (attempt {hard_attempts}/{MAX_RETRY_ATTEMPTS})",
                        file=sys.stderr,
                    )
                    time.sleep(sleep_s)
                    continue

                response.raise_for_status()

            else:
                # --- Direct path: urllib ---
                throttle.wait_before_request()
                request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
                with urlopen(request, timeout=REQUEST_TIMEOUT) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                throttle.note_success()
                return payload

        except HTTPError as exc:
            last_error = exc
            if exc.code == 429:
                hard_attempts += 1
                sleep_s = throttle.note_throttle(parse_retry_after(exc.headers.get("Retry-After")))
                _log(
                    f"[rate-limit] 429; sleeping {sleep_s:.2f}s "
                    f"(attempt {hard_attempts}/{MAX_RETRY_ATTEMPTS})",
                    file=sys.stderr,
                )
                time.sleep(sleep_s)
                continue
            hard_attempts += 1
            if 500 <= exc.code < 600:
                sleep_s = throttle.note_error()
                _log(
                    f"[warn] server error {exc.code}; sleeping {sleep_s:.2f}s "
                    f"(attempt {hard_attempts}/{MAX_RETRY_ATTEMPTS})",
                    file=sys.stderr,
                )
                time.sleep(sleep_s)
                continue
            raise

        except URLError as exc:
            last_error = exc
            hard_attempts += 1
            sleep_s = throttle.note_error()
            _log(
                f"[warn] network error: {exc}; sleeping {sleep_s:.2f}s "
                f"(attempt {hard_attempts}/{MAX_RETRY_ATTEMPTS})",
                file=sys.stderr,
            )
            time.sleep(sleep_s)

        except Exception as exc:
            # requests exceptions (ConnectionError, Timeout, etc.)
            last_error = exc
            hard_attempts += 1
            sleep_s = throttle.note_error()
            _log(
                f"[warn] request error: {exc}; sleeping {sleep_s:.2f}s "
                f"(attempt {hard_attempts}/{MAX_RETRY_ATTEMPTS})",
                file=sys.stderr,
            )
            time.sleep(sleep_s)

    if last_error:
        raise RuntimeError(f"Request failed after retries: {last_error}") from last_error
    raise RuntimeError("Request failed without a captured error")


# ---------------------------------------------------------------------------
# Wikipedia API
# ---------------------------------------------------------------------------

def wiki_api_base(lang: str) -> str:
    return f"https://{lang}.wikipedia.org/w/api.php"


def wiki_article_url(lang: str, title: str) -> str:
    return f"https://{lang}.wikipedia.org/wiki/{quote(title.replace(' ', '_'))}"


def fetch_main_page(lang: str, throttle: RequestThrottle, router: Optional[TorRouter] = None) -> str:
    url = f"{wiki_api_base(lang)}?action=query&format=json&meta=siteinfo&siprop=general"
    data = api_get_json(url, throttle, router)
    main_page = data.get("query", {}).get("general", {}).get("mainpage")
    if not main_page:
        raise RuntimeError(f"No main page found for language '{lang}'")
    return main_page


def fetch_page_bundle(
    lang: str,
    title: str,
    throttle: RequestThrottle,
    router: Optional[TorRouter] = None,
) -> Tuple[int, str, List[str], List[Tuple[str, str]]]:
    article_links: List[str] = []
    language_links: List[Tuple[str, str]] = []
    ll_continue: Optional[str] = None
    pl_continue: Optional[str] = None
    canonical_title: Optional[str] = None
    page_id: Optional[int] = None

    while True:
        page_selector = f"titles={quote(title)}&redirects=1" if page_id is None else f"pageids={page_id}"
        lang_filter = "|".join(PRIORITY_LANGS)
        url = (
            f"{wiki_api_base(lang)}?action=query&format=json&{page_selector}"
            f"&prop=links|langlinks&plnamespace=0&pllimit=max&lllimit=max&lllang={lang_filter}"
        )
        if pl_continue:
            url += f"&plcontinue={quote(pl_continue)}"
        if ll_continue:
            url += f"&llcontinue={quote(ll_continue)}"

        data = api_get_json(url, throttle, router)
        pages = data.get("query", {}).get("pages", {})
        if not pages:
            raise RuntimeError(f"No page data returned for {lang}:{title}")

        page = next(iter(pages.values()))
        if "missing" in page:
            raise PageNotFoundError(f"Page not found: {lang}:{title}")

        if page_id is None:
            page_id = int(page["pageid"])
            canonical_title = page["title"]

        for link in page.get("links", []):
            linked_title = link.get("title")
            if linked_title:
                article_links.append(linked_title)

        for langlink in page.get("langlinks", []):
            lang_code = langlink.get("lang")
            linked_title = langlink.get("*")
            if lang_code and linked_title:
                language_links.append((lang_code, linked_title))

        cont = data.get("continue", {})
        pl_continue = cont.get("plcontinue")
        ll_continue = cont.get("llcontinue")
        if not pl_continue and not ll_continue:
            break

    if page_id is None or canonical_title is None:
        raise RuntimeError(f"Could not resolve page metadata for {lang}:{title}")

    return page_id, canonical_title, article_links, language_links


# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------

def append_jsonl(path: Path, rows: Iterable[Dict]) -> None:
    with _FILE_WRITE_LOCK:
        with path.open("a", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_json_atomic(path: Path, payload: Dict) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    for attempt in range(5):
        try:
            tmp_path.replace(path)
            return
        except PermissionError:
            if attempt == 4:
                raise
            time.sleep(0.1 * (attempt + 1))


def save_stats(stats: Dict) -> None:
    write_json_atomic(STATS_PATH, stats)


def log_page(lang: str, title: str, depth: int, page_id: int, source: str) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    append_jsonl(PAGES_PATH, [{
        "lang": lang, "title": title, "page_id": page_id,
        "depth": depth, "source": source,
        "url": wiki_article_url(lang, title),
        "saved_at_unix": time.time(),
    }])


def log_edges_batch(
    from_lang: str,
    from_title: str,
    from_page_id: int,
    article_links: List[str],
    language_links: List[Tuple[str, str]],
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    for title in article_links:
        rows.append({
            "from_lang": from_lang, "from_title": from_title, "from_page_id": from_page_id,
            "to_lang": from_lang, "to_title": title, "to_page_id": None,
            "edge_type": "article", "to_url": wiki_article_url(from_lang, title),
            "saved_at_unix": time.time(),
        })
    for linked_lang, linked_title in language_links:
        rows.append({
            "from_lang": from_lang, "from_title": from_title, "from_page_id": from_page_id,
            "to_lang": linked_lang, "to_title": linked_title, "to_page_id": None,
            "edge_type": "langlink", "to_url": wiki_article_url(linked_lang, linked_title),
            "saved_at_unix": time.time(),
        })
    if rows:
        append_jsonl(EDGES_PATH, rows)


# ---------------------------------------------------------------------------
# Crawl state (fully thread-safe)
# ---------------------------------------------------------------------------

class CrawlState:
    def __init__(self, started_at: Optional[float] = None) -> None:
        self._lock = threading.Lock()
        self.priority_frontier: Deque[Tuple[str, str, int]] = deque()
        self.normal_frontier: Deque[Tuple[str, str, int]] = deque()
        self.queued: Set[Tuple[str, str]] = set()
        self.visited: Set[Tuple[str, str]] = set()
        self.known_page_ids: Dict[Tuple[str, str], int] = {}
        self.discovery_depth: Dict[Tuple[str, str], int] = {}
        self.total_edges = 0
        self.total_pages_written = 0
        self.started_at = started_at or time.time()
        self.last_stats_flush = 0.0
        self.last_state_flush = 0.0
        self.priority_pop_count = 0
        self.inflight: Set[Tuple[str, str, int]] = set()

    def push(self, lang: str, title: str, depth: int, front: bool = False) -> None:
        with self._lock:
            self._push_locked(lang, title, depth, front)

    def pop(self) -> Optional[Tuple[str, str, int]]:
        with self._lock:
            item = self._pop_locked()
            if item is not None:
                self.inflight.add(item)
            return item

    def mark_done(self, item: Tuple[str, str, int]) -> None:
        with self._lock:
            self.inflight.discard(item)

    def record_page(
        self,
        lang: str,
        requested_title: str,
        canonical_title: str,
        page_id: int,
        depth: int,
        article_links: List[str],
        language_links: List[Tuple[str, str]],
        item: Tuple[str, str, int],
    ) -> int:
        with self._lock:
            self.visited.add((lang, requested_title))
            self.visited.add((lang, canonical_title))
            self.known_page_ids[(lang, canonical_title)] = page_id
            self.discovery_depth[(lang, canonical_title)] = depth
            self.total_pages_written += 1
            self.inflight.discard(item)

            edge_count = len(article_links) + len(language_links)
            self.total_edges += edge_count

            for title in article_links:
                self._push_locked(lang, title, depth + 1)
            for linked_lang, linked_title in language_links:
                self._push_locked(linked_lang, linked_title, depth + 1)

            return edge_count

    def is_visited(self, lang: str, title: str) -> bool:
        with self._lock:
            return (lang, title) in self.visited

    def to_resume_payload(self, seed_count: int, throttle: RequestThrottle) -> Dict:
        with self._lock:
            return {
                "seed_count": seed_count,
                "started_at": self.started_at,
                "priority_frontier": list(self.priority_frontier),
                "normal_frontier": list(self.normal_frontier),
                "priority_pop_count": self.priority_pop_count,
                "total_edges": self.total_edges,
                "total_pages_written": self.total_pages_written,
                "inflight": [list(i) for i in self.inflight],
                "adaptive_delay_seconds": throttle.current_delay,
                "saved_at_unix": time.time(),
            }

    @classmethod
    def from_resume_payload(cls, payload: Dict) -> "CrawlState":
        state = cls(started_at=payload.get("started_at"))
        state.priority_frontier = deque(tuple(i) for i in payload.get("priority_frontier", []))  # type: ignore
        state.normal_frontier = deque(tuple(i) for i in payload.get("normal_frontier", []))  # type: ignore
        state.priority_pop_count = int(payload.get("priority_pop_count", 0))
        state.total_edges = int(payload.get("total_edges", 0))
        state.total_pages_written = int(payload.get("total_pages_written", 0))

        inflight_raw = payload.get("inflight")
        if inflight_raw:
            if inflight_raw and not isinstance(inflight_raw[0], (list, tuple)):
                inflight_raw = [inflight_raw]
            for entry in inflight_raw:
                state.priority_frontier.appendleft(tuple(entry))  # type: ignore

        for lang, title, depth in list(state.priority_frontier) + list(state.normal_frontier):
            state.queued.add((lang, title))
            state.discovery_depth.setdefault((lang, title), depth)
        return state

    def _push_locked(self, lang: str, title: str, depth: int, front: bool = False) -> None:
        key = (lang, title)
        if key in self.visited or key in self.queued:
            return
        self.queued.add(key)
        self.discovery_depth.setdefault(key, depth)
        target = self.priority_frontier if lang in PRIORITY_LANGS else self.normal_frontier
        item = (lang, title, depth)
        if front:
            target.appendleft(item)
        else:
            target.append(item)

    def _pop_locked(self) -> Optional[Tuple[str, str, int]]:
        if self.priority_frontier and self.normal_frontier:
            if self.priority_pop_count < NON_PRIORITY_POP_INTERVAL:
                self.priority_pop_count += 1
                return self.priority_frontier.popleft()
            else:
                self.priority_pop_count = 0
                return self.normal_frontier.popleft()
        if self.priority_frontier:
            self.priority_pop_count += 1
            return self.priority_frontier.popleft()
        if self.normal_frontier:
            self.priority_pop_count = 0
            return self.normal_frontier.popleft()
        return None


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------

def hydrate_state_from_pages_log(state: CrawlState) -> None:
    if not PAGES_PATH.exists():
        return
    page_count = 0
    with PAGES_PATH.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            key = (row["lang"], row["title"])
            state.visited.add(key)
            if row.get("page_id"):
                state.known_page_ids[key] = int(row["page_id"])
            if row.get("depth") is not None:
                state.discovery_depth.setdefault(key, int(row["depth"]))
            page_count += 1
    state.total_pages_written = max(state.total_pages_written, page_count)


def infer_started_at_from_stats() -> float:
    if not STATS_PATH.exists():
        return time.time()
    try:
        stats = json.loads(STATS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return time.time()
    updated_at = stats.get("updated_at_unix")
    duration = stats.get("duration_seconds")
    if isinstance(updated_at, (int, float)) and isinstance(duration, (int, float)):
        return float(updated_at) - float(duration)
    return time.time()


def reconstruct_state_from_logs() -> Optional[Tuple[CrawlState, int]]:
    if not PAGES_PATH.exists() or not EDGES_PATH.exists():
        return None

    state = CrawlState(started_at=infer_started_at_from_stats())
    visited_depths: Dict[Tuple[str, str], int] = {}
    page_count = 0

    with PAGES_PATH.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            key = (row["lang"], row["title"])
            state.visited.add(key)
            if row.get("page_id"):
                state.known_page_ids[key] = int(row["page_id"])
            depth = int(row.get("depth", 0))
            visited_depths[key] = depth
            state.discovery_depth.setdefault(key, depth)
            page_count += 1

    if page_count == 0:
        return None

    state.total_pages_written = page_count
    frontier_depths: Dict[Tuple[str, str], int] = {}

    with EDGES_PATH.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            src = (row["from_lang"], row["from_title"])
            dst = (row["to_lang"], row["to_title"])
            state.total_edges += 1
            if dst in state.visited:
                continue
            src_depth = visited_depths.get(src)
            proposed = (src_depth + 1) if src_depth is not None else 1
            if dst not in frontier_depths or proposed < frontier_depths[dst]:
                frontier_depths[dst] = proposed

    for (lang, title), depth in sorted(
        frontier_depths.items(),
        key=lambda x: (x[1], 0 if x[0][0] in PRIORITY_LANGS else 1, x[0][0], x[0][1]),
    ):
        state.push(lang, title, depth)

    if not state.queued:
        return None
    return state, len(PRIORITY_LANGS)


def save_runtime_state(state: CrawlState, seed_count: int, throttle: RequestThrottle) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    write_json_atomic(FRONTIER_STATE_PATH, state.to_resume_payload(seed_count, throttle))


def build_stats(state: CrawlState, seed_count: int, throttle: RequestThrottle, resumed: bool) -> Dict:
    with state._lock:
        return {
            "priority_languages": list(PRIORITY_LANGS),
            "seed_count": seed_count,
            "resumed": resumed,
            "visited_pages": len(state.visited),
            "queued_pages": len(state.queued),
            "inflight": len(state.inflight),
            "priority_frontier": len(state.priority_frontier),
            "normal_frontier": len(state.normal_frontier),
            "edges_written": state.total_edges,
            "pages_written": state.total_pages_written,
            "adaptive_delay_seconds": round(throttle.current_delay, 3),
            "duration_seconds": round(time.time() - state.started_at, 3),
            "updated_at_unix": time.time(),
        }


def load_or_seed_state(
    throttle: RequestThrottle,
    router: Optional[TorRouter] = None,
) -> Tuple[CrawlState, int, bool]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if FRONTIER_STATE_PATH.exists():
        payload = json.loads(FRONTIER_STATE_PATH.read_text(encoding="utf-8"))
        state = CrawlState.from_resume_payload(payload)
        hydrate_state_from_pages_log(state)
        throttle.current_delay = float(payload.get("adaptive_delay_seconds", INITIAL_REQUEST_DELAY_SECONDS))
        _log(
            f"[resume] loaded queue from {FRONTIER_STATE_PATH.name}: "
            f"visited={len(state.visited)} queued={len(state.queued)} "
            f"adaptive_delay={throttle.current_delay:.2f}s"
        )
        return state, int(payload.get("seed_count", len(PRIORITY_LANGS))), True

    reconstructed = reconstruct_state_from_logs()
    if reconstructed is not None:
        state, seed_count = reconstructed
        _log(f"[resume] rebuilt queue from logs: visited={len(state.visited)} queued={len(state.queued)}")
        return state, seed_count, True

    _log(f"[init] seeding priority languages: {', '.join(PRIORITY_LANGS)}")
    main_pages: List[Tuple[str, str]] = []
    for lang in PRIORITY_LANGS:
        try:
            main_title = fetch_main_page(lang, throttle, router)
            main_pages.append((lang, main_title))
            _log(f"[seed] {lang}:{main_title}")
        except Exception as exc:
            _log(f"[warn] failed to fetch main page for {lang}: {exc}", file=sys.stderr)

    state = CrawlState()
    for lang, title in main_pages:
        state.push(lang, title, 0)
    return state, len(main_pages), False


# ---------------------------------------------------------------------------
# Worker thread
# ---------------------------------------------------------------------------
# Live external-sync watcher
# ---------------------------------------------------------------------------

def _extern_sync_thread(state: CrawlState, interval: float = 60.0) -> None:
    """Tail pages.jsonl for rows added externally (e.g. by sync.py from Pi).

    Marks new pages as visited so workers don't re-crawl them.
    Runs as a daemon thread — exits automatically when the main process does.
    """
    last_line_count = 0
    while True:
        time.sleep(interval)
        if not PAGES_PATH.exists():
            continue
        try:
            with PAGES_PATH.open("r", encoding="utf-8") as f:
                lines = [ln for ln in f if ln.strip()]
            new_lines = lines[last_line_count:]
            last_line_count = len(lines)
            if not new_lines:
                continue
            added = 0
            with state._lock:
                for ln in new_lines:
                    try:
                        row = json.loads(ln)
                        key = (row["lang"], row["title"])
                        if key not in state.visited:
                            state.visited.add(key)
                            state.queued.add(key)   # prevents re-queuing from incoming edges
                            added += 1
                    except Exception:
                        pass
            if added:
                _log(f"[extern-sync] marked {added} Pi-crawled pages as visited")
        except Exception:
            pass


# ---------------------------------------------------------------------------

def _worker(
    state: CrawlState,
    throttle: RequestThrottle,
    routers: List[Optional[TorRouter]],
    stop_event: threading.Event,
    worker_id: int,
) -> None:
    router: Optional[TorRouter] = routers[worker_id % len(routers)] if routers else None

    while not stop_event.is_set():
        item = state.pop()
        if item is None:
            time.sleep(0.3)
            continue

        lang, requested_title, depth = item

        if state.is_visited(lang, requested_title):
            state.mark_done(item)
            continue

        try:
            page_id, canonical_title, article_links, language_links = fetch_page_bundle(
                lang, requested_title, throttle, router,
            )
        except PageNotFoundError as exc:
            _log(f"[warn] missing {lang}:{requested_title}: {exc}", file=sys.stderr)
            state.mark_done(item)
            continue
        except Exception as exc:
            _log(f"[warn] fetch failed {lang}:{requested_title}: {exc}; re-queueing", file=sys.stderr)
            state.mark_done(item)
            state.push(lang, requested_title, depth, front=False)
            continue

        state.record_page(lang, requested_title, canonical_title, page_id, depth, article_links, language_links, item)

        log_page(lang, canonical_title, depth, page_id, "crawl")
        log_edges_batch(lang, canonical_title, page_id, article_links, language_links)

        _log(f"[t{worker_id:02d}] {lang}:{canonical_title} depth={depth} links={len(article_links)+len(language_links)}")


# ---------------------------------------------------------------------------
# Main crawl loop
# ---------------------------------------------------------------------------

def crawl_forever(
    routers: Optional[List[TorRouter]] = None,
    num_threads: int = 16,
    langs: Optional[Tuple[str, ...]] = None,
) -> None:
    global PRIORITY_LANGS
    if langs:
        PRIORITY_LANGS = langs
        _log(f"[crawl] priority languages overridden to: {', '.join(PRIORITY_LANGS)}")

    seed_router: Optional[TorRouter] = routers[0] if routers else None
    throttle = RequestThrottle()
    state, seed_count, resumed = load_or_seed_state(throttle, seed_router)

    save_runtime_state(state, seed_count, throttle)
    save_stats(build_stats(state, seed_count, throttle, resumed))

    effective_routers: List[Optional[TorRouter]] = routers if routers else [None]

    # Daemon thread: marks pages written by sync.py as visited within ~60s
    threading.Thread(target=_extern_sync_thread, args=(state,), daemon=True, name="extern-sync").start()

    stop_event = threading.Event()
    workers = [
        threading.Thread(
            target=_worker,
            args=(state, throttle, effective_routers, stop_event, i),
            daemon=True,
            name=f"crawler-{i:02d}",
        )
        for i in range(num_threads)
    ]
    for w in workers:
        w.start()

    _log(f"[crawl] {num_threads} worker threads started")
    if routers:
        _log(
            f"[crawl] {len(routers)} Tor instance(s), "
            f"SOCKS ports: {[r.socks_port for r in routers]}, "
            f"~{num_threads // len(routers)} threads each"
        )

    try:
        while True:
            time.sleep(STATE_FLUSH_EVERY_SECONDS)
            save_runtime_state(state, seed_count, throttle)
            now = time.time()
            if now - state.last_stats_flush >= STATS_FLUSH_EVERY_SECONDS:
                stats = build_stats(state, seed_count, throttle, resumed)
                save_stats(stats)
                state.last_stats_flush = now
                _log(
                    f"[stats] visited={stats['visited_pages']} "
                    f"queued={stats['queued_pages']} "
                    f"inflight={stats['inflight']} "
                    f"edges={stats['edges_written']} "
                    f"pages/s≈{stats['pages_written'] / max(stats['duration_seconds'], 1):.1f}"
                )
    except KeyboardInterrupt:
        _log("\n[info] stopping workers …", file=sys.stderr)
        stop_event.set()
    finally:
        for w in workers:
            w.join(timeout=10)
        save_runtime_state(state, seed_count, throttle)
        save_stats(build_stats(state, seed_count, throttle, resumed))
        _log("[info] state and stats saved.", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(description="Wikipedia crawler for Wiki-Race datasets")
    parser.add_argument(
        "--tor",
        action="store_true",
        help="Route requests through Tor. By default, auto-starts N Tor instances.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=16,
        metavar="N",
        help="Number of concurrent worker threads (default: 16)",
    )
    parser.add_argument(
        "--tor-instances",
        type=int,
        default=0,
        metavar="N",
        help=(
            f"Number of Tor instances to launch (default: threads // {THREADS_PER_TOR_INSTANCE}). "
            "Each instance gets its own SOCKS port and IP. Ignored without --tor."
        ),
    )
    parser.add_argument(
        "--tor-exe",
        default=TOR_EXE_DEFAULT,
        metavar="PATH",
        help=f"Path to tor binary (default: {TOR_EXE_DEFAULT}). Ignored with --no-start-tor.",
    )
    parser.add_argument(
        "--tor-data-dir",
        default=None,
        metavar="DIR",
        help=(
            f"Base directory for Tor DataDirectories and torrc files "
            f"(default: {TOR_DATA_BASE_DEFAULT}). Ignored with --no-start-tor."
        ),
    )
    parser.add_argument(
        "--no-start-tor",
        action="store_true",
        help=(
            "Connect to pre-running Tor instances instead of launching them. "
            "Expects instances on consecutive port pairs starting at 9050/9051."
        ),
    )
    args = parser.parse_args()

    manager: Optional[TorInstanceManager] = None
    routers: Optional[List[TorRouter]] = None

    if args.tor:
        n_instances = args.tor_instances or max(1, args.threads // THREADS_PER_TOR_INSTANCE)

        if args.no_start_tor:
            _log(f"[tor] connecting to {n_instances} pre-running Tor instance(s) …")
            routers = []
            for i in range(n_instances):
                socks_port = TOR_SOCKS_BASE_PORT + i * TOR_PORT_STEP
                ctrl_port = socks_port + 1
                r = TorRouter(socks_port=socks_port, control_port=ctrl_port)
                try:
                    r.connect()
                except Exception as exc:
                    _log(f"[tor] failed to connect to instance {i} (ports {socks_port}/{ctrl_port}): {exc}", file=sys.stderr)
                    return 1
                routers.append(r)
        else:
            data_base = Path(args.tor_data_dir) if args.tor_data_dir else None
            try:
                manager = TorInstanceManager(
                    n_instances=n_instances,
                    tor_exe=args.tor_exe,
                    data_base=data_base,
                )
                routers = manager.start()
            except Exception as exc:
                _log(f"[tor] failed to start Tor instances: {exc}", file=sys.stderr)
                return 1

    try:
        crawl_forever(routers, num_threads=args.threads)
    except KeyboardInterrupt:
        _log("\n[info] crawl stopped by Ctrl+C", file=sys.stderr)
        return 130
    except Exception as exc:
        _log(f"[error] {exc}", file=sys.stderr)
        return 1
    finally:
        if manager is not None:
            _log("[tor] shutting down Tor instances …", file=sys.stderr)
            manager.stop()
        elif routers:
            for r in routers:
                r.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
