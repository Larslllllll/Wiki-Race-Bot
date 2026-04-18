from __future__ import annotations

import json
import random
import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

from .types import GameSettings, GameSnapshot, PageRef, SessionInfo

_PUSHER_KEY  = "932edcd098e03d77349f"
_PUSHER_HOST = "ws.wiki-race.com"


class PusherPresence:
    """Lightweight Pusher presence-channel client.

    Connects to ws.wiki-race.com, authenticates via /api/game/pusher/auth,
    subscribes to presence-game-{game_id}, then keeps the socket alive so
    other browser clients see the bot in the player bubble list.
    """

    def __init__(
        self,
        base_url: str,
        game_id: str,
        session: "SessionInfo",
        player_name: str,
        requests_session: "requests.Session",
    ) -> None:
        self._base_url    = base_url
        self._game_id     = game_id
        self._session     = session
        self._player_name = player_name
        self._req         = requests_session
        self._ws          = None
        self._thread: Optional[threading.Thread] = None
        self._stop        = threading.Event()

    # ------------------------------------------------------------------

    def start(self) -> None:
        """Connect in a background daemon thread."""
        self._thread = threading.Thread(target=self._run, daemon=True, name="pusher")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass

    # ------------------------------------------------------------------

    def _run(self) -> None:
        import websocket  # websocket-client

        url = (
            f"wss://{_PUSHER_HOST}/app/{_PUSHER_KEY}"
            "?protocol=7&client=py-bot&version=7.0.3&flash=false"
        )

        while not self._stop.is_set():
            try:
                ws = websocket.WebSocket()
                ws.connect(url, timeout=20)
                self._ws = ws

                # ── handshake ────────────────────────────────────────
                msg   = json.loads(ws.recv())
                event = msg.get("event", "")
                if event != "pusher:connection_established":
                    print(f"[pusher] unexpected handshake event: {event!r}", flush=True)
                    ws.close()
                    time.sleep(5)
                    continue

                data      = json.loads(msg["data"])
                socket_id = data["socket_id"]
                channel   = f"presence-game-{self._game_id}"
                print(f"[pusher] connected  socket_id={socket_id}  channel={channel}", flush=True)

                # ── authenticate ──────────────────────────────────────
                # Pusher sends auth.params in the POST body alongside socket_id/channel_name
                resp = self._req.post(
                    f"{self._base_url}/api/game/pusher/auth",
                    data={
                        "socket_id":    socket_id,
                        "channel_name": channel,
                        "sessionId":    self._session.id,
                        "secretToken":  self._session.secret_token,
                        "gameId":       self._game_id,
                    },
                    timeout=10,
                )
                if not resp.ok:
                    print(f"[pusher] auth failed  HTTP {resp.status_code}: {resp.text[:200]}", flush=True)
                    ws.close()
                    time.sleep(5)
                    continue
                auth_data = resp.json()
                print(f"[pusher] auth OK  keys={list(auth_data.keys())}", flush=True)

                # ── subscribe ──────────────────────────────────────────
                ws.send(json.dumps({
                    "event": "pusher:subscribe",
                    "data": {
                        "auth":         auth_data.get("auth", ""),
                        "channel_data": auth_data.get("channel_data", ""),
                        "channel":      channel,
                    },
                }))

                # ── keep-alive loop ───────────────────────────────────
                ws.settimeout(30)
                while not self._stop.is_set():
                    try:
                        raw = ws.recv()
                        if not raw:
                            break
                        parsed = json.loads(raw)
                        evt    = parsed.get("event", "")
                        if evt == "pusher:subscription_succeeded":
                            print(f"[pusher] subscribed to {channel} — bot now visible in lobby", flush=True)
                        elif evt == "pusher:subscription_error":
                            print(f"[pusher] subscription error: {parsed}", flush=True)
                        elif evt == "pusher:ping":
                            ws.send(json.dumps({"event": "pusher:pong", "data": {}}))
                        elif evt == "pusher:error":
                            print(f"[pusher] error: {parsed}", flush=True)
                    except websocket.WebSocketTimeoutException:
                        # Send our own ping to keep the connection alive
                        ws.send(json.dumps({"event": "pusher:ping", "data": {}}))
                    except Exception:
                        break

                ws.close()

            except Exception as exc:
                print(f"[pusher] connection error: {exc}", flush=True)

            if not self._stop.is_set():
                time.sleep(5)   # reconnect after error


_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 OPR/105.0.0.0",
]

def _random_ua() -> str:
    return random.choice(_USER_AGENTS)

NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
    re.DOTALL,
)


class WikiRaceApiError(RuntimeError):
    pass


@dataclass
class CreateOrJoinResult:
    game_id: str
    session: SessionInfo
    player_name: str


class WikiRaceClient:
    def __init__(self, base_url: str = "https://wiki-race.com") -> None:
        self.base_url = base_url.rstrip("/")
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": _random_ua()})

    # ------------------------------------------------------------------
    # Lobby / game lifecycle
    # ------------------------------------------------------------------

    def create_game(self, player_name: str) -> CreateOrJoinResult:
        payload = self._post("/api/game", {"playerName": player_name})
        return CreateOrJoinResult(
            game_id=payload["gameId"],
            session=SessionInfo.from_dict(payload["session"]),
            player_name=player_name,
        )

    def join_game(self, game_id: str, player_name: str) -> CreateOrJoinResult:
        name = player_name
        for attempt in range(5):
            try:
                # Fresh random User-Agent per join so the server sees a different browser
                self._session.headers.update({"User-Agent": _random_ua()})
                payload = self._post(
                    "/api/game/join",
                    {"gameId": game_id, "playerName": name},
                )
                return CreateOrJoinResult(
                    game_id=payload["gameId"],
                    session=SessionInfo.from_dict(payload["session"]),
                    player_name=name,
                )
            except WikiRaceApiError as exc:
                if "already taken" in str(exc).lower() and attempt < 4:
                    name = f"{player_name}_{random.randint(10, 99)}"
                    continue
                raise

    def update_settings(
        self, game_id: str, session: SessionInfo, settings: GameSettings
    ) -> None:
        self._request("PUT", "/api/game", {
            "gameId": game_id,
            "session": session.to_api_payload(),
            "settings": settings.to_api_payload(),
        })

    def start_game(
        self, game_id: str, session: SessionInfo, settings: GameSettings
    ) -> None:
        self._post("/api/game/start", {
            "gameId": game_id,
            "session": session.to_api_payload(),
            "settings": settings.to_api_payload(),
        })

    def submit_finished_path(
        self, game_id: str, session: SessionInfo, path: List[PageRef]
    ) -> None:
        self._post("/api/game/location", {
            "gameId": game_id,
            "session": session.to_api_payload(),
            "path": [p.to_path_entry() for p in path],
        })

    def surrender(self, game_id: str, session: SessionInfo) -> None:
        self._post("/api/game/surrender", {
            "gameId": game_id,
            "session": session.to_api_payload(),
        })

    def continue_game(self, game_id: str, session: SessionInfo) -> None:
        self._post("/api/game/continue", {
            "gameId": game_id,
            "session": session.to_api_payload(),
        })

    def connect_presence(
        self,
        game_id: str,
        session: SessionInfo,
        player_name: str,
    ) -> PusherPresence:
        """Subscribe to the Pusher presence channel so we appear in the lobby."""
        pres = PusherPresence(
            base_url=self.base_url,
            game_id=game_id,
            session=session,
            player_name=player_name,
            requests_session=self._session,
        )
        pres.start()
        return pres

    # ------------------------------------------------------------------
    # State polling
    # ------------------------------------------------------------------

    def fetch_snapshot(self, game_id: str, session: SessionInfo) -> GameSnapshot:
        resp = self._session.get(
            f"{self.base_url}/game",
            params={
                "gameId": game_id,
                "sessionId": session.id,
                "secretToken": session.secret_token,
            },
            headers={"Accept": "text/html"},
            timeout=20,
        )
        resp.raise_for_status()
        match = NEXT_DATA_RE.search(resp.text)
        if not match:
            raise WikiRaceApiError("Could not find __NEXT_DATA__ on /game page")
        payload = json.loads(match.group(1))
        page_props = payload["props"]["pageProps"]
        return GameSnapshot.from_page_props(page_props)

    def wait_for_state(
        self,
        game_id: str,
        session: SessionInfo,
        desired_state: str,
        *,
        timeout: float = 600.0,
        poll_interval: float = 2.0,
        verbose: bool = True,
    ) -> GameSnapshot:
        """Poll until game reaches desired_state. Default timeout: 10 minutes."""
        deadline = time.time() + timeout
        last_state: str | None = None
        while True:
            snapshot = self.fetch_snapshot(game_id, session)
            if snapshot.state != last_state and verbose:
                print(f"[lobby] state={snapshot.state!r}")
                last_state = snapshot.state
            if snapshot.state == desired_state:
                return snapshot
            remaining = deadline - time.time()
            if remaining <= 0:
                raise TimeoutError(
                    f"Timed out after {timeout:.0f}s waiting for state={desired_state!r} "
                    f"(last seen: {snapshot.state!r})"
                )
            time.sleep(min(poll_interval, remaining))

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", path, payload)

    def _request(
        self, method: str, path: str, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        resp = self._session.request(
            method,
            f"{self.base_url}{path}",
            json=payload,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=20,
        )
        try:
            data = resp.json()
        except Exception:
            data: Any = {"error": resp.text[:500]}

        if not resp.ok:
            msg = data.get("error") or data if isinstance(data, dict) else str(data)
            raise WikiRaceApiError(f"{method} {path} → HTTP {resp.status_code}: {msg}")
        return data
