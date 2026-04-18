from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class PageRef:
    lang: str
    title: str
    pageid: Optional[int] = None
    content: Optional[str] = None

    @classmethod
    def from_dict(cls, lang: str, payload: Dict[str, Any]) -> "PageRef":
        return cls(
            lang=lang,
            title=payload["title"],
            pageid=payload.get("pageid"),
            content=payload.get("content"),
        )

    def to_path_entry(self) -> Dict[str, Any]:
        entry: Dict[str, Any] = {"title": self.title}
        if self.pageid is not None:
            entry["pageid"] = self.pageid
        return entry


@dataclass(frozen=True)
class SessionInfo:
    id: str
    secret_token: str
    name: Optional[str] = None

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "SessionInfo":
        return cls(
            id=payload["id"],
            secret_token=payload["secretToken"],
            name=payload.get("name"),
        )

    def to_api_payload(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "secretToken": self.secret_token,
        }


@dataclass(frozen=True)
class GameSettings:
    language: str
    start: PageRef
    destination: PageRef

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "GameSettings":
        language = payload["language"]
        return cls(
            language=language,
            start=PageRef.from_dict(language, payload["start"]),
            destination=PageRef.from_dict(language, payload["destination"]),
        )

    def to_api_payload(self) -> Dict[str, Any]:
        return {
            "language": self.language,
            "start": self.start.to_path_entry(),
            "destination": self.destination.to_path_entry(),
        }


@dataclass(frozen=True)
class GameSnapshot:
    id: str
    state: str
    settings: GameSettings
    master: Optional[str]
    last_winner: Optional[Dict[str, Any]]
    players: List[Dict[str, Any]]
    session: SessionInfo

    @classmethod
    def from_page_props(cls, page_props: Dict[str, Any]) -> "GameSnapshot":
        game = page_props["game"]
        session = SessionInfo.from_dict(page_props["session"])
        return cls(
            id=game["id"],
            state=game["state"],
            settings=GameSettings.from_dict(game["settings"]),
            master=game.get("master"),
            last_winner=game.get("lastWinner"),
            players=game.get("players", []),
            session=session,
        )
