from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import requests

from .types import PageRef


USER_AGENT = "WikiRaceBot/2.0"

_session = requests.Session()
_session.headers.update({"User-Agent": USER_AGENT})


_last_request_time: float = 0.0
_MIN_INTERVAL = 0.3   # max ~3 requests/sec — stays well under Wikipedia's limit

def _get(url: str) -> dict:
    import time as _time
    global _last_request_time

    # Gentle rate-limit: enforce minimum gap between requests
    gap = _MIN_INTERVAL - (_time.time() - _last_request_time)
    if gap > 0:
        _time.sleep(gap)

    delay = 4.0
    for attempt in range(8):
        _last_request_time = _time.time()
        resp = _session.get(url, timeout=15)
        if resp.status_code == 429:
            _time.sleep(delay)
            delay = min(delay * 2, 60.0)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"Request failed after 8 attempts (persistent 429): {url}")


@dataclass
class RenderedPage:
    page: PageRef
    links: List[PageRef]


class WikipediaClient:
    def __init__(self) -> None:
        self._links_cache: Dict[Tuple[str, str], List[PageRef]] = {}
        self._info_cache: Dict[Tuple[str, str], PageRef] = {}

    # ------------------------------------------------------------------
    # Random pages
    # ------------------------------------------------------------------

    def fetch_random_pages(self, lang: str, limit: int) -> List[PageRef]:
        url = (
            f"https://{lang}.wikipedia.org/w/api.php?action=query&list=random"
            f"&rnnamespace=0&rnlimit={limit}&format=json&origin=*"
        )
        data = _get(url)
        return [
            PageRef(lang=lang, title=row["title"], pageid=int(row["id"]))
            for row in data.get("query", {}).get("random", [])
        ]

    # ------------------------------------------------------------------
    # Page info (title + pageid, no HTML)
    # ------------------------------------------------------------------

    def fetch_page_info(
        self,
        lang: str,
        *,
        page: Optional[str] = None,
        pageid: Optional[int] = None,
    ) -> PageRef:
        cache_key = (lang, str(pageid) if pageid else (page or ""))
        if cache_key in self._info_cache:
            return self._info_cache[cache_key]

        query = f"pageids={pageid}" if pageid else f"titles={quote(page or '')}"
        url = (
            f"https://{lang}.wikipedia.org/w/api.php?action=query&format=json"
            f"&{query}&redirects=1&origin=*"
        )
        data = _get(url)
        pages = data.get("query", {}).get("pages", {})
        raw = next(iter(pages.values()))
        raw_pageid = raw.get("pageid")

        if "missing" in raw or raw_pageid is None or int(raw_pageid) < 0:
            title = raw.get("title") or page or str(pageid)
            ref = PageRef(lang=lang, title=title, pageid=None)
        else:
            ref = PageRef(lang=lang, title=raw["title"], pageid=int(raw_pageid))

        self._info_cache[cache_key] = ref
        self._info_cache[(lang, ref.title)] = ref
        return ref

    # ------------------------------------------------------------------
    # Page links (namespace 0)
    # ------------------------------------------------------------------

    def fetch_page_links(
        self,
        lang: str,
        title: str,
        *,
        max_pages: int = 3,
    ) -> List[PageRef]:
        """Return all namespace-0 links from *title*, including pageids.

        Uses generator=links + prop=info so each returned PageRef has a real
        pageid — required by the wiki-race.com API for path submissions.
        max_pages caps pagination (each page ≤ 500 links).
        """
        cache_key = (lang, title)
        if cache_key in self._links_cache:
            return self._links_cache[cache_key]

        links: List[PageRef] = []
        seen: set = set()
        cont_params: Optional[Dict[str, str]] = None
        pages_fetched = 0

        while pages_fetched < max_pages:
            url = (
                f"https://{lang}.wikipedia.org/w/api.php?action=query&format=json"
                f"&generator=links&titles={quote(title)}&gplnamespace=0&gpllimit=max"
                f"&prop=info&origin=*"
            )
            if cont_params:
                for k, v in cont_params.items():
                    url += f"&{k}={quote(str(v))}"

            data = _get(url)
            pages_fetched += 1

            raw_pages = data.get("query", {}).get("pages", {})
            for pid_str, page_data in (raw_pages or {}).items():
                linked_title = page_data.get("title")
                linked_pageid = page_data.get("pageid")
                if (
                    linked_title
                    and linked_title not in seen
                    and linked_pageid is not None
                    and int(linked_pageid) > 0
                ):
                    seen.add(linked_title)
                    links.append(PageRef(lang=lang, title=linked_title, pageid=int(linked_pageid)))

            cont = data.get("continue")
            if not cont:
                break
            cont_params = {k: v for k, v in cont.items() if k != "continue"}
            # "continue" key itself is a sentinel; the real continuation params are the rest
            if not cont_params:
                break

        self._links_cache[cache_key] = links
        return links

    # ------------------------------------------------------------------
    # Backlinks  (pages that link TO a given title)
    # ------------------------------------------------------------------

    def fetch_backlinks(self, lang: str, title: str, *, limit: int = 500) -> set[str]:
        """Return a set of article titles that link TO *title*.

        Useful for 1-hop look-ahead: if any current-page link is in this set,
        clicking it reaches destination in one more hop.
        """
        url = (
            f"https://{lang}.wikipedia.org/w/api.php?action=query&format=json"
            f"&titles={quote(title)}&prop=linkshere&lhnamespace=0"
            f"&lhlimit={min(limit, 500)}&origin=*"
        )
        data = _get(url)
        pages = data.get("query", {}).get("pages", {})
        result: set[str] = set()
        for page_data in pages.values():
            for entry in page_data.get("linkshere", []):
                t = entry.get("title")
                if t:
                    result.add(t)
        return result

    # ------------------------------------------------------------------
    # Rendered page HTML (used for display / debugging only)
    # ------------------------------------------------------------------

    def fetch_rendered_page(
        self,
        lang: str,
        *,
        page: Optional[str] = None,
        pageid: Optional[int] = None,
    ) -> PageRef:
        query = f"pageid={pageid}" if pageid else f"page={quote(page or '')}"
        url = (
            f"https://{lang}.wikipedia.org/w/api.php?action=parse&prop=text&{query}"
            "&format=json&disableeditsection=1&redirects=true&useskin=minerva&origin=*"
        )
        data = _get(url)
        parse = data["parse"]
        return PageRef(
            lang=lang,
            title=parse["title"],
            pageid=int(parse["pageid"]),
        )
