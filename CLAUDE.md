# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Crawl (direct connection)
python wiki.py --threads 16

# Crawl via Tor (auto-launches N tor instances, ~4 threads each)
python wiki.py --tor --threads 32

# Crawl via Tor with explicit instance count
python wiki.py --tor --tor-instances 4 --threads 16

# Crawl via Tor using pre-running instances (don't auto-launch)
python wiki.py --tor --no-start-tor

# Train the link-scorer model from crawl output
python wiki_race_bot.py train

# Play a game (auto-creates lobby, trains model if needed)
python wiki_race_bot.py play --language en

# Join an existing lobby
python wiki_race_bot.py play --join LOBBYCODE
```

Dependencies: `pip install stem requests[socks]` for Tor mode.

## Architecture

### Two separate programs

**`wiki.py`** — standalone crawler, no imports from `wikibot/`. Crawls Wikipedia and writes two append-only JSONL files:
- `crawl_output/pages.jsonl` — one row per visited page (lang, title, page_id, depth)
- `crawl_output/edges.jsonl` — one row per link (from→to, edge_type: article or langlink)

**`wiki_race_bot.py`** — uses the `wikibot/` package to train a model from crawl output and then play live games on wiki-race.com.

### Crawler (`wiki.py`) key design points

- **Thread-safe frontier** (`CrawlState`): priority deque for `en`/`de`, normal deque for all others; `NON_PRIORITY_POP_INTERVAL=40` interleaving ratio.
- **State persistence**: `frontier_state.json` is flushed every 8s via atomic rename; on restart it reloads or reconstructs state from the JSONL logs.
- **Tor mode**: `TorInstanceManager` auto-launches N `tor.exe` sub-processes (one per 4 threads by default). Each instance gets its own SOCKS port (`9050 + i*2`) and control port (`9051 + i*2`), its own DataDirectory under `C:\tor\data_i\`, and its own `TorRouter` instance. Threads are round-robin assigned to routers. Each `TorRouter` tracks its own consecutive-429 counter (threshold=5) and rotates only its own circuit via `NEWNYM` — other instances/threads are unaffected. Thread-local `requests.Session` objects hold per-thread SOCKS proxy connections (`socks5h://`).
- **`--no-start-tor`**: skips launching processes, just connects controllers to existing instances at the standard ports.

### Bot package (`wikibot/`)

| File | Role |
|---|---|
| `graph.py` | `WikiGraph` — in-memory directed graph loaded from `edges.jsonl`. BFS (`reverse_distances`) from destination for scoring. |
| `model.py` | `LinearLinkScorer` — logistic regression trained via SGD on `TrainingExample` pairs (positive neighbor = improves BFS distance, negative = doesn't). Saved/loaded as JSON weights. |
| `bot.py` | `WikiRaceBot` — orchestrates a game: fetches live Wikipedia page HTML, scores each link candidate, picks the best, submits click. After winning, calls `train()` to update weights (online learning). |
| `client.py` | `WikiRaceClient` — HTTP client for wiki-race.com: create/join lobby, update settings, start game, poll state, submit clicks. |
| `wikipedia.py` | `WikipediaClient` — fetches rendered page HTML (transforms `<a>` tags to add `data-wiki-page` attribute) and link lists via MediaWiki API. |
| `types.py` | Shared dataclasses: `PageRef`, `GameSettings`, `GameSnapshot`. |

### Feature vector (`model.py:build_feature_vector`)

The scorer uses: exact-match flag, title token overlap with destination, candidate in/out-degree (log-scaled), BFS reverse distance from destination (if known), distance-improvement flag, and per-destination-token hit flags. Unknown-distance candidates get a separate `candidate_distance_unknown` feature.
