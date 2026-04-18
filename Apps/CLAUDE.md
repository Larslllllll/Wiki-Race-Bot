# WikiRace Bot — Apps

This folder contains native clients for the WikiRace bot (wiki-race.com).
The Python reference implementation lives one level up in `../wiki_race_bot.py`.

## Architecture overview

### wiki-race.com API
| Endpoint | Method | Purpose |
|---|---|---|
| `/api/game` | POST `{playerName}` | Create lobby → returns `{gameId, session}` |
| `/api/game/join` | POST `{gameId, playerName}` | Join lobby |
| `/api/game/start` | POST `{gameId, session, settings}` | Start game (host only) |
| `/api/game/location` | POST `{gameId, session, path[]}` | Submit current path |
| `/api/game/surrender` | POST `{gameId, session}` | Surrender |
| `/api/game/continue` | POST `{gameId, session}` | Vote continue after round |
| `/api/game/pusher/auth` | POST form `socket_id + channel_name + sessionId + secretToken + gameId` | Pusher auth |

**Session object:**
```json
{ "id": "...", "secretToken": "..." }
```

**Path entry:**
```json
{ "title": "Adolf Hitler", "pageid": 4536 }
```

### Pusher (real-time presence)
- App key: `932edcd098e03d77349f`
- Host: `ws.wiki-race.com` (WSS port 443)
- Channel: `presence-game-{gameId}`
- Auth endpoint: POST `/api/game/pusher/auth` (form-encoded body, see above)
- **Must subscribe** to appear in the browser player bubble list

### graph.db (SQLite)
Located at `../crawl_output/graph.db`.

```sql
-- Nodes
SELECT lang, title, page_id FROM nodes WHERE lang='en' AND title='Albert Einstein';

-- Neighbors (BFS forward step)
SELECT to_lang, to_title FROM edges WHERE from_lang='en' AND from_title='Albert Einstein';
```

BFS finds shortest path from start → destination in ~1-5 seconds over ~165M edges.

### Wikipedia link API (Android fallback, no local graph)
```
GET https://en.wikipedia.org/w/api.php
  ?action=query&prop=links&titles=TITLE&pllimit=500&format=json&redirects=1
```

## Windows app (`Windows/`)
- .NET 8 console app
- Uses local `graph.db` via Microsoft.Data.Sqlite
- Full BFS path finding
- Run: `dotnet run -- --join LOBBYCODE --name WikiBot`

## Android app (`Android/`)
- Kotlin, min SDK 26
- No local graph — uses Wikipedia API navigation
- Enter lobby code + player name in the UI, tap Play
- Stays connected via Pusher WebSocket

## How to scrape faster → see parent CLAUDE.md
