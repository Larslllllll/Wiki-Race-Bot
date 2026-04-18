# WikiRace Bot

> **Vibe-coded** — dieses Projekt wurde nicht von Hand geschrieben, sondern gemeinsam mit Claude (Anthropic AI) entwickelt. Der gesamte Code, die Architektur und die Optimierungen entstanden im Dialog. Kein Stack Overflow, kein Copy-Paste — reines Vibecodin'.

Ein vollständiger Bot für [wiki-race.com](https://wiki-race.com), der Wikipedia-Artikel über kürzeste Pfade verbindet — mit lokalem SQLite-Graphen (945M+ Kanten), neuronalem Scorer und Tor-gestütztem Crawler.

---

## Was ist das hier?

Wiki-Race ist ein Spiel: Starte auf einem Wikipedia-Artikel, klick dich über Links zum Zielartikel — wer am schnellsten ist, gewinnt. Dieser Bot spielt das automatisch, indem er einen kompletten Graphen der Wikipedia-Verlinkungen in einer SQLite-Datenbank hält und per BFS den kürzesten Pfad findet.

---

## Architektur

```
wiki.py                  Crawler — schreibt edges.jsonl + pages.jsonl
fast_dump.py             Schnellimport aus Wikipedia SQL-Dumps → graph.db
wiki_race_bot.py         Haupt-Einstiegspunkt: spielen, trainieren, indexieren
export_db_to_edges.py    Exportiert graph.db zurück nach edges.jsonl (mit Resume)
visualisation.py         Erstellt brain.html — interaktive D3.js Graphvisualisierung
wait_then_crawl.py       Wartet auf Export, startet dann automatisch den Crawler

wikibot/
  bot.py                 Spiellogik: BFS-Pfad → semantisch → neural
  graph_db.py            SQLite-BFS — on-demand, kein RAM-Loading
  client.py              HTTP-Client für wiki-race.com + Pusher Presence
  wikipedia.py           Wikipedia API: Links, Backlinks, Seiteninfos
  neural.py              DistilBERT-basierter Link-Scorer (Intel Arc via DirectML)
  model.py               Lineares Modell als Fallback
  similarity.py          TF-IDF Ähnlichkeit für semantisches Ranking

Apps/
  Android/               Kotlin-App für Android
  Windows/               C# .NET App für Windows
```

---

## Setup

### 1. Abhängigkeiten

```bash
pip install requests tqdm psutil stem sentence-transformers torch-directml
```

Für Tor-Modus zusätzlich:
```bash
pip install stem requests[socks]
# tor.exe unter C:\tor\tor\tor.exe
```

### 2. Graph aufbauen

**Option A — Wikipedia SQL-Dumps** (schnellste Methode, ~1-2h für EN+DE):
```bash
# Dumps herunterladen von https://dumps.wikimedia.org/
fast_dump.bat
```

**Option B — Crawler** (fortlaufend, beliebig lange):
```bash
python wiki.py --threads 16
# mit Tor (mehr parallele IPs, weniger Rate-Limits):
python wiki.py --tor --threads 64
```

**Option C — Beide kombinieren** (Dumps als Basis, Crawler für neue Artikel):
```bash
fast_dump.bat          # einmalig
python wiki.py --threads 16   # danach fortlaufend
python wiki_race_bot.py build-index   # neue Kanten in graph.db einbauen
```

---

## Spielen

```bash
# Neuer Lobby, automatisch starten:
python wiki_race_bot.py play --name MeinBot --language en

# Bestehender Lobby beitreten:
python wiki_race_bot.py play --join ABCDE --name MeinBot --stay

# Mit interaktiver Shell (pause/resume/human-mode):
python wiki_race_bot.py play --join ABCDE --name MeinBot --stay
# Shell-Befehle: start | stop | start human | stay | exit
```

---

## Trainieren

```bash
# Neuronales Modell (empfohlen, nutzt GPU):
python wiki_race_bot.py train

# Lineares Modell (kein GPU nötig):
python wiki_race_bot.py train --linear
```

---

## Visualisierung

Erzeugt eine interaktive Gehirn-Visualisierung des Wikipedia-Graphen:

```bash
python visualisation.py --nodes 3000 --lang en --out brain.html
```

Dann `brain.html` im Browser öffnen — Force-directed Graph mit Zoom, Pan, Hover, Suche, Wikipedia-Links per Klick.

---

## Wie der Bot navigiert

1. **Graph-BFS** — kennt der lokale Graph beide Artikel, findet er den kürzesten Pfad in ~1-5s aus 945M+ Kanten
2. **Bridge-Nodes** — Artikel nicht im Graph? Bot fetcht Wikipedia-Links und findet einen Knoten im Graph als Brücke
3. **Neural Scorer** — DistilBERT bewertet welcher Link dem Ziel am nächsten ist
4. **Semantische Ähnlichkeit** — TF-IDF Fallback ohne GPU

---

## Datenbank

Die `graph.db` (~86 GB) enthält:
- ~19M englische Wikipedia-Artikel
- ~5M deutsche Wikipedia-Artikel  
- ~945M Verlinkungen zwischen Artikeln

Sie wird nicht im Repo versioniert — entweder selbst bauen (siehe Setup) oder via `export_db_to_edges.py` aus einer bestehenden DB exportieren.

---

## API-Endpunkte (wiki-race.com)

Der Bot nutzt dieselben Endpunkte wie der Browser:

| Endpunkt | Methode | Beschreibung |
|---|---|---|
| `/api/game` | POST | Lobby erstellen |
| `/api/game/join` | POST | Lobby beitreten |
| `/api/game` | PUT | Einstellungen setzen |
| `/api/game/start` | POST | Spiel starten |
| `/api/game/location` | POST | Aktuellen Pfad übermitteln |
| `/api/game/surrender` | POST | Aufgeben |
| `/api/game/continue` | POST | Nächste Runde |

Spielstatus wird über die `/game`-Seite (Next.js Page Props) und Pusher Presence Channels abgefragt.

---

*Gebaut mit [Claude Code](https://claude.ai/code) — 100% vibecodiert, 0% Langeweile.*
