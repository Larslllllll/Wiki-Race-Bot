# WikiRace Bot

> **Vibe-coded** — dieses Projekt wurde nicht von Hand geschrieben, sondern gemeinsam mit [Claude Code](https://claude.ai/code) (Anthropic AI) entwickelt. Der gesamte Code, die Architektur und alle Optimierungen entstanden im Dialog. Kein Stack Overflow, kein Copy-Paste — reines Vibecodin'.

Ein vollständiger Bot für [wiki-race.com](https://wiki-race.com), der Wikipedia-Artikel über kürzeste Pfade verbindet — mit lokalem SQLite-Graphen (945M+ Kanten), neuronalem Scorer und Tor-gestütztem Crawler.

---

## Inhaltsverzeichnis

- [Was ist Wiki-Race?](#was-ist-wiki-race)
- [Voraussetzungen installieren](#voraussetzungen-installieren)
  - [Python](#1-python)
  - [Git](#2-git)
  - [Python-Pakete](#3-python-pakete)
  - [7-Zip](#4-7-zip-nur-für-fast_dumpbat)
  - [Tor](#5-tor-nur-für-crawler-mit-tor)
- [Graph-Datenbank aufbauen](#graph-datenbank-aufbauen)
  - [Dumps herunterladen](#schritt-1--wikipedia-dumps-herunterladen)
  - [Pfade anpassen](#schritt-2--pfade-in-fast_dumpbat-anpassen)
  - [Import starten](#schritt-3--import-starten)
- [Bot spielen lassen](#bot-spielen-lassen)
- [Crawler](#crawler)
- [Modell trainieren](#modell-trainieren)
- [Visualisierung](#visualisierung)
- [Architektur](#architektur)
- [Apps](#apps)

---

## Was ist Wiki-Race?

[wiki-race.com](https://wiki-race.com) ist ein Multiplayer-Spiel: Alle Spieler starten auf demselben Wikipedia-Artikel und müssen per Klick auf Links so schnell wie möglich zum Zielartikel navigieren. Wer zuerst ankommt, gewinnt.

Dieser Bot spielt automatisch — er hält einen kompletten Graphen aller Wikipedia-Verlinkungen lokal als SQLite-Datenbank (~86 GB) und findet per BFS den kürzesten Pfad in Sekunden.

---

## Voraussetzungen installieren

### 1. Python

Mindestens **Python 3.10** wird benötigt.

1. Auf [python.org/downloads](https://www.python.org/downloads/) die neueste Version herunterladen
2. Installer starten
3. **Wichtig:** Haken bei **"Add Python to PATH"** setzen
4. Installation abschließen

Prüfen ob es funktioniert hat:
```
python --version
```

---

### 2. Git

1. Auf [git-scm.com/downloads](https://git-scm.com/downloads) herunterladen
2. Installer starten, alle Standardoptionen beibehalten
3. Prüfen:
```
git --version
```

Repo klonen:
```
git clone https://github.com/Larslllllll/Wiki-Race-Bot.git
cd Wiki-Race-Bot
```

---

### 3. Python-Pakete

Im Projektordner ausführen:
```
pip install requests tqdm psutil stem sentence-transformers
```

Für GPU-Unterstützung (Intel Arc):
```
pip install torch-directml
```

Für NVIDIA GPU:
```
pip install torch
```

---

### 4. 7-Zip (nur für fast_dump.bat)

7-Zip beschleunigt das Entpacken der Wikipedia-Dumps erheblich.

1. Auf [7-zip.org](https://www.7-zip.org/) herunterladen
2. Installieren (Standard-Pfad `C:\Program Files\7-Zip\` beibehalten)

---

### 5. Tor (nur für Crawler mit Tor)

Nur nötig wenn du den Crawler mit `--tor` betreiben willst (mehr parallele IPs, weniger Rate-Limits).

1. Auf [torproject.org](https://www.torproject.org/download/tor/) den **Tor Expert Bundle** herunterladen
2. Entpacken nach `C:\tor\`
3. Pfad zur `tor.exe` merken (Standard: `C:\tor\tor\tor.exe`)

Zusätzliche Pakete:
```
pip install stem requests[socks]
```

---

## Graph-Datenbank aufbauen

Der Bot braucht eine lokale SQLite-Datenbank mit allen Wikipedia-Verlinkungen. Diese wird aus offiziellen Wikipedia SQL-Dumps erstellt und ist danach ~86 GB groß.

> Die fertige Datenbank kann nicht im Repo liegen (zu groß für GitHub) — sie muss einmalig selbst gebaut werden. Das dauert ca. 1-2 Stunden.

---

### Schritt 1 — Wikipedia Dumps herunterladen

Von [dumps.wikimedia.org](https://dumps.wikimedia.org/) folgende Dateien herunterladen:

**Englisch** (~15 GB gesamt):
- [`enwiki-latest-page.sql.gz`](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-page.sql.gz)
- [`enwiki-latest-pagelinks.sql.gz`](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pagelinks.sql.gz)
- [`enwiki-latest-linktarget.sql.gz`](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-linktarget.sql.gz)

**Deutsch** (optional, ~5 GB):
- [`dewiki-latest-page.sql.gz`](https://dumps.wikimedia.org/dewiki/latest/dewiki-latest-page.sql.gz)
- [`dewiki-latest-pagelinks.sql.gz`](https://dumps.wikimedia.org/dewiki/latest/dewiki-latest-pagelinks.sql.gz)
- [`dewiki-latest-linktarget.sql.gz`](https://dumps.wikimedia.org/dewiki/latest/dewiki-latest-linktarget.sql.gz)

---

### Schritt 2 — Pfade in `fast_dump.bat` anpassen

`fast_dump.bat` öffnen und die Pfade oben auf den eigenen Download-Ordner anpassen:

```bat
set PAGES_EN=C:\Users\DEINNAME\Downloads\enwiki-latest-page.sql.gz
...
```

---

### Schritt 3 — Import starten

```
fast_dump.bat
```

Oder direkt:
```
python fast_dump.py --pages-en PFAD\enwiki-latest-page.sql.gz --links-en PFAD\enwiki-latest-pagelinks.sql.gz --linktarget-en PFAD\enwiki-latest-linktarget.sql.gz
```

Die fertige Datenbank landet in `crawl_output/graph.db`.

---

## Bot spielen lassen

```
python wiki_race_bot.py play --name MeinBot --language en
```

Bestehender Lobby beitreten:
```
python wiki_race_bot.py play --join LOBBYCODE --name MeinBot --stay
```

**Shell-Befehle** während der Bot läuft:

| Befehl | Beschreibung |
|---|---|
| `start` | Bot spielen lassen |
| `stop` | Nach aktuellem Spiel pausieren |
| `start human` | Mit menschlichen Verzögerungen spielen |
| `stay` | In Lobby bleiben nach Spielende |
| `exit` | Beenden |

---

## Crawler

Für neue Artikel die nach dem Dump-Import erschienen sind:

```
python wiki.py --threads 16
```

Mit Tor (empfohlen für viele Threads):
```
python wiki.py --tor --threads 64
```

Neue gecrawlte Kanten in die Datenbank einbauen:
```
python wiki_race_bot.py build-index
```

---

## Modell trainieren

Der Bot hat ein neuronales Modell (DistilBERT) das bewertet, welcher Link dem Ziel am nächsten ist. Training aus dem gecrawlten Graphen:

```
python wiki_race_bot.py train
```

Ohne GPU (lineares Modell):
```
python wiki_race_bot.py train --linear
```

---

## Visualisierung

Erzeugt eine interaktive Gehirn-Visualisierung des Wikipedia-Graphen als HTML:

```
python visualisation.py --nodes 3000 --lang en --out brain.html
```

Dann `brain.html` im Browser öffnen. Features: Zoom, Pan, Hover (zeigt Verlinkungen), Klick öffnet Wikipedia-Artikel, Suchfeld.

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
  neural.py              DistilBERT-basierter Link-Scorer
  model.py               Lineares Modell als Fallback
  similarity.py          TF-IDF Ähnlichkeit für semantisches Ranking

Apps/
  Android/               Kotlin-App für Android
  Windows/               C# .NET App für Windows
```

### Wie der Bot navigiert

1. **Graph-BFS** — kennt der lokale Graph beide Artikel, findet er den kürzesten Pfad in ~1-5s aus 945M+ Kanten
2. **Bridge-Nodes** — Artikel nicht im Graph? Bot fetcht Wikipedia-Links und findet einen Knoten im Graph als Brücke
3. **Neural Scorer** — DistilBERT bewertet welcher Link dem Ziel am nächsten ist
4. **Semantische Ähnlichkeit** — TF-IDF Fallback ohne GPU

---

## Apps

### Windows (.NET)
```
cd Apps/Windows
dotnet run -- --join LOBBYCODE --name WikiBot
```

### Android
APK in `Apps/Android/` mit Android Studio bauen oder direkt auf Gerät sideloaden. Lobby-Code und Namen in der App eingeben, Play drücken.

---

*Gebaut mit [Claude Code](https://claude.ai/code) — 100% vibecodiert, 0% Langeweile.*
