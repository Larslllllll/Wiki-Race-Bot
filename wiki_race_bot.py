#!/usr/bin/env python3
"""Train and run a WikiRace bot against wiki-race.com."""

from __future__ import annotations

import argparse
import builtins as _builtins
import json
import random
import sys
import time
from datetime import datetime as _datetime
from pathlib import Path
from typing import Optional

# ── timestamp every [tag] print line across all modules ───────────────────────
_orig_print = _builtins.print
def _ts_print(*args, **kwargs):
    if args and isinstance(args[0], str):
        s = args[0]
        lead = len(s) - len(s.lstrip('\n'))
        rest = s[lead:]
        if rest.startswith('['):
            ts  = _datetime.now().strftime('%H:%M:%S')
            args = ('\n' * lead + f'[{ts}]' + rest,) + args[1:]
    _orig_print(*args, **kwargs)
_builtins.print = _ts_print
# ──────────────────────────────────────────────────────────────────────────────

from wikibot import LinearLinkScorer, WikiGraph, WikiRaceBot, WikiRaceClient, train_model
from wikibot.client import WikiRaceApiError
from wikibot.graph_db import WikiGraphDB, DEFAULT_DB
from wikibot.model import generate_training_examples
from wikibot.types import GameSettings, PageRef
from wikibot.wikipedia import WikipediaClient


DEFAULT_EDGES      = Path("crawl_output/edges.jsonl")
DEFAULT_PAGES      = Path("crawl_output/pages.jsonl")
DEFAULT_MODEL      = Path("models/wiki_race_model.json")
DEFAULT_NEURAL_DIR = Path("models/neural_scorer")


# ── CLI ────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="WikiRace bot and trainer")
    sub = parser.add_subparsers(dest="command", required=True)

    # ── train ──────────────────────────────────────────────────────────
    tp = sub.add_parser("train", help="Train a link ranker from crawl_output")
    tp.add_argument("--linear", action="store_true",
                    help="Train linear logistic regression instead of neural model")
    tp.add_argument("--gpu", type=int, default=0, metavar="N",
                    help="GPU index (default: 0)")
    tp.add_argument("--epochs", type=int, default=0, metavar="N",
                    help="Training epochs (0 = infinite until Ctrl+C, default: 0)")
    tp.add_argument("--reload-every", type=int, default=3, metavar="N",
                    help="Reload graph + regenerate examples every N epochs (default: 3)")
    tp.add_argument("--batch-size", type=int, default=128)
    tp.add_argument("--max-examples", type=int, default=600_000, metavar="N",
                    help="Cap training examples (neural, default: 600 000)")
    tp.add_argument("--edges-path", default=str(DEFAULT_EDGES))
    tp.add_argument("--pages-path", default=str(DEFAULT_PAGES))
    tp.add_argument("--model-path", default=str(DEFAULT_MODEL),
                    help="Output path for linear model (.json)")
    tp.add_argument("--neural-dir", default=str(DEFAULT_NEURAL_DIR),
                    help="Output directory for neural model")
    tp.add_argument("--max-edges", type=int, default=15_000_000, metavar="N",
                    help="Cap edges loaded into RAM (0 = all)")

    # ── build-index ────────────────────────────────────────────────────
    ip = sub.add_parser("build-index",
                        help="Build SQLite graph DB from crawl_output (run once, then --graph is instant)")
    ip.add_argument("--edges-path", default=str(DEFAULT_EDGES))
    ip.add_argument("--pages-path", default=str(DEFAULT_PAGES))
    ip.add_argument("--db-path",    default=str(DEFAULT_DB))
    ip.add_argument("--watch", action="store_true",
                    help="Keep running: re-index new edges every --interval seconds")
    ip.add_argument("--interval", type=int, default=300,
                    help="Seconds between re-index runs in --watch mode (default: 300)")

    # ── parse-dump ─────────────────────────────────────────────────────
    dp = sub.add_parser(
        "parse-dump",
        help="Import Wikipedia SQL dumps (page + pagelinks) directly into graph.db",
    )
    dp.add_argument("--pages-en",      metavar="FILE", help="enwiki-latest-page.sql.gz")
    dp.add_argument("--links-en",      metavar="FILE", help="enwiki-latest-pagelinks.sql.gz")
    dp.add_argument("--linktarget-en", metavar="FILE", help="enwiki-latest-linktarget.sql.gz (required for 2022+ dumps)")
    dp.add_argument("--pages-de",      metavar="FILE", help="dewiki-latest-page.sql.gz")
    dp.add_argument("--links-de",      metavar="FILE", help="dewiki-latest-pagelinks.sql.gz")
    dp.add_argument("--linktarget-de", metavar="FILE", help="dewiki-latest-linktarget.sql.gz (required for 2022+ dumps)")
    dp.add_argument("--db-path", default=str(DEFAULT_DB),
                    help=f"Output graph.db path (default: {DEFAULT_DB}). Merged into existing DB.")

    # ── play ───────────────────────────────────────────────────────────
    pp = sub.add_parser("play", help="Play on wiki-race.com")
    pp.add_argument("--name", default="WikiBot")
    pp.add_argument("--join", metavar="CODE", help="Join an existing lobby by code")
    pp.add_argument("--language", default="en")
    pp.add_argument("--start",       help="Start article (when creating a game)")
    pp.add_argument("--destination", help="Destination article (when creating a game)")
    pp.add_argument("--no-auto-start", action="store_true")

    # scorer selection
    pp.add_argument("--graph", action="store_true", default=True,
                    help="Use SQLite graph DB for instant BFS (default: on if DB exists)")
    pp.add_argument("--db-path", default=str(DEFAULT_DB),
                    help="Path to graph SQLite DB (default: crawl_output/graph.db)")
    pp.add_argument("--semantic", action="store_true",
                    help="Semantic similarity only — no neural model, no graph")

    pp.add_argument("--gpu", type=int, default=0, metavar="N",
                    help="GPU index for neural scorer (default: 0)")
    pp.add_argument("--max-moves", type=int, default=10_000)
    pp.add_argument("--game-timeout", type=float, default=300.0)
    pp.add_argument("--stay", action="store_true",
                    help="Stay in lobby after each game (keeps name, waits for next round)")
    pp.add_argument("--human", action="store_true",
                    help="Add human-like delays between moves (~2-5s per hop)")
    pp.add_argument("--human-delay", type=float, default=3.0, metavar="SECS",
                    help="Mean delay between moves in human mode (default: 3.0)")
    pp.add_argument("--shell", action="store_true",
                    help="Open interactive shell for live control")
    pp.add_argument("--rounds", type=int, default=1,
                    help="Rounds to play (0 = infinite)")
    pp.add_argument("--edges-path", default=str(DEFAULT_EDGES))
    pp.add_argument("--pages-path", default=str(DEFAULT_PAGES))
    pp.add_argument("--model-path", default=str(DEFAULT_MODEL))
    pp.add_argument("--neural-dir", default=str(DEFAULT_NEURAL_DIR))
    pp.add_argument("--base-url", default="https://wiki-race.com")
    pp.add_argument("--no-learn", action="store_true",
                    help="Disable online learning (linear model only)")
    return parser


# ── helpers ────────────────────────────────────────────────────────────────────

def _load_graph(edges_path: Path, pages_path: Path, max_edges=None) -> Optional[WikiGraph]:
    if not edges_path.exists():
        return None
    cap = f" (cap: {max_edges:,} edges)" if max_edges else ""
    print(f"[init] loading graph{cap} …", flush=True)
    g = WikiGraph.load(
        edges_path=edges_path,
        pages_path=pages_path if pages_path.exists() else None,
        max_edges=max_edges,
    )
    print(f"[init] graph: {len(g.nodes):,} nodes", flush=True)
    return g


def _load_linear_scorer(graph: Optional[WikiGraph], model_path: Path) -> Optional[LinearLinkScorer]:
    if graph is None:
        return None
    if model_path.exists():
        print(f"[model] loading linear scorer from {model_path}")
        return LinearLinkScorer.load(model_path)
    print("[model] no saved model — training linear scorer from scratch …")
    scorer = train_model(graph, verbose=True)
    scorer.save(model_path)
    print(f"[model] saved → {model_path}")
    return scorer


def _load_neural_scorer(neural_dir: Path, gpu: int):
    from wikibot.neural import NeuralLinkScorer, get_device
    if not neural_dir.exists():
        sys.exit(
            f"[error] Neural model directory not found: {neural_dir}\n"
            "Run:  python wiki_race_bot.py train --neural"
        )
    device = get_device(gpu)
    return NeuralLinkScorer(neural_dir, device=device)


def random_crawled_page(pages_path: Path, language: str) -> Optional[PageRef]:
    if not pages_path.exists():
        return None
    candidates = []
    with pages_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                if row.get("lang") == language:
                    candidates.append(row)
            except Exception:
                pass
    if not candidates:
        return None
    row = random.choice(candidates)
    return PageRef(lang=row["lang"], title=row["title"], pageid=row.get("page_id"))


# ── commands ───────────────────────────────────────────────────────────────────

def build_index_command(args: argparse.Namespace) -> int:
    import time as _time
    edges_path = Path(args.edges_path)
    pages_path = Path(args.pages_path)
    db_path    = Path(args.db_path)
    if not edges_path.exists():
        print(f"[error] edges file not found: {edges_path}")
        return 1
    interval = getattr(args, "interval", 300)

    WikiGraphDB.build(db_path, edges_path, pages_path if pages_path.exists() else None)

    if getattr(args, "watch", False):
        print(f"[watch] re-indexing every {interval}s — press Ctrl+C to stop")
        try:
            while True:
                _time.sleep(interval)
                WikiGraphDB.build(db_path, edges_path, pages_path if pages_path.exists() else None)
        except KeyboardInterrupt:
            print("\n[watch] stopped.")
    return 0


def parse_dump_command(args: argparse.Namespace) -> int:
    from wikibot.dump_parser import parse_lang

    db_path = Path(args.db_path)

    langs_requested = []
    if getattr(args, "pages_en", None) or getattr(args, "links_en", None):
        langs_requested.append(("en", getattr(args, "pages_en", None), getattr(args, "links_en", None)))
    if getattr(args, "pages_de", None) or getattr(args, "links_de", None):
        langs_requested.append(("de", getattr(args, "pages_de", None), getattr(args, "links_de", None)))

    if not langs_requested:
        print("[error] specify at least --pages-en/--links-en or --pages-de/--links-de")
        return 1

    for lang, pages, links in langs_requested:
        if not pages:
            print(f"[error] --pages-{lang} is required")
            return 1
        if not links:
            print(f"[error] --links-{lang} is required")
            return 1
        for p in (pages, links):
            if not Path(p).exists():
                print(f"[error] file not found: {p}")
                return 1
        lt_attr = f"linktarget_{lang}"
        lt_path = Path(getattr(args, lt_attr)) if getattr(args, lt_attr, None) else None
        if lt_path and not lt_path.exists():
            print(f"[error] file not found: {lt_path}")
            return 1
        print(f"\n[parse-dump] lang={lang}  DB={db_path}")
        parse_lang(lang, Path(pages), Path(links), db_path,
                   linktarget_path=lt_path, verbose=True)

    print("\n[parse-dump] all done.")
    return 0


def train_command(args: argparse.Namespace) -> int:
    edges_path = Path(args.edges_path)
    pages_path = Path(args.pages_path)
    max_edges  = args.max_edges if args.max_edges > 0 else None

    if not edges_path.exists():
        sys.exit(f"[error] edges file not found: {edges_path}\nRun the crawler first.")

    if args.linear:
        return _train_linear(args, edges_path, pages_path, max_edges)
    else:
        return _train_neural(args, edges_path, pages_path, max_edges)


def _train_neural(args, edges_path, pages_path, max_edges) -> int:
    from wikibot.neural import NeuralLinkScorer, generate_neural_examples, get_device

    neural_dir   = Path(args.neural_dir)
    batch_size   = args.batch_size
    gpu          = args.gpu
    reload_every = args.reload_every
    max_epochs   = args.epochs if args.epochs > 0 else None  # None = infinite rounds

    device    = get_device(gpu)
    round_num = 0

    try:
        while True:
            round_num += 1
            print(f"\n[train] ── round {round_num} — reloading graph …")
            graph = WikiGraph.load(
                edges_path=edges_path,
                pages_path=pages_path if pages_path.exists() else None,
                max_edges=max_edges,
            )
            print(f"[train] graph: {len(graph.nodes):,} nodes")

            examples = generate_neural_examples(
                graph,
                destination_count=2048,
                max_pages_per_dest=80,
                negative_ratio=3,
                pages_path=pages_path if pages_path.exists() else None,
                verbose=True,
            )

            # After round 1 continue from saved model, not from base distilbert
            has_weights = any((neural_dir / f).exists() for f in ("model.safetensors", "pytorch_model.bin"))
            start_from = str(neural_dir) if has_weights else None

            NeuralLinkScorer.train(
                examples,
                output_dir=neural_dir,
                device=device,
                epochs=reload_every,
                batch_size=batch_size,
                max_examples=args.max_examples,
                start_from=start_from,
            )

            # Check if total epoch budget reached
            if max_epochs is not None and round_num * reload_every >= max_epochs:
                break

    except KeyboardInterrupt:
        pass

    print(f"[train] done — play with:  python wiki_race_bot.py play --join <CODE>")
    return 0


def _train_linear(args, edges_path, pages_path, max_edges) -> int:
    model_path = Path(args.model_path)
    epochs     = args.epochs if args.epochs > 0 else None

    scorer    = LinearLinkScorer.load(model_path) if model_path.exists() else LinearLinkScorer()
    round_num = 0
    try:
        while True:
            round_num += 1
            print(f"\n[train] ── round {round_num} ── reloading graph …")
            graph = WikiGraph.load(
                edges_path=edges_path,
                pages_path=pages_path if pages_path.exists() else None,
                max_edges=max_edges,
            )
            examples = generate_training_examples(graph, verbose=True, pages_path=pages_path)
            scorer.train(examples, epochs=epochs, verbose=True)
            scorer.save(model_path)
            print(f"[train] model saved → {model_path}")
    except KeyboardInterrupt:
        scorer.save(model_path)
        print(f"\n[train] stopped after {round_num} round(s) — model saved → {model_path}")
    return 0


class BotControl:
    """Thread-safe control object shared between shell thread and game loop."""
    def __init__(self, stay: bool = False, human: bool = False) -> None:
        import threading as _t
        self.paused        = _t.Event()   # set = paused
        self.exit_req      = _t.Event()   # set = exit after current game
        self.stay          = stay
        self.human         = human

    def wait_if_paused(self) -> bool:
        """Block while paused. Returns False if exit was requested."""
        while self.paused.is_set():
            if self.exit_req.is_set():
                return False
            time.sleep(0.3)
        return not self.exit_req.is_set()


def _run_shell(ctrl: BotControl) -> None:
    """Interactive shell thread — reads commands from stdin."""
    cmds = (
        "  start        — resume playing\n"
        "  stop         — pause after current game\n"
        "  start human  — resume with human-like delays\n"
        "  stay         — stay in lobby after game ends\n"
        "  exit         — leave lobby and quit\n"
    )
    print(f"\n[shell] Bot control shell active. Commands:\n{cmds}")
    while not ctrl.exit_req.is_set():
        try:
            cmd = input("[shell]> ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            ctrl.exit_req.set()
            break
        if cmd == "start":
            ctrl.paused.clear()
            ctrl.human = False
            print("[shell] resumed (fast mode)")
        elif cmd == "stop":
            ctrl.paused.set()
            print("[shell] will pause after current game")
        elif cmd == "start human":
            ctrl.paused.clear()
            ctrl.human = True
            print("[shell] resumed (human mode)")
        elif cmd == "stay":
            ctrl.stay = True
            print("[shell] will stay in lobby after each game")
        elif cmd == "exit":
            ctrl.exit_req.set()
            print("[shell] exiting after current game …")
            break
        elif cmd:
            print(f"[shell] unknown command: {cmd!r}")


def play_command(args: argparse.Namespace) -> int:
    pages_path  = Path(args.pages_path)
    model_path  = Path(args.model_path)
    neural_dir  = Path(args.neural_dir)
    db_path     = Path(args.db_path)
    human_delay = args.human_delay if getattr(args, "human", False) else 0.0

    import threading as _threading

    # ── Control object (shared with shell thread) ─────────────────────
    ctrl = BotControl(
        stay  = getattr(args, "stay",  False),
        human = getattr(args, "human", False),
    )
    if getattr(args, "shell", False) or ctrl.stay:
        _threading.Thread(target=_run_shell, args=(ctrl,), daemon=True, name="shell").start()

    # ── 1. Join/create lobby immediately (bot appears in player list) ─
    client = WikiRaceClient(base_url=args.base_url)
    if args.join:
        result = client.join_game(args.join, args.name)
        print(f"[lobby] joined  id={result.game_id}  as '{result.player_name}'")
    else:
        result = client.create_game(args.name)
        print(f"[lobby] created id={result.game_id}  as '{result.player_name}'")
        print(f"[lobby] share:  {args.base_url}/?lobbyCode={result.game_id}")

    # ── Connect to Pusher presence channel so bot appears in browser lobby ──
    _pusher_presence = client.connect_presence(result.game_id, result.session, result.player_name)
    print(f"[lobby] Pusher presence channel connecting … (bot will appear in player list)")

    # ── 2. Load graph + neural model in background while waiting ─────
    graph         = None
    neural_scorer = None
    _load_error   = [None]

    def _load_models():
        nonlocal graph, neural_scorer
        try:
            if args.semantic:
                print("[init] semantic-only mode")
                return
            if args.graph:
                if not db_path.exists():
                    print(f"[init] graph DB not found — falling back to semantic scorer")
                else:
                    graph = WikiGraphDB(db_path)
                    print(f"[init] graph DB opened: {db_path}")
            if neural_dir.exists():
                label = "fallback" if graph else "primary scorer"
                print(f"[init] neural scorer ({label})")
                neural_scorer = _load_neural_scorer(neural_dir, args.gpu)
            elif not graph:
                print("[init] no neural model — using semantic scorer")
        except Exception as exc:
            _load_error[0] = exc

    _loader = _threading.Thread(target=_load_models, daemon=True)
    _loader.start()

    wikipedia = WikipediaClient()
    # bot is created after _loader finishes (joined below after wait_for_state)

    learn      = not args.no_learn
    max_rounds = args.rounds if args.rounds > 0 else float("inf")
    rounds_played = 0
    bot = None   # created after first wait_for_state so model loading overlaps lobby wait

    while rounds_played < max_rounds:
        rounds_played += 1

        if rounds_played == 1 and not args.join and not args.no_auto_start:
            _loader.join()   # must finish before we need settings
            if _load_error[0]:
                print(f"[warn] model load error: {_load_error[0]}")
            if bot is None:
                bot = WikiRaceBot(wikipedia, graph=graph, scorer=None,
                                  neural_scorer=neural_scorer,
                                  max_moves=args.max_moves, game_timeout=args.game_timeout)
            settings = _build_settings(args, bot, wikipedia, pages_path)
            client.update_settings(result.game_id, result.session, settings)
            client.start_game(result.game_id, result.session, settings)
            print(
                f"[game] started  lang={settings.language}  "
                f"'{settings.start.title}' → '{settings.destination.title}'"
            )
        elif rounds_played == 1:
            print("[lobby] waiting for host to start …")

        # ── Wait for game start (model loads in background during this wait) ──
        snapshot = None
        while snapshot is None:
            try:
                snapshot = client.wait_for_state(
                    result.game_id, result.session, "in_progress",
                    timeout=120.0, verbose=True,
                )
            except TimeoutError:
                if ctrl.stay or ctrl.paused.is_set():
                    if ctrl.exit_req.is_set():
                        return 0
                    continue   # keep waiting
                print("[lobby] game didn't start in time — exiting")
                return 1
            except Exception as exc:
                print(f"[warn] poll error: {exc} — retrying …")
                time.sleep(3)

        # Ensure model is ready before playing
        if _loader.is_alive():
            print("[init] waiting for model to finish loading …")
            _loader.join()
        if _load_error[0]:
            print(f"[warn] model load error: {_load_error[0]}")

        # Apply current human mode from control
        current_delay = human_delay if ctrl.human else 0.0
        if bot is None or bot.human_delay != current_delay:
            bot = WikiRaceBot(wikipedia, graph=graph, scorer=None,
                              neural_scorer=neural_scorer,
                              max_moves=args.max_moves, game_timeout=args.game_timeout,
                              human_delay=current_delay)

        # ── Play ───────────────────────────────────────────────────────
        if not ctrl.wait_if_paused():
            print("[shell] exit requested — leaving lobby")
            break
        try:
            t0   = time.time()
            path = bot.play(client, snapshot, verbose=True, learn=learn)
            dt   = time.time() - t0
            print(f"[result] {len(path) - 1} hop(s) in {dt:.1f}s")
        except Exception as exc:
            print(f"[error] play failed: {exc}")
            try:
                client.surrender(result.game_id, result.session)
                print("[game] surrendered")
            except Exception:
                pass
            if not ctrl.stay:
                return 1
            print("[lobby] staying in lobby …")

        # Inject visited pages into crawler frontier with top priority
        _inject_play_frontier(path if 'path' in dir() else [], lang=snapshot.settings.language)

        if learn and bot.scorer is not None:
            bot.scorer.save(model_path)
            print(f"[model] updated → {model_path}")

        if ctrl.exit_req.is_set():
            print("[shell] exiting")
            break

        if rounds_played >= max_rounds and not ctrl.stay:
            break

        print(f"\n[lobby] round {rounds_played} done — waiting for next round …")
        try:
            client.continue_game(result.game_id, result.session)
        except Exception:
            pass
        # Loop back to top — wait_for_state at the top handles the next round

    return 0


def _inject_play_frontier(path: list, lang: str) -> None:
    """Prepend pages visited during play to the crawler's priority frontier.

    These will be the very first pages crawled when wiki.py next starts,
    ensuring the bot's navigation graph has full link data for game pages.
    """
    hints_path = Path("crawl_output/play_hints.jsonl")
    hints_path.parent.mkdir(parents=True, exist_ok=True)

    # Append new hints (deduplicated later by wiki.py)
    seen: set = set()
    new_entries = []
    for page in path:
        key = (lang, page.title)
        if key not in seen:
            seen.add(key)
            new_entries.append({"lang": lang, "title": page.title})

    with hints_path.open("a", encoding="utf-8") as fh:
        for entry in new_entries:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")

    print(f"[frontier] {len(new_entries)} pages queued for priority crawl → {hints_path}")


def _build_settings(args, bot, wikipedia, pages_path) -> GameSettings:
    lang = args.language
    if args.start and args.destination:
        return GameSettings(
            language=lang,
            start=wikipedia.fetch_page_info(lang, page=args.start),
            destination=wikipedia.fetch_page_info(lang, page=args.destination),
        )
    start_row = random_crawled_page(pages_path, lang)
    dest_row  = random_crawled_page(pages_path, lang)
    if start_row and dest_row and start_row.pageid != dest_row.pageid:
        return GameSettings(
            language=lang,
            start=wikipedia.fetch_page_info(lang, page=start_row.title),
            destination=wikipedia.fetch_page_info(lang, page=dest_row.title),
        )
    return bot.create_random_settings(lang)


# ── main ───────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = build_parser()
    args   = parser.parse_args()
    if args.command == "train":
        return train_command(args)
    if args.command == "play":
        return play_command(args)
    if args.command == "build-index":
        return build_index_command(args)
    if args.command == "parse-dump":
        return parse_dump_command(args)
    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
