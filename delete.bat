@echo off
echo Cleaning up unused files...
echo.

REM ── old crawler / scraping tools ──────────────────────────────────────────
del /q "wiki_crawler.py"          2>nul
del /q "parse_dumps.py"           2>nul
del /q "brain.html"               2>nul
del /q "dashboard.py"             2>nul
del /q "start-tor.ps1"            2>nul

REM ── old crawl state files (not the DB itself!) ────────────────────────────
del /q "crawl_output\frontier_state.json"     2>nul
del /q "crawl_output\frontier_state.json.tmp" 2>nul
del /q "crawl_output\play_hints.jsonl"        2>nul
del /q "crawl_output\stats.json"              2>nul

REM ── AI / neural model (257 MB BERT, can't retrain anyway) ─────────────────
rmdir /s /q "models"              2>nul

REM ── Python bytecode caches ────────────────────────────────────────────────
rmdir /s /q "wikibot\__pycache__" 2>nul
rmdir /s /q "__pycache__"         2>nul

echo Done. What remains:
echo   wiki.py              (crawler, keep if needed)
echo   wiki_race_bot.py     (bot - play command)
echo   wikibot\             (bot package)
echo   fast_dump.py         (dump importer)
echo   fast_dump.bat        (run fast_dump.py)
echo   timer.py             (auto-detect + run dump)
echo   crawl_output\        (graph.db lives here)
echo   Apps\                (C# + Android apps)
echo.
pause
