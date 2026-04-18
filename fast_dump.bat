@echo off
chcp 65001 >nul
cd /d "%~dp0"

REM ── Pfade zu den heruntergeladenen Wikipedia SQL-Dumps anpassen ───────────
set PAGES_EN=C:\Users\%USERNAME%\Downloads\enwiki-latest-page.sql.gz
set LINKS_EN=C:\Users\%USERNAME%\Downloads\enwiki-latest-pagelinks.sql.gz
set LT_EN=C:\Users\%USERNAME%\Downloads\enwiki-latest-linktarget.sql.gz
set PAGES_DE=C:\Users\%USERNAME%\Downloads\dewiki-latest-page.sql.gz
set LINKS_DE=C:\Users\%USERNAME%\Downloads\dewiki-latest-pagelinks.sql.gz
set LT_DE=C:\Users\%USERNAME%\Downloads\dewiki-latest-linktarget.sql.gz

echo Deleting old graph.db for fast write mode...
del /q "crawl_output\graph.db"     2>nul
del /q "crawl_output\graph.db-shm" 2>nul
del /q "crawl_output\graph.db-wal" 2>nul

echo Starting fast_dump...
echo.

python fast_dump.py ^
  --pages-en      "%PAGES_EN%" ^
  --links-en      "%LINKS_EN%" ^
  --linktarget-en "%LT_EN%" ^
  --pages-de      "%PAGES_DE%" ^
  --links-de      "%LINKS_DE%" ^
  --linktarget-de "%LT_DE%"

pause
