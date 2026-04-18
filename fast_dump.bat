@echo off
echo Deleting old graph.db for fast write mode...
del /q "crawl_output\graph.db"     2>nul
del /q "crawl_output\graph.db-shm" 2>nul
del /q "crawl_output\graph.db-wal" 2>nul
echo Starting fast_dump...
echo.
python fast_dump.py ^
  --pages-en      "C:\Users\lars\Downloads\enwiki-latest-page.sql.gz" ^
  --links-en      "C:\Users\lars\Downloads\enwiki-latest-pagelinks.sql.gz" ^
  --linktarget-en "C:\Users\lars\Downloads\enwiki-latest-linktarget.sql.gz" ^
  --pages-de      "C:\Users\lars\Downloads\dewiki-latest-page.sql.gz" ^
  --links-de      "C:\Users\lars\Downloads\dewiki-latest-pagelinks.sql.gz" ^
  --linktarget-de "C:\Users\lars\Downloads\dewiki-latest-linktarget.sql.gz"
pause
