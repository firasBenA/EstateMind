@echo off
echo ==========================================
echo  Fi-Dari Daily Scraper - %date% %time%
echo ==========================================
cd /d "B:\EstateMind\fi_dari_scraper"
"C:\Users\chtou\anaconda3\python.exe" daily_scrape.py >> "logs\daily_log.txt" 2>&1
echo Termine : %date% %time% >> "logs\daily_log.txt"
