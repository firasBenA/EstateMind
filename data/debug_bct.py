"""
debug_bct.py
============
Inspects BCT and INS pages to show exact HTML structure.

Put this at EstateMind/data/ and run:
    python debug_bct.py

Paste the full output back so we can fix the scraper.
"""
import time
from bs4 import BeautifulSoup


def _make_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    )
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=options)
    except Exception:
        return webdriver.Chrome(options=options)


URLS = {
    "BCT_taux_directeur": "https://www.bct.gov.tn/bct/siteprod/tableau_statistique_a.jsp?params=PL203260",
    "BCT_exchange":       "https://www.bct.gov.tn/bct/siteprod/tableau_statistique_a.jsp?params=PL212010",
    "INS_ipc_construction": "https://www.ins.tn/statistiques/indice-des-prix-a-la-construction",
}

print("Starting Chrome...\n")
driver = _make_driver()

for name, url in URLS.items():
    print(f"\n{'='*60}")
    print(f"PAGE : {name}")
    print(f"URL  : {url}")
    print("="*60)

    driver.get(url)
    time.sleep(6)
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    # Page title
    title = soup.find("title")
    print(f"Title: {title.get_text(strip=True) if title else 'none'}")
    print(f"HTML length: {len(html)} chars")

    # All tables
    tables = soup.find_all("table")
    print(f"Tables found: {len(tables)}")
    for i, table in enumerate(tables[:5]):
        trs = table.find_all("tr")
        ths = [th.get_text(strip=True) for th in table.find_all("th")]
        print(f"\n  Table[{i}]: {len(trs)} rows | headers: {ths[:8]}")
        for j, tr in enumerate(trs[:4]):
            cells = [td.get_text(strip=True) for td in tr.find_all(["td","th"])]
            print(f"    row[{j}]: {cells[:6]}")

    # All download links
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if any(x in href for x in [".xls", ".xlsx", ".csv", ".zip", "download", "telecharger"]):
            print(f"  LINK: {a['href']}")

    # Any divs/spans that look like they contain numbers
    # (in case data is not in a <table>)
    text_sample = soup.get_text()[:500]
    print(f"\n  Text preview: {text_sample[:300]!r}")

    # Save HTML
    fname = f"debug_{name}.html"
    with open(fname, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n  Saved: {fname}")

driver.quit()
print("\n\nDone — paste this output and share the .html files if needed.")