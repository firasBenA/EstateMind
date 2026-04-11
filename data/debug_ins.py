"""
debug_ins.py
============
Inspects INS pages after full JavaScript rendering.

Put at EstateMind/data/ and run:
    python debug_ins.py

Paste the full output here.
"""
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By


def _make_driver():
    options = Options()
    # Run WITHOUT headless so you can see what's happening
    # (comment out the next line if you want headless)
    # options.add_argument("--headless")
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


URL = "https://www.ins.tn/statistiques/indice-des-prix-a-la-construction"

print("Opening Chrome (non-headless so you can watch)...")
driver = _make_driver()

print(f"\nLoading: {URL}")
driver.get(URL)

print("Waiting 5s for initial load...")
time.sleep(5)

# Scroll down
driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
time.sleep(2)

# Print all buttons/links/selects on the page
print("\n--- BUTTONS found ---")
btns = driver.find_elements(By.TAG_NAME, "button")
for b in btns[:20]:
    print(f"  button: text={b.text!r} | class={b.get_attribute('class')!r}")

print("\n--- SELECTS found ---")
sels = driver.find_elements(By.TAG_NAME, "select")
for s in sels[:10]:
    print(f"  select: name={s.get_attribute('name')!r} | options={[o.text for o in s.find_elements(By.TAG_NAME, 'option')][:5]}")

print("\n--- LINKS with xls/xlsx/csv/download ---")
links = driver.find_elements(By.TAG_NAME, "a")
for a in links:
    href = a.get_attribute("href") or ""
    text = a.text.strip()
    if any(x in href.lower() for x in [".xls", ".xlsx", ".csv", ".zip", "download", "telecharger", "export"]):
        print(f"  DOWNLOAD: {href!r} | text={text!r}")

html = driver.page_source
soup = BeautifulSoup(html, "html.parser")

print(f"\n--- PAGE INFO ---")
print(f"Title: {soup.title.get_text() if soup.title else 'n/a'}")
print(f"HTML length: {len(html)}")
print(f"Tables: {len(soup.find_all('table'))}")
print(f"Divs with 'table' in class: {len([d for d in soup.find_all('div') if 'table' in (d.get('class') or [])])}")

# Print all iframes
iframes = soup.find_all("iframe")
print(f"\nIframes: {len(iframes)}")
for fr in iframes:
    print(f"  src={fr.get('src')!r}")

# Print first 500 chars of page text
text = soup.get_text(" ", strip=True)
print(f"\nPage text preview:\n{text[:600]!r}")

# Save HTML
with open("debug_ins_page.html", "w", encoding="utf-8") as f:
    f.write(html)
print("\nSaved: debug_ins_page.html")

print("\n\nNOW: Look at the browser window that opened.")
print("  - Is there a table with data visible?")
print("  - Are there any dropdown/filter controls?")
print("  - Is there a download button?")
print("\nPress ENTER here when done looking...")
input()

driver.quit()
print("Done.")