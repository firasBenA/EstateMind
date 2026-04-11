"""
EstateMind — Macro / Time-Series Scrapers
==========================================
BCT/INS use Selenium. BVMT uses requests.

BCT page structure (confirmed from debug):
  Table[1] is a PIVOT TABLE:
    row[0]  = headers: ['Indicateurs', '2021', '2022', '2023', '2024', '2025']
    row[1]  = empty separator
    row[2+] = ['Janvier', val_2021, val_2022, val_2023, ...]
              ['Février',  ...                              ]
              etc.
  → years are columns, months are rows.
  → We build dates as YYYY-MM-01 by combining column year + row month.

  Exchange rate table (PL212010) same pivot structure:
    row[0]  = ['Indicateurs', '2020', '2021', '2022', '2023', '2024']
    row[2+] = ['Dinar Algérien', 0.22, ...], ['Dollar des USA', ...], ['EURO', ...]
  → currencies are rows, years are columns.
  → We look for 'EURO' and 'Dollar des USA' rows.

INS page structure (confirmed from debug):
  0 tables found — data is in a JS component, needs longer wait + scroll.

Run:
    python -m scrapers.macro_scrapers --source all --start 2005
    python -m scrapers.macro_scrapers --source bvmt   (confirmed: 28k rows)
    python -m scrapers.macro_scrapers --source bct
    python -m scrapers.macro_scrapers --source ins
"""
from __future__ import annotations

import io
import os
import re
import sqlite3
import subprocess
import sys
import time
import random
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Settings / logger
# ---------------------------------------------------------------------------
try:
    from config.settings import settings
    _DB_PATH: Path = Path(settings.TIMESERIES_DB_PATH)
except Exception:
    _DB_PATH = Path("data/estatemind_timeseries.db")

try:
    from config.logging_config import log
except Exception:
    import logging
    log = logging.getLogger("macro_scrapers")
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s %(name)s: %(message)s")

HISTORY_START: int = 2005
HISTORY_END:   int = datetime.now().year


# =============================================================================
# AUTO-INSTALL SELENIUM
# =============================================================================

def _ensure_selenium():
    missing = []
    try:
        import selenium
    except ImportError:
        missing.append("selenium")
    try:
        import webdriver_manager
    except ImportError:
        missing.append("webdriver-manager")
    if missing:
        log.info(f"Installing: {missing}")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet"] + missing
        )


# =============================================================================
# DATABASE
# =============================================================================

_SCHEMA = """
CREATE TABLE IF NOT EXISTS macro_indicators (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    date        TEXT NOT NULL,
    indicator   TEXT NOT NULL,
    value       REAL NOT NULL,
    unit        TEXT,
    source      TEXT,
    scraped_at  TEXT,
    UNIQUE(date, indicator)
);
CREATE INDEX IF NOT EXISTS idx_macro_date      ON macro_indicators(date);
CREATE INDEX IF NOT EXISTS idx_macro_indicator ON macro_indicators(indicator);
CREATE INDEX IF NOT EXISTS idx_macro_source    ON macro_indicators(source);
"""


def _get_db() -> sqlite3.Connection:
    os.makedirs(str(_DB_PATH.parent), exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


def _upsert(conn: sqlite3.Connection, rows: List[Dict]) -> int:
    now = datetime.now().isoformat()
    count = 0
    for row in rows:
        try:
            conn.execute(
                """INSERT INTO macro_indicators
                       (date, indicator, value, unit, source, scraped_at)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(date, indicator) DO UPDATE SET
                       value=excluded.value, scraped_at=excluded.scraped_at""",
                (row["date"], row["indicator"], row["value"],
                 row.get("unit", ""), row.get("source", ""), now),
            )
            count += 1
        except Exception as exc:
            log.warning(f"[db] {exc}")
    conn.commit()
    return count


# =============================================================================
# UTILITIES
# =============================================================================

# BCT month names (French) → zero-padded month number
_BCT_MONTHS = {
    "janvier": "01", "février": "02", "mars": "03", "avril": "04",
    "mai": "05", "juin": "06", "juillet": "07", "août": "08",
    "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
    # short forms
    "janv": "01", "févr": "02", "avr": "04", "juil": "07",
    "aoû": "08", "aout": "08", "sept": "09", "oct": "10", "nov": "11", "déc": "12",
}

_FR_MONTHS = {
    "jan": "01", "fév": "02", "fev": "02", "mar": "03", "avr": "04",
    "mai": "05", "jun": "06", "jui": "07", "jul": "07",
    "aoû": "08", "aou": "08", "sep": "09", "oct": "10",
    "nov": "11", "déc": "12", "dec": "12",
}


def _month_num(raw: str) -> Optional[str]:
    """Convert French month name to zero-padded number string."""
    key = raw.strip().lower()
    # try full name
    if key in _BCT_MONTHS:
        return _BCT_MONTHS[key]
    # try first 4 chars
    if key[:4] in _BCT_MONTHS:
        return _BCT_MONTHS[key[:4]]
    # try first 3 chars
    if key[:3] in _FR_MONTHS:
        return _FR_MONTHS[key[:3]]
    return None


def _date(raw: str) -> Optional[str]:
    """Normalise date string → YYYY-MM-01."""
    raw = str(raw).strip()
    if not raw:
        return None
    m = re.match(r"^(\d{4})[-/](\d{2})$", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"
    m = re.match(r"^(\d{2})[-/](\d{4})$", raw)
    if m:
        return f"{m.group(2)}-{m.group(1)}-01"
    m = re.match(r"^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$", raw)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-01"
    m = re.match(r"^(\d{4})$", raw)
    if m:
        return f"{m.group(1)}-01-01"
    m = re.search(r"[TQ](\d)[-/\s]?(\d{4})|(\d{4})[-/\s]?[TQ](\d)", raw, re.I)
    if m:
        q   = int(m.group(1) or m.group(4))
        yr  = m.group(2) or m.group(3)
        mon = {1: "01", 2: "04", 3: "07", 4: "10"}.get(q, "01")
        return f"{yr}-{mon}-01"
    m = re.match(r"([a-zéûîà]{3,6})[\s\-./]?(\d{4})", raw, re.I)
    if m:
        key = m.group(1).lower()[:3]
        if key in _FR_MONTHS:
            return f"{m.group(2)}-{_FR_MONTHS[key]}-01"
    return None


def _num(raw: str) -> Optional[float]:
    cleaned = (
        str(raw).replace("\xa0", "").replace("\u202f", "")
        .replace(" ", "").replace(",", ".").strip()
    )
    cleaned = re.sub(r"[^\d.]", "", cleaned)
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def _row(d: str, ind: str, val: float, unit: str, src: str) -> Dict:
    return {"date": d, "indicator": ind, "value": val,
            "unit": unit, "source": src}


# =============================================================================
# BCT PIVOT TABLE PARSERS
# =============================================================================

def _parse_bct_pivot(html: str, indicator: str, unit: str,
                     source: str) -> List[Dict]:
    """
    Parse BCT's pivot table format.

    Structure (confirmed from debug):
      Table[1]:
        row[0] = ['Indicateurs', '2021', '2022', '2023', '2024', '2025']
        row[1] = ['']   ← empty separator, skip
        row[2] = ['Janvier', '6,25000', '6,25000', '8,00000', '8,00000', '8,00000']
        row[3] = ['Février', '6,25000', ...]
        ...

    We extract all years from row[0], then for each data row:
      date = YYYY-MM-01  where YYYY = column header, MM = month from row label
    """
    soup = BeautifulSoup(html, "html.parser")
    rows_out = []

    for table in soup.find_all("table"):
        trs = table.find_all("tr")
        if len(trs) < 3:
            continue

        # Row 0 = header with years
        header_cells = [td.get_text(strip=True)
                        for td in trs[0].find_all(["td", "th"])]

        # Extract year columns (integers 4-digit)
        year_cols = {}  # col_index → year_string
        for i, cell in enumerate(header_cells):
            if re.match(r"^\d{4}$", cell.strip()):
                year_cols[i] = cell.strip()

        if not year_cols:
            continue

        # Data rows start at row[2] (row[1] is empty separator)
        for tr in trs[2:]:
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if not cells or not cells[0]:
                continue

            # col 0 = month name
            month_num = _month_num(cells[0])
            if not month_num:
                continue

            # For each year column, extract the value
            for col_idx, year in year_cols.items():
                if col_idx >= len(cells):
                    continue
                val = _num(cells[col_idx])
                if val is None or val <= 0:
                    continue
                d = f"{year}-{month_num}-01"
                rows_out.append(_row(d, indicator, val, unit, source))

    return rows_out


def _parse_bct_exchange_pivot(html: str, source: str) -> List[Dict]:
    """
    Parse BCT exchange rate pivot table.

    Structure (confirmed from debug):
      row[0] = ['Indicateurs', '2020', '2021', '2022', '2023', '2024']
      row[2] = ['Dinar Algérien', 0.22, ...]
      row[N] = ['Dollar des USA', 3.xx, ...]
      row[M] = ['EURO', 3.xx, ...]

    We extract only EUR (→ tnd_eur) and USD (→ tnd_usd).
    Values are annual averages so we store as YYYY-01-01.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows_out = []

    CURRENCY_MAP = {
        "dollar des usa": "tnd_usd",
        "dollar usa":     "tnd_usd",
        "usd":            "tnd_usd",
        "euro":           "tnd_eur",
        "eur":            "tnd_eur",
    }

    for table in soup.find_all("table"):
        trs = table.find_all("tr")
        if len(trs) < 3:
            continue

        header_cells = [td.get_text(strip=True)
                        for td in trs[0].find_all(["td", "th"])]
        year_cols = {}
        for i, cell in enumerate(header_cells):
            if re.match(r"^\d{4}$", cell.strip()):
                year_cols[i] = cell.strip()

        if not year_cols:
            continue

        for tr in trs[2:]:
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if not cells or not cells[0]:
                continue

            currency_label = cells[0].lower().strip()
            indicator = next(
                (ind for key, ind in CURRENCY_MAP.items()
                 if key in currency_label),
                None,
            )
            if not indicator:
                continue

            for col_idx, year in year_cols.items():
                if col_idx >= len(cells):
                    continue
                val = _num(cells[col_idx])
                if val and val > 0:
                    unit = "TND/EUR" if indicator == "tnd_eur" else "TND/USD"
                    rows_out.append(_row(f"{year}-01-01", indicator,
                                        val, unit, source))

    return rows_out


def _parse_bct_credits_pivot(html: str, source: str) -> List[Dict]:
    """
    BCT credits table — same pivot format.
    We look for rows whose label contains immobilier/habitat/logement.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows_out = []
    KEYWORDS = ["immobilier", "habitat", "logement", "construction"]

    for table in soup.find_all("table"):
        trs = table.find_all("tr")
        if len(trs) < 3:
            continue

        header_cells = [td.get_text(strip=True)
                        for td in trs[0].find_all(["td", "th"])]
        year_cols = {}
        for i, cell in enumerate(header_cells):
            if re.match(r"^\d{4}$", cell.strip()):
                year_cols[i] = cell.strip()

        if not year_cols:
            continue

        for tr in trs[2:]:
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if not cells or not cells[0]:
                continue
            label = cells[0].lower()
            if not any(k in label for k in KEYWORDS):
                continue
            for col_idx, year in year_cols.items():
                if col_idx >= len(cells):
                    continue
                val = _num(cells[col_idx])
                if val and val > 0:
                    rows_out.append(
                        _row(f"{year}-01-01", "credits_immobiliers",
                             val, "MDT", source)
                    )

    return rows_out


# =============================================================================
# SELENIUM HELPERS
# =============================================================================

def _make_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
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


def _page(driver, url: str, wait: float = 5.0) -> str:
    driver.get(url)
    time.sleep(wait)
    # Scroll to trigger lazy-loaded content
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)
    return driver.page_source


# =============================================================================
# BCT SCRAPER
# =============================================================================

class BCTScraper:
    SOURCE = "BCT"
    BASE   = "https://www.bct.gov.tn/bct/siteprod"

    PAGES = {
        "taux_directeur": (f"{BASE}/tableau_statistique_a.jsp?params=PL203260", "%"),
        "tmm":            (f"{BASE}/tableau_statistique_a.jsp?params=PL203105", "%"),
    }
    EXCHANGE_ANNUAL  = f"{BASE}/tableau_statistique_a.jsp?params=PL212010"
    EXCHANGE_MONTHLY = f"{BASE}/tableau_statistique.jsp?params=PL213010"
    CREDITS          = f"{BASE}/tableau_statistique.jsp?params=PL203030&prov=1"

    def run(self) -> Dict[str, Any]:
        _ensure_selenium()
        log.info(f"[BCT] Scraping {HISTORY_START}–{HISTORY_END}")
        driver = None
        rows: List[Dict] = []

        try:
            driver = _make_driver()

            # taux_directeur and tmm — pivot: months as rows, years as cols
            for indicator, (url, unit) in self.PAGES.items():
                log.info(f"[BCT] {indicator}")
                html     = _page(driver, url, wait=6)
                new_rows = _parse_bct_pivot(html, indicator, unit, self.SOURCE)
                rows    += new_rows
                log.info(f"[BCT]   → {len(new_rows)} rows")
                time.sleep(random.uniform(2, 4))

            # Exchange rates — pivot: currencies as rows, years as cols
            log.info("[BCT] exchange rates (annual)")
            html  = _page(driver, self.EXCHANGE_ANNUAL, wait=6)
            new   = _parse_bct_exchange_pivot(html, self.SOURCE)
            rows += new
            log.info(f"[BCT]   → {len(new)} rows")
            time.sleep(random.uniform(2, 4))

            # Monthly exchange rates — same pivot but months as rows
            log.info("[BCT] exchange rates (monthly)")
            html  = _page(driver, self.EXCHANGE_MONTHLY, wait=6)
            new   = _parse_bct_pivot(html, "tmm_monthly", "%", self.SOURCE)
            # Also try exchange parsing on monthly page
            new  += _parse_bct_exchange_pivot(html, self.SOURCE)
            rows += new
            log.info(f"[BCT]   → {len(new)} rows")
            time.sleep(random.uniform(2, 4))

            # Credits
            log.info("[BCT] credits_immobiliers")
            html  = _page(driver, self.CREDITS, wait=6)
            new   = _parse_bct_credits_pivot(html, self.SOURCE)
            rows += new
            log.info(f"[BCT]   → {len(new)} rows")

        except Exception as exc:
            log.error(f"[BCT] error: {exc}")
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

        # Remove any junk indicator added above
        rows = [r for r in rows if r["indicator"] != "tmm_monthly"]

        if not rows:
            log.warning("[BCT] 0 rows")
            return {"source": self.SOURCE, "rows": 0, "status": "empty"}

        conn     = _get_db()
        inserted = _upsert(conn, rows)
        conn.close()
        log.info(f"[BCT] Saved {inserted}/{len(rows)} rows")
        return {"source": self.SOURCE, "rows": inserted, "status": "ok"}


# =============================================================================
# INS SCRAPER
# =============================================================================

class INSScraper:
    """
    INS pages load data via JavaScript (0 tables found in first render).
    Strategy:
      1. Wait longer (10s) for JS to finish
      2. Scroll to trigger lazy loading
      3. Try to find and click a "Afficher" / "Rechercher" button
      4. Parse resulting table
      5. Fallback: look for XLS download link and download directly
    """

    SOURCE   = "INS"
    BASE_URL = "https://www.ins.tn"

    PAGES = {
        "ipc_construction": (
            "https://www.ins.tn/statistiques/indice-des-prix-a-la-construction",
            "index_2015=100",
        ),
        "ipc_general_ins": (
            "https://www.ins.tn/statistiques/indice-des-prix-a-la-consommation",
            "index_2015=100",
        ),
        "permis_construire": (
            "https://www.ins.tn/statistiques/construction-et-travaux-publics",
            "count",
        ),
        "chomage_rate": (
            "https://www.ins.tn/statistiques/emploi-et-chomage",
            "%",
        ),
        "pib_courant": (
            "https://www.ins.tn/statistiques/comptes-nationaux",
            "MDT",
        ),
    }

    def run(self) -> Dict[str, Any]:
        _ensure_selenium()
        log.info(f"[INS] Scraping {HISTORY_START}–{HISTORY_END}")
        driver = None
        rows: List[Dict] = []

        try:
            driver = _make_driver()
            for indicator, (url, unit) in self.PAGES.items():
                log.info(f"[INS] {indicator}")
                new_rows = self._scrape_one(driver, indicator, url, unit)
                rows    += new_rows
                log.info(f"[INS]   → {len(new_rows)} rows")
                time.sleep(random.uniform(2, 4))
        except Exception as exc:
            log.error(f"[INS] error: {exc}")
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

        if not rows:
            log.warning("[INS] 0 rows")
            return {"source": self.SOURCE, "rows": 0, "status": "empty"}

        conn     = _get_db()
        inserted = _upsert(conn, rows)
        conn.close()
        log.info(f"[INS] Saved {inserted}/{len(rows)} rows")
        return {"source": self.SOURCE, "rows": inserted, "status": "ok"}

    def _scrape_one(self, driver, indicator: str,
                    url: str, unit: str) -> List[Dict]:
        from selenium.webdriver.common.by import By

        # Load page with long wait for JS
        driver.get(url)
        time.sleep(10)

        # Scroll down to trigger lazy loading
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)

        # Try to click any search/display button
        for btn_text in ["Afficher", "Rechercher", "Valider", "OK", "Search"]:
            try:
                btns = driver.find_elements(
                    By.XPATH,
                    f"//button[contains(text(),'{btn_text}')] | "
                    f"//input[@value='{btn_text}'] | "
                    f"//a[contains(text(),'{btn_text}')]"
                )
                if btns:
                    btns[0].click()
                    time.sleep(5)
                    break
            except Exception:
                pass

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        # Try XLS download link (direct requests, doesn't need JS)
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if not any(href.endswith(ext) for ext in (".xls", ".xlsx", ".csv")):
                continue
            full_url = (a["href"] if a["href"].startswith("http")
                        else self.BASE_URL + a["href"])
            try:
                resp = requests.get(
                    full_url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=60,
                )
                resp.raise_for_status()
                rows = (self._parse_csv(resp.content, indicator, unit)
                        if href.endswith(".csv")
                        else self._parse_xls(resp.content, indicator, unit))
                if rows:
                    log.info(f"[INS]   file download → {len(rows)} rows")
                    return rows
            except Exception as exc:
                log.warning(f"[INS]   file error: {exc}")

        # Parse HTML table
        rows_out = []
        is_const = indicator == "ipc_construction"
        for table in soup.find_all("table"):
            trs = table.find_all("tr")
            if len(trs) < 2:
                continue
            headers = [th.get_text(strip=True).lower()
                       for th in (trs[0].find_all(["th","td"]) if trs else [])]
            mat_col = next(
                (i for i, h in enumerate(headers)
                 if "matériau" in h or "material" in h),
                None,
            ) if is_const else None

            for tr in trs[1:]:
                cells = tr.find_all(["td", "th"])
                if len(cells) < 2:
                    continue
                d   = _date(cells[0].get_text(strip=True))
                val = _num(cells[1].get_text(strip=True))
                if d and val and val > 0:
                    rows_out.append(_row(d, indicator, val, unit, self.SOURCE))
                if mat_col and len(cells) > mat_col:
                    val_m = _num(cells[mat_col].get_text(strip=True))
                    if d and val_m and val_m > 0:
                        rows_out.append(
                            _row(d, "ipc_materiaux", val_m, unit, self.SOURCE)
                        )

        # Last resort: try INS API endpoint pattern
        if not rows_out:
            rows_out += self._try_ins_api(indicator, unit)

        return rows_out

    def _try_ins_api(self, indicator: str, unit: str) -> List[Dict]:
        """
        INS sometimes exposes a JSON API endpoint.
        Try common patterns.
        """
        API_PATTERNS = [
            f"https://www.ins.tn/api/statistiques/{indicator}",
            f"https://www.ins.tn/sites/default/files/statistiques/{indicator}.json",
        ]
        for api_url in API_PATTERNS:
            try:
                resp = requests.get(
                    api_url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=15,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    rows_out = []
                    if isinstance(data, list):
                        for item in data:
                            d   = _date(str(item.get("date", "")))
                            val = _num(str(item.get("value", "")))
                            if d and val and val > 0:
                                rows_out.append(
                                    _row(d, indicator, val, unit, self.SOURCE)
                                )
                    if rows_out:
                        return rows_out
            except Exception:
                pass
        return []

    def _parse_xls(self, content: bytes, indicator: str, unit: str) -> List[Dict]:
        try:
            import openpyxl
            wb       = openpyxl.load_workbook(
                io.BytesIO(content), read_only=True, data_only=True
            )
            rows_out = []
            for ws in wb.worksheets:
                for i, row in enumerate(ws.iter_rows(values_only=True)):
                    if i == 0 or not row or row[0] is None:
                        continue
                    d = _date(str(row[0]))
                    if not d and hasattr(row[0], "strftime"):
                        d = row[0].strftime("%Y-%m-01")
                    if not d:
                        continue
                    for cell in row[1:]:
                        if cell is None:
                            continue
                        val = _num(str(cell))
                        if val and val > 0:
                            rows_out.append(
                                _row(d, indicator, val, unit, self.SOURCE)
                            )
                            break
            return rows_out
        except Exception as exc:
            log.warning(f"[INS] XLS error: {exc}")
            return []

    def _parse_csv(self, content: bytes, indicator: str, unit: str) -> List[Dict]:
        import csv
        rows_out = []
        try:
            text   = content.decode("utf-8", errors="replace")
            reader = csv.reader(io.StringIO(text))
            for i, row in enumerate(reader):
                if i == 0 or not row:
                    continue
                d = _date(row[0])
                if not d:
                    continue
                for cell in row[1:]:
                    val = _num(cell)
                    if val and val > 0:
                        rows_out.append(
                            _row(d, indicator, val, unit, self.SOURCE)
                        )
                        break
        except Exception as exc:
            log.warning(f"[INS] CSV error: {exc}")
        return rows_out


# =============================================================================
# BVMT SCRAPER
# =============================================================================

class BVMTScraper:
    """
    BVMT — requests only (no blocking).
    ZIP files confirmed working for 2016–2025.
    Market URL confirmed: /fr/market-place
    """

    SOURCE  = "BVMT"
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Referer": "https://www.bvmt.com.tn/",
    }

    HISTO_ZIP_URL  = (
        "http://www.bvmt.com.tn/sites/default/files/"
        "historiques/indices/histo_indice_{year}.zip"
    )
    MARKET_URL     = "https://www.bvmt.com.tn/fr/market-place"
    HISTO_HTML_URL = "https://www.bvmt.com.tn/fr/historique-indices"

    RE_STOCKS = {
        "SAH":   "sah_price",
        "SITS":  "sits_price",
        "SPDIT": "spdit_price",
        "SIM":   "sim_price",
    }

    def run(self) -> Dict[str, Any]:
        log.info(f"[BVMT] Scraping {HISTORY_START}–{HISTORY_END}")
        rows: List[Dict] = []
        rows += self._scrape_tunindex()
        rows += self._scrape_re_stocks()

        if not rows:
            log.warning("[BVMT] 0 rows")
            return {"source": self.SOURCE, "rows": 0, "status": "empty"}

        conn     = _get_db()
        inserted = _upsert(conn, rows)
        conn.close()
        log.info(f"[BVMT] Saved {inserted}/{len(rows)} rows")
        return {"source": self.SOURCE, "rows": inserted, "status": "ok"}

    def _get(self, url: str, timeout: int = 30) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                r = requests.get(url, headers=self.HEADERS, timeout=timeout)
                r.raise_for_status()
                return r
            except Exception as exc:
                log.warning(f"[BVMT] attempt {attempt+1}: {exc}")
                time.sleep(random.uniform(2, 4))
        return None

    def _scrape_tunindex(self) -> List[Dict]:
        rows: List[Dict] = []
        seen: set         = set()

        # ZIP files — exist for 2016–2025
        log.info("[BVMT] ZIPs (2016–2025)")
        for year in range(2016, min(HISTORY_END + 1, 2026)):
            url  = self.HISTO_ZIP_URL.format(year=year)
            resp = self._get(url, timeout=30)
            if not resp:
                continue
            try:
                yr_rows = self._parse_zip(resp.content)
                new     = [r for r in yr_rows if r["date"] not in seen]
                rows   += new
                seen   |= {r["date"] for r in new}
                if new:
                    log.info(f"[BVMT]   ZIP {year}: {len(new)} rows")
            except Exception as exc:
                log.warning(f"[BVMT]   ZIP {year}: {exc}")
            time.sleep(random.uniform(0.5, 1.5))

        # HTML for current year
        log.info("[BVMT] HTML current year")
        resp = self._get(self.HISTO_HTML_URL)
        if resp:
            soup    = BeautifulSoup(resp.text, "html.parser")
            hr_rows = self._parse_tunindex_html(soup)
            new     = [r for r in hr_rows if r["date"] not in seen]
            rows   += new
            seen   |= {r["date"] for r in new}
            if new:
                log.info(f"[BVMT]   HTML: {len(new)} rows")

        return rows

    def _parse_zip(self, content: bytes) -> List[Dict]:
        rows_out = []
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            for name in zf.namelist():
                with zf.open(name) as f:
                    text = f.read().decode("latin-1", errors="replace")
                    for i, line in enumerate(text.splitlines()):
                        if i == 0:
                            continue
                        parts = re.split(r"[;\t|,]", line.strip())
                        if len(parts) < 2:
                            continue
                        d   = _date(parts[0].strip())
                        val = _num(parts[1].strip())
                        if d and val and val > 0:
                            rows_out.append(
                                _row(d, "tunindex", val, "points", self.SOURCE)
                            )
        return rows_out

    def _parse_tunindex_html(self, soup: BeautifulSoup) -> List[Dict]:
        rows_out = []
        for table in soup.find_all("table"):
            trs     = table.find_all("tr")
            headers = [
                th.get_text(strip=True).upper()
                for th in (trs[0].find_all(["th", "td"]) if trs else [])
            ]
            t_col = next(
                (i for i, h in enumerate(headers)
                 if "TUNINDEX" in h or "INDICE" in h), None
            )
            if t_col is None:
                continue
            for tr in trs[1:]:
                cells    = tr.find_all(["td", "th"])
                raw_date = cells[0].get_text(strip=True) if cells else ""
                m = re.match(r"(\d{2})/(\d{2})/(\d{4})", raw_date)
                d = (f"{m.group(3)}-{m.group(2)}-01"
                     if m else _date(raw_date))
                if not d:
                    continue
                if len(cells) > t_col:
                    val = _num(cells[t_col].get_text(strip=True))
                    if val:
                        rows_out.append(
                            _row(d, "tunindex", val, "points", self.SOURCE)
                        )
        return rows_out

    def _scrape_re_stocks(self) -> List[Dict]:
        log.info("[BVMT] RE stocks")
        resp = self._get(self.MARKET_URL)
        if not resp:
            return []
        soup     = BeautifulSoup(resp.text, "html.parser")
        today    = datetime.now().strftime("%Y-%m-01")
        rows_out = []
        for table in soup.find_all("table"):
            for tr in table.find_all("tr"):
                cells  = tr.find_all(["td", "th"])
                if not cells:
                    continue
                ticker = cells[0].get_text(strip=True).upper()
                if ticker not in self.RE_STOCKS:
                    continue
                for col in [1, 2, 3]:
                    if len(cells) <= col:
                        continue
                    val = _num(cells[col].get_text(strip=True))
                    if val and val > 0:
                        rows_out.append(
                            _row(today, self.RE_STOCKS[ticker],
                                 val, "TND", self.SOURCE)
                        )
                        break
        return rows_out


# =============================================================================
# RUNNER
# =============================================================================

def run_all_macro_scrapers() -> List[Dict[str, Any]]:
    results = []
    for cls in [BCTScraper, INSScraper, BVMTScraper]:
        try:
            result = cls().run()
            results.append(result)
            time.sleep(random.uniform(3, 6))
        except Exception as exc:
            log.error(f"[macro] {cls.__name__} crashed: {exc}")
            results.append({
                "source": cls.__name__, "rows": 0,
                "status": f"error: {exc}",
            })
    return results


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["bct", "ins", "bvmt", "all"],
                        default="all")
    parser.add_argument("--start", type=int, default=HISTORY_START)
    parser.add_argument("--end",   type=int, default=HISTORY_END)
    args = parser.parse_args()

    HISTORY_START = args.start  # type: ignore[assignment]
    HISTORY_END   = args.end    # type: ignore[assignment]

    cls_map = {"bct": BCTScraper, "ins": INSScraper, "bvmt": BVMTScraper}
    if args.source == "all":
        for r in run_all_macro_scrapers():
            print(r)
    else:
        print(cls_map[args.source]().run())


__all__ = [
    "BCTScraper", "INSScraper", "BVMTScraper", "run_all_macro_scrapers",
]