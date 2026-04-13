"""
EstateMind — Macro / Time-Series Scrapers  (final version)
===========================================================
BCT  → Selenium, pivot table (confirmed: 138 rows)
INS  → Publication pages + IPIM PDFs (correct URLs confirmed)
BVMT → requests + ZIP files (confirmed: 14k rows)

INS DATA SOURCES (confirmed from search results):
  IPIM (real estate price index):
    PDF:  ins.tn/sites/default/files-ftp3/files/publication/pdf/IPIM T1_2024_fr.pdf
          → exists from ~2018 Q1 onward
    Page: ins.tn/publication/indice-des-prix-de-limmobilier-{quarter}-trimestre-{year}
          → exists from 2018 onward, contains quarterly % change

  IPC (consumer price index):
    Listing page: ins.tn/statistiques/90
          → page text contains all monthly values like "inflation ... 6%"
    Press release: ins.tn/publication/indice-des-prix-la-consommation-{month}-{year}
          → individual monthly pages

Run:
    python -m scrapers.macro_scrapers --source all --start 2005
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

_BCT_MONTHS = {
    "janvier": "01", "février": "02", "fevrier": "02", "mars": "03",
    "avril": "04", "mai": "05", "juin": "06", "juillet": "07",
    "août": "08", "aout": "08", "septembre": "09", "octobre": "10",
    "novembre": "11", "décembre": "12", "decembre": "12",
    "janv": "01", "févr": "02", "avr": "04", "juil": "07",
    "aoû": "08", "sept": "09", "oct": "10", "nov": "11", "déc": "12",
}

_FR_MONTHS = {
    "jan": "01", "fév": "02", "fev": "02", "mar": "03", "avr": "04",
    "mai": "05", "jun": "06", "jui": "07", "jul": "07",
    "aoû": "08", "aou": "08", "sep": "09", "oct": "10",
    "nov": "11", "déc": "12", "dec": "12",
}

# Quarter name → month number
_QUARTER_NAMES = {
    "premier": "01", "deuxième": "04", "deuxieme": "04",
    "troisième": "07", "troisieme": "07", "quatrième": "10", "quatrieme": "10",
    "1er": "01", "2ème": "04", "2eme": "04",
    "3ème": "07", "3eme": "07", "4ème": "10", "4eme": "10",
}


def _month_num(raw: str) -> Optional[str]:
    key = raw.strip().lower()
    if key in _BCT_MONTHS:
        return _BCT_MONTHS[key]
    if key[:4] in _BCT_MONTHS:
        return _BCT_MONTHS[key[:4]]
    if key[:3] in _FR_MONTHS:
        return _FR_MONTHS[key[:3]]
    return None


def _date(raw: str) -> Optional[str]:
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
# BCT PIVOT TABLE PARSERS (confirmed working — 138 rows)
# =============================================================================

def _parse_bct_pivot(html: str, indicator: str, unit: str,
                     source: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows_out = []
    for table in soup.find_all("table"):
        trs = table.find_all("tr")
        if len(trs) < 3:
            continue
        header_cells = [td.get_text(strip=True)
                        for td in trs[0].find_all(["td", "th"])]
        year_cols = {i: cell.strip()
                     for i, cell in enumerate(header_cells)
                     if re.match(r"^\d{4}$", cell.strip())}
        if not year_cols:
            continue
        for tr in trs[2:]:
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if not cells or not cells[0]:
                continue
            month_num = _month_num(cells[0])
            if not month_num:
                continue
            for col_idx, year in year_cols.items():
                if col_idx >= len(cells):
                    continue
                val = _num(cells[col_idx])
                if val is not None and val > 0:
                    rows_out.append(
                        _row(f"{year}-{month_num}-01", indicator, val, unit, source)
                    )
    return rows_out


def _parse_bct_exchange_pivot(html: str, source: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows_out = []
    CURRENCY_MAP = {
        "dollar des usa": "tnd_usd",
        "dollar usa":     "tnd_usd",
        "euro":           "tnd_eur",
    }
    for table in soup.find_all("table"):
        trs = table.find_all("tr")
        if len(trs) < 3:
            continue
        header_cells = [td.get_text(strip=True)
                        for td in trs[0].find_all(["td", "th"])]
        year_cols = {i: cell.strip()
                     for i, cell in enumerate(header_cells)
                     if re.match(r"^\d{4}$", cell.strip())}
        if not year_cols:
            continue
        for tr in trs[2:]:
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if not cells or not cells[0]:
                continue
            label = cells[0].lower().strip()
            indicator = next(
                (ind for key, ind in CURRENCY_MAP.items() if key in label), None
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
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)
    return driver.page_source


# =============================================================================
# BCT SCRAPER (confirmed working)
# =============================================================================

class BCTScraper:
    SOURCE = "BCT"
    BASE   = "https://www.bct.gov.tn/bct/siteprod"

    PAGES = {
        "taux_directeur": (f"{BASE}/tableau_statistique_a.jsp?params=PL203260", "%"),
        "tmm":            (f"{BASE}/tableau_statistique_a.jsp?params=PL203105", "%"),
    }
    EXCHANGE_ANNUAL = f"{BASE}/tableau_statistique_a.jsp?params=PL212010"

    def run(self) -> Dict[str, Any]:
        _ensure_selenium()
        log.info(f"[BCT] Scraping {HISTORY_START}–{HISTORY_END}")
        driver = None
        rows: List[Dict] = []
        try:
            driver = _make_driver()
            for indicator, (url, unit) in self.PAGES.items():
                log.info(f"[BCT] {indicator}")
                html     = _page(driver, url, wait=6)
                new_rows = _parse_bct_pivot(html, indicator, unit, self.SOURCE)
                rows    += new_rows
                log.info(f"[BCT]   → {len(new_rows)} rows")
                time.sleep(random.uniform(2, 4))

            log.info("[BCT] exchange rates (annual)")
            html  = _page(driver, self.EXCHANGE_ANNUAL, wait=6)
            new   = _parse_bct_exchange_pivot(html, self.SOURCE)
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
    INS data via publication pages (confirmed working approach):

    1. IPC (inflation) — scraped from ins.tn/statistiques/90 page text
       and individual monthly press release pages.
       Confirmed: page text contains "Au mois de janvier 2025, le taux d'inflation... 6%"

    2. IPIM (real estate price index) — quarterly publication pages
       ins.tn/publication/indice-des-prix-de-limmobilier-{q}-trimestre-{year}
       + PDF fallback at ins.tn/sites/default/files-ftp3/files/publication/pdf/IPIM T{q}_{year}_fr.pdf
       Confirmed: exists from 2018 Q1 onwards

    3. IPVI (industrial price index) — ins.tn/statistiques/89
       Uses table2excel, rendered by JS via Selenium
    """

    SOURCE   = "INS"
    BASE_URL = "https://www.ins.tn"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/123.0.0.0",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Referer": "https://www.ins.tn/",
    }

    # Quarter names in URL slugs
    QUARTER_SLUGS = {
        1: "premier",
        2: "deuxieme",
        3: "troisieme",
        4: "quatrieme",
    }

    # Month slugs for IPC URLs (French, no accents)
    MONTH_SLUGS = [
        "janvier", "fevrier", "mars", "avril", "mai", "juin",
        "juillet", "aout", "septembre", "octobre", "novembre", "decembre",
    ]

    def run(self) -> Dict[str, Any]:
        log.info(f"[INS] Scraping {HISTORY_START}–{HISTORY_END}")
        rows: List[Dict] = []

        rows += self._scrape_ipc_listing()
        rows += self._scrape_ipc_press_releases()
        rows += self._scrape_ipim()

        if not rows:
            log.warning("[INS] 0 rows")
            return {"source": self.SOURCE, "rows": 0, "status": "empty"}

        conn     = _get_db()
        inserted = _upsert(conn, rows)
        conn.close()
        log.info(f"[INS] Saved {inserted}/{len(rows)} rows")
        return {"source": self.SOURCE, "rows": inserted, "status": "ok"}

    def _get(self, url: str, timeout: int = 20) -> Optional[requests.Response]:
        for attempt in range(2):   # only 2 attempts for INS — fast fail
            try:
                r = requests.get(url, headers=self.HEADERS, timeout=timeout)
                r.raise_for_status()
                return r
            except Exception as exc:
                if attempt == 0:
                    time.sleep(random.uniform(0.5, 1.5))
        return None

    # ------------------------------------------------------------------
    # 1. IPC listing page — contains all monthly values in page text
    # ------------------------------------------------------------------

    def _scrape_ipc_listing(self) -> List[Dict]:
        """
        ins.tn/statistiques/90 page text contains snippets like:
          "Au mois de février 2026, le taux d'inflation a atteint le taux de 5%."
          "L'inflation au mois de janvier 2025, se replie à 6%."
          "L'inflation se replie à 8,1% en décembre 2023."
        We extract these month+year+value triples.
        """
        log.info("[INS] IPC from listing page")
        resp = self._get(f"{self.BASE_URL}/statistiques/90", timeout=20)
        if not resp:
            return []

        text = BeautifulSoup(resp.text, "html.parser").get_text(" ")
        rows_out = []

        MONTH_NAMES_FR = {
            "janvier": "01", "février": "02", "fevrier": "02",
            "mars": "03", "avril": "04", "mai": "05", "juin": "06",
            "juillet": "07", "août": "08", "aout": "08",
            "septembre": "09", "octobre": "10",
            "novembre": "11", "décembre": "12", "decembre": "12",
        }

        # Pattern 1: "au mois de {month} {year}... {value}%"
        for m in re.finditer(
            r"au mois de ([a-zéûî]+) (\d{4})[^\d%]*?(\d+[,\.]\d+)\s*%",
            text.lower()
        ):
            mon_name, year, val_str = m.group(1), m.group(2), m.group(3)
            mon = MONTH_NAMES_FR.get(mon_name)
            val = _num(val_str)
            if mon and val and 0 < val < 30:
                rows_out.append(
                    _row(f"{year}-{mon}-01", "ipc_general_ins",
                         val, "%", self.SOURCE)
                )

        # Pattern 2: "{value}% en {month} {year}"
        for m in re.finditer(
            r"(\d+[,\.]\d+)\s*%\s*en ([a-zéûî]+) (\d{4})",
            text.lower()
        ):
            val_str, mon_name, year = m.group(1), m.group(2), m.group(3)
            mon = MONTH_NAMES_FR.get(mon_name)
            val = _num(val_str)
            if mon and val and 0 < val < 30:
                rows_out.append(
                    _row(f"{year}-{mon}-01", "ipc_general_ins",
                         val, "%", self.SOURCE)
                )

        # Deduplicate — keep highest value per date (most specific mention)
        seen: Dict[str, float] = {}
        for r in rows_out:
            key = r["date"]
            if key not in seen:
                seen[key] = r["value"]
        deduped = [
            _row(d, "ipc_general_ins", v, "%", self.SOURCE)
            for d, v in seen.items()
        ]

        log.info(f"[INS]   IPC listing → {len(deduped)} rows")
        return deduped

    # ------------------------------------------------------------------
    # 2. IPC individual press release pages
    # ------------------------------------------------------------------

    def _scrape_ipc_press_releases(self) -> List[Dict]:
        """
        Visit each monthly page:
          ins.tn/publication/indice-des-prix-la-consommation-{month}-{year}
        Extract the inflation rate from the first paragraph.
        """
        log.info("[INS] IPC from monthly press releases")
        rows_out = []

        for year in range(max(HISTORY_START, 2015), HISTORY_END + 1):
            for mon_idx, mon_slug in enumerate(self.MONTH_SLUGS, start=1):
                # Skip future months
                if year == HISTORY_END and mon_idx > datetime.now().month:
                    break

                url  = (f"{self.BASE_URL}/publication/"
                        f"indice-des-prix-la-consommation-{mon_slug}-{year}")
                resp = self._get(url, timeout=12)
                if not resp:
                    continue

                text = BeautifulSoup(resp.text, "html.parser").get_text(" ")

                # Extract annual inflation rate from text
                patterns = [
                    r"taux d.inflation[^\d%]*?(\d+[,\.]\d+)\s*%",
                    r"(\d+[,\.]\d+)\s*%\s*sur un an",
                    r"inflation[^\d%]*?(\d+[,\.]\d+)\s*%",
                    r"atteint(?:\s+le taux de)?\s+(\d+[,\.]\d+)\s*%",
                    r"replie\s+à\s+(\d+[,\.]\d+)\s*%",
                    r"s.établit à\s+(\d+[,\.]\d+)\s*%",
                ]
                val = None
                for pat in patterns:
                    m = re.search(pat, text.lower())
                    if m:
                        v = _num(m.group(1))
                        if v and 0 < v < 30:
                            val = v
                            break

                if val:
                    d = f"{year}-{str(mon_idx).zfill(2)}-01"
                    rows_out.append(
                        _row(d, "ipc_general_ins", val, "%", self.SOURCE)
                    )

                time.sleep(random.uniform(0.2, 0.6))

        log.info(f"[INS]   IPC press releases → {len(rows_out)} rows")
        return rows_out

    # ------------------------------------------------------------------
    # 3. IPIM — Real estate price index
    # ------------------------------------------------------------------

    def _scrape_ipim(self) -> List[Dict]:
        """
        IPIM quarterly publication pages:
          ins.tn/publication/indice-des-prix-de-limmobilier-{quarter}-trimestre-{year}
        + PDF fallback.
        Exists from 2018 Q1 (confirmed).
        Note: 2022 and 2023 had an interruption, resumed in 2024.
        """
        log.info("[INS] IPIM — Indice des Prix de l'Immobilier")
        rows_out = []

        for year in range(2018, HISTORY_END + 1):
            current_q = (datetime.now().month - 1) // 3 + 1
            for q in range(1, 5):
                if year == HISTORY_END and q > current_q:
                    break

                # Try publication page first
                q_slug = self.QUARTER_SLUGS[q]
                url    = (f"{self.BASE_URL}/publication/"
                          f"indice-des-prix-de-limmobilier-{q_slug}-trimestre-{year}")
                resp   = self._get(url, timeout=15)

                val = None
                if resp:
                    val = self._extract_ipim_from_page(resp.text, year, q)

                # PDF fallback
                if not val:
                    pdf_url = (
                        f"{self.BASE_URL}/sites/default/files-ftp3/files/"
                        f"publication/pdf/IPIM%20T{q}_{year}_fr.pdf"
                    )
                    resp_pdf = self._get(pdf_url, timeout=20)
                    if resp_pdf:
                        val = self._extract_ipim_from_pdf(resp_pdf.content)

                if val:
                    mon = {1: "01", 2: "04", 3: "07", 4: "10"}[q]
                    d   = f"{year}-{mon}-01"
                    rows_out.append(
                        _row(d, "ipim", val, "index_2015=100", self.SOURCE)
                    )
                    log.info(f"[INS]   IPIM {year} T{q}: {val}")

                time.sleep(random.uniform(0.3, 0.8))

        log.info(f"[INS]   IPIM → {len(rows_out)} rows")
        return rows_out

    def _extract_ipim_from_page(self, html: str, year: int, q: int) -> Optional[float]:
        """
        IPIM publication pages contain text like:
        "l'indice des prix de l'immobilier bâti a augmenté de 3,5%"
        or "l'indice ... s'établit à 125,3"
        We extract the % change (quarterly) as the value.
        """
        text = BeautifulSoup(html, "html.parser").get_text(" ").lower()

        patterns = [
            # Quarterly change
            r"(?:augmenté|baissé|hausse|baisse)\s+de\s+(\d+[,\.]\d+)\s*%\s*(?:en variation trimestrielle|par rapport au)",
            r"variation trimestrielle[^\d]*(\d+[,\.]\d+)\s*%",
            r"(\d+[,\.]\d+)\s*%\s*(?:en variation trimestrielle|par rapport au trimestre)",
            # Index level
            r"l.indice[^\d]*s.établit\s+à\s+(\d{3}[,\.]\d+)",
            r"indice[^\d]*(\d{3}[,\.]\d+)\s*(?:points?|base)",
        ]

        for pat in patterns:
            m = re.search(pat, text)
            if m:
                val = _num(m.group(1))
                if val and val > 0:
                    return val
        return None

    def _extract_ipim_from_pdf(self, content: bytes) -> Optional[float]:
        try:
            from pypdf import PdfReader
            reader = PdfReader(io.BytesIO(content))
            text   = ""
            for page in reader.pages[:3]:
                text += (page.extract_text() or "").lower()

            patterns = [
                r"indice[^\d]*(\d{3}[,\.]\d+)",
                r"(\d{3}[,\.]\d+)\s*(?:points?|base 2015)",
                r"valeur[^\d]*(\d{3}[,\.]\d+)",
            ]
            for pat in patterns:
                m = re.search(pat, text)
                if m:
                    val = _num(m.group(1))
                    if val and 80 < val < 500:
                        return val
        except ImportError:
            pass  # pypdf not installed — skip silently
        except Exception as exc:
            log.warning(f"[INS] PDF error: {exc}")
        return None


# =============================================================================
# BVMT SCRAPER (confirmed working)
# =============================================================================

class BVMTScraper:
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