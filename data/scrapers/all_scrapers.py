"""
EstateMind — All 10 Tunisian real estate scrapers.

Every URL has been corrected based on the live run log failures.
Each scraper follows the same pattern:
  fetch_listings() → yields PropertyListing objects
  run() → alias to fetch_listings (called by BaseScraper)
"""
from __future__ import annotations

import re
import json
import time
import random
import hashlib
from datetime import datetime
from typing import Generator, List, Optional, Dict, Any, Iterator
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from core.base_scraper import BaseScraper, parse_tunisian_price, parse_surface, parse_rooms
from core.base_scraper import infer_property_type, infer_transaction_type, make_source_id
from core.base_scraper import infer_governorate
from core.models import PropertyListing, Location
from config.logging_config import log
from selenium.webdriver.support.ui import WebDriverWait

# ─── Selenium helpers (lazy import — only when needed) ────────────────────────

def _make_selenium_driver(headless: bool = True):
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    import shutil

    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    )
    options.add_argument("--log-level=3")

    # On Railway/Linux use system chromium, on Windows use webdriver-manager
    chrome_path = shutil.which("chromium") or shutil.which("chromium-browser")
    chromedriver_path = shutil.which("chromedriver")

    if chrome_path:
        options.binary_location = chrome_path
        service = Service(chromedriver_path)
    else:
        from webdriver_manager.chrome import ChromeDriverManager
        from webdriver_manager.core.driver_cache import DriverCacheManager
        cache_manager = DriverCacheManager(root_dir="./temp_drivers")
        service = Service(ChromeDriverManager(cache_manager=cache_manager).install())

    return webdriver.Chrome(service=service, options=options)


def _is_selenium_available() -> bool:
    import shutil
    return any([
        shutil.which("chromium"),
        shutil.which("chromium-browser"),
        shutil.which("google-chrome"),
    ])

def _selenium_get_html(driver, url: str, wait_selector: str = "body",
                        timeout: int = 15) -> Optional[str]:
    """Navigate to URL and return page source. Returns None on failure."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    try:
        driver.get(url)
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, wait_selector))
        )
        time.sleep(random.uniform(1.5, 3.0))
        return driver.page_source
    except Exception as e:
        log.warning(f"Selenium get {url}: {e}")
        return None


# ─── Shared Houzez detail parser ─────────────────────────────────────────────

def _parse_houzez_detail(html: str, url: str, source_name: str,
                          transaction_type: str) -> Optional[PropertyListing]:
    """
    Most Tunisian RE sites use the Houzez WordPress theme.
    This shared parser handles all of them.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Title
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else ""
    if not title:
        og = soup.find("meta", property="og:title")
        title = og.get("content", "") if og else url

    # Description
    desc_el = (soup.find("div", id="property-description-wrap") or
               soup.find("div", class_="property-description-wrap") or
               soup.find(class_="detail-description"))
    description = desc_el.get_text(" ", strip=True) if desc_el else None
    if not description:
        og = soup.find("meta", property="og:description")
        description = og.get("content") if og else None

    # Price — try structured first
    price = None
    price_el = (soup.find(class_="item-price") or
                soup.find(class_="price") or
                soup.find(class_="houzez-price"))
    if price_el:
        price = parse_tunisian_price(price_el.get_text(strip=True))
    if not price:
        og = soup.find("meta", property="og:description")
        if og:
            price = parse_tunisian_price(og.get("content", ""))

    # Surface & rooms from Houzez property-detail list
    surface = None
    rooms = None
    bathrooms = None
    for ul in soup.find_all("ul", class_=re.compile(r"list-2-cols|houzez-properties-list")):
        for li in ul.find_all("li"):
            label_el = li.find("strong") or li.find("span", class_="label")
            label = label_el.get_text(strip=True).lower() if label_el else ""
            spans = li.find_all("span")
            value = spans[-1].get_text(strip=True) if spans else li.get_text(strip=True)

            if any(k in label for k in ["surface", "superficie"]):
                surface = parse_surface(value) or parse_surface(li.get_text())
            elif any(k in label for k in ["chambre", "pièce", "typ"]):
                rooms = parse_rooms(value) or parse_rooms(li.get_text())
            elif any(k in label for k in ["salle", "bain"]):
                m = re.search(r"(\d+)", value)
                if m:
                    try:
                        bathrooms = int(m.group(1))
                    except Exception:
                        pass

    # Coordinates — houzez_single_property_map script (most reliable)
    lat, lon = None, None
    for script in soup.find_all("script"):
        txt = script.string or ""
        if "houzez_single_property_map" in txt:
            ml = re.search(r'"lat"\s*:\s*"(-?\d+\.\d+)"', txt)
            mn = re.search(r'"lng"\s*:\s*"(-?\d+\.\d+)"', txt)
            if ml and mn:
                try:
                    lat = float(ml.group(1))
                    lon = float(mn.group(1))
                except Exception:
                    pass
            break

    # Location from Houzez breadcrumb / property address
    city, governorate, district, address_str = None, None, None, None
    addr_el = soup.find(class_=re.compile(r"houzez-single-address|property-address"))
    if addr_el:
        address_str = addr_el.get_text(" ", strip=True)
        parts = [p.strip() for p in address_str.split(",")]
        city = parts[0] if parts else None
        governorate = infer_governorate(city or address_str)

    if not city:
        # Try title "à City"
        m = re.search(r" [àa]\s+([^\|\[]+)", title, re.IGNORECASE)
        if m:
            city = m.group(1).strip()
            governorate = infer_governorate(city)

    # Images
    images: List[str] = []
    seen_img: set = set()
    for img in soup.find_all("img"):
        src = (img.get("data-src") or img.get("data-lazy") or img.get("src") or "")
        if not src or "logo" in src.lower() or "icon" in src.lower():
            continue
        if not any(ext in src.lower() for ext in [".jpg", ".jpeg", ".png", ".webp"]):
            continue
        full = src if src.startswith("http") else urljoin(url, src)
        if full not in seen_img:
            images.append(full)
            seen_img.add(full)

    source_id = make_source_id(url, source_name)
    location = Location(
        governorate=governorate,
        city=city,
        district=district,
        address=address_str,
        latitude=lat,
        longitude=lon,
    )

    return PropertyListing(
        source_id=source_id,
        source_name=source_name,
        url=url,
        title=title,
        description=description,
        price=price,
        currency="TND",
        property_type=infer_property_type(title, description or ""),
        transaction_type=transaction_type,
        location=location,
        surface_area_m2=surface,
        rooms=rooms,
        bathrooms=bathrooms,
        images=images,
        scraped_at=datetime.utcnow(),
        raw_content=html,
    )


# ─── Houzez-based list scraper (shared by zitouna, verdar, darcom, newkey) ───

class _HouzezListScraper(BaseScraper):
    """
    Generic scraper for Houzez-based sites.
    Subclasses just set source_name, base_url, list_urls.
    """

    list_urls: List[Dict] = []      # [{"url": ..., "type": "Sale"/"Rent"}]
    link_pattern: str = "/bien/details/"
    max_pages: int = 50
    min_delay: float = 2.0
    max_delay: float = 4.0
    needs_selenium: bool = False

    def __init__(self):
        super().__init__(self.source_name, self.base_url)
        self._driver = None

    def _get_driver(self):
        if self._driver is None:
            self._driver = _make_selenium_driver()
        return self._driver

    def _get_html(self, url: str) -> Optional[str]:
        if self.needs_selenium:
            return _selenium_get_html(self._get_driver(), url)
        resp = self._get_request(url)
        return resp.text if resp else None

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        try:
            for cat in self.list_urls:
                cat_url = cat["url"]
                trans_type = cat["type"]
                seen_urls: set = set()   # track across ALL pages for this category
                prev_page_urls: set = set()  # detect pagination loops

                for page in range(1, self.max_pages + 1):
                    page_url = f"{cat_url}?page={page}" if page > 1 else cat_url
                    log.info(f"[{self.source_name}] {trans_type} p{page}: {page_url}")

                    html = self._get_html(page_url)
                    if not html:
                        break

                    soup = BeautifulSoup(html, "html.parser")
                    links = soup.find_all("a", href=lambda h: h and self.link_pattern in h)
                    page_urls = set()
                    for a in links:
                        href = a["href"]
                        full = href if href.startswith("http") else urljoin(self.base_url, href)
                        page_urls.add(full)

                    if not page_urls:
                        log.info(f"[{self.source_name}] No listings on page {page}, stopping")
                        break

                    # Detect pagination loop — same URLs as previous page means no more pages
                    if page_urls == prev_page_urls:
                        log.info(f"[{self.source_name}] Pagination ended at page {page} (same URLs)")
                        break
                    prev_page_urls = page_urls

                    # Only scrape URLs we haven't seen before
                    new_urls = page_urls - seen_urls
                    seen_urls.update(page_urls)

                    if not new_urls:
                        log.info(f"[{self.source_name}] No new listings on page {page}, stopping")
                        break

                    log.info(f"[{self.source_name}] Found {len(new_urls)} new links on page {page}")

                    for detail_url in new_urls:
                        try:
                            detail_html = self._get_html(detail_url)
                            if detail_html:
                                listing = _parse_houzez_detail(
                                    detail_html, detail_url, self.source_name, trans_type
                                )
                                if listing:
                                    yield listing
                        except Exception as e:
                            log.error(f"[{self.source_name}] Detail error {detail_url}: {e}")

                    self._random_delay(self.min_delay, self.max_delay)
        finally:
            if self._driver:
                try:
                    self._driver.quit()
                except Exception:
                    pass
                self._driver = None


# ─── Individual scrapers ──────────────────────────────────────────────────────

class ZitounaImmoScraper(_HouzezListScraper):
    source_name = "zitouna_immo"
    base_url = "https://www.zitounaimmo.com"
    list_urls = [
        {"url": "https://www.zitounaimmo.com/acheter", "type": "Sale"},
        {"url": "https://www.zitounaimmo.com/louer", "type": "Rent"},
    ]
    needs_selenium = False
    min_delay = 4.0
    max_delay = 8.0


class VerdarScraper(_HouzezListScraper):
    source_name = "verdar"
    base_url = "https://www.verdar.tn"
    list_urls = [
        {"url": "https://www.verdar.tn/acheter", "type": "Sale"},  # FIXED: was /ads?type=vente
        {"url": "https://www.verdar.tn/louer", "type": "Rent"},
    ]
    needs_selenium = False
    min_delay = 2.0
    max_delay = 5.0


class DarcomScraper(_HouzezListScraper):
    source_name = "darcom"
    base_url = "https://www.darcomtunisia.com"
    list_urls = [
        {"url": "https://www.darcomtunisia.com/vente", "type": "Sale"},
        {"url": "https://www.darcomtunisia.com/location", "type": "Rent"},
    ]
    needs_selenium = True
    min_delay = 2.0
    max_delay = 5.0

    def fetch_listings(self):
        if not _is_selenium_available():
            log.warning(f"[{self.source_name}] Selenium/Chrome not available — skipping")
            return
        yield from super().fetch_listings()


class NewKeyScraper(_HouzezListScraper):
    source_name = "newkey"
    base_url = "https://www.newkey.com.tn"
    list_urls = [
        {"url": "https://www.newkey.com.tn/acheter", "type": "Sale"},
        {"url": "https://www.newkey.com.tn/louer", "type": "Rent"},
    ]
    needs_selenium = True
    min_delay = 2.0
    max_delay = 4.0

    def fetch_listings(self):
        if not _is_selenium_available():
            log.warning(f"[{self.source_name}] Selenium/Chrome not available — skipping")
            return
        yield from super().fetch_listings()


class TecnocasaScraper(_HouzezListScraper):
    source_name = "tecnocasa"
    base_url = "https://www.tecnocasa.tn"
    list_urls = [
        # FIXED: was /vendita-immobili/ (Italian) — 404. French paths below.
        {"url": "https://www.tecnocasa.tn/vente-immobilier-tunisie/", "type": "Sale"},
        {"url": "https://www.tecnocasa.tn/location-immobilier-tunisie/", "type": "Rent"},
    ]
    link_pattern = "/property/"
    needs_selenium = True
    min_delay = 3.0
    max_delay = 6.0

    def _get_html(self, url: str) -> Optional[str]:
        # WordPress pagination style: /page/N/
        return _selenium_get_html(self._get_driver(), url)

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        if not _is_selenium_available():
            log.warning(f"[{self.source_name}] Selenium/Chrome not available — skipping")
            return
        try:
            for cat in self.list_urls:
                cat_url = cat["url"]
                trans_type = cat["type"]
                seen_urls: set = set()
                prev_page_urls: set = set()
                for page in range(1, self.max_pages + 1):
                    page_url = cat_url if page == 1 else cat_url.rstrip("/") + f"/page/{page}/"
                    log.info(f"[{self.source_name}] {trans_type} p{page}: {page_url}")
                    html = self._get_html(page_url)
                    if not html:
                        break
                    soup = BeautifulSoup(html, "html.parser")
                    links = soup.find_all("a", href=lambda h: h and "/property/" in h)
                    page_urls = {
                        a["href"] if a["href"].startswith("http")
                        else urljoin(self.base_url, a["href"])
                        for a in links
                        if "page" not in a["href"] and "status" not in a["href"]
                    }
                    if not page_urls or page_urls == prev_page_urls:
                        break
                    prev_page_urls = page_urls
                    new_urls = page_urls - seen_urls
                    seen_urls.update(page_urls)
                    if not new_urls:
                        break
                    for detail_url in new_urls:
                        html2 = self._get_html(detail_url)
                        if html2:
                            listing = _parse_houzez_detail(
                                html2, detail_url, self.source_name, trans_type
                            )
                            if listing:
                                yield listing
                    self._random_delay(self.min_delay, self.max_delay)
        finally:
            if self._driver:
                try:
                    self._driver.quit()
                except Exception:
                    pass
                self._driver = None


class Century21Scraper(BaseScraper):
    """
    Century21 TN — WordPress/Houzez with CAPTCHA.
    Uses Selenium with longer delays to avoid detection.
    """
    source_name = "century21"
    base_url = "https://century21.tn"

    CATEGORIES = [
        {"url": "https://century21.tn/status/vente-immobilier-tunisie/", "type": "Sale"},
        {"url": "https://century21.tn/status/location-immobilier-tunisie/", "type": "Rent"},
    ]

    def __init__(self):
        super().__init__(self.source_name, self.base_url)
        self._driver = None

    def _get_driver(self):
        if self._driver is None:
            self._driver = _make_selenium_driver()
        return self._driver

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        """
        Scrape Century21.tn using plain headless Selenium.
        Skips gracefully if Chrome not available (Railway without Chrome).
        Breaks out after 3 consecutive CAPTCHAs.
        """
        if not _is_selenium_available():
            log.warning(f"[{self.source_name}] Selenium/Chrome not available — skipping")
            return
        driver = self._get_driver()
        _captcha_hits = 0

        try:
            for category in self.CATEGORIES:
                cat_url    = category["url"]
                trans_type = category["type"]
                _captcha_hits = 0  # reset per category

                for page in range(1, 51):
                    page_url = cat_url if page == 1 else f"{cat_url}page/{page}/"
                    log.info(f"[{self.source_name}] {trans_type} p{page}: {page_url}")

                    driver.get(page_url)

                    # CAPTCHA detection — break after 3 consecutive hits
                    if "captcha" in driver.page_source.lower():
                        _captcha_hits += 1
                        log.warning(
                            f"[{self.source_name}] CAPTCHA {_captcha_hits}/3 on {page_url}"
                        )
                        if _captcha_hits >= 3:
                            log.error(
                                f"[{self.source_name}] IP blocked — skipping category"
                            )
                            break
                        time.sleep(120)
                        continue

                    _captcha_hits = 0  # reset on clean page

                    # Wait for property links
                    try:
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located(
                                (By.CSS_SELECTOR, "a[href*='/property/']")
                            )
                        )
                    except Exception:
                        log.warning(
                            f"[{self.source_name}] No listings or timeout on {page_url}"
                        )
                        break

                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    unique_urls = set()
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        if "/property/" in href and "status" not in href and "page" not in href:
                            if href.startswith("http"):
                                unique_urls.add(href)

                    if not unique_urls:
                        log.info(
                            f"[{self.source_name}] No listings on p{page}, stopping {trans_type}"
                        )
                        break

                    log.info(
                        f"[{self.source_name}] Found {len(unique_urls)} listings on p{page}"
                    )

                    for detail_url in unique_urls:
                        try:
                            html = _selenium_get_html(driver, detail_url, "h1", timeout=20)
                            if html:
                                listing = _parse_houzez_detail(
                                    html, detail_url, self.source_name, trans_type
                                )
                                if listing:
                                    yield listing
                        except Exception as e:
                            log.error(f"[{self.source_name}] Detail error: {e}")
                        time.sleep(random.uniform(2, 4))

        finally:
            if self._driver:
                try:
                    self._driver.quit()
                except Exception:
                    pass
                self._driver = None

class AqariScraper(BaseScraper):
    """
    Aqari.tn — React SPA. Selenium required.
    Correct list URLs: /vente and /location (NOT /properties?status=...)
    """
    source_name = "aqari"
    base_url = "https://www.aqari.tn"

    def __init__(self):
        super().__init__(self.source_name, self.base_url)
        self._driver = None

    def _get_driver(self):
        if self._driver is None:
            self._driver = _make_selenium_driver()
        return self._driver

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        if not _is_selenium_available():
            log.warning(f"[{self.source_name}] Selenium/Chrome not available — skipping")
            return
        try:
            driver = self._get_driver()
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC

            for cat_path, trans_type in [("/vente", "Sale"), ("/location", "Rent")]:
                url = self.base_url + cat_path
                log.info(f"[{self.source_name}] Loading {trans_type}: {url}")
                driver.get(url)
                time.sleep(5)

                page = 1
                max_pages = 20
                while page <= max_pages:
                    log.info(f"[{self.source_name}] Page {page}")
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    # Aqari uses /property/ links
                    links = soup.find_all("a", href=lambda h: h and "/property/" in h)
                    urls = set()
                    for a in links:
                        href = a["href"]
                        full = href if href.startswith("http") else urljoin(self.base_url, href)
                        urls.add(full)

                    if not urls:
                        log.info(f"[{self.source_name}] No listings on page {page}")
                        break

                    for detail_url in urls:
                        try:
                            html = _selenium_get_html(driver, detail_url, "h1")
                            if html:
                                listing = self._parse_detail(html, detail_url, trans_type)
                                if listing:
                                    yield listing
                        except Exception as e:
                            log.error(f"[{self.source_name}] Detail error: {e}")
                        time.sleep(random.uniform(2, 4))

                    # Pagination — click Next button
                    try:
                        next_btn = driver.find_element(
                            By.XPATH,
                            "//button[contains(text(),'Suivant') or contains(text(),'Next')] | "
                            "//a[contains(text(),'Suivant') or contains(text(),'Next')]"
                        )
                        if next_btn.is_enabled():
                            next_btn.click()
                            time.sleep(4)
                            page += 1
                        else:
                            break
                    except Exception:
                        break
        finally:
            if self._driver:
                try:
                    self._driver.quit()
                except Exception:
                    pass
                self._driver = None

    def _parse_detail(self, html: str, url: str, trans_type: str) -> Optional[PropertyListing]:
        soup = BeautifulSoup(html, "html.parser")
        title = ""
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)

        # Try JSON-LD structured data
        price, surface, rooms = None, None, None
        description = None
        city, governorate = None, None
        images: List[str] = []

        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.get_text())
                if data.get("@type") in ["RealEstateListing", "Product", "Apartment",
                                          "House", "SingleFamilyResidence"]:
                    if not title and data.get("name"):
                        title = data["name"]
                    if data.get("description"):
                        description = data["description"]
                    offers = data.get("offers", {})
                    if offers.get("price"):
                        try: price = float(offers["price"])
                        except Exception: pass
                    if data.get("floorSize", {}).get("value"):
                        try: surface = float(data["floorSize"]["value"])
                        except Exception: pass
                    if data.get("numberOfRooms"):
                        try: rooms = int(data["numberOfRooms"])
                        except Exception: pass
                    addr = data.get("address", {})
                    city = addr.get("addressLocality")
                    governorate = addr.get("addressRegion") or infer_governorate(city or "")
                    if isinstance(data.get("image"), list):
                        images.extend(data["image"])
                    elif isinstance(data.get("image"), str):
                        images.append(data["image"])
                    break
            except Exception:
                continue

        # Fallback
        if not price:
            og = soup.find("meta", property="og:description")
            if og:
                price = parse_tunisian_price(og.get("content", ""))
        if not surface:
            full_text = soup.get_text(" ", strip=True)
            surface = parse_surface(full_text)
        if not rooms:
            rooms = parse_rooms(soup.get_text(" ", strip=True))

        source_id = make_source_id(url, self.source_name)
        location = Location(
            governorate=governorate or infer_governorate(city or ""),
            city=city,
        )
        return PropertyListing(
            source_id=source_id,
            source_name=self.source_name,
            url=url,
            title=title or url,
            description=description,
            price=price,
            currency="TND",
            property_type=infer_property_type(title, description or ""),
            transaction_type=trans_type,
            location=location,
            surface_area_m2=surface,
            rooms=rooms,
            images=list(dict.fromkeys(images)),
            scraped_at=datetime.utcnow(),
        )

    # def fetch_listings(self):
    #     return self._fetch()

    # def _fetch(self):
    #     yield from self.fetch_listings()  # handled above


class MubawabScraper(BaseScraper):
    """
    Mubawab — Correct URL is mubawab.tn (NOT .com.tn which doesn't resolve).
    requests + BS4, no Selenium needed.
    """
    source_name = "mubawab"
    base_url = "https://www.mubawab.tn"

    CATEGORIES = [
        ("appartements-a-vendre",         "Sale", "Apartment"),
        ("maisons-et-villas-a-vendre",    "Sale", "Villa"),
        ("terrains-et-fermes-a-vendre",   "Sale", "Land"),
        ("bureaux-et-commerces-a-vendre", "Sale", "Commercial"),
        ("appartements-a-louer",          "Rent", "Apartment"),
        ("maisons-et-villas-a-louer",     "Rent", "Villa"),
        ("bureaux-et-commerces-a-louer",  "Rent", "Commercial"),
    ]

    def __init__(self):
        super().__init__(self.source_name, self.base_url)

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        for path, trans_type, prop_type in self.CATEGORIES:
            seen_urls: set = set()   # reset per category
            prev_items_count = -1
            for page in range(1, 21):
                url = f"{self.base_url}/fr/{path}"
                if page > 1:
                    url += f"?page={page}"
                log.info(f"[{self.source_name}] {trans_type}/{prop_type} p{page}")
                resp = self._get_request(url)
                if not resp:
                    break
                soup = BeautifulSoup(resp.text, "html.parser")
                items = self._get_list_items(soup)
                if not items:
                    break
                # Detect pagination loop — same number of items as before with no new URLs
                if len(items) == prev_items_count:
                    log.info(f"[{self.source_name}] Pagination ended at p{page}")
                    break
                prev_items_count = len(items)
                new_found = 0
                for item in items:
                    link = (item.select_one("h2.listingTit a") or
                            item.select_one("h2 a") or
                            item.select_one("a[href*='/fr/']"))
                    if not link:
                        continue
                    href = link.get("href", "")
                    detail_url = href if href.startswith("http") else urljoin(self.base_url, href)
                    if detail_url in seen_urls:
                        continue
                    seen_urls.add(detail_url)
                    new_found += 1
                    title = link.get_text(strip=True)
                    price_el = item.select_one("span.priceTag, .price")
                    price = parse_tunisian_price(price_el.get_text() if price_el else "")
                    subtitle_el = item.select_one("span.listingDetails, .adLocation")
                    subtitle = subtitle_el.get_text(strip=True) if subtitle_el else ""
                    listing = self._get_detail(detail_url, title, price,
                                               subtitle, trans_type, prop_type)
                    if listing:
                        yield listing
                if new_found == 0:
                    log.info(f"[{self.source_name}] No new listings at p{page}, stopping category")
                    break
                self._random_delay(2, 5)

    def _get_list_items(self, soup: BeautifulSoup) -> List:
        for sel in ["li.listingBox", "li.promotionListing", ".ulListing li"]:
            items = soup.select(sel)
            if items:
                return items
        return []

    def _get_detail(self, url: str, title: str, price: Optional[float],
                    subtitle: str, trans_type: str, prop_type: str) -> Optional[PropertyListing]:
        resp = self._get_request(url)
        if not resp or len(resp.text) < 1000:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        full = soup.get_text(" ", strip=True)
        surface = parse_surface(full)
        rooms = parse_rooms(full)
        if not price:
            price = parse_tunisian_price(full)
        desc_el = soup.select_one(".blockProp p, .adDescription, #description")
        description = desc_el.get_text(" ", strip=True) if desc_el else None
        images = []
        seen_img: set = set()
        for img in soup.find_all("img"):
            src = (img.get("data-big") or img.get("data-src") or img.get("src") or "")
            if not src or "logo" in src.lower():
                continue
            full_src = src if src.startswith("http") else urljoin(self.base_url, src)
            if full_src not in seen_img:
                images.append(full_src)
                seen_img.add(full_src)
        parts = [p.strip() for p in subtitle.split(",")]
        city = parts[0] if parts else None
        governorate = infer_governorate(city or "")
        source_id = make_source_id(url, self.source_name)
        return PropertyListing(
            source_id=source_id,
            source_name=self.source_name,
            url=url,
            title=title,
            description=description,
            price=price,
            currency="TND",
            property_type=prop_type,
            transaction_type=trans_type,
            location=Location(city=city, governorate=governorate),
            surface_area_m2=surface,
            rooms=rooms,
            images=images,
            scraped_at=datetime.utcnow(),
        )


class AffareScraper(BaseScraper):
    """
    Affare.tn — Next.js site with __NEXT_DATA__ JSON.
    FIXED URL: /petites-annonces/tunisie/immobilier (not /Immobilier/Vente which 404s)
    """
    source_name = "affare"
    base_url = "https://www.affare.tn"
    LIST_URL = "https://www.affare.tn/petites-annonces/tunisie/immobilier"

    def __init__(self):
        super().__init__(self.source_name, self.base_url)

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        seen_urls: set = set()
        for page in range(1, 31):
            url = self.LIST_URL if page == 1 else f"{self.LIST_URL}?page={page}"
            log.info(f"[{self.source_name}] p{page}: {url}")
            resp = self._get_request(url)
            if not resp:
                break
            soup = BeautifulSoup(resp.text, "html.parser")
            links = [
                urljoin(self.base_url, a["href"])
                for a in soup.find_all("a", href=True)
                if "/annonce/" in a["href"]
            ]
            links = list(dict.fromkeys(links))
            if not links:
                break
            for detail_url in links:
                if detail_url in seen_urls:
                    continue
                seen_urls.add(detail_url)
                listing = self._scrape_detail(detail_url)
                if listing:
                    yield listing
            self._random_delay(2, 4)

    def _scrape_detail(self, url: str) -> Optional[PropertyListing]:
        resp = self._get_request(url)
        if not resp:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        # Try __NEXT_DATA__ first (most structured)
        nd = soup.find("script", id="__NEXT_DATA__")
        if nd:
            try:
                data = json.loads(nd.string or "{}")
                annonce = (data.get("props", {})
                           .get("pageProps", {})
                           .get("annonce"))
                if annonce:
                    region_obj = annonce.get("region") or {}
                    ville_obj = annonce.get("ville") or {}
                    city = ville_obj.get("nom")
                    governorate = region_obj.get("nom") or infer_governorate(city or "")
                    title = annonce.get("titre") or url
                    description = annonce.get("description")
                    price = parse_tunisian_price(str(annonce.get("prix", "")))
                    surface = None
                    try: surface = float(annonce.get("surface") or 0) or None
                    except Exception: pass
                    rooms = None
                    try: rooms = int(annonce.get("chambres") or 0) or None
                    except Exception: pass
                    images = [p["url"] for p in annonce.get("photos", []) if p.get("url")]
                    source_id = str(annonce.get("id") or make_source_id(url, self.source_name))
                    trans_type = "Rent" if "louer" in (title or "").lower() else "Sale"
                    return PropertyListing(
                        source_id=source_id,
                        source_name=self.source_name,
                        url=url,
                        title=title,
                        description=description,
                        price=price,
                        currency="TND",
                        property_type=infer_property_type(title, description or ""),
                        transaction_type=trans_type,
                        location=Location(city=city, governorate=governorate),
                        surface_area_m2=surface,
                        rooms=rooms,
                        images=images,
                        scraped_at=datetime.utcnow(),
                    )
            except Exception as e:
                log.warning(f"[{self.source_name}] __NEXT_DATA__ parse error: {e}")

        # Fallback: HTML scraping
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else url
        og_desc = soup.find("meta", property="og:description")
        description = og_desc.get("content") if og_desc else None
        full = soup.get_text(" ", strip=True)
        price = parse_tunisian_price(full)
        surface = parse_surface(full)
        rooms = parse_rooms(full)
        m = re.search(r" [àa]\s+([^\|\[,]+)", title, re.IGNORECASE)
        city = m.group(1).strip() if m else None
        governorate = infer_governorate(city or "")
        return PropertyListing(
            source_id=make_source_id(url, self.source_name),
            source_name=self.source_name,
            url=url,
            title=title,
            description=description,
            price=price,
            currency="TND",
            property_type=infer_property_type(title, description or ""),
            transaction_type=infer_transaction_type(title, description or "", url),
            location=Location(city=city, governorate=governorate),
            surface_area_m2=surface,
            rooms=rooms,
            scraped_at=datetime.utcnow(),
        )


class TunisieAnnonceScraper(BaseScraper):
    """
    TunisieAnnonce — Old ASP.NET site.
    FIXED URL: http://www.tunisie-annonce.com/AnnoncesImmobilier.asp
    (NOT https://www.tunisieannonce.com which gives HTTP 400)
    """
    source_name = "tunisieannonce"
    base_url = "http://www.tunisie-annonce.com"
    LIST_URL = "http://www.tunisie-annonce.com/AnnoncesImmobilier.asp"

    def __init__(self):
        super().__init__(self.source_name, self.base_url)

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        for page in range(1, 51):
            url = f"{self.LIST_URL}?rech_page_num={page}"
            log.info(f"[{self.source_name}] p{page}: {url}")
            resp = self._get_request(url)
            if not resp or resp.status_code != 200:
                break
            soup = BeautifulSoup(resp.text, "lxml")
            rows = self._extract_rows(soup)
            if not rows:
                break
            for row_data in rows:
                listing = self._to_listing(row_data)
                if listing:
                    yield listing
            self._random_delay(2, 4)

    def _extract_rows(self, soup: BeautifulSoup) -> List[Dict]:
        header_row = soup.find("tr", class_="Entete1")
        if not header_row:
            return []
        table = header_row.find_parent("table")
        if not table:
            return []
        rows = []
        header_found = False
        for tr in table.find_all("tr"):
            if not header_found:
                if tr == header_row:
                    header_found = True
                continue
            cells = [td.get_text(strip=True) for td in tr.find_all("td") if td.get_text(strip=True)]
            if len(cells) < 5:
                continue
            link = tr.find("a")
            url = None
            if link and link.get("href"):
                href = link["href"]
                url = href if href.startswith("http") else f"{self.base_url}/{href.lstrip('/')}"
            if not url:
                continue
            rows.append({
                "region": cells[0],
                "nature": cells[1],
                "type": cells[2],
                "description": cells[3],
                "price_text": cells[4] if len(cells) > 4 else "",
                "url": url,
            })
        return rows

    def _to_listing(self, data: Dict) -> Optional[PropertyListing]:
        region = data.get("region", "Tunis")
        title = f"{data.get('type', 'Bien')} à {region}"
        description = data.get("description", "")
        price = parse_tunisian_price(data.get("price_text", ""))
        full = f"{title} {description}"
        surface = parse_surface(full)
        rooms = parse_rooms(full)
        trans_type = "Rent" if any(
            k in (data.get("nature", "") or "").lower()
            for k in ["louer", "location"]
        ) else "Sale"
        prop_type = infer_property_type(title, description)
        source_id = make_source_id(data["url"], self.source_name)
        return PropertyListing(
            source_id=source_id,
            source_name=self.source_name,
            url=data["url"],
            title=title,
            description=description,
            price=price,
            currency="TND",
            property_type=prop_type,
            transaction_type=trans_type,
            location=Location(
                governorate=infer_governorate(region) or region,
                city=region,
            ),
            surface_area_m2=surface,
            rooms=rooms,
            scraped_at=datetime.utcnow(),
        )


# ─── Export ───────────────────────────────────────────────────────────────────

def build_all_scrapers() -> List[BaseScraper]:
    """Return one instance of every active scraper."""
    return [
        
        ZitounaImmoScraper(),
        DarcomScraper(),
        VerdarScraper(),
        NewKeyScraper(),
        TecnocasaScraper(),
        AffareScraper(),
        MubawabScraper(),
        TunisieAnnonceScraper(),
        Century21Scraper(),
        AqariScraper(),
    ]


__all__ = [
    "AqariScraper",
    "ZitounaImmoScraper",
    "Century21Scraper",
    "DarcomScraper",
    "VerdarScraper",
    "NewKeyScraper",
    "TecnocasaScraper",
    "AffareScraper",
    "MubawabScraper",
    "TunisieAnnonceScraper",
    "build_all_scrapers",
]