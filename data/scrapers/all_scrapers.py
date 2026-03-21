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
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.driver_cache import DriverCacheManager

import requests
from bs4 import BeautifulSoup

from core.base_scraper import BaseScraper, parse_tunisian_price, parse_surface, parse_rooms
from core.base_scraper import infer_property_type, infer_transaction_type, make_source_id
from core.base_scraper import infer_governorate
from core.base_scraper import BaseScraper
from core.models import PropertyListing, Location
from core.geolocation import infer_governorate
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
    Scraper for Century 21 Tunisia (century21.tn) using Selenium.
    """
    def __init__(self):
        super().__init__(source_name="century21", base_url="https://century21.tn")
        self.driver = None
        self.categories = [
            {"url": f"{self.base_url}/status/vente-immobilier-tunisie/", "type": "Sale"},
            {"url": f"{self.base_url}/status/location-immobilier-tunisie/", "type": "Rent"}
        ]

    def _setup_driver(self):
        if self.driver:
            return
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        options.add_argument("--log-level=3")
        
        cache_manager = DriverCacheManager(root_dir="./temp_drivers")
        service = Service(ChromeDriverManager(cache_manager=cache_manager).install())
        self.driver = webdriver.Chrome(service=service, options=options)

    def _scrape_detail(self, url: str) -> Optional[PropertyListing]:
        """
        Scrapes a single detail page using Selenium.
        Used by reprocess_listings.py
        """
        try:
            self._setup_driver()
            log.info(f"Fetching detail with Selenium: {url}")
            self.driver.get(url)
            
            # Wait for main content
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "h1"))
                )
            except:
                log.warning(f"Timeout waiting for content on {url}")

            # Get page source
            html_content = self.driver.page_source
            
            # Extract ID if possible
            listing_id = "unknown"
            match = re.search(r'ref-([a-zA-Z0-9]+)', url)
            if match:
                listing_id = match.group(1)
            else:
                listing_id = str(abs(hash(url)))
                
            # Determine transaction type (heuristic from URL or content)
            trans_type = "Sale" # Default
            if "location" in url or "louer" in url:
                trans_type = "Rent"
            
            return self.parse_listing(html_content, url, listing_id, trans_type)
            
        except Exception as e:
            log.error(f"Error scraping detail {url}: {e}")
            # Reset driver on error to recover from session crashes
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
                self.driver = None
            return None

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        try:
            self._setup_driver()
            log.info(f"Starting {self.source_name} scraper with Selenium...")
            
            for category in self.categories:
                cat_url = category["url"]
                trans_type = category["type"]
                page = 1
                max_pages = 50
                
                while page <= max_pages:
                    # WordPress pagination style: .../page/2/
                    url = f"{cat_url}page/{page}/" if page > 1 else cat_url
                    log.info(f"Scraping {trans_type} page {page}: {url}")
                    
                    self.driver.get(url)
                    
                    # Wait for listings
                    try:
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/property/']"))
                        )
                    except:
                        log.warning(f"No listings found or timeout on page {page}")
                        break
                    
                    soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                    
                    # Find links
                    links = soup.find_all("a", href=True)
                    unique_urls = set()
                    
                    for link in links:
                        href = link['href']
                        if "/property/" in href:
                            if "status" in href or "page" in href:
                                continue
                            if href not in unique_urls and href.startswith("http"):
                                unique_urls.add(href)

                    if not unique_urls:
                        log.warning(f"No listings found on page {page}. Stopping {trans_type}.")
                        break
                    
                    log.info(f"Found {len(unique_urls)} listings on page {page}")
                    
                    for link_url in unique_urls:
                        try:
                            listing = self._scrape_detail(link_url)
                            if listing:
                                yield listing
                        except Exception as e:
                            log.error(f"Error processing listing {link_url}: {e}")
                    
                    page += 1
                    
        except Exception as e:
            log.error(f"Error running Century21 scraper: {e}")
        finally:
            if self.driver:
                self.driver.quit()
                self.driver = None

    def _infer_property_type(self, title: str, description: str) -> str:
        text = (title + " " + description).lower()
        if "terrain" in text:
            return "Land"
        if "bureau" in text or "commercial" in text:
            return "Office"
        if "villa" in text or "maison" in text:
            return "House"
        if "appartement" in text:
            return "Apartment"
        if "s+" in text:
            return "Apartment"
        return "Other"

    def parse_listing(self, html_content: str, url: str, listing_id: str, transaction_type: str) -> PropertyListing:
        soup = BeautifulSoup(html_content, "html.parser")

        title_tag = soup.find("h1")
        title = title_tag.get_text(strip=True) if title_tag else "No Title"

        price = 0.0
        price_text = ""
        price_tag = soup.find(class_="item-price") or soup.find(class_="price")
        if price_tag:
            price_text = price_tag.get_text(strip=True)
            clean = re.sub(r"[^\d]", "", price_text)
            if clean:
                try:
                    price = float(clean)
                except Exception:
                    pass

        # Description (fallbacks)
        description = ""
        desc_tag = soup.find(class_="detail-description") or soup.find("div", id="description")
        if desc_tag:
            description = desc_tag.get_text(" ", strip=True)
        if not description:
            section = soup.find("div", id="property-description-wrap") or soup.find(
                "div", class_="property-description-wrap"
            )
            if section:
                paragraphs = [p.get_text(" ", strip=True) for p in section.find_all("p")]
                paragraphs = [t for t in paragraphs if len(t) > 40]
                if paragraphs:
                    description = " ".join(paragraphs)
        if not description:
            paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
            paragraphs = [t for t in paragraphs if len(t) > 80]
            if paragraphs:
                description = max(paragraphs, key=len)
        if not description:
            log.warning(f"[{self.source_name}] No description extracted for {url}")

        # Images
        images: List[str] = []
        for a in soup.find_all("a", class_="houzez-photoswipe-trigger"):
            img = a.find("img", src=True)
            if img:
                src = img.get("src")
                if src and src.startswith("http"):
                    images.append(src)
        if not images:
            for img in soup.find_all("img", src=True):
                src = img.get("src")
                if not src or not src.startswith("http"):
                    continue
                if any(token in src for token in ["Properties", "digitaloceanspaces", "/bien/", "/property/"]):
                    images.append(src)
        images = list(dict.fromkeys(images))

        # Location: try structured "Adresse / Ville / Gouvernerat / Quartier" block
        governorate = None
        city = None
        district = None
        address = None
        
        # Try specific classes first (more robust)
        addr_li = soup.find("li", class_="detail-address")
        if addr_li:
            sp = addr_li.find_all("span")[-1] if addr_li.find_all("span") else None
            if sp: address = sp.get_text(strip=True)
            
        city_li = soup.find("li", class_="detail-city")
        if city_li:
            sp = city_li.find_all("span")[-1] if city_li.find_all("span") else None
            if sp: city = sp.get_text(strip=True)
            
        gov_li = soup.find("li", class_="detail-state")
        if gov_li:
            sp = gov_li.find_all("span")[-1] if gov_li.find_all("span") else None
            if sp: governorate = sp.get_text(strip=True)
            
        area_li = soup.find("li", class_="detail-area")
        if area_li:
            sp = area_li.find_all("span")[-1] if area_li.find_all("span") else None
            if sp: district = sp.get_text(strip=True)

        # Fallback: Extract from script data (houzez_single_property_map)
        if not address or not city:
            scripts = soup.find_all("script")
            for s in scripts:
                if s.string and "houzez_single_property_map" in s.string:
                    m_addr = re.search(r'"address"\s*:\s*"([^"]+)"', s.string)
                    if m_addr:
                        script_address = m_addr.group(1).encode('utf-8').decode('unicode_escape')
                        if not address:
                            address = script_address
                        if not city and not district:
                            # Heuristic: use script address as district/city if we have nothing
                            district = script_address
                    break

        # Fallback to text parsing if classes not found
        if not city and not governorate:
            addr_block = ""
            # Try to find any element containing "Adresse"
            for el in soup.find_all(string=re.compile(r"Adresse", re.IGNORECASE)):
                try:
                    parent = el.find_parent()
                    # Go up to the block wrapper if possible
                    block = parent.find_parent(class_="block-content-wrap") or parent
                    if block:
                        addr_block = block.get_text(" ", strip=True)
                        break
                except Exception:
                    continue
            
            if addr_block:
                text = re.sub(r"\s+", " ", addr_block)
                m_addr = re.search(r"Adresse\s+(.+?)\s+Ville", text, re.IGNORECASE)
                if m_addr: address = m_addr.group(1).strip()
                m_city = re.search(r"Ville\s+(.+?)\s+Gouvernerat", text, re.IGNORECASE)
                if m_city: city = m_city.group(1).strip()
                m_gov = re.search(r"Gouvernerat\s+(.+?)\s+Quartier", text, re.IGNORECASE)
                if m_gov: governorate = m_gov.group(1).strip()
                m_quarter = re.search(r"Quartier\s+(.+)", text, re.IGNORECASE)
                if m_quarter: district = m_quarter.group(1).strip()

        # Details: surface, rooms, bathrooms, reference
        # Iterate over ALL lists because address block might be the first list-2-cols

        og_desc = soup.find("meta", property="og:description")
        meta_desc_content = ""
        if og_desc:
            meta_desc_content = og_desc.get("content", "")
        
        meta_surface = None
        m_surf = re.search(r"Superficie:\s*(\d+(?:[\.,]\d+)?)\s*m[²2]", meta_desc_content, re.IGNORECASE)
        if m_surf:
            try:
                meta_surface = float(m_surf.group(1).replace(",", "."))
            except:
                pass

        surface_m2: Optional[float] = meta_surface
        rooms: Optional[int] = None
        bathrooms: Optional[int] = None
        features: List[str] = []
        
        for ul in soup.find_all("ul", class_="list-2-cols"):
            for li in ul.find_all("li"):
                label_tag = li.find("strong")
                # Value is usually in the last span, sometimes nested
                spans = li.find_all("span")
                if not spans:
                    # Fallback: check text content directly if no span
                    # e.g. <li><strong>Surface:</strong> 120 m²</li>
                    full_text = li.get_text(strip=True)
                    if label_tag:
                        label_text = label_tag.get_text(strip=True)
                        value_text = full_text.replace(label_text, "").strip()
                    else:
                        continue
                else:
                    value_text = spans[-1].get_text(strip=True)
                
                label = label_tag.get_text(strip=True).lower() if label_tag else ""
                
                # Surface parsing
                if any(k in label for k in ["surface", "superficie"]):
                    # e.g. "185 m²"
                    m = re.search(r"(\d+[\d\s,\.]*)", value_text)
                    if m:
                        digits = m.group(1).replace(" ", "").replace(",", ".")
                        try:
                            surface_m2 = float(digits)
                        except Exception:
                            pass
                
                # Rooms parsing
                elif any(k in label for k in ["chambres", "chambre", "pièces", "typologie"]):
                    # Try to find simple number first
                    m = re.search(r"^(\d+)$", value_text)
                    if m:
                        try:
                            rooms = int(m.group(1))
                        except:
                            pass
                    else:
                        # Try S+N pattern (common in Tunisia)
                        # e.g. "S+2", "s+3"
                        m_s = re.search(r"[sS]\s*\+\s*(\d+)", value_text)
                        if m_s:
                            try:
                                rooms = int(m_s.group(1))
                            except:
                                pass
                        else:
                            # Fallback for "3 chambres"
                            m = re.search(r"(\d+)", value_text)
                            if m:
                                try:
                                    rooms = int(m.group(1))
                                except:
                                    pass

                # Bathrooms parsing
                elif any(k in label for k in ["salles de bains", "salle de bains", "salle d'eau"]):
                    m = re.search(r"(\d+)", value_text)
                    if m:
                        try:
                            bathrooms = int(m.group(1))
                        except Exception:
                            pass

        # Try to extract coordinates from raw HTML (map widget or scripts)
        latitude: Optional[float] = None
        longitude: Optional[float] = None
        
        # 0. Look for houzez_single_property_map in scripts (Most reliable)
        if latitude is None:
            scripts = soup.find_all("script")
            for s in scripts:
                if s.string and "houzez_single_property_map" in s.string:
                    m_lat = re.search(r'"lat"\s*:\s*"(-?\d+\.\d+)"', s.string)
                    m_lng = re.search(r'"lng"\s*:\s*"(-?\d+\.\d+)"', s.string)
                    if m_lat and m_lng:
                        try:
                            latitude = float(m_lat.group(1))
                            longitude = float(m_lng.group(1))
                            break
                        except:
                            pass

        # 1. Look for Houzez map attributes
        map_div = soup.find("div", id="houzez-single-listing-map")
        if map_div:
            lat_attr = map_div.get("data-lat")
            lng_attr = map_div.get("data-lng")
            if lat_attr and lng_attr:
                try:
                    latitude = float(lat_attr)
                    longitude = float(lng_attr)
                except:
                    pass

        # 2. Look for Google Maps link if map div failed
        if latitude is None:
            gmap_link = soup.find("a", href=re.compile(r"maps\.google\.com"))
            if gmap_link:
                href = gmap_link.get("href", "")
                # q=Les%20berges%20du%20Lac%202 -> we can't get lat/lon directly but we can use it as address
                # q=36.85,10.19 -> we can get lat/lon
                m_coord = re.search(r"q=(-?\d+\.\d+),(-?\d+\.\d+)", href)
                if m_coord:
                    try:
                        latitude = float(m_coord.group(1))
                        longitude = float(m_coord.group(2))
                    except:
                        pass
                # If q is text, we might rely on geocoding downstream using this text
                elif "q=" in href and not address:
                    # Fallback address from map link if we missed it
                    q_val = href.split("q=")[1].split("&")[0]
                    from urllib.parse import unquote
                    address = unquote(q_val)

        # 3. Regex scan of raw HTML (last resort)
        if latitude is None:
            patterns = [
                r"data-lat=[\"']?(-?\d+\.\d+)",
                r"data-lng=[\"']?(-?\d+\.\d+)",
                r"data-longitude=[\"']?(-?\d+\.\d+)",
                r"lat\s*[:=]\s*([0-9]+\.[0-9]+)",
                r"lng\s*[:=]\s*([0-9]+\.[0-9]+)",
                r"longitude\s*[:=]\s*([0-9]+\.[0-9]+)",
            ]
            lat_val = None
            lon_val = None
            for pat in patterns:
                for match in re.finditer(pat, html_content):
                    val = match.group(1)
                    if "lat" in pat and lat_val is None:
                        lat_val = val
                    elif ("lng" in pat or "long" in pat) and lon_val is None:
                        lon_val = val
            try:
                if lat_val is not None and lon_val is not None:
                    latitude = float(lat_val)
                    longitude = float(lon_val)
            except Exception:
                pass


        property_type = self._infer_property_type(title, description)

        location = Location(
            governorate=governorate,
            city=city,
            district=district,
            address=address,
            latitude=latitude,
            longitude=longitude,
        )

        return PropertyListing(
            source_name=self.source_name,
            source_id=listing_id,
            url=url,
            title=title,
            description=description,
            price=price,
            currency="TND",
            property_type=property_type,
            transaction_type=transaction_type,
            location=location,
            surface_area_m2=surface_m2,
            rooms=rooms,
            bathrooms=bathrooms,
            images=images,
            features=features,
            raw_content=html_content,
        )

class AqariScraper(BaseScraper):
    def __init__(self):
        super().__init__("aqari", "https://www.aqari.tn")
        self.driver = None
        self.detail_driver = None

    def _setup_driver(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        options.add_argument("--log-level=3")
        
        cache_manager = DriverCacheManager(root_dir="./temp_drivers")
        service = Service(ChromeDriverManager(cache_manager=cache_manager).install())
        self.driver = webdriver.Chrome(service=service, options=options)
        self.detail_driver = webdriver.Chrome(service=service, options=options)

    def run(self) -> Iterator[PropertyListing]:
        try:
            self._setup_driver()
            log.info(f"Starting {self.source_name} scraper with Selenium...")
            
            # 1. Scrape Sale Listings
            yield from self.fetch_listings("Sale")
            
            # 2. Scrape Rent Listings
            yield from self.fetch_listings("Rent")
            
        except Exception as e:
            log.error(f"Error running Aqari scraper: {e}")
        finally:
            if self.driver:
                self.driver.quit()
            if self.detail_driver:
                self.detail_driver.quit()

    def fetch_listings(self, transaction_type: str) -> Iterator[PropertyListing]:
        # Valid endpoints based on inspection: /vente, /location
        category_url = "vente" if transaction_type == "Sale" else "location"
        url = f"{self.base_url}/{category_url}"
        
        log.info(f"Scraping {transaction_type} from: {url}")
        self.driver.get(url)
        
        # Wait for listings to load (hydration)
        try:
            WebDriverWait(self.driver, 20).until(
                lambda d: len(d.find_elements(By.CSS_SELECTOR, "a[href*='/property/']")) > 0
            )
            time.sleep(2) # Extra buffer for images/text
        except Exception:
             log.warning("Timeout waiting for listings to load. Trying to parse anyway.")

        # Pagination Loop
        page = 1
        max_pages = 20 # Limit for now
        
        while page <= max_pages:
            log.info(f"Processing page {page}")
            
            # Parse current page source with BeautifulSoup
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            
            # Find all links that point to a property
            # The <a> tag wraps the entire card
            property_links = [a for a in soup.find_all("a", href=True) if "/property/" in a["href"]]
            
            if not property_links:
                log.warning(f"No listings found on page {page}. Stopping.")
                break
                
            count = 0
            # Use a set to avoid duplicates on the same page (if any)
            seen_urls = set()
            
            for item in property_links:
                try:
                    href = item["href"]
                    if href in seen_urls:
                        continue
                    seen_urls.add(href)
                    
                    listing = self.parse_listing(item, transaction_type)
                    if listing:
                        yield listing
                        count += 1
                except Exception as e:
                    log.error(f"[{self.source_name}] Error parsing listing card {href}: {e}")
            
            log.info(f"Found {count} listings on page {page}")
            
            # Handle Pagination
            try:
                # Scroll to bottom
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                
                # Find the "Next" button. 
                next_buttons = self.driver.find_elements(By.XPATH, "//button[contains(text(), 'Suivant') or contains(text(), 'Next')] | //a[contains(text(), 'Suivant') or contains(text(), 'Next')]")
                
                if not next_buttons:
                     next_buttons = self.driver.find_elements(By.XPATH, "//button[contains(text(), '>')] | //a[contains(text(), '>')]")

                if not next_buttons:
                    next_page_num = page + 1
                    next_buttons = self.driver.find_elements(By.XPATH, f"//button[text()='{next_page_num}'] | //a[text()='{next_page_num}']")

                if next_buttons:
                    btn = next_buttons[-1]
                    if btn.is_enabled():
                        btn.click()
                        time.sleep(random.uniform(3, 5)) # Wait for load
                        page += 1
                    else:
                        log.info("Next button disabled.")
                        break
                else:
                    log.info("No next page button found.")
                    break
            except Exception as e:
                log.error(f"Error navigating to next page: {e}")
                break

    def _scrape_detail(self, url: str) -> Optional[PropertyListing]:
        """
        Wrapper for parse_detail_page to support reprocess_listings.py interface
        """
        # Aqari sometimes needs transaction type. If not provided, we might need to guess or it defaults.
        # parse_detail_page signature: (url, transaction_type)
        # We can try to guess from URL
        trans_type = "Sale"
        if "/location" in url:
            trans_type = "Rent"
            
        return self.parse_detail_page(url, trans_type)

    def parse_detail_page(self, url: str, transaction_type: str) -> Optional[PropertyListing]:
        """
        Parses a detail page directly given its URL.
        Useful for reprocessing or when card data is unavailable.
        """
        if not self.detail_driver:
            self._setup_driver()

        try:
            log.info(f"[{self.source_name}] Fetching detail page: {url}")
            self.detail_driver.get(url)
            try:
                WebDriverWait(self.detail_driver, 20).until(
                    lambda d: len(d.page_source) > 5000
                )
            except:
                log.warning(f"[{self.source_name}] Timeout waiting for page load, proceeding with available source.")

            detail_html = self.detail_driver.page_source
            detail_soup = BeautifulSoup(detail_html, "html.parser")
            
            # Defaults
            title = "No Title"
            description = ""
            price = 0.0
            surface_area = 0.0
            rooms = 0
            location_obj = Location(governorate="Tunis", city="Tunis")
            images = []
            prop_type = "Other"
            
            # 1. Try JSON-LD Parsing (Primary Strategy)
            ld_scripts = detail_soup.find_all("script", type="application/ld+json")
            for script in ld_scripts:
                try:
                    data = json.loads(script.get_text())
                    if isinstance(data, dict):
                        # Extract from RealEstateListing or Product
                        if data.get("@type") in ["RealEstateListing", "Product", "Apartment", "House", "SingleFamilyResidence"]:
                            if "name" in data: title = data["name"]
                            if "description" in data: description = data["description"]
                            
                            # Price
                            if "offers" in data and "price" in data["offers"]:
                                try: price = float(data["offers"]["price"])
                                except: pass
                            elif "price" in data: # Direct price property
                                try: 
                                    if isinstance(data["price"], dict) and "value" in data["price"]:
                                        price = float(data["price"]["value"])
                                    else:
                                        price = float(data["price"])
                                except: pass
                                
                            # Location
                            if "address" in data:
                                addr = data["address"]
                                if isinstance(addr, dict):
                                    if "addressLocality" in addr: 
                                        location_obj.city = addr["addressLocality"]
                                        inferred = infer_governorate(location_obj.city)
                                        if inferred:
                                            location_obj.governorate = inferred
                                    if "addressRegion" in addr: location_obj.governorate = addr["addressRegion"]
                                    
                            # Surface (floorSize)
                            if "floorSize" in data:
                                fs = data["floorSize"]
                                if isinstance(fs, dict) and "value" in fs:
                                    try: surface_area = float(fs["value"])
                                    except: pass
                            
                            # Rooms
                            if "numberOfRooms" in data:
                                try: rooms = int(data["numberOfRooms"])
                                except: pass
                                
                            # Property Type
                            if "propertyType" in data:
                                prop_type = data["propertyType"]
                                
                            # Images
                            if "image" in data:
                                if isinstance(data["image"], str): images.append(data["image"])
                                elif isinstance(data["image"], list): images.extend(data["image"])

                except Exception as e:
                    continue

            # 2. Try Meta Tags (Secondary Strategy - very reliable in Aqari)
            if price == 0:
                meta_price = detail_soup.find("meta", property="og:price:amount") or detail_soup.find("meta", property="product:price:amount")
                if meta_price:
                    try: price = float(meta_price.get("content"))
                    except: pass
            
            if location_obj.city == "Tunis" and location_obj.governorate == "Tunis":
                meta_loc = detail_soup.find("meta", property="og:locality")
                if meta_loc:
                    loc_content = meta_loc.get("content", "")
                    parts = [p.strip() for p in loc_content.split(",")]
                    if len(parts) >= 2:
                        location_obj.city = parts[0]
                        location_obj.governorate = parts[1]
                    elif len(parts) == 1:
                        location_obj.city = parts[0]
                        inferred = infer_governorate(location_obj.city)
                        if inferred:
                            location_obj.governorate = inferred

            if title == "No Title":
                meta_title = detail_soup.find("meta", property="og:title")
                if meta_title: title = meta_title.get("content")

            if not description:
                meta_desc = detail_soup.find("meta", property="og:description")
                if meta_desc: description = meta_desc.get("content")

            # 3. Fallback to Regex on Text
            full_text = detail_soup.get_text(" ", strip=True)
            
            if surface_area == 0:
                surf_match = re.search(r"(\d+)\s*m[²2]", full_text, re.IGNORECASE)
                if surf_match:
                    try: surface_area = float(surf_match.group(1))
                    except: pass
            
            if rooms == 0:
                room_match = re.search(r"(\d+)\s*chambres?", full_text, re.IGNORECASE)
                if room_match:
                    try: rooms = int(room_match.group(1))
                    except: pass

            # ID
            source_id = "unknown"
            match = re.search(r'/property/([a-zA-Z0-9-]+)', url)
            if match:
                source_id = match.group(1)
            else:
                import hashlib
                source_id = hashlib.md5(url.encode()).hexdigest()

            return PropertyListing(
                source_id=source_id,
                source_name=self.source_name,
                url=url,
                title=title,
                description=description,
                price=price,
                currency="TND",
                surface_area_m2=surface_area,
                rooms=rooms,
                location=location_obj,
                images=list(dict.fromkeys(images)), # Deduplicate
                scraped_at=datetime.now(),
                transaction_type=transaction_type,
                property_type=prop_type
            )

        except Exception as e:
            log.error(f"[{self.source_name}] Error parsing detail page {url}: {e}")
            if self.detail_driver:
                try:
                    self.detail_driver.quit()
                except:
                    pass
                self.detail_driver = None
            return None

    def _scrape_detail(self, url: str) -> Optional[PropertyListing]:
        """
        Scrapes a single detail page.
        Used by reprocess_listings.py
        """
        transaction_type = "Sale" # Default
        if "/location/" in url:
            transaction_type = "Rent"
            
        return self.parse_detail_page(url, transaction_type)

    def parse_listing(self, item: Any, transaction_type: str) -> Optional[PropertyListing]:
        # Reuse parse_detail_page logic if we navigate to it, 
        # but here we might want to just grab the URL and let the reprocessing handle details
        # OR we can try to get basic info from the card.
        # Given the user wants high quality data, and we have Selenium, 
        # we can either navigate to detail page HERE or just extract minimal info.
        # Since 'reprocess_listings.py' is the main goal for "no null values", 
        # let's extract what we can from the card and rely on reprocessing for the rest,
        # OR if we are running the scraper fresh, we might want to click through.
        
        # For now, let's keep the lightweight card extraction, but ensure the URL is correct
        # so reprocessing can do its job.
        
        try:
            relative_url = item["href"]
            if not relative_url.startswith("http"):
                 url = f"{self.base_url.rstrip('/')}/{relative_url.lstrip('/')}"
            else:
                 url = relative_url
            
            # If we want to be thorough during the main run, we can call parse_detail_page
            # But that slows down the main scraper significantly. 
            # The user asked for "reprocess all old data", implying the main scraper might be light.
            # However, for new data, we want it to be good too.
            # Let's call parse_detail_page if we are in the main run loop.
            
            return self.parse_detail_page(url, transaction_type)
            
        except Exception as e:
            log.error(f"Parse listing error: {e}")
            return None


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