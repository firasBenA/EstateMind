"""
EstateMind BaseScraper — shared logic for all site scrapers.

Improvements over old version:
- Full list of all 24 Tunisian governorates
- Correct Tunisian price parser (handles "450 000 TND" space-thousands format)
- UA rotation via fake_useragent
- S+N room parsing
- Transaction type: URL checked first (most reliable), then text
"""
from __future__ import annotations

import re
import time
import random
import hashlib
from abc import ABC, abstractmethod
from typing import Generator, Optional, Dict, Any, List , Tuple
from datetime import datetime
from bs4 import BeautifulSoup
import requests
from fake_useragent import UserAgent

from core.models import PropertyListing, Location
from config.logging_config import log


# ── All 24 Tunisian governorates ──────────────────────────────────────────────

GOVERNORATE_MAP: Dict[str, str] = {
    "tunis": "Tunis",
    "ariana": "Ariana",
    "ben arous": "Ben Arous",
    "manouba": "Manouba",
    "nabeul": "Nabeul",
    "zaghouan": "Zaghouan",
    "bizerte": "Bizerte",
    "beja": "Béja",
    "béja": "Béja",
    "jendouba": "Jendouba",
    "kef": "Le Kef",
    "le kef": "Le Kef",
    "siliana": "Siliana",
    "sousse": "Sousse",
    "monastir": "Monastir",
    "mahdia": "Mahdia",
    "sfax": "Sfax",
    "kairouan": "Kairouan",
    "kasserine": "Kasserine",
    "sidi bouzid": "Sidi Bouzid",
    "gabes": "Gabès",
    "gabès": "Gabès",
    "mednine": "Médenine",
    "medenine": "Médenine",
    "médenine": "Médenine",
    "tataouine": "Tataouine",
    "gafsa": "Gafsa",
    "tozeur": "Tozeur",
    "kebili": "Kébili",
    "kébili": "Kébili",
    # Common districts that map to governorate
    "la marsa": "Tunis",
    "carthage": "Tunis",
    "sidi bou said": "Tunis",
    "lac": "Tunis",
    "lac 1": "Tunis",
    "lac 2": "Tunis",
    "berges du lac": "Tunis",
    "la soukra": "Ariana",
    "raoued": "Ariana",
    "ennasr": "Ariana",
    "djerba": "Médenine",
    "hammamet": "Nabeul",
    "yasmine hammamet": "Nabeul",
}

ZONE_MAP: Dict[str, str] = {
    frozenset(["tunis", "ariana", "ben arous", "manouba", "nabeul", "bizerte",
               "béja", "jendouba", "le kef", "siliana", "zaghouan"]): "north",
    frozenset(["sousse", "monastir", "mahdia", "sfax"]): "east",
    frozenset(["béja", "jendouba", "le kef", "siliana", "kairouan",
               "kasserine", "sidi bouzid"]): "west",
    frozenset(["sfax", "gabès", "médenine", "gafsa", "tozeur",
               "kébili", "tataouine"]): "south",
}


def _make_selenium_driver(headless: bool = True):
    """Create a Selenium Chrome driver with proper configuration."""
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    import shutil
    import os
    import platform

    options = Options()
    env_h = os.getenv("HEADLESS")
    if env_h is not None:
        headless = env_h.lower() in ("1", "true", "yes", "on")
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
    try:
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
    except Exception:
        pass

    chrome_binary_env = os.getenv("CHROME_BINARY") or os.getenv("CHROME_BINARY_PATH") or os.getenv("GOOGLE_CHROME_BIN")
    chromedriver_env = os.getenv("CHROMEDRIVER_PATH")

    chrome_path = chrome_binary_env or shutil.which("chromium") or shutil.which("chromium-browser") or shutil.which("google-chrome")
    if not chrome_path and platform.system().lower().startswith("win"):
        default_paths = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        ]
        for p in default_paths:
            if os.path.exists(p):
                chrome_path = p
                break

    chromedriver_path = chromedriver_env or shutil.which("chromedriver")

    if chrome_path:
        options.binary_location = chrome_path
        if chromedriver_path:
            service = Service(chromedriver_path)
        else:
            from webdriver_manager.chrome import ChromeDriverManager
            from webdriver_manager.core.driver_cache import DriverCacheManager
            cache_manager = DriverCacheManager(root_dir="./temp_drivers")
            service = Service(ChromeDriverManager(cache_manager=cache_manager).install())
    else:
        from webdriver_manager.chrome import ChromeDriverManager
        from webdriver_manager.core.driver_cache import DriverCacheManager
        cache_manager = DriverCacheManager(root_dir="./temp_drivers")
        service = Service(ChromeDriverManager(cache_manager=cache_manager).install())

    return webdriver.Chrome(service=service, options=options)

def infer_governorate(text: str) -> Optional[str]:
    """Return normalized governorate name from any city/district text."""
    if not text:
        return None
    t = text.strip().lower()
    # Direct match
    if t in GOVERNORATE_MAP:
        return GOVERNORATE_MAP[t]
    # Partial match
    for key, gov in GOVERNORATE_MAP.items():
        if key in t or t in key:
            return gov
    return None


def infer_zone(governorate: str) -> Optional[str]:
    if not governorate:
        return None
    gov_lower = governorate.lower()
    for gov_set, zone in ZONE_MAP.items():
        if gov_lower in {g.lower() for g in gov_set}:
            return zone
    return None


def parse_tunisian_price(text: str) -> Optional[float]:
    """
    Parse Tunisian price strings:
      "450 000 TND" → 450000.0
      "1 200 DT"    → 1200.0
      "2,500.00"    → 2500.0
      "350000"      → 350000.0
    """
    if not text:
        return None
    lower = str(text).strip().lower()

    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(mdt|m\s*dt|millions?|million)\b", lower)
    if m:
        try:
            return float(m.group(1).replace(",", ".")) * 1_000_000
        except Exception:
            return None

    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(k|mille)\b", lower)
    if m:
        try:
            return float(m.group(1).replace(",", ".")) * 1_000
        except Exception:
            return None

    # Remove non-numeric chars except space, comma, dot
    clean = re.sub(r"[^\d\s,.]", "", lower)
    # Remove space-thousands separator: "450 000" → "450000"
    clean = re.sub(r"(\d)\s+(\d{3})\b", r"\1\2", clean).strip()
    # Remove comma-thousands: "450,000" → "450000"
    if re.search(r"\d{1,3},\d{3}($|\D)", clean):
        clean = clean.replace(",", "")
    # Replace comma decimal: "1,5" → "1.5" only if not thousands
    clean = clean.replace(",", ".")
    # Take first numeric token
    m = re.search(r"\d+(?:\.\d+)?", clean)
    if m:
        try:
            val = float(m.group())
            return val if val > 0 else None
        except Exception:
            return None
    return None


def parse_surface(text: str) -> Optional[float]:
    if not text:
        return None
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:m²|m2|m2|m)", text, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1).replace(",", "."))
        except Exception:
            pass
    return None


def parse_rooms(text: str) -> Optional[int]:
    if not text:
        return None
    # S+N pattern first (very common in Tunisia)
    m = re.search(r"[sS]\s*\+\s*(\d+)", text)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            pass
    m = re.search(r"(\d+)\s*(?:chambres?|pièces?|rooms?)", text, re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            pass
    return None


def infer_transaction_type(title: str, description: str, url: str) -> str:
    """URL is the most reliable signal — check it first."""
    for src in [url, title, description or ""]:
        lower = src.lower()
        if any(k in lower for k in ["location", "louer", "rent", "à louer"]):
            return "Rent"
        if any(k in lower for k in ["vente", "vendre", "sale", "acheter", "achat"]):
            return "Sale"
    return "Sale"  # default


def infer_property_type(title: str, description: str) -> str:
    text = (title + " " + (description or "")).lower()
    if "terrain" in text or "lotissement" in text:
        return "Land"
    if any(k in text for k in ["bureau", "local commercial", "commerce", "dépôt"]):
        return "Commercial"
    if any(k in text for k in ["villa", "maison", "duplex", "triplex", "rdc"]):
        return "Villa"
    if any(k in text for k in ["appartement", "studio", "apartment"]):
        return "Apartment"
    if "s+" in text:
        return "Apartment"
    return "Other"


def make_source_id(url: str, source_name: str) -> str:
    """Generate a stable ID from URL."""
    # Try to extract numeric ID from URL
    m = re.search(r"[/-](\d{4,})", url)
    if m:
        return f"{source_name}_{m.group(1)}"
    # Fallback to URL hash
    return f"{source_name}_{hashlib.md5(url.encode()).hexdigest()[:12]}"


# ── Base scraper ──────────────────────────────────────────────────────────────

class BaseScraper(ABC):
    """Abstract base for all EstateMind scrapers."""

    def __init__(self, source_name: str, base_url: str):
        self.source_name = source_name
        self.base_url = base_url
        self.session = requests.Session()
        self.ua = UserAgent()

    def _get_request(self, url: str, params: dict = None,
                     retries: int = 3) -> Optional[requests.Response]:
        for attempt in range(1, retries + 1):
            try:
                headers = {
                    "User-Agent": self.ua.random,
                    "Accept-Language": "fr-FR,fr;q=0.9,ar;q=0.8",
                    "Accept": "text/html,application/xhtml+xml",
                }
                resp = self.session.get(
                    url, params=params, headers=headers, timeout=25
                )
                if resp.status_code == 429:
                    wait = 60 + random.uniform(0, 30)
                    log.warning(f"[{self.source_name}] 429 — waiting {wait:.0f}s")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp
            except requests.HTTPError as e:
                log.warning(f"[{self.source_name}] HTTP error attempt {attempt}: {e}")
            except requests.RequestException as e:
                log.warning(f"[{self.source_name}] Request error attempt {attempt}: {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        log.error(f"[{self.source_name}] All {retries} attempts failed for {url}")
        return None

    def _random_delay(self, min_s: float = 1.5, max_s: float = 4.0):
        time.sleep(random.uniform(min_s, max_s))

    def _build_location(
        self,
        city: Optional[str] = None,
        governorate: Optional[str] = None,
        municipalite: Optional[str] = None,
        district: Optional[str] = None,
        address: Optional[str] = None,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
    ) -> Location:
        """Build a Location with auto-inferred governorate, zone, and missing details via data.ts."""
        from core.geolocation import _match_local_delegation, infer_governorate

        # Step 1: Infer governorate from city if missing
        if not governorate and city:
            governorate = infer_governorate(city)
        
        # Step 2: Consolidate district/municipality
        if not municipalite and district:
            municipalite = district

        # Step 3: Attempt to match against data.ts to fill missing fields
        local_match = _match_local_delegation(city, governorate, address or municipalite)
        if local_match:
            # If data.ts found a match, fill in the blanks
            if not governorate and local_match.get("governorate_name"):
                governorate = local_match["governorate_name"].title()
            
            # Use matched municipality if we don't have one
            muni_val = local_match.get("muni_value") or local_match.get("muni_name")
            if not municipalite and muni_val:
                municipalite = muni_val.title()
                
            # If coordinates are missing, use data.ts coordinates
            if latitude is None and local_match.get("lat") is not None:
                latitude = float(local_match["lat"])
            if longitude is None and local_match.get("lon") is not None:
                longitude = float(local_match["lon"])

        # Step 4: Infer zone based on final governorate
        zone = infer_zone(governorate) if governorate else None
        
        return Location(
            governorate=governorate,
            zone=zone,
            city=city,
            municipalite=municipalite,
            district=district,
            address=address,
            latitude=latitude,
            longitude=longitude,
        )

    @abstractmethod
    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        pass

    def run(self) -> Generator[PropertyListing, None, None]:
        log.info(f"[{self.source_name}] Starting scraper")
        for item in self.fetch_listings():
            if isinstance(item, PropertyListing):
                yield item
            else:
                try:
                    parsed = self.parse_listing(item) if hasattr(self, "parse_listing") else None
                    if parsed:
                        yield parsed
                except Exception as e:
                    log.error(f"[{self.source_name}] parse_listing error: {e}")

    # Shared helpers available to all scrapers
    parse_tunisian_price = staticmethod(parse_tunisian_price)
    parse_surface = staticmethod(parse_surface)
    parse_rooms = staticmethod(parse_rooms)
    infer_transaction_type = staticmethod(infer_transaction_type)
    infer_property_type = staticmethod(infer_property_type)
    make_source_id = staticmethod(make_source_id)
    infer_governorate = staticmethod(infer_governorate)
