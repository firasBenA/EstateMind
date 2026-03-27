# scrapers/all_scrapers.py (COMPLETE FIXED VERSION)

"""
EstateMind — All 9 Tunisian real estate scrapers in one file.
Each scraper is built from actual HTML structure of the source site.
"""
from __future__ import annotations

import json
import re
import time
import random
from datetime import datetime
from typing import Generator, Optional, List, Dict, Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from core.base_scraper import BaseScraper
from core.models import PropertyListing, Location
from config.logging_config import log


# =============================================================================
# AFFARE.TN SCRAPER
# =============================================================================

class AffareScraper(BaseScraper):
    """Scraper for affare.tn - Next.js site with __NEXT_DATA__ JSON"""
    
    def __init__(self):
        super().__init__(source_name="affare", base_url="https://www.affare.tn")
        self.LIST_URL = "https://www.affare.tn/petites-annonces/tunisie/immobilier"

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        seen_urls: set = set()
        page = 1
        
        while page <= 30:
            url = self.LIST_URL if page == 1 else f"{self.LIST_URL}?page={page}"
            log.info(f"[{self.source_name}] p{page}: {url}")
            
            resp = self._get_request(url)
            if not resp:
                break
                
            soup = BeautifulSoup(resp.text, "html.parser")
            
            links = []
            for a in soup.find_all("a", href=True):
                if "/annonce/" in a["href"]:
                    href = a["href"]
                    full_url = href if href.startswith("http") else urljoin(self.base_url, href)
                    links.append(full_url)
            
            links = list(dict.fromkeys(links))
            if not links:
                break
                
            new_urls = [u for u in links if u not in seen_urls]
            seen_urls.update(new_urls)
            
            for detail_url in new_urls:
                listing = self._scrape_detail(detail_url)
                if listing:
                    yield listing
                self._random_delay(1.5, 3)
                
            page += 1
            self._random_delay(2, 4)

    def _scrape_detail(self, url: str) -> Optional[PropertyListing]:
        resp = self._get_request(url)
        if not resp:
            return None
            
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Try __NEXT_DATA__ first
        next_data = soup.find("script", id="__NEXT_DATA__")
        if next_data and next_data.string:
            try:
                data = json.loads(next_data.string)
                annonce = data.get("props", {}).get("pageProps", {}).get("annonce")
                if annonce:
                    return self._parse_from_json(annonce, url, soup)
            except Exception as e:
                log.debug(f"[{self.source_name}] JSON parse error: {e}")
        
        return self._parse_from_html(soup, url)

    def _parse_from_json(self, annonce: Dict, url: str, soup: BeautifulSoup) -> Optional[PropertyListing]:
        try:
            source_id = str(annonce.get("id", self.make_source_id(url, self.source_name)))
            title = annonce.get("titre", "")
            description = annonce.get("description2") or annonce.get("description", "")
            
            price = None
            prix_val = annonce.get("prix")
            if prix_val:
                price = self.parse_tunisian_price(str(prix_val))
            
            transaction_type = "Sale"
            if "location" in url.lower() or "louer" in title.lower():
                transaction_type = "Rent"
            
            property_type = self.infer_property_type(title, description or "")
            
            region = annonce.get("region", {})
            ville = region.get("ville", {})
            city = ville.get("nom") or region.get("nom")
            governorate = region.get("nom")
            if not governorate and city:
                governorate = self.infer_governorate(city)
            
            surface = None
            rooms = None
            features = []
            
            for p in annonce.get("params", []):
                slogan = p.get("slogan", "").lower()
                valeur = p.get("valeur", "").lower()
                
                if slogan == "superficie":
                    surface = self.parse_surface(valeur)
                elif slogan == "chambre":
                    rooms = self.parse_rooms(valeur)
                elif slogan in ["meublee", "meuble"] and valeur == "oui":
                    features.append("Meublé")
                elif slogan == "ascenseur" and valeur == "oui":
                    features.append("Ascenseur")
                elif slogan == "parking" and valeur == "oui":
                    features.append("Parking")
                elif slogan in ["climatiseurs", "climatisation"] and valeur == "oui":
                    features.append("Climatisation")
                elif slogan == "jardin" and valeur == "oui":
                    features.append("Jardin")
                elif slogan == "piscine" and valeur == "oui":
                    features.append("Piscine")
            
            images = []
            for img in annonce.get("images", []):
                img_path = img.get("image")
                if img_path:
                    images.append(f"https://www.affare.tn/image/{img_path}")
            if not images and annonce.get("image"):
                images.append(f"https://www.affare.tn/image/{annonce.get('image')}")
            
            # Get coordinates if available in the JSON
            lat = annonce.get("latitude")
            lon = annonce.get("longitude")
            location = self._build_location(city=city, governorate=governorate, latitude=lat, longitude=lon)
            
            return PropertyListing(
                source_id=source_id,
                source_name=self.source_name,
                url=url,
                title=title,
                description=description,
                price=price,
                currency="TND",
                property_type=property_type,
                transaction_type=transaction_type,
                location=location,
                surface_area_m2=surface,
                rooms=rooms,
                images=images,
                features=features,
                scraped_at=datetime.utcnow(),
            )
        except Exception as e:
            log.error(f"[{self.source_name}] JSON parse error for {url}: {e}")
            return None

    def _parse_from_html(self, soup: BeautifulSoup, url: str) -> Optional[PropertyListing]:
        try:
            title = soup.find("h1")
            title_text = title.get_text(strip=True) if title else ""
            
            desc_div = soup.find("div", class_="Annonce_description__ixLWq")
            description = desc_div.get_text(strip=True) if desc_div else None
            
            price = None
            price_span = soup.find("span", class_="Annonce_price__tE_l1")
            if price_span:
                price = self.parse_tunisian_price(price_span.get_text(strip=True))
            
            transaction_type = "Sale"
            if "louer" in title_text.lower() or "location" in url.lower():
                transaction_type = "Rent"
            
            city = None
            governorate = None
            location_div = soup.find("div", class_="Annonce_f201510__BNC4l")
            if location_div:
                text = location_div.get_text(strip=True)
                parts = [p.strip() for p in text.split(",")]
                if len(parts) >= 2:
                    governorate = parts[0]
                    city = parts[1]
            
            surface = None
            rooms = None
            features = []
            
            params_div = soup.find("div", class_="Annonce_box_params__nX87s")
            if params_div:
                for div in params_div.find_all("div", class_="Annonce_flx785550__AnK7v"):
                    label_div = div.find("div")
                    value_div = div.find_all("div")[-1] if len(div.find_all("div")) > 1 else None
                    
                    if label_div and value_div:
                        label = label_div.get_text(strip=True).lower()
                        value = value_div.get_text(strip=True).lower()
                        
                        if label == "chambre":
                            rooms = self.parse_rooms(value)
                        elif label == "superficie":
                            surface = self.parse_surface(value)
                        elif label in ["meublee", "meuble"] and value == "oui":
                            features.append("Meublé")
                        elif label == "ascenseur" and value == "oui":
                            features.append("Ascenseur")
                        elif label == "parking" and value == "oui":
                            features.append("Parking")
            
            images = []
            for img in soup.find_all("img"):
                src = img.get("src") or img.get("data-src")
                if src and "image/" in src and not any(k in src.lower() for k in ["logo", "icon"]):
                    full_url = src if src.startswith("http") else urljoin(self.base_url, src)
                    images.append(full_url)
            
            location = self._build_location(city=city, governorate=governorate)
            
            return PropertyListing(
                source_id=self.make_source_id(url, self.source_name),
                source_name=self.source_name,
                url=url,
                title=title_text,
                description=description,
                price=price,
                currency="TND",
                property_type=self.infer_property_type(title_text, description or ""),
                transaction_type=transaction_type,
                location=location,
                surface_area_m2=surface,
                rooms=rooms,
                images=images,
                features=features,
                scraped_at=datetime.utcnow(),
            )
        except Exception as e:
            log.error(f"[{self.source_name}] HTML parse error for {url}: {e}")
            return None


# =============================================================================
# CENTURY21.TN SCRAPER
# =============================================================================

class Century21Scraper(BaseScraper):
    """Scraper for century21.tn - Houzez WordPress theme"""
    
    def __init__(self):
        super().__init__(source_name="century21", base_url="https://century21.tn")
        self._driver = None
        self.LIST_URLS = [
            {"url": "https://century21.tn/status/vente-immobilier-tunisie/", "type": "Sale"},
            {"url": "https://century21.tn/status/location-immobilier-tunisie/", "type": "Rent"},
        ]

    def _get_driver(self):
        if self._driver is None:
            from core.base_scraper import _make_selenium_driver
            self._driver = _make_selenium_driver()
        return self._driver

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        try:
            driver = self._get_driver()
            
            for cat in self.LIST_URLS:
                cat_url = cat["url"]
                trans_type = cat["type"]
                seen_urls = set()
                page = 1
                
                while page <= 30:
                    url = f"{cat_url}page/{page}/" if page > 1 else cat_url
                    log.info(f"[{self.source_name}] {trans_type} p{page}: {url}")
                    
                    driver.get(url)
                    time.sleep(random.uniform(2, 4))
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    
                    links = []
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        if "/property/" in href and "page" not in href and "status" not in href:
                            full_url = href if href.startswith("http") else urljoin(self.base_url, href)
                            links.append(full_url)
                    
                    links = list(dict.fromkeys(links))
                    if not links:
                        break
                    
                    new_urls = [u for u in links if u not in seen_urls]
                    seen_urls.update(new_urls)
                    
                    for detail_url in new_urls:
                        listing = self._scrape_detail(detail_url, trans_type)
                        if listing:
                            yield listing
                        self._random_delay(1.5, 3)
                    
                    page += 1
                    self._random_delay(2, 4)
        finally:
            if self._driver:
                self._driver.quit()
                self._driver = None

    def _scrape_detail(self, url: str, transaction_type: str) -> Optional[PropertyListing]:
        resp = self._get_request(url)
        if not resp:
            return None
            
        soup = BeautifulSoup(resp.text, "html.parser")
        
        title = ""
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)
        
        description = ""
        desc_div = soup.find("div", id="property-description-wrap") or soup.find("div", class_="property-description-wrap")
        if desc_div:
            description = desc_div.get_text(" ", strip=True)
        
        price = None
        price_span = soup.find("span", class_="item-price")
        if price_span:
            price = self.parse_tunisian_price(price_span.get_text(strip=True))
        
        property_type = self.infer_property_type(title, description)
        
        city = None
        governorate = None
        district = None
        address = None
        
        addr_li = soup.find("li", class_="detail-address")
        if addr_li:
            spans = addr_li.find_all("span")
            if spans:
                address = spans[-1].get_text(strip=True)
        
        city_li = soup.find("li", class_="detail-city")
        if city_li:
            spans = city_li.find_all("span")
            if spans:
                city = spans[-1].get_text(strip=True)
        
        gov_li = soup.find("li", class_="detail-state")
        if gov_li:
            spans = gov_li.find_all("span")
            if spans:
                governorate = spans[-1].get_text(strip=True)
        
        area_li = soup.find("li", class_="detail-area")
        if area_li:
            spans = area_li.find_all("span")
            if spans:
                district = spans[-1].get_text(strip=True)
        
        surface = None
        rooms = None
        bathrooms = None
        features = []
        
        detail_uls = soup.find_all("ul", class_="list-2-cols")
        for ul in detail_uls:
            for li in ul.find_all("li"):
                strong = li.find("strong")
                if not strong:
                    continue
                label = strong.get_text(strip=True).lower()
                spans = li.find_all("span")
                value = spans[-1].get_text(strip=True) if spans else ""
                raw_text = li.get_text(" ", strip=True)
                
                if "prix" in label and price is None:
                    price = self.parse_tunisian_price(raw_text) or self.parse_tunisian_price(value)
                elif "surface" in label and surface is None:
                    surface = self.parse_surface(raw_text) or self.parse_surface(value)
                elif "chambres" in label and rooms is None:
                    m = re.search(r"\d+", value) or re.search(r"\d+", raw_text)
                    rooms = int(m.group()) if m else None
                elif "salle" in label and "bain" in label and bathrooms is None:
                    m = re.search(r"\d+", value) or re.search(r"\d+", raw_text)
                    bathrooms = int(m.group()) if m else None
        
        features_ul = soup.find("ul", class_="list-features")
        if features_ul:
            for li in features_ul.find_all("li"):
                text = li.get_text(strip=True)
                if text and len(text) < 50 and not li.find("a"):
                    features.append(text)
        
        if price is None:
            for li in soup.find_all("li"):
                t = li.get_text(" ", strip=True).lower()
                if "prix" in t:
                    price = self.parse_tunisian_price(t)
                    if price:
                        break
        
        lat = None
        lon = None
        for script in soup.find_all("script"):
            if script.string and "houzez_single_property_map" in script.string:
                ml = re.search(r'"lat"\s*:\s*"(-?\d+\.\d+)"', script.string)
                mn = re.search(r'"lng"\s*:\s*"(-?\d+\.\d+)"', script.string)
                if ml and mn:
                    try:
                        lat = float(ml.group(1))
                        lon = float(mn.group(1))
                    except:
                        pass
                break
        
        images = []
        for a in soup.find_all("a", class_="houzez-photoswipe-trigger"):
            img = a.find("img")
            if img:
                src = img.get("src") or img.get("data-src")
                if src:
                    full_url = src if src.startswith("http") else urljoin(self.base_url, src)
                    images.append(full_url)
        
        location = self._build_location(
            city=city,
            governorate=governorate,
            municipalite=district,
            district=district,
            address=address,
            latitude=lat,
            longitude=lon,
        )
        
        return PropertyListing(
            source_id=self.make_source_id(url, self.source_name),
            source_name=self.source_name,
            url=url,
            title=title,
            description=description,
            price=price,
            currency="TND",
            property_type=property_type,
            transaction_type=transaction_type,
            location=location,
            surface_area_m2=surface,
            rooms=rooms,
            bathrooms=bathrooms,
            images=images,
            features=features,
            scraped_at=datetime.utcnow(),
        )


# =============================================================================
# DARCOMTUNISIA.COM SCRAPER
# =============================================================================

class DarcomScraper(BaseScraper):
    """Scraper for darcomtunisia.com - Fixed with all property types"""
    
    def __init__(self):
        super().__init__(source_name="darcom", base_url="https://www.darcomtunisia.com")
        self._driver = None
        
        # Darcom has separate pages for each property type
        self.LIST_URLS = [
            # Sale
            {"url": "https://www.darcomtunisia.com/vente", "type": "Sale"},
            {"url": "https://www.darcomtunisia.com/vente-appartement", "type": "Sale"},
            {"url": "https://www.darcomtunisia.com/vente-villa", "type": "Sale"},
            {"url": "https://www.darcomtunisia.com/vente-maison", "type": "Sale"},
            {"url": "https://www.darcomtunisia.com/vente-terrain", "type": "Sale"},
            {"url": "https://www.darcomtunisia.com/vente-local-commercial", "type": "Sale"},
            {"url": "https://www.darcomtunisia.com/vente-bureau", "type": "Sale"},
            # Rent
            {"url": "https://www.darcomtunisia.com/location", "type": "Rent"},
            {"url": "https://www.darcomtunisia.com/location-appartement", "type": "Rent"},
            {"url": "https://www.darcomtunisia.com/location-villa", "type": "Rent"},
            {"url": "https://www.darcomtunisia.com/location-maison", "type": "Rent"},
            {"url": "https://www.darcomtunisia.com/location-terrain", "type": "Rent"},
            {"url": "https://www.darcomtunisia.com/location-local-commercial", "type": "Rent"},
            {"url": "https://www.darcomtunisia.com/location-bureau", "type": "Rent"},
        ]

    def _get_driver(self):
        if self._driver is None:
            from core.base_scraper import _make_selenium_driver
            self._driver = _make_selenium_driver()
        return self._driver

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        try:
            driver = self._get_driver()
            
            for cat in self.LIST_URLS:
                cat_url = cat["url"]
                trans_type = cat["type"]
                seen_urls = set()
                page = 1
                
                while page <= 30:
                    # Darcom pagination: add ?page=X
                    url = cat_url if page == 1 else f"{cat_url}?page={page}"
                    log.info(f"[{self.source_name}] {trans_type} p{page}: {url}")
                    
                    driver.get(url)
                    time.sleep(random.uniform(2, 4))
                    
                    # Scroll to load lazy content
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                    
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    
                    # Find property detail links - look for /bien/details/
                    links = []
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        if "/bien/details/" in href:
                            full_url = href if href.startswith("http") else urljoin(self.base_url, href)
                            links.append(full_url)
                    
                    links = list(dict.fromkeys(links))
                    if not links:
                        log.info(f"[{self.source_name}] No links found on page {page}")
                        break
                    
                    new_urls = [u for u in links if u not in seen_urls]
                    seen_urls.update(new_urls)
                    
                    if not new_urls:
                        log.info(f"[{self.source_name}] No new listings on page {page}")
                        break
                    
                    log.info(f"[{self.source_name}] Found {len(new_urls)} new listings on page {page}")
                    
                    for detail_url in new_urls:
                        listing = self._scrape_detail(detail_url, trans_type)
                        if listing:
                            yield listing
                        self._random_delay(1.5, 3)
                    
                    page += 1
                    self._random_delay(2, 4)
                    
        finally:
            if self._driver:
                self._driver.quit()
                self._driver = None
# =============================================================================
# MUBAWAB.TN SCRAPER
# =============================================================================

# scrapers/all_scrapers.py - FIXED MubawabScraper

class MubawabScraper(BaseScraper):
    """Scraper for mubawab.tn"""
    
    def __init__(self):
        super().__init__(source_name="mubawab", base_url="https://www.mubawab.tn")
        
        # CORRECT URLs based on site structure
        self.CATEGORIES = [
            # Sale
            ("fr/sc/appartements-a-vendre", "Sale", "Apartment"),
            ("en/sc/villas-and-luxury-homes-for-sale", "Sale", "Villa"),
            ("en/sc/houses-for-sale", "Sale", "Villa"),
            ("fr/sc/terrains-a-vendre", "Sale", "Land"),
            ("fr/sc/bureaux-et-commerces-a-vendre", "Sale", "Commercial"),
            # Rent
            ("fr/sc/appartements-a-louer", "Rent", "Apartment"),
            ("en/sc/houses-for-rent", "Rent", "Villa"),
            ("fr/sc/bureaux-et-commerces-a-louer", "Rent", "Commercial"),
            # Vacation / Short term
            ("fr/sc/appartements-vacational", "Rent", "Vacation"),
            # New developments
            ("fr/listing-promotion", "Sale", "NewDevelopment"),
        ]

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        for path, trans_type, prop_type in self.CATEGORIES:
            seen_urls = set()
            page = 1
            max_pages = 30
            
            while page <= max_pages:
                url = f"{self.base_url}/{path}"
                if page > 1:
                    url += f"?page={page}"
                log.info(f"[{self.source_name}] {trans_type} p{page}: {url}")
                
                resp = self._get_request(url)
                if not resp or resp.status_code != 200:
                    log.warning(f"[{self.source_name}] Failed to load {url}")
                    break
                    
                soup = BeautifulSoup(resp.text, "html.parser")
                
                links = []
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if not href:
                        continue
                    if "/fr/is/" in href or "/en/is/" in href:
                        continue
                    if not re.search(r"/(?:fr|en)/(?:pa|a)/\d+", href):
                        continue
                    full_url = href if href.startswith("http") else urljoin(self.base_url, href)
                    if full_url not in links:
                        links.append(full_url)
                
                if not links:
                    log.info(f"[{self.source_name}] No listing links found on page {page}")
                    break
                
                links = list(dict.fromkeys(links))
                new_urls = [u for u in links if u not in seen_urls]
                seen_urls.update(new_urls)
                
                log.info(f"[{self.source_name}] Found {len(new_urls)} new listings on page {page}")
                
                for detail_url in new_urls:
                    listing = self._scrape_detail(detail_url, trans_type, prop_type)
                    if listing:
                        yield listing
                    self._random_delay(1, 2)
                
                page += 1
                self._random_delay(2, 4)
    
    def _scrape_detail(self, url: str, transaction_type: str, property_type: str) -> Optional[PropertyListing]:
        """Scrape a single property detail page"""
        resp = self._get_request(url)
        if not resp:
            return None
            
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Title
        title = ""
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)
        
        # Description
        description = ""
        desc_div = soup.find("div", class_="descrBlockProp") or soup.find("div", class_="description")
        if desc_div:
            description = desc_div.get_text(" ", strip=True)
        
        # Price
        price = None
        price_span = soup.find("span", class_="priceTag") or soup.find("div", class_="item-price")
        if price_span:
            price = self.parse_tunisian_price(price_span.get_text(strip=True))
        
        # Location - from the detail page structure
        city = None
        governorate = None
        district = None
        
        # Find location in breadcrumb or subtitle
        breadcrumb = soup.find("div", class_="adBreadBlock")
        if breadcrumb:
            for a in breadcrumb.find_all("a"):
                text = a.get_text(strip=True)
                if "Kantaoui" in text:
                    city = "El Kantaoui"
                    governorate = "Sousse"
                elif "Hammam Sousse" in text:
                    city = "Hammam Sousse"
                    governorate = "Sousse"
                elif "Sousse" in text and not governorate:
                    governorate = "Sousse"
                elif "Tunis" in text:
                    governorate = "Tunis"
                elif "Ariana" in text:
                    governorate = "Ariana"
                elif "Ben Arous" in text:
                    governorate = "Ben Arous"
        
        # Also check the address block
        address_span = soup.find("span", class_="breadcrumbs-sub-title")
        if address_span:
            text = address_span.get_text(strip=True)
            if "El Kantaoui" in text:
                city = "El Kantaoui"
                governorate = "Sousse"
        
        # Surface and rooms from detail page
        surface = None
        rooms = None
        bathrooms = None
        
        # Look for the property details in the left column
        detail_items = soup.find_all("li", class_="price") or soup.find_all("div", class_="detail-item")
        for item in detail_items:
            text = item.get_text(strip=True).lower()
            if "m²" in text:
                surface = self.parse_surface(text)
            elif "chambre" in text or "pièces" in text:
                rooms = self.parse_rooms(text)
            elif "salle de bain" in text:
                bathrooms = self.parse_rooms(text)
        
        # Also check full text for numbers
        full_text = soup.get_text(" ", strip=True)
        if not surface:
            surface = self.parse_surface(full_text)
        if not rooms:
            rooms = self.parse_rooms(full_text)
        
        # Features
        features = []
        features_ul = soup.find("ul", class_="list-features") or soup.find("div", class_="features-list")
        if features_ul:
            for li in features_ul.find_all("li"):
                text = li.get_text(strip=True)
                if text and len(text) < 40 and ":" not in text:
                    features.append(text)
        
        # Also check the description for features
        if description:
            desc_lower = description.lower()
            if "piscine" in desc_lower:
                features.append("Piscine")
            if "climatisation" in desc_lower:
                features.append("Climatisation")
            if "ascenseur" in desc_lower:
                features.append("Ascenseur")
            if "parking" in desc_lower:
                features.append("Parking")
        
        # Images
        images = []
        for img in soup.find_all("img"):
            src = img.get("data-big") or img.get("data-src") or img.get("src")
            if src and "mubawab-media.com" in src:
                # Get the high-res version
                if "thumb" in src:
                    src = src.replace("thumb", "large")
                full_url = src if src.startswith("http") else urljoin(self.base_url, src)
                if full_url not in images:
                    images.append(full_url)
        
        # Coordinates from map script
        lat = None
        lon = None
        for script in soup.find_all("script"):
            if script.string and "houzez_single_property_map" in script.string:
                ml = re.search(r'"lat"\s*:\s*"(-?\d+\.\d+)"', script.string)
                mn = re.search(r'"lng"\s*:\s*"(-?\d+\.\d+)"', script.string)
                if ml and mn:
                    try:
                        lat = float(ml.group(1))
                        lon = float(mn.group(1))
                    except:
                        pass
                break
        
        location = self._build_location(
            city=city,
            governorate=governorate,
            municipalite=district,
            district=district,
            latitude=lat,
            longitude=lon,
        )
        
        return PropertyListing(
            source_id=self.make_source_id(url, self.source_name),
            source_name=self.source_name,
            url=url,
            title=title,
            description=description,
            price=price,
            currency="TND",
            property_type=property_type,
            transaction_type=transaction_type,
            location=location,
            surface_area_m2=surface,
            rooms=rooms,
            bathrooms=bathrooms,
            images=images,
            features=features,
            scraped_at=datetime.utcnow(),
        )
# =============================================================================
# NEWKEY.COM.TN SCRAPER
# =============================================================================

class NewKeyScraper(BaseScraper):
    """Scraper for newkey.com.tn"""
    
    def __init__(self):
        super().__init__(source_name="newkey", base_url="https://www.newkey.com.tn")
        self._driver = None
        self.LIST_URLS = [
            {"url": "https://www.newkey.com.tn/acheter", "type": "Sale"},
            {"url": "https://www.newkey.com.tn/louer", "type": "Rent"},
        ]

    def _get_driver(self):
        if self._driver is None:
            from core.base_scraper import _make_selenium_driver
            self._driver = _make_selenium_driver()
        return self._driver

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        try:
            driver = self._get_driver()
            
            for cat in self.LIST_URLS:
                cat_url = cat["url"]
                trans_type = cat["type"]
                seen_urls = set()
                page = 1
                
                while page <= 30:
                    url = cat_url if page == 1 else f"{cat_url}/page/{page}/"
                    log.info(f"[{self.source_name}] {trans_type} p{page}: {url}")
                    
                    driver.get(url)
                    time.sleep(random.uniform(2, 4))
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    
                    links = []
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        if "/bien/details/" in href:
                            full_url = href if href.startswith("http") else urljoin(self.base_url, href)
                            links.append(full_url)
                    
                    links = list(dict.fromkeys(links))
                    if not links:
                        break
                    
                    new_urls = [u for u in links if u not in seen_urls]
                    seen_urls.update(new_urls)
                    
                    for detail_url in new_urls:
                        listing = self._scrape_detail(detail_url, trans_type)
                        if listing:
                            yield listing
                        self._random_delay(1.5, 3)
                    
                    page += 1
                    self._random_delay(2, 4)
        finally:
            if self._driver:
                self._driver.quit()
                self._driver = None

    def _scrape_detail(self, url: str, transaction_type: str) -> Optional[PropertyListing]:
        resp = self._get_request(url)
        if not resp:
            return None
            
        soup = BeautifulSoup(resp.text, "html.parser")
        
        title = ""
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)
        
        description = ""
        desc_div = soup.find("div", id="description")
        if desc_div:
            description = desc_div.get_text(" ", strip=True)
        
        price = None
        price_span = soup.find("span", class_="item-price")
        if price_span:
            price = self.parse_tunisian_price(price_span.get_text(strip=True))
        
        property_type = self.infer_property_type(title, description)
        
        city = None
        governorate = None
        breadcrumb = soup.find("ol", class_="breadcrumb")
        if breadcrumb:
            for li in breadcrumb.find_all("li"):
                text = li.get_text(strip=True)
                if "Tunis" in text:
                    governorate = "Tunis"
                elif "Ben arous" in text.lower():
                    governorate = "Ben Arous"
                elif "Ariana" in text:
                    governorate = "Ariana"
                elif "Nabeul" in text:
                    governorate = "Nabeul"
                elif li.find("a") and "location" not in text:
                    city = text
        
        surface = None
        rooms = None
        bathrooms = None
        features = []
        
        detail_ul = soup.find("ul", class_="list-three-col")
        if detail_ul:
            for li in detail_ul.find_all("li"):
                strong = li.find("strong")
                if strong:
                    label = strong.get_text(strip=True).lower()
                    spans = li.find_all("span")
                    value = spans[-1].get_text(strip=True) if spans else ""
                    
                    if "surface" in label:
                        surface = self.parse_surface(value)
                    elif "chambres" in label:
                        rooms = self.parse_rooms(value)
                    elif "salle" in label:
                        bathrooms = self.parse_rooms(value)
        
        features_ul = soup.find("ul", class_="list-features")
        if features_ul:
            for li in features_ul.find_all("li"):
                if not li.find("a"):
                    text = li.get_text(strip=True)
                    if text and len(text) < 50:
                        features.append(text)
        
        images = []
        for img in soup.find_all("img"):
            src = img.get("data-src") or img.get("src")
            if src and "uploads" in src and not any(k in src.lower() for k in ["logo", "icon"]):
                full_url = src if src.startswith("http") else urljoin(self.base_url, src)
                images.append(full_url)
        
        location = self._build_location(city=city, governorate=governorate)
        
        return PropertyListing(
            source_id=self.make_source_id(url, self.source_name),
            source_name=self.source_name,
            url=url,
            title=title,
            description=description,
            price=price,
            currency="TND",
            property_type=property_type,
            transaction_type=transaction_type,
            location=location,
            surface_area_m2=surface,
            rooms=rooms,
            bathrooms=bathrooms,
            images=images,
            features=features,
            scraped_at=datetime.utcnow(),
        )


# =============================================================================
# TECNOCASA.TN SCRAPER
# =============================================================================

# scrapers/all_scrapers.py - FIXED TecnocasaScraper

# In all_scrapers.py - REPLACE the entire TecnocasaScraper class

class TecnocasaScraper(BaseScraper):
    """Scraper for tecnocasa.tn - Extracts data from embedded JSON"""
    
    _DETAIL_URL_RE = re.compile(r"/(vendre|louer)/.+?/(\d+)\.html(?:$|[?#])", re.IGNORECASE)

    def __init__(self):
        super().__init__(source_name="tecnocasa", base_url="https://www.tecnocasa.tn")
        self._driver = None
        
        # Listing URLs
        self.LIST_URLS = [
            {"url": "https://www.tecnocasa.tn/vendre/terrain/nord-est-ne/cap-bon/hammamet.html", "type": "Sale"},
            {"url": "https://www.tecnocasa.tn/vendre/terrain/grand-tunis/tunis.html", "type": "Sale"},
            {"url": "https://www.tecnocasa.tn/vendre/appartement/nord-est-ne/cap-bon/hammamet.html", "type": "Sale"},
            {"url": "https://www.tecnocasa.tn/vendre/appartement/grand-tunis/tunis.html", "type": "Sale"},
            {"url": "https://www.tecnocasa.tn/vendre/villa/nord-est-ne/cap-bon/hammamet.html", "type": "Sale"},
            {"url": "https://www.tecnocasa.tn/louer/appartement/grand-tunis/tunis.html", "type": "Rent"},
            {"url": "https://www.tecnocasa.tn/louer/villa/grand-tunis/tunis.html", "type": "Rent"},
        ]

    def _get_driver(self):
        if self._driver is None:
            from core.base_scraper import _make_selenium_driver
            self._driver = _make_selenium_driver()
        return self._driver

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        try:
            driver = self._get_driver()
            
            for cat in self.LIST_URLS:
                cat_url = cat["url"]
                trans_type = cat["type"]
                seen_urls = set()
                page = 1
                
                while page <= 30:
                    url = cat_url if page == 1 else cat_url.replace('.html', f'/pag-{page}.html')
                    log.info(f"[{self.source_name}] {trans_type} p{page}: {url}")
                    
                    driver.get(url)
                    time.sleep(random.uniform(2, 4))
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    
                    # Find property links
                    links = []
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        m = self._DETAIL_URL_RE.search(href)
                        if not m:
                            continue
                        full_url = href if href.startswith("http") else urljoin(self.base_url, href)
                        links.append(full_url)
                    
                    links = list(dict.fromkeys(links))
                    if not links:
                        break
                    
                    new_urls = [u for u in links if u not in seen_urls]
                    seen_urls.update(new_urls)
                    
                    for detail_url in new_urls:
                        listing = self._scrape_detail(detail_url, trans_type)
                        if listing:
                            yield listing
                        self._random_delay(1.5, 3)
                    
                    page += 1
                    self._random_delay(2, 4)
        finally:
            if self._driver:
                self._driver.quit()
                self._driver = None

    def _scrape_detail(self, url: str, transaction_type: str) -> Optional[PropertyListing]:
        """Extract data from the embedded JSON"""
        resp = self._get_request(url)
        if not resp:
            return None
            
        soup = BeautifulSoup(resp.text, "html.parser")

        estate_data = self._extract_estate_json(soup)
        if not estate_data:
            log.warning(f"[{self.source_name}] No JSON data for {url}")
            return None

        return self._parse_estate_json(estate_data, url, transaction_type)

    def _extract_estate_json(self, soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        estate_component = soup.find("estate-show-v2")
        if estate_component and estate_component.get(":estate"):
            try:
                json_str = estate_component.get(":estate")
                json_str = json_str.replace('&quot;', '"').replace('&apos;', "'")
                return json.loads(json_str)
            except Exception:
                pass

        for script in soup.find_all("script"):
            if script.string and "estate-show-v2" in script.string:
                match = re.search(r':estate="({.+?})"', script.string, re.DOTALL)
                if match:
                    try:
                        json_str = match.group(1).replace('&quot;', '"').replace('&apos;', "'")
                        return json.loads(json_str)
                    except Exception:
                        pass

        return None

    def _parse_estate_json(
        self,
        estate_data: Dict[str, Any],
        url: str,
        transaction_type: str,
    ) -> PropertyListing:
        property_id = str(estate_data.get("id", self.make_source_id(url, self.source_name)))
        title = estate_data.get("title", "")
        subtitle = estate_data.get("subtitle", "")
        full_title = f"{title} - {subtitle}" if subtitle else title

        description = estate_data.get("description", "")
        if description:
            description = re.sub(r"<[^>]+>", " ", description)
            description = re.sub(r"\s+", " ", description).strip()

        price = estate_data.get("numeric_price")

        surface = None
        numeric_surface = estate_data.get("numeric_surface")
        if numeric_surface:
            try:
                surface = float(numeric_surface)
            except Exception:
                pass

        rooms = estate_data.get("rooms")
        if rooms:
            try:
                rooms = int(rooms)
            except Exception:
                rooms = None

        city = None
        governorate = None
        city_data = estate_data.get("city")
        if city_data and isinstance(city_data, dict):
            city = city_data.get("title")

        province = estate_data.get("province")
        if province and isinstance(province, dict):
            governorate = province.get("title")

        lat = estate_data.get("latitude")
        lon = estate_data.get("longitude")

        property_type = self.infer_property_type(full_title or "", description or "")

        features = []
        features_data = estate_data.get("features", {})
        feature_map = {
            "free": "Libre",
            "furnitured": "Meublé",
            "air_conditioning": "Climatisation",
            "elevator": "Ascenseur",
            "garden": "Jardin",
            "balconies": "Balcon",
            "terraces": "Terrasse",
            "car_places": "Parking",
            "box": "Garage",
            "heating": "Chauffage",
        }

        for key, value in features_data.items():
            if value and value not in ["", "0", "null"] and key in feature_map:
                features.append(feature_map[key])

        data_array = estate_data.get("data", [])
        for item in data_array:
            if not isinstance(item, dict):
                continue
            label = str(item.get("label", "")).strip().lower()
            value = str(item.get("valore", "")).strip()
            if label in {"sous-type", "catégorie"} and value:
                features.append(value)

        images = []
        media_data = estate_data.get("media", {})
        if media_data:
            for img in media_data.get("images", []):
                if isinstance(img, dict):
                    img_urls = img.get("url", {})
                    detail_url = img_urls.get("detail") or img_urls.get("gallery")
                    if detail_url:
                        images.append(detail_url)

        location = self._build_location(
            city=city,
            governorate=governorate,
            latitude=lat,
            longitude=lon,
        )

        return PropertyListing(
            source_id=property_id,
            source_name=self.source_name,
            url=url,
            title=full_title[:500] if full_title else "Propriété Tecnocasa",
            description=description[:5000] if description else None,
            price=price,
            currency="TND",
            property_type=property_type,
            transaction_type=transaction_type,
            location=location,
            surface_area_m2=surface,
            rooms=rooms,
            images=images,
            features=features,
            scraped_at=datetime.utcnow(),
        )
# =============================================================================
# TUNISIEANNONCE.COM SCRAPER
# =============================================================================

# scrapers/all_scrapers.py - FIXED TunisieAnnonceScraper

# scrapers/all_scrapers.py - FIXED TunisieAnnonceScraper (final)

class TunisieAnnonceScraper(BaseScraper):
    """Scraper for tunisie-annonce.com"""
    
    def __init__(self):
        super().__init__(source_name="tunisieannonce", base_url="http://www.tunisie-annonce.com")
        self.LIST_URL = "http://www.tunisie-annonce.com/AnnoncesImmobilier.asp"

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        for page in range(1, 51):
            url = f"{self.LIST_URL}?rech_page_num={page}"
            log.info(f"[{self.source_name}] p{page}: {url}")
            
            resp = self._get_request(url)
            if not resp:
                break
            
            soup = BeautifulSoup(resp.text, "lxml")
            
            # Find all rows with class "Tableau1" (listing rows)
            rows = soup.find_all("tr", class_="Tableau1")
            
            if not rows:
                log.info(f"[{self.source_name}] No listing rows found on page {page}")
                break
            
            listings = []
            for tr in rows:
                cells = tr.find_all("td")
                if len(cells) < 6:
                    continue
                
                link = tr.find("a", href=re.compile(r"Details_Annonces_Immobilier\.asp\?cod_ann=\d+"))
                if not link:
                    continue
                
                href = link["href"]
                detail_url = href if href.startswith("http") else f"{self.base_url}/{href.lstrip('/')}"
                
                city = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                governorate = self.infer_governorate(city)
                
                listings.append({
                    "city": city,
                    "governorate": governorate,
                    "nature": cells[3].get_text(strip=True) if len(cells) > 3 else "",
                    "type": cells[5].get_text(strip=True) if len(cells) > 5 else "",
                    "description": link.get_text(strip=True),
                    "price_text": cells[9].get_text(strip=True) if len(cells) > 9 else "",
                    "date": cells[11].get_text(strip=True) if len(cells) > 11 else "",
                    "url": detail_url,
                })
            
            if not listings:
                log.info(f"[{self.source_name}] No listings extracted on page {page}")
                break
            
            log.info(f"[{self.source_name}] Found {len(listings)} listings on page {page}")
            
            for row_data in listings:
                listing = self._to_listing(row_data)
                if listing:
                    yield listing
                    
            self._random_delay(2, 4)

    def _to_listing(self, data: Dict) -> Optional[PropertyListing]:
        url = data["url"]
        log.info(f"[{self.source_name}] Fetching detail: {url}")
        
        resp = self._get_request(url)
        if not resp:
            return None
        
        soup = BeautifulSoup(resp.text, "lxml")
        full_text = soup.get_text(" ", strip=True)
        
        # Images
        images = []
        for img in soup.find_all("img", id=re.compile(r"PhotoMax_\d+")):
            src = img.get("src")
            if src and "upload2" in src:
                full_url = src if src.startswith("http") else urljoin(self.base_url, src)
                images.append(full_url)
        
        # Title from da_entete span
        title_span = soup.find("span", class_="da_entete")
        title = title_span.get_text(strip=True) if title_span else data.get("type", "Bien")
        
        # Description from the "Texte" field
        description = ""
        for td in soup.find_all("td", class_="da_field_text"):
            prev_td = td.find_previous_sibling("td", class_="da_label_field")
            if prev_td and "Texte" in prev_td.get_text():
                description = td.get_text(" ", strip=True)
                break
        
        # Price from the "Prix" field
        price = self.parse_tunisian_price(data.get("price_text", ""))
        if not price:
            for td in soup.find_all("td", class_="da_field_text"):
                prev_td = td.find_previous_sibling("td", class_="da_label_field")
                if prev_td and "Prix" in prev_td.get_text():
                    price = self.parse_tunisian_price(td.get_text(strip=True))
                    break
        
        # Location
        city = data.get("city") or ""
        governorate = data.get("governorate") or self.infer_governorate(city) or None
        # Try to get full location from detail page
        for td in soup.find_all("td", class_="da_field_text"):
            prev_td = td.find_previous_sibling("td", class_="da_label_field")
            if prev_td and "Localisation" in prev_td.get_text():
                loc_text = td.get_text(strip=True)
                parts = [p.strip() for p in loc_text.split(">")]
                if len(parts) >= 2:
                    governorate = parts[0] or governorate
                    city = parts[-1] or city
                break
        
        # Surface from detail page
        surface = None
        for td in soup.find_all("td", class_="da_field_text"):
            prev_td = td.find_previous_sibling("td", class_="da_label_field")
            if prev_td and "Surface" in prev_td.get_text():
                surface = self.parse_surface(td.get_text(strip=True))
                break
        if not surface:
            surface = self.parse_surface(full_text)
        
        # Rooms
        rooms = self.parse_rooms(full_text)
        
        # Transaction type
        nature = data.get("nature", "").lower()
        transaction_type = "Rent" if "location" in nature else "Sale"
        
        property_type = self.infer_property_type(title, description)
        
        location = self._build_location(city=city, governorate=governorate)
        
        return PropertyListing(
            source_id=self.make_source_id(url, self.source_name),
            source_name=self.source_name,
            url=url,
            title=title[:200] if title else "Annonce Tunisie",
            description=description[:1000] if description else None,
            price=price,
            currency="TND",
            property_type=property_type,
            transaction_type=transaction_type,
            location=location,
            surface_area_m2=surface,
            rooms=rooms,
            images=images,
            features=[],
            scraped_at=datetime.utcnow(),
        )

# =============================================================================
# VERDAR.TN SCRAPER
# =============================================================================

class VerdarScraper(BaseScraper):
    """Scraper for verdar.tn"""
    
    def __init__(self):
        super().__init__(source_name="verdar", base_url="https://www.verdar.tn")
        self._driver = None
        self.LIST_URLS = [
            {"url": "https://www.verdar.tn/acheter", "type": "Sale"},
            {"url": "https://www.verdar.tn/louer", "type": "Rent"},
        ]

    def _get_driver(self):
        if self._driver is None:
            from core.base_scraper import _make_selenium_driver
            self._driver = _make_selenium_driver()
        return self._driver

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        try:
            driver = self._get_driver()
            
            for cat in self.LIST_URLS:
                cat_url = cat["url"]
                trans_type = cat["type"]
                seen_urls = set()
                page = 1
                
                while page <= 30:
                    url = cat_url if page == 1 else f"{cat_url}?page={page}"
                    log.info(f"[{self.source_name}] {trans_type} p{page}: {url}")
                    
                    driver.get(url)
                    time.sleep(random.uniform(2, 4))
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    
                    links = []
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        if "/bien/details/" in href:
                            full_url = href if href.startswith("http") else urljoin(self.base_url, href)
                            links.append(full_url)
                    
                    links = list(dict.fromkeys(links))
                    if not links:
                        break
                    
                    new_urls = [u for u in links if u not in seen_urls]
                    seen_urls.update(new_urls)
                    
                    for detail_url in new_urls:
                        listing = self._scrape_detail(detail_url, trans_type)
                        if listing:
                            yield listing
                        self._random_delay(1.5, 3)
                    
                    page += 1
                    self._random_delay(2, 4)
        finally:
            if self._driver:
                self._driver.quit()
                self._driver = None

    def _scrape_detail(self, url: str, transaction_type: str) -> Optional[PropertyListing]:
        resp = self._get_request(url)
        if not resp:
            return None
            
        soup = BeautifulSoup(resp.text, "html.parser")
        
        title = ""
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)
        
        description = ""
        desc_div = soup.find("div", class_="unit")
        if desc_div:
            description = desc_div.get_text(" ", strip=True)
        
        price = None
        price_div = soup.find("div", class_="pd-price")
        if price_div:
            price = self.parse_tunisian_price(price_div.get_text(strip=True))
        
        property_type = self.infer_property_type(title, description)
        
        city = None
        governorate = None
        breadcrumb = soup.find("ol", class_="breadcrumb")
        if breadcrumb:
            for li in breadcrumb.find_all("li"):
                text = li.get_text(strip=True)
                if "Ben arous" in text.lower():
                    governorate = "Ben Arous"
                elif "Tunis" in text:
                    governorate = "Tunis"
                elif "Ariana" in text:
                    governorate = "Ariana"
                elif "Nabeul" in text:
                    governorate = "Nabeul"
        
        for div in soup.find_all("div", class_="pro-new-title"):
            text = div.get_text(strip=True)
            if "Ville:" in text or "Localité:" in text:
                parts = text.split(":")
                if len(parts) > 1:
                    city = parts[1].strip()
        
        surface = None
        rooms = None
        features = []
        
        for div in soup.find_all("div", class_="pro-new-title"):
            text = div.get_text(strip=True)
            if "Surface terrain:" in text:
                parts = text.split(":")
                if len(parts) > 1:
                    surface = self.parse_surface(parts[1])
            elif "Nb.chambres:" in text:
                parts = text.split(":")
                if len(parts) > 1:
                    rooms = self.parse_rooms(parts[1])
        
        if description:
            desc_lower = description.lower()
            if "jardin" in desc_lower:
                features.append("Jardin")
            if "piscine" in desc_lower:
                features.append("Piscine")
            if "parking" in desc_lower:
                features.append("Parking")
            if "climatisation" in desc_lower:
                features.append("Climatisation")
        
        images = []
        for img in soup.find_all("img", class_="lazy"):
            src = img.get("data-src") or img.get("src")
            if src and "property" in src and not any(k in src.lower() for k in ["logo", "icon"]):
                full_url = src if src.startswith("http") else urljoin(self.base_url, src)
                images.append(full_url)
        
        location = self._build_location(city=city, governorate=governorate)
        
        return PropertyListing(
            source_id=self.make_source_id(url, self.source_name),
            source_name=self.source_name,
            url=url,
            title=title,
            description=description,
            price=price,
            currency="TND",
            property_type=property_type,
            transaction_type=transaction_type,
            location=location,
            surface_area_m2=surface,
            rooms=rooms,
            images=images,
            features=features,
            scraped_at=datetime.utcnow(),
        )


# =============================================================================
# ZITOUNAIMMO.COM SCRAPER
# =============================================================================

class ZitounaImmoScraper(BaseScraper):
    """Scraper for zitounaimmo.com"""
    
    def __init__(self):
        super().__init__(source_name="zitouna_immo", base_url="https://www.zitounaimmo.com")
        self._driver = None
        self.LIST_URLS = [
            {"url": "https://www.zitounaimmo.com/acheter", "type": "Sale"},
            {"url": "https://www.zitounaimmo.com/louer", "type": "Rent"},
        ]

    def _get_driver(self):
        if self._driver is None:
            from core.base_scraper import _make_selenium_driver
            self._driver = _make_selenium_driver()
        return self._driver

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        try:
            driver = self._get_driver()
            
            for cat in self.LIST_URLS:
                cat_url = cat["url"]
                trans_type = cat["type"]
                seen_urls = set()
                page = 1
                
                while page <= 30:
                    url = cat_url if page == 1 else f"{cat_url}?page={page}"
                    log.info(f"[{self.source_name}] {trans_type} p{page}: {url}")
                    
                    driver.get(url)
                    time.sleep(random.uniform(2, 4))
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    
                    links = []
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        if "/bien/details/" in href:
                            full_url = href if href.startswith("http") else urljoin(self.base_url, href)
                            links.append(full_url)
                    
                    links = list(dict.fromkeys(links))
                    if not links:
                        break
                    
                    new_urls = [u for u in links if u not in seen_urls]
                    seen_urls.update(new_urls)
                    
                    for detail_url in new_urls:
                        listing = self._scrape_detail(detail_url, trans_type)
                        if listing:
                            yield listing
                        self._random_delay(1.5, 3)
                    
                    page += 1
                    self._random_delay(2, 4)
        finally:
            if self._driver:
                self._driver.quit()
                self._driver = None

    def _scrape_detail(self, url: str, transaction_type: str) -> Optional[PropertyListing]:
        resp = self._get_request(url)
        if not resp:
            return None
            
        soup = BeautifulSoup(resp.text, "html.parser")
        
        title = ""
        h2 = soup.find("h2")
        if h2:
            title = h2.get_text(strip=True)
        
        description = ""
        desc_div = soup.find("div", class_="listing_single_description")
        if desc_div:
            description = desc_div.get_text(" ", strip=True)
        
        price = None
        price_div = soup.find("div", class_="fp_price")
        if price_div:
            price = self.parse_tunisian_price(price_div.get_text(strip=True))
        
        property_type = self.infer_property_type(title, description)
        
        city = None
        governorate = None
        breadcrumb = soup.find("ol", class_="breadcrumb")
        if breadcrumb:
            for li in breadcrumb.find_all("li"):
                text = li.get_text(strip=True)
                if "Ben arous" in text.lower():
                    governorate = "Ben Arous"
                elif "Tunis" in text:
                    governorate = "Tunis"
                elif "Ariana" in text:
                    governorate = "Ariana"
        
        addr_div = soup.find("div", class_="single_property_title")
        if addr_div:
            p = addr_div.find("p")
            if p:
                text = p.get_text(strip=True)
                parts = [p.strip() for p in text.split(",")]
                if len(parts) >= 2:
                    governorate = parts[0]
                    city = parts[1]
        
        surface = None
        rooms = None
        features = []
        
        detail_ul = soup.find("ul", class_="list-unstyled")
        if detail_ul:
            for li in detail_ul.find_all("li"):
                text = li.get_text(strip=True)
                if "Surface" in text:
                    surface = self.parse_surface(text)
                elif "Chambres" in text:
                    rooms = self.parse_rooms(text)
        
        avantages = soup.find("div", class_="avantages")
        if avantages:
            for li in avantages.find_all("li"):
                text = li.get_text(strip=True)
                if text and len(text) < 40 and ":" not in text:
                    features.append(text)
        
        images = []
        for img in soup.find_all("img"):
            src = img.get("src") or img.get("data-src")
            if src and "property" in src and not any(k in src.lower() for k in ["logo", "icon"]):
                full_url = src if src.startswith("http") else urljoin(self.base_url, src)
                images.append(full_url)
        
        location = self._build_location(city=city, governorate=governorate)
        
        return PropertyListing(
            source_id=self.make_source_id(url, self.source_name),
            source_name=self.source_name,
            url=url,
            title=title,
            description=description,
            price=price,
            currency="TND",
            property_type=property_type,
            transaction_type=transaction_type,
            location=location,
            surface_area_m2=surface,
            rooms=rooms,
            images=images,
            features=features,
            scraped_at=datetime.utcnow(),
        )


# =============================================================================
# BUILDER FUNCTION
# =============================================================================

def build_all_scrapers() -> List[BaseScraper]:
    """Return one instance of every active scraper."""
    return [
        AffareScraper(),
        Century21Scraper(),
        DarcomScraper(),
        MubawabScraper(),
        NewKeyScraper(),
        TecnocasaScraper(),
        TunisieAnnonceScraper(),
        VerdarScraper(),
        ZitounaImmoScraper(),
    ]


__all__ = [
    "AffareScraper",
    "Century21Scraper",
    "DarcomScraper",
    "MubawabScraper",
    "NewKeyScraper",
    "TecnocasaScraper",
    "TunisieAnnonceScraper",
    "VerdarScraper",
    "ZitounaImmoScraper",
    "build_all_scrapers",
]
