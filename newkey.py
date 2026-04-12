# -*- coding: utf-8 -*-
import os
import sys
import time
import random
import json
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from hashlib import md5
from datetime import datetime
from typing import Optional, List, Dict, Set
from urllib.parse import urljoin
from enum import Enum
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page
from pydantic import BaseModel, Field, model_validator

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')


# CONFIGURATION
DB_CONFIG = {
    'dbname': 'realestate',
    'user': 'postgres',
    'password': 'sarra',
    'host': 'localhost',
    'port': '5432'
}

MAX_PAGES = 50
MAX_CONSECUTIVE_EMPTY_PAGES = 3
DOWNLOAD_IMAGES = True
HEADLESS = True
MAX_RETRIES = 3
IMG_DIR = "data/images/newkey"
CSV_DIR = "snapshots"

SCRAPE_VENTE = True
SCRAPE_LOCATION = True

# URLs de listing NewKey
LISTING_URLS = {
    "vente": "https://www.newkey.com.tn/acheter",
    "location": "https://www.newkey.com.tn/louer",
    "bureaux": "https://www.newkey.com.tn/bureaux-et-commerces",
    "terrains": "https://www.newkey.com.tn/terrains",
}

SOURCE_SITE = "newkey"


# MODELES PYDANTIC - Meme schema que Tecnocasa
class TransactionType(str, Enum):
    VENTE = "vendre"
    LOCATION = "louer"


class PropertyType(str, Enum):
    APPARTEMENT = "Appartement"
    VILLA = "Villa"
    TERRAIN = "Terrain"
    IMMEUBLE = "Immeuble"
    ENTREPOT = "Entrepôt"
    FERME = "Ferme"
    BUREAU = "Bureau"
    LOCAL_COMMERCIAL = "Local Commercial"
    LOCAL_INDUSTRIEL = "Local Industriel"
    AUTRE = "Autre"


class PropertyImage(BaseModel):
    url: str
    local_path: Optional[str] = None
    hash: str
    downloaded: bool = False


class PropertyData(BaseModel):
    property_id: str
    url: str
    transaction_type: TransactionType = TransactionType.VENTE
    type: PropertyType = PropertyType.AUTRE
    title: str = ""
    subtitle: str = ""
    price: Optional[float] = None
    price_period: Optional[str] = None
    surface: Optional[float] = None
    rooms: Optional[int] = None
    region: Optional[str] = None
    zone: Optional[str] = None
    city: Optional[str] = None
    municipality: Optional[str] = None
    images: List[PropertyImage] = Field(default_factory=list)
    pdf_link: Optional[str] = None
    features: Dict[str, str] = Field(default_factory=dict)
    poi: Dict[str, List[str]] = Field(default_factory=dict)
    scraped_at: datetime = Field(default_factory=datetime.now)
    last_updated: datetime = Field(default_factory=datetime.now)

    @model_validator(mode='before')
    @classmethod
    def detect_from_url(cls, data):
        url = data.get('url', '').lower()
        # Detection transaction
        if '/louer' in url or 'location' in url:
            data['transaction_type'] = TransactionType.LOCATION
        elif '/acheter' in url or '/vendre' in url or 'vente' in url:
            data['transaction_type'] = TransactionType.VENTE
        # Detection type de bien
        url_and_title = url + data.get('title', '').lower()
        if 'appartement' in url_and_title or 'duplex' in url_and_title or 'studio' in url_and_title or 'penthouse' in url_and_title:
            data['type'] = PropertyType.APPARTEMENT
        elif 'villa' in url_and_title or 'maison' in url_and_title:
            data['type'] = PropertyType.VILLA
        elif 'terrain' in url_and_title:
            data['type'] = PropertyType.TERRAIN
        elif 'immeuble' in url_and_title:
            data['type'] = PropertyType.IMMEUBLE
        elif 'bureau' in url_and_title:
            data['type'] = PropertyType.BUREAU
        elif 'local-commercial' in url_and_title or 'local commercial' in url_and_title or 'boutique' in url_and_title:
            data['type'] = PropertyType.LOCAL_COMMERCIAL
        elif 'local-industriel' in url_and_title or 'depot' in url_and_title or 'entrepot' in url_and_title:
            data['type'] = PropertyType.LOCAL_INDUSTRIEL
        return data

    class Config:
        use_enum_values = True
        str_strip_whitespace = True


# SCRAPER NEWKEY
class NewKeyScraper:
    BASE_URL = "https://www.newkey.com.tn"

    # Mapping type NewKey -> PropertyType
    TYPE_MAPPING = {
        "appartement": PropertyType.APPARTEMENT,
        "duplex": PropertyType.APPARTEMENT,
        "penthouse": PropertyType.APPARTEMENT,
        "studio": PropertyType.APPARTEMENT,
        "loft": PropertyType.APPARTEMENT,
        "villa": PropertyType.VILLA,
        "maison": PropertyType.VILLA,
        "rez-de-chaussée": PropertyType.VILLA,
        "étage de villa": PropertyType.VILLA,
        "terrain": PropertyType.TERRAIN,
        "immeuble": PropertyType.IMMEUBLE,
        "bureau": PropertyType.BUREAU,
        "local commercial": PropertyType.LOCAL_COMMERCIAL,
        "local industriel": PropertyType.LOCAL_INDUSTRIEL,
        "dépôt": PropertyType.LOCAL_INDUSTRIEL,
        "showroom": PropertyType.LOCAL_COMMERCIAL,
        "boutique": PropertyType.LOCAL_COMMERCIAL,
    }

    def __init__(self, img_dir: str = IMG_DIR):
        self.img_dir = img_dir
        os.makedirs(img_dir, exist_ok=True)

    def scrape_detail(self, page_html: str, url: str, transaction_type: TransactionType) -> PropertyData:
        soup = BeautifulSoup(page_html, "html.parser")

        # Extraire l'ID depuis l'URL : /bien/details/116335-duplex-...
        parts = url.split("/")
        detail_part = ""
        for i, p in enumerate(parts):
            if p == "details" and i + 1 < len(parts):
                detail_part = parts[i + 1]
                break
        property_id = detail_part.split("-")[0] if detail_part else md5(url.encode()).hexdigest()[:12]
        property_id = f"newkey_{property_id}"

        title = self._extract_title(soup)

        data = {
            "url": url,
            "property_id": property_id,
            "transaction_type": transaction_type,
            "title": title,
            "subtitle": self._extract_subtitle(soup),
            "surface": self._extract_surface(soup),
            "rooms": self._extract_rooms(soup),
            "features": self._extract_features(soup),
            "poi": self._extract_poi(soup),
            "images": self._extract_images(soup),
            "pdf_link": None,
        }

        price, price_period = self._extract_price(soup, transaction_type)
        data["price"] = price
        data["price_period"] = price_period

        location = self._extract_location(soup)
        data.update(location)

        # Override du type depuis la page de détail
        prop_type = self._extract_type(soup, url, title)
        if prop_type:
            data["type"] = prop_type

        return PropertyData(**data)

    def _extract_title(self, soup: BeautifulSoup) -> str:
        tag = soup.select_one("h1")
        return tag.get_text(strip=True) if tag else ""

    def _extract_subtitle(self, soup: BeautifulSoup) -> str:
        # Breadcrumb ou sous-titre
        tag = soup.select_one("h2")
        return tag.get_text(strip=True) if tag else ""

    def _extract_price(self, soup: BeautifulSoup, transaction_type: TransactionType) -> tuple:
        # Chercher le prix dans les détails
        for tag in soup.find_all(string=True):
            txt = tag.strip()
            if "DT" in txt and any(c.isdigit() for c in txt):
                if "prix sur demande" in txt.lower():
                    return None, "unique"
                try:
                    clean = txt.replace("DT", "").replace(" ", "").replace("\xa0", "").replace(",", "")
                    clean = clean.replace("/mois", "").replace("/an", "").strip()
                    price = float(clean)
                    period = "mois" if transaction_type == TransactionType.LOCATION else "unique"
                    return price, period
                except ValueError:
                    continue
        return None, None

    def _extract_surface(self, soup: BeautifulSoup) -> Optional[float]:
        # Chercher "Surface habitable" ou "Surface terrain"
        for row in soup.select(".detail-item, li, .property-detail"):
            txt = row.get_text(strip=True)
            if "surface" in txt.lower() and "m²" in txt.lower():
                try:
                    parts = txt.replace("m²", "").split(":")
                    if len(parts) >= 2:
                        val = parts[-1].strip().replace(" ", "").replace(",", ".")
                        return float(val)
                except (ValueError, IndexError):
                    continue
        # Fallback: chercher dans les details
        for li in soup.select("ul li, .detail li"):
            txt = li.get_text(strip=True)
            if "m²" in txt and ("habitable" in txt.lower() or "terrain" in txt.lower()):
                try:
                    import re
                    match = re.search(r'(\d[\d\s,]*)\s*m²', txt)
                    if match:
                        val = match.group(1).replace(" ", "").replace(",", ".")
                        return float(val)
                except (ValueError, AttributeError):
                    continue
        return None

    def _extract_rooms(self, soup: BeautifulSoup) -> Optional[int]:
        for tag in soup.find_all(string=True):
            txt = tag.strip().lower()
            if "chambre" in txt:
                try:
                    import re
                    match = re.search(r'(\d+)\s*chambre', txt)
                    if match:
                        return int(match.group(1))
                except (ValueError, AttributeError):
                    continue
        return None

    def _extract_type(self, soup: BeautifulSoup, url: str, title: str) -> Optional[PropertyType]:
        # Chercher dans les détails de la page
        for tag in soup.find_all(string=True):
            txt = tag.strip().lower()
            for key, ptype in self.TYPE_MAPPING.items():
                if key in txt:
                    return ptype
        return None

    def _extract_features(self, soup: BeautifulSoup) -> dict:
        features = {}
        # Section "Détail" et "Adresse"
        for li in soup.select("ul li, .detail-list li"):
            txt = li.get_text(strip=True)
            if ":" in txt:
                parts = txt.split(":", 1)
                key = parts[0].strip()
                val = parts[1].strip()
                if key and val:
                    features[key] = val
        # Commodités
        commodites = []
        for tag in soup.select("a[href='javascript:void(0)']"):
            txt = tag.get_text(strip=True)
            if txt and len(txt) > 2:
                commodites.append(txt)
        if commodites:
            features["Commodités"] = ", ".join(commodites)
        return features

    def _extract_poi(self, soup: BeautifulSoup) -> dict:
        # NewKey n'a pas de section POI structurée, on retourne vide
        return {}

    def _extract_images(self, soup: BeautifulSoup) -> List[PropertyImage]:
        images = []
        seen = set()
        for img in soup.select("img"):
            src = img.get("data-src") or img.get("src") or ""
            if not src or "data:image" in src or "logo" in src.lower():
                continue
            full_url = urljoin(self.BASE_URL, src)
            if full_url in seen:
                continue
            seen.add(full_url)
            hash_val = md5(full_url.encode()).hexdigest()
            images.append(PropertyImage(url=full_url, hash=hash_val))
        return images

    def _extract_location(self, soup: BeautifulSoup) -> dict:
        location = {"region": None, "zone": None, "city": None, "municipality": None}

        # Chercher gouvernorat, délégation, localité dans les liens
        for a in soup.select("a[href*='/gouvernorat/']"):
            location["region"] = a.get_text(strip=True).lower().replace(" ", "-")
            break
        for a in soup.select("a[href*='/delegation/']"):
            location["municipality"] = a.get_text(strip=True)
            break
        for a in soup.select("a[href*='/localite/']"):
            location["city"] = a.get_text(strip=True)
            break

        return location

    def download_images(self, property_data: PropertyData) -> PropertyData:
        folder = os.path.join(self.img_dir, property_data.property_id)
        os.makedirs(folder, exist_ok=True)

        for image in property_data.images:
            if image.downloaded:
                continue
            filename = f"{image.hash}.jpg"
            path = os.path.join(folder, filename)
            if os.path.exists(path):
                image.local_path = path
                image.downloaded = True
                continue
            try:
                headers = {"User-Agent": "Mozilla/5.0"}
                r = requests.get(image.url, headers=headers, timeout=12)
                r.raise_for_status()
                with open(path, "wb") as f:
                    f.write(r.content)
                image.local_path = path
                image.downloaded = True
            except Exception as e:
                print(f"      Echec image {image.url}: {e}")
            time.sleep(random.uniform(0.3, 1.0))
        return property_data


# GESTIONNAIRE BASE DE DONNEES - Meme schema que Tecnocasa
class DatabaseManager:
    def __init__(self, dbname, user, password, host="localhost", port="5432"):
        self.conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        self.create_tables()

    def create_tables(self):
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS properties (
            id SERIAL PRIMARY KEY,
            property_id VARCHAR(255) UNIQUE NOT NULL,
            url TEXT UNIQUE NOT NULL,
            transaction_type VARCHAR(20),
            type VARCHAR(50),
            title TEXT,
            subtitle TEXT,
            price REAL,
            price_period VARCHAR(20),
            surface REAL,
            rooms INTEGER,
            region VARCHAR(100),
            zone VARCHAR(100),
            city VARCHAR(255),
            municipality VARCHAR(255),
            pdf_link TEXT,
            features JSONB,
            poi JSONB,
            image_paths JSONB,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS images (
            id SERIAL PRIMARY KEY,
            property_id VARCHAR(255) REFERENCES properties(property_id) ON DELETE CASCADE,
            url TEXT NOT NULL,
            local_path TEXT,
            hash VARCHAR(32) NOT NULL,
            downloaded BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(property_id, hash)
        )
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_properties_region ON properties(region);
        CREATE INDEX IF NOT EXISTS idx_properties_type ON properties(type);
        CREATE INDEX IF NOT EXISTS idx_properties_transaction ON properties(transaction_type);
        CREATE INDEX IF NOT EXISTS idx_properties_price ON properties(price);
        """)
        self.conn.commit()
        cur.close()
        print("Tables creees/verifiees")

    def insert_property(self, prop: PropertyData) -> bool:
        cur = self.conn.cursor()
        try:
            features_json = json.dumps(prop.features, ensure_ascii=False)
            poi_json = json.dumps(prop.poi, ensure_ascii=False)
            image_paths_json = json.dumps([img.local_path for img in prop.images if img.local_path], ensure_ascii=False)

            transaction_val = prop.transaction_type.value if hasattr(prop.transaction_type, 'value') else str(prop.transaction_type)
            type_val = prop.type.value if hasattr(prop.type, 'value') else str(prop.type)
            region_val = str(prop.region) if prop.region else None
            zone_val = str(prop.zone) if prop.zone else None

            cur.execute("""
            INSERT INTO properties
            (property_id, url, transaction_type, type, title, subtitle, price, price_period,
             surface, rooms, region, zone, city, municipality, pdf_link, features, poi,
             image_paths, scraped_at, last_updated)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (property_id) DO UPDATE SET
                transaction_type = EXCLUDED.transaction_type,
                type = EXCLUDED.type,
                title = EXCLUDED.title,
                subtitle = EXCLUDED.subtitle,
                price = EXCLUDED.price,
                price_period = EXCLUDED.price_period,
                surface = EXCLUDED.surface,
                rooms = EXCLUDED.rooms,
                region = EXCLUDED.region,
                zone = EXCLUDED.zone,
                city = EXCLUDED.city,
                municipality = EXCLUDED.municipality,
                features = EXCLUDED.features,
                poi = EXCLUDED.poi,
                image_paths = EXCLUDED.image_paths,
                last_updated = EXCLUDED.last_updated
            """, (
                prop.property_id, prop.url, transaction_val, type_val,
                prop.title, prop.subtitle, prop.price, prop.price_period,
                prop.surface, prop.rooms, region_val, zone_val,
                prop.city, prop.municipality, prop.pdf_link,
                features_json, poi_json, image_paths_json,
                prop.scraped_at, prop.last_updated
            ))

            if prop.images:
                execute_values(cur, """
                INSERT INTO images (property_id, url, local_path, hash, downloaded)
                VALUES %s ON CONFLICT (property_id, hash) DO NOTHING
                """, [(prop.property_id, img.url, img.local_path, img.hash, img.downloaded) for img in prop.images])

            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(f"Erreur BDD: {e}")
            return False
        finally:
            cur.close()

    def property_exists(self, property_id: str) -> bool:
        cur = self.conn.cursor()
        cur.execute("SELECT 1 FROM properties WHERE property_id = %s", (property_id,))
        exists = cur.fetchone() is not None
        cur.close()
        return exists

    def get_stats(self) -> dict:
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM properties WHERE property_id LIKE 'newkey_%'")
        total = cur.fetchone()[0]
        cur.execute("SELECT transaction_type, COUNT(*) FROM properties WHERE property_id LIKE 'newkey_%' GROUP BY transaction_type")
        by_transaction = {row[0]: row[1] for row in cur.fetchall()}
        cur.execute("SELECT type, COUNT(*) FROM properties WHERE property_id LIKE 'newkey_%' GROUP BY type ORDER BY COUNT(*) DESC")
        by_type = {row[0]: row[1] for row in cur.fetchall()}
        cur.close()
        return {"total": total, "by_transaction": by_transaction, "by_type": by_type}

    def close(self):
        if self.conn:
            self.conn.close()


# AGENT PRINCIPAL
class NewKeyAgent:
    def __init__(self, db_config: dict, img_dir: str = IMG_DIR,
                 csv_dir: str = CSV_DIR, headless: bool = True, max_retries: int = 3):
        self.img_dir = img_dir
        self.csv_dir = csv_dir
        self.headless = headless
        self.max_retries = max_retries

        os.makedirs(img_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        self.db = DatabaseManager(**db_config)
        self.scraper = NewKeyScraper(img_dir)
        self.scraped_urls: Set[str] = set()
        self.all_properties: List[PropertyData] = []
        self.total_errors = 0

    def run(self, download_images: bool = True,
            include_vente: bool = True, include_location: bool = True):
        print("\n" + "="*80)
        print("AGENT NEWKEY - DEMARRAGE")
        print("="*80)

        listing_tasks = []
        if include_vente:
            listing_tasks.append(("vente", LISTING_URLS["vente"], TransactionType.VENTE))
            listing_tasks.append(("bureaux", LISTING_URLS["bureaux"], TransactionType.VENTE))
            listing_tasks.append(("terrains", LISTING_URLS["terrains"], TransactionType.VENTE))
        if include_location:
            listing_tasks.append(("location", LISTING_URLS["location"], TransactionType.LOCATION))

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = context.new_page()

            for task_name, base_url, transaction_type in listing_tasks:
                print(f"\n{'─'*80}")
                print(f"CATEGORIE: {task_name.upper()}")
                print(f"URL: {base_url}")
                print(f"{'─'*80}")
                self._scrape_listing(page, base_url, transaction_type, download_images)
                time.sleep(random.uniform(8, 15))

            browser.close()

        self._finalize()

    def _scrape_listing(self, page: Page, base_url: str,
                        transaction_type: TransactionType, download_images: bool):
        page_num = 1
        consecutive_empty = 0

        while page_num <= MAX_PAGES and consecutive_empty < MAX_CONSECUTIVE_EMPTY_PAGES:
            # Pagination NewKey: /acheter/page/2
            if page_num == 1:
                url = base_url
            else:
                url = f"{base_url}/page/{page_num}"

            print(f"\n  Page {page_num}")

            try:
                page.goto(url, timeout=120000, wait_until="domcontentloaded")
                page.wait_for_selector("body", timeout=45000)
                time.sleep(random.uniform(3, 6))

                property_urls = self._extract_listing_urls(page)

                if not property_urls:
                    consecutive_empty += 1
                    print(f"     Page vide ({consecutive_empty}/{MAX_CONSECUTIVE_EMPTY_PAGES})")
                    if consecutive_empty >= MAX_CONSECUTIVE_EMPTY_PAGES:
                        break
                    page_num += 1
                    continue

                consecutive_empty = 0
                print(f"     {len(property_urls)} annonces trouvees")

                new_count = 0
                for prop_url in property_urls:
                    if prop_url in self.scraped_urls:
                        continue
                    prop = self._scrape_property(page, prop_url, transaction_type, download_images)
                    if prop:
                        self.all_properties.append(prop)
                        self.scraped_urls.add(prop_url)
                        new_count += 1

                print(f"     {new_count} nouveaux biens scraped")
                page_num += 1
                time.sleep(random.uniform(4, 10))

            except Exception as e:
                print(f"     Erreur page {page_num}: {e}")
                consecutive_empty += 1
                page_num += 1

    def _extract_listing_urls(self, page: Page) -> List[str]:
        links = page.query_selector_all("a[href*='/bien/details/']")
        found = []
        for link in links:
            href = link.get_attribute("href")
            if href and "/bien/details/" in href:
                full_url = urljoin("https://www.newkey.com.tn", href)
                found.append(full_url)
        return list(set(found))

    def _scrape_property(self, page: Page, url: str,
                         transaction_type: TransactionType, download_images: bool) -> Optional[PropertyData]:
        # Extraire l'ID pour verifier l'existence
        parts = url.split("/")
        detail_part = ""
        for i, p in enumerate(parts):
            if p == "details" and i + 1 < len(parts):
                detail_part = parts[i + 1]
                break
        raw_id = detail_part.split("-")[0] if detail_part else ""
        property_id = f"newkey_{raw_id}"

        if self.db.property_exists(property_id):
            print(f"     Existe deja: {property_id}")
            return None

        print(f"     Scraping: {property_id}")

        for attempt in range(self.max_retries):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=90000)
                page.wait_for_selector("body", timeout=30000)
                time.sleep(random.uniform(2, 5))

                content = page.content()
                if len(content) < 5000:
                    print(f"        Page trop courte")
                    return None

                prop = self.scraper.scrape_detail(content, url, transaction_type)

                if download_images and prop.images:
                    print(f"        Telechargement {len(prop.images)} images...")
                    prop = self.scraper.download_images(prop)

                if self.db.insert_property(prop):
                    print(f"        Sauvegarde: {prop.title[:50]}")
                    return prop
                else:
                    print(f"        Echec BDD")
                    return None

            except Exception as e:
                print(f"        Tentative {attempt + 1}/{self.max_retries}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(random.uniform(5, 10))
                else:
                    self.total_errors += 1
                    return None

    def _finalize(self):
        print("\n" + "="*80)
        print("RESULTATS NEWKEY")
        print("="*80)
        print(f"Biens scraped cette session: {len(self.all_properties)}")
        print(f"Erreurs: {self.total_errors}")

        if self.all_properties:
            df_data = [{
                'property_id': p.property_id,
                'url': p.url,
                'transaction_type': p.transaction_type,
                'type': p.type,
                'title': p.title,
                'price': p.price,
                'price_period': p.price_period,
                'surface': p.surface,
                'rooms': p.rooms,
                'region': p.region,
                'city': p.city,
                'municipality': p.municipality,
                'nb_images': len(p.images),
                'scraped_at': p.scraped_at
            } for p in self.all_properties]

            df = pd.DataFrame(df_data)
            snapshot_file = os.path.join(
                self.csv_dir,
                f"newkey_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            )
            df.to_csv(snapshot_file, index=False, encoding='utf-8-sig')
            print(f"\nSnapshot CSV: {snapshot_file}")

        stats = self.db.get_stats()
        print(f"\nSTATISTIQUES NEWKEY EN BDD:")
        print(f"   Total: {stats['total']} biens")
        if stats['by_transaction']:
            print("   Par transaction:")
            for k, v in stats['by_transaction'].items():
                print(f"     {k}: {v}")
        if stats['by_type']:
            print("   Par type:")
            for k, v in stats['by_type'].items():
                print(f"     {k}: {v}")

        print("\n" + "="*80)
        print("SCRAPING NEWKEY TERMINE")
        print("="*80 + "\n")

        self.db.close()


def main():
    agent = NewKeyAgent(
        db_config=DB_CONFIG,
        img_dir=IMG_DIR,
        csv_dir=CSV_DIR,
        headless=HEADLESS,
        max_retries=MAX_RETRIES
    )
    agent.run(
        download_images=DOWNLOAD_IMAGES,
        include_vente=SCRAPE_VENTE,
        include_location=SCRAPE_LOCATION
    )


if __name__ == "__main__":
    main()