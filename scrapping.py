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
from urllib.parse import urljoin, quote_plus
from enum import Enum
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page
from pydantic import BaseModel, Field, field_validator, model_validator

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

MAX_PAGES_PER_REGION = 50
MAX_CONSECUTIVE_EMPTY_PAGES = 3
DOWNLOAD_IMAGES = True
HEADLESS = True
MAX_RETRIES = 3
IMG_DIR = "data/images"
CSV_DIR = "snapshots"

SCRAPE_VENTE = True
SCRAPE_LOCATION = True


# MODELES PYDANTIC
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


class Region(str, Enum):
    BIZERTE = "bizerte"
    CAP_BON = "cap-bon"
    GRAND_TUNIS = "grand-tunis"
    KAIROUAN = "kairouan"
    MAHDIA = "mahdia"
    MONASTIR = "monastir"
    SFAX = "sfax"
    SOUSSE = "sousse"


class ZoneGeographique(str, Enum):
    NORD_EST = "nord-est-ne"
    NORD_OUEST = "nord-ouest-no"
    CENTRE_EST = "centre-est-ce"
    CENTRE_OUEST = "centre-ouest-co"
    SUD_EST = "sud-est-se"
    SUD_OUEST = "sud-ouest-so"


class PropertyImage(BaseModel):
    url: str = Field(..., description="URL originale")
    local_path: Optional[str] = Field(None, description="Chemin local apres telechargement")
    hash: str = Field(..., description="Hash MD5 de l'URL")
    downloaded: bool = Field(False, description="Telechargee ou non")


class PropertyData(BaseModel):
    property_id: str = Field(..., description="ID unique extrait de l'URL")
    url: str = Field(..., description="URL de l'annonce")
    transaction_type: TransactionType = Field(TransactionType.VENTE, description="Type de transaction")
    type: PropertyType = Field(PropertyType.AUTRE, description="Type de bien")
    title: str = Field("", description="Titre principal")
    subtitle: str = Field("", description="Sous-titre")
    price: Optional[float] = Field(None, description="Prix en DT")
    price_period: Optional[str] = Field(None, description="Période du prix (mois/an/unique)")
    surface: Optional[float] = Field(None, description="Surface en m²")
    rooms: Optional[int] = Field(None, description="Nombre de pieces")
    region: Optional[Region] = Field(None, description="Region")
    zone: Optional[ZoneGeographique] = Field(None, description="Zone geographique")
    city: Optional[str] = Field(None, description="Ville")
    municipality: Optional[str] = Field(None, description="Municipalite extraite de l'URL")
    images: List[PropertyImage] = Field(default_factory=list, description="Images")
    pdf_link: Optional[str] = Field(None, description="Lien PDF")
    features: Dict[str, str] = Field(default_factory=dict, description="Caracteristiques")
    poi: Dict[str, List[str]] = Field(default_factory=dict, description="Points d'interet")
    scraped_at: datetime = Field(default_factory=datetime.now, description="Date du scraping")
    last_updated: datetime = Field(default_factory=datetime.now, description="Derniere mise a jour")

    # FIX: model_validator(mode='before') a acces a TOUTES les donnees brutes simultanement,
    # contrairement a field_validator qui ne voit que les champs deja valides (donc pas 'url'
    # quand 'transaction_type' est traite, car il est declare apres dans le modele).
    @model_validator(mode='before')
    @classmethod
    def detect_from_url(cls, data):
        url = data.get('url', '').lower()
        # Detection du type de transaction
        if '/louer/' in url:
            data['transaction_type'] = TransactionType.LOCATION
        elif '/vendre/' in url:
            data['transaction_type'] = TransactionType.VENTE
        # Detection du type de bien
        if 'appartement' in url:
            data['type'] = PropertyType.APPARTEMENT
        elif 'villa' in url:
            data['type'] = PropertyType.VILLA
        elif 'terrain' in url:
            data['type'] = PropertyType.TERRAIN
        elif 'immeuble' in url:
            data['type'] = PropertyType.IMMEUBLE
        elif 'entrepot' in url:
            data['type'] = PropertyType.ENTREPOT
        elif 'ferme' in url:
            data['type'] = PropertyType.FERME
        elif 'bureau' in url:
            data['type'] = PropertyType.BUREAU
        elif 'local-commercial' in url:
            data['type'] = PropertyType.LOCAL_COMMERCIAL
        elif 'local-industriel' in url:
            data['type'] = PropertyType.LOCAL_INDUSTRIEL
        return data

    class Config:
        use_enum_values = True
        str_strip_whitespace = True


class ScrapingTask(BaseModel):
    zone: ZoneGeographique
    region: Region
    transaction_type: TransactionType
    start_url: str
    max_pages: int = Field(50, description="Nombre max de pages")
    completed: bool = Field(False, description="Tache terminee")
    properties_found: int = Field(0, description="Nombre de biens trouves")
    errors: List[str] = Field(default_factory=list, description="Erreurs rencontrees")


class ScrapingStats(BaseModel):
    total_properties: int = 0
    total_images: int = 0
    total_errors: int = 0
    regions_scraped: List[str] = Field(default_factory=list)
    start_time: datetime = Field(default_factory=datetime.now)
    end_time: Optional[datetime] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


# GENERATEUR D'URLS
class URLGenerator:
    BASE_URL = "https://www.tecnocasa.tn"

    ZONE_REGIONS: Dict[ZoneGeographique, List[Region]] = {
        ZoneGeographique.NORD_EST: [
            Region.BIZERTE,
            Region.CAP_BON,
            Region.GRAND_TUNIS,
        ],
        ZoneGeographique.CENTRE_EST: [
            Region.SOUSSE,
            Region.MONASTIR,
            Region.MAHDIA,
            Region.SFAX,
        ],
        ZoneGeographique.CENTRE_OUEST: [
            Region.KAIROUAN,
        ],
    }

    @classmethod
    def generate_all_tasks(cls, max_pages_per_region: int = 50,
                           include_vente: bool = True,
                           include_location: bool = True) -> List[ScrapingTask]:
        tasks = []
        transaction_types = []

        if include_vente:
            transaction_types.append(TransactionType.VENTE)
        if include_location:
            transaction_types.append(TransactionType.LOCATION)

        for transaction in transaction_types:
            for zone, regions in cls.ZONE_REGIONS.items():
                for region in regions:
                    url = cls.build_url(zone, region, transaction)
                    task = ScrapingTask(
                        zone=zone,
                        region=region,
                        transaction_type=transaction,
                        start_url=url,
                        max_pages=max_pages_per_region
                    )
                    tasks.append(task)
        return tasks

    @classmethod
    def build_url(cls, zone: ZoneGeographique, region: Region,
                  transaction: TransactionType = TransactionType.VENTE) -> str:
        return f"{cls.BASE_URL}/{transaction.value}/immeubles/{zone.value}/{region.value}.html"


# SCRAPER DE PROPRIETES
class PropertyScraper:
    def __init__(self, base_url: str = "https://www.tecnocasa.tn", img_dir: str = "data/images"):
        self.base_url = base_url
        self.img_dir = img_dir
        os.makedirs(self.img_dir, exist_ok=True)

    def scrape(self, page_html: str, url: str) -> PropertyData:
        soup = BeautifulSoup(page_html, "html.parser")

        parts = url.rstrip('/').split('/')
        last_part = parts[-1] if parts else ""
        property_id = last_part.replace(".html", "")

        if not property_id or len(property_id) < 3:
            import hashlib
            property_id = hashlib.md5(url.encode()).hexdigest()[:12]

        data = {
            "url": url,
            "property_id": property_id,
            # transaction_type et type seront detectes automatiquement par model_validator
            "title": self._extract_title(soup),
            "subtitle": self._extract_subtitle(soup),
            "surface": self._extract_surface(soup),
            "rooms": self._extract_rooms(soup),
            "pdf_link": self._extract_pdf_link(soup),
            "features": self._extract_features(soup),
            "poi": self._extract_poi(soup),
            "images": self._extract_images(soup, url),
        }

        price, price_period = self._extract_price(soup)
        data["price"] = price
        data["price_period"] = price_period

        location_info = self._extract_location(url, soup)
        data.update(location_info)

        return PropertyData(**data)

    def _extract_title(self, soup: BeautifulSoup) -> str:
        title_tag = soup.select_one("h1.estate-title")
        return title_tag.get_text(strip=True) if title_tag else ""

    def _extract_subtitle(self, soup: BeautifulSoup) -> str:
        subtitle_tag = soup.select_one("h2.estate-subtitle")
        return subtitle_tag.get_text(strip=True) if subtitle_tag else ""

    def _extract_price(self, soup: BeautifulSoup) -> tuple[Optional[float], Optional[str]]:
        price_tag = soup.select_one(".estate-price .current-price")
        if not price_tag:
            return None, None
        try:
            txt = price_tag.get_text(strip=True)

            price_period = None
            if "/mois" in txt.lower() or "/ mois" in txt.lower():
                price_period = "mois"
            elif "/an" in txt.lower() or "/ an" in txt.lower():
                price_period = "an"
            else:
                price_period = "unique"

            txt = txt.replace("DT", "").replace("/", "").replace("mois", "").replace("an", "")
            txt = txt.strip()

            if "," in txt and txt.count(",") == 1:
                parts = txt.split(",")
                if len(parts) == 2 and len(parts[1].replace(" ", "")) == 3:
                    txt = txt.replace(",", "")
                else:
                    txt = txt.replace(",", ".")

            txt = txt.replace(" ", "")
            return float(txt), price_period
        except (ValueError, AttributeError):
            return None, None

    def _extract_surface(self, soup: BeautifulSoup) -> Optional[float]:
        surface_tag = soup.select_one(".estate-card-data-element.estate-card-surface span")
        if not surface_tag:
            return None
        try:
            txt = surface_tag.get_text(strip=True)
            txt = txt.replace("m²", "").replace(" ", "").replace(",", ".")
            return float(txt)
        except (ValueError, AttributeError):
            return None

    def _extract_rooms(self, soup: BeautifulSoup) -> Optional[int]:
        rooms_tag = soup.select_one(".estate-card-data-element.estate-card-rooms span")
        if not rooms_tag:
            return None
        try:
            txt = rooms_tag.get_text(strip=True)
            return int(''.join(filter(str.isdigit, txt)))
        except (ValueError, AttributeError):
            return None

    def _extract_pdf_link(self, soup: BeautifulSoup) -> Optional[str]:
        pdf_tag = soup.select_one("a.print-pdf")
        if pdf_tag and "href" in pdf_tag.attrs:
            return urljoin(self.base_url, pdf_tag["href"])
        return None

    def _extract_features(self, soup: BeautifulSoup) -> dict:
        features = {}
        for row in soup.select(".estate-features .row"):
            key_tag = row.select_one(".estate-features-title")
            val_tag = row.select_one(".estate-features-value")
            if key_tag and val_tag:
                key = key_tag.get_text(strip=True).rstrip(":").strip()
                val = val_tag.get_text(strip=True)
                if key and val:
                    features[key] = val
        return features

    def _extract_poi(self, soup: BeautifulSoup) -> dict:
        poi = {}
        for cat in soup.select(".poi-category"):
            cat_name_tag = cat.select_one("h3")
            cat_name = cat_name_tag.get_text(strip=True) if cat_name_tag else "Autre"
            poi_list = []
            for row in cat.select(".row"):
                cols = row.find_all("div")
                if len(cols) >= 2:
                    name = cols[0].get_text(strip=True)
                    dist = cols[1].get_text(strip=True)
                    if name and dist:
                        poi_list.append(f"{name} ({dist})")
            if poi_list:
                poi[cat_name] = poi_list
        return poi

    def _extract_images(self, soup: BeautifulSoup, property_url: str) -> List[PropertyImage]:
        images = []
        for img in soup.select("img"):
            img_url = img.get("data-src") or img.get("src")
            if img_url and "/estates/" in img_url:
                full_url = urljoin(self.base_url, img_url)
                hash_val = md5(full_url.encode()).hexdigest()
                image = PropertyImage(
                    url=full_url,
                    hash=hash_val,
                    local_path=None,
                    downloaded=False
                )
                images.append(image)
        return images

    def _extract_location(self, url: str, soup: BeautifulSoup) -> dict:
        location = {"region": None, "zone": None, "city": None, "municipality": None}
        url_parts = url.split("/")

        try:
            if len(url_parts) >= 6:
                region_str = url_parts[5]

                for region in Region:
                    if region.value == region_str:
                        location["region"] = region
                        break

                if len(url_parts) >= 7:
                    municipality_str = url_parts[6].replace(".html", "")
                    if not municipality_str.isdigit() and municipality_str != region_str:
                        location["municipality"] = municipality_str

            if location["region"]:
                if location["region"] in [Region.BIZERTE, Region.CAP_BON, Region.GRAND_TUNIS]:
                    location["zone"] = ZoneGeographique.NORD_EST
                elif location["region"] in [Region.SOUSSE, Region.MONASTIR, Region.MAHDIA, Region.SFAX]:
                    location["zone"] = ZoneGeographique.CENTRE_EST
                elif location["region"] == Region.KAIROUAN:
                    location["zone"] = ZoneGeographique.CENTRE_OUEST

        except Exception as e:
            print(f"        Erreur extraction location: {e}")

        return location

    def download_images(self, property_data: PropertyData) -> PropertyData:
        property_id = property_data.property_id
        folder = os.path.join(self.img_dir, property_id)
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
                print(f"      Image telechargee: {filename}")
            except Exception as e:
                print(f"      Echec telechargement {image.url}: {e}")
                continue
            time.sleep(random.uniform(0.5, 1.5))
        return property_data


# GESTIONNAIRE BASE DE DONNEES
class DatabaseManager:
    def __init__(self, dbname: str, user: str, password: str,
                 host: str = "localhost", port: str = "5432"):
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
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
        CREATE TABLE IF NOT EXISTS scraping_stats (
            id SERIAL PRIMARY KEY,
            total_properties INTEGER,
            total_images INTEGER,
            total_errors INTEGER,
            regions_scraped JSONB,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            duration_seconds REAL
        )
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_properties_region ON properties(region);
        CREATE INDEX IF NOT EXISTS idx_properties_zone ON properties(zone);
        CREATE INDEX IF NOT EXISTS idx_properties_type ON properties(type);
        CREATE INDEX IF NOT EXISTS idx_properties_transaction ON properties(transaction_type);
        CREATE INDEX IF NOT EXISTS idx_properties_price ON properties(price);
        CREATE INDEX IF NOT EXISTS idx_images_property ON images(property_id);
        """)

        self.conn.commit()
        cur.close()
        print("Tables creees/verifiees")

    def insert_property(self, property_data: PropertyData) -> bool:
        cur = self.conn.cursor()
        try:
            features_json = json.dumps(property_data.features, ensure_ascii=False)
            poi_json = json.dumps(property_data.poi, ensure_ascii=False)
            image_paths_json = json.dumps(
                [img.local_path for img in property_data.images if img.local_path],
                ensure_ascii=False
            )

            query = """
            INSERT INTO properties
            (property_id, url, transaction_type, type, title, subtitle, price, price_period, surface, rooms,
             region, zone, city, municipality, pdf_link, features, poi, image_paths,
             scraped_at, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (property_id)
            DO UPDATE SET
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
            """

            transaction_val = property_data.transaction_type.value if hasattr(property_data.transaction_type, 'value') else str(property_data.transaction_type)
            type_val = property_data.type.value if hasattr(property_data.type, 'value') else str(property_data.type)
            region_val = property_data.region.value if property_data.region and hasattr(property_data.region, 'value') else (str(property_data.region) if property_data.region else None)
            zone_val = property_data.zone.value if property_data.zone and hasattr(property_data.zone, 'value') else (str(property_data.zone) if property_data.zone else None)

            cur.execute(query, (
                property_data.property_id, property_data.url, transaction_val, type_val,
                property_data.title, property_data.subtitle, property_data.price,
                property_data.price_period, property_data.surface, property_data.rooms,
                region_val, zone_val, property_data.city, property_data.municipality,
                property_data.pdf_link, features_json, poi_json, image_paths_json,
                property_data.scraped_at, property_data.last_updated
            ))

            if property_data.images:
                image_query = """
                INSERT INTO images (property_id, url, local_path, hash, downloaded)
                VALUES %s
                ON CONFLICT (property_id, hash) DO NOTHING
                """
                image_values = [
                    (property_data.property_id, img.url, img.local_path,
                     img.hash, img.downloaded)
                    for img in property_data.images
                ]
                execute_values(cur, image_query, image_values)

            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(f"Erreur insertion BDD: {e}")
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
        stats = {
            "total_properties": 0,
            "total_images": 0,
            "by_region": {},
            "by_type": {},
            "by_transaction": {},
            "price_stats": {}
        }

        cur.execute("SELECT COUNT(*) FROM properties")
        stats["total_properties"] = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM images WHERE downloaded = TRUE")
        stats["total_images"] = cur.fetchone()[0]

        cur.execute("""
            SELECT region, COUNT(*) as count
            FROM properties
            WHERE region IS NOT NULL
            GROUP BY region
            ORDER BY count DESC
        """)
        stats["by_region"] = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute("""
            SELECT type, COUNT(*) as count
            FROM properties
            GROUP BY type
            ORDER BY count DESC
        """)
        stats["by_type"] = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute("""
            SELECT transaction_type, COUNT(*) as count
            FROM properties
            GROUP BY transaction_type
            ORDER BY count DESC
        """)
        stats["by_transaction"] = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute("""
            SELECT
                MIN(price) as min_price,
                MAX(price) as max_price,
                AVG(price) as avg_price,
                COUNT(*) FILTER (WHERE price IS NOT NULL) as properties_with_price
            FROM properties
        """)
        row = cur.fetchone()
        stats["price_stats"] = {
            "min": row[0],
            "max": row[1],
            "avg": row[2],
            "count": row[3]
        }

        cur.close()
        return stats

    def save_scraping_stats(self, stats: ScrapingStats):
        cur = self.conn.cursor()
        regions_json = json.dumps(stats.regions_scraped, ensure_ascii=False)
        cur.execute("""
            INSERT INTO scraping_stats
            (total_properties, total_images, total_errors, regions_scraped,
             start_time, end_time, duration_seconds)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            stats.total_properties, stats.total_images, stats.total_errors,
            regions_json, stats.start_time, stats.end_time, stats.duration_seconds
        ))
        self.conn.commit()
        cur.close()

    def close(self):
        if self.conn:
            self.conn.close()


# AGENT PRINCIPAL
class TecnocasaAgent:
    def __init__(self, db_config: dict, img_dir: str = "data/images",
                 csv_dir: str = "snapshots", headless: bool = True, max_retries: int = 3):
        self.base_url = "https://www.tecnocasa.tn"
        self.img_dir = img_dir
        self.csv_dir = csv_dir
        self.headless = headless
        self.max_retries = max_retries

        os.makedirs(img_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        self.db = DatabaseManager(**db_config)
        self.property_scraper = PropertyScraper(self.base_url, img_dir)

        self.stats = ScrapingStats()
        self.scraped_urls: Set[str] = set()
        self.all_properties: List[PropertyData] = []

    def run(self, max_pages_per_region: int = 50, download_images: bool = True,
            include_vente: bool = True, include_location: bool = True):
        print("\n" + "="*80)
        print("AGENT TECNOCASA - DEMARRAGE")
        print("="*80)

        self.stats.start_time = datetime.now()
        tasks = URLGenerator.generate_all_tasks(
            max_pages_per_region,
            include_vente=include_vente,
            include_location=include_location
        )

        print(f"\n{len(tasks)} regions a scraper")
        print(f"Vente: {'Oui' if include_vente else 'Non'}")
        print(f"Location: {'Oui' if include_location else 'Non'}")
        print(f"Images: {self.img_dir}")
        print(f"Base de donnees: PostgreSQL")
        print(f"Telechargement images: {'Oui' if download_images else 'Non'}")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = context.new_page()

            for task in tasks:
                print(f"\n{'─'*80}")
                print(f"REGION: {task.region.value.upper().replace('-', ' ')}")
                print(f"   Zone: {task.zone.value}")
                print(f"   Transaction: {task.transaction_type.value.upper()}")
                print(f"   URL: {task.start_url}")
                print(f"{'─'*80}")

                try:
                    self._scrape_region(page, task, download_images)
                    task.completed = True
                    self.stats.regions_scraped.append(task.region.value)
                except Exception as e:
                    print(f"Erreur region {task.region.value}: {e}")
                    task.errors.append(str(e))
                    self.stats.total_errors += 1

                time.sleep(random.uniform(10, 20))

            browser.close()

        self._finalize()

    def _scrape_region(self, page: Page, task: ScrapingTask, download_images: bool):
        page_num = 1
        consecutive_empty = 0
        consecutive_all_scraped = 0

        while page_num <= task.max_pages and consecutive_empty < MAX_CONSECUTIVE_EMPTY_PAGES:
            if page_num == 1:
                url = task.start_url
            else:
                url = task.start_url.replace('.html', f'.html/pag-{page_num}')

            print(f"\n  Page {page_num}/{task.max_pages}")

            try:
                page.goto(url, timeout=120000, wait_until="domcontentloaded")
                page.wait_for_selector("body", timeout=45000)
                time.sleep(random.uniform(3, 7))

                property_urls = self._extract_property_urls(page, task.transaction_type)

                if not property_urls:
                    consecutive_empty += 1
                    print(f"     Aucune annonce (page vide {consecutive_empty}/{MAX_CONSECUTIVE_EMPTY_PAGES})")
                    if consecutive_empty >= MAX_CONSECUTIVE_EMPTY_PAGES:
                        print(f"     Fin pagination atteinte")
                        break
                    page_num += 1
                    continue

                consecutive_empty = 0
                print(f"     {len(property_urls)} annonces detectees")

                new_properties_count = 0
                for prop_url in property_urls:
                    if prop_url in self.scraped_urls:
                        print(f"     Deja scrapee: {prop_url.split('/')[-1]}")
                        continue

                    property_data = self._scrape_property(page, prop_url, download_images)

                    if property_data:
                        task.properties_found += 1
                        self.stats.total_properties += 1
                        self.all_properties.append(property_data)
                        self.scraped_urls.add(prop_url)
                        new_properties_count += 1

                if new_properties_count == 0:
                    consecutive_all_scraped += 1
                    print(f"     Toutes les annonces deja scrapees ({consecutive_all_scraped}/3)")
                    if consecutive_all_scraped >= 3:
                        print(f"     Toutes les annonces sont deja en BDD - fin region")
                        break
                else:
                    consecutive_all_scraped = 0

                page_num += 1
                time.sleep(random.uniform(5, 12))

            except Exception as e:
                print(f"     Erreur page {page_num}: {e}")
                task.errors.append(f"Page {page_num}: {str(e)}")
                consecutive_empty += 1
                if consecutive_empty >= MAX_CONSECUTIVE_EMPTY_PAGES:
                    break
                page_num += 1

        print(f"\n  Region terminee: {task.properties_found} biens trouves")

    def _extract_property_urls(self, page: Page, transaction_type: TransactionType) -> List[str]:
        if transaction_type == TransactionType.LOCATION:
            links = page.query_selector_all("a[href*='/louer/']")
            property_types = [
                "/louer/appartement/", "/louer/villa/", "/louer/terrain/",
                "/louer/immeubles/", "/louer/entrepot/", "/louer/ferme/",
                "/louer/bureau/", "/louer/local-commercial/", "/louer/local-industriel/",
            ]
        else:
            links = page.query_selector_all("a[href*='/vendre/']")
            property_types = [
                "/vendre/appartement/", "/vendre/villa/", "/vendre/terrain/",
                "/vendre/immeubles/", "/vendre/entrepot/", "/vendre/ferme/",
                "/vendre/bureau/", "/vendre/local-commercial/", "/vendre/local-industriel/",
            ]

        found_urls = []
        for link in links:
            href = link.get_attribute("href")
            if href and any(pt in href for pt in property_types) and href.endswith(".html"):
                last_part = href.rstrip('/').split('/')[-1].replace('.html', '')
                if last_part.isdigit():
                    full_url = urljoin(self.base_url, href)
                    found_urls.append(full_url)

        return list(set(found_urls))

    def _scrape_property(self, page: Page, url: str, download_images: bool) -> PropertyData | None:
        property_id = url.split("/")[-1].replace(".html", "")

        if self.db.property_exists(property_id):
            print(f"     Existe en BDD: {property_id}")
            return None

        transaction_debug = "LOCATION" if "/louer/" in url.lower() else "VENTE"
        print(f"     Scraping: {property_id} (Type: {transaction_debug})")

        for attempt in range(self.max_retries):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=90000)
                page.wait_for_selector("body", timeout=30000)
                time.sleep(random.uniform(2, 5))

                content = page.content()

                if len(content) < 8000:
                    print(f"        Page trop courte ({len(content)} chars)")
                    return None

                property_data = self.property_scraper.scrape(content, url)

                if download_images and property_data.images:
                    print(f"        Telechargement {len(property_data.images)} images...")
                    property_data = self.property_scraper.download_images(property_data)
                    self.stats.total_images += len([img for img in property_data.images if img.downloaded])

                success = self.db.insert_property(property_data)

                if success:
                    print(f"        Sauvegarde: {property_data.title[:50]}")
                    return property_data
                else:
                    print(f"        Echec BDD")
                    return None

            except Exception as e:
                print(f"        Tentative {attempt + 1}/{self.max_retries}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(random.uniform(5, 10))
                else:
                    self.stats.total_errors += 1
                    return None

    def _finalize(self):
        self.stats.end_time = datetime.now()

        print("\n" + "="*80)
        print("RESULTATS DU SCRAPING")
        print("="*80)
        print(f"Proprietes scrapees: {self.stats.total_properties}")
        print(f"Images telechargees: {self.stats.total_images}")
        print(f"Regions couvertes: {len(self.stats.regions_scraped)}")
        print(f"Erreurs: {self.stats.total_errors}")

        if self.stats.duration_seconds:
            print(f"Duree: {self.stats.duration_seconds / 60:.1f} minutes")

        self.db.save_scraping_stats(self.stats)

        if self.all_properties:
            df_data = []
            for prop in self.all_properties:
                df_data.append({
                    'property_id': prop.property_id,
                    'url': prop.url,
                    'transaction_type': prop.transaction_type,
                    'type': prop.type,
                    'title': prop.title,
                    'subtitle': prop.subtitle,
                    'price': prop.price,
                    'price_period': prop.price_period,
                    'surface': prop.surface,
                    'rooms': prop.rooms,
                    'region': prop.region.value if prop.region else None,
                    'zone': prop.zone.value if prop.zone else None,
                    'city': prop.city,
                    'municipality': prop.municipality,
                    'nb_images': len(prop.images),
                    'scraped_at': prop.scraped_at
                })

            df = pd.DataFrame(df_data)
            snapshot_file = os.path.join(
                self.csv_dir,
                f"tecnocasa_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            )
            df.to_csv(snapshot_file, index=False, encoding='utf-8-sig')
            print(f"\nSnapshot CSV: {snapshot_file}")

        db_stats = self.db.get_stats()
        print("\nSTATISTIQUES BASE DE DONNEES:")
        print(f"   Total en BDD: {db_stats['total_properties']} proprietes")
        print(f"   Images stockees: {db_stats['total_images']}")

        if db_stats['by_transaction']:
            print("\n   Par type de transaction:")
            for trans_type, count in sorted(db_stats['by_transaction'].items(), key=lambda x: x[1], reverse=True):
                print(f"     {trans_type}: {count}")

        if db_stats['by_type']:
            print("\n   Par type de bien:")
            for ptype, count in sorted(db_stats['by_type'].items(), key=lambda x: x[1], reverse=True):
                print(f"     {ptype}: {count}")

        if db_stats['price_stats']['count'] > 0:
            print("\n   Prix:")
            print(f"     Min: {db_stats['price_stats']['min']:,.0f} DT")
            print(f"     Max: {db_stats['price_stats']['max']:,.0f} DT")
            print(f"     Moyenne: {db_stats['price_stats']['avg']:,.0f} DT")

        print("\n" + "="*80)
        print("SCRAPING TERMINE")
        print("="*80 + "\n")


def main():
    agent = TecnocasaAgent(
        db_config=DB_CONFIG,
        img_dir=IMG_DIR,
        csv_dir=CSV_DIR,
        headless=HEADLESS,
        max_retries=MAX_RETRIES
    )

    agent.run(
        max_pages_per_region=MAX_PAGES_PER_REGION,
        download_images=DOWNLOAD_IMAGES,
        include_vente=SCRAPE_VENTE,
        include_location=SCRAPE_LOCATION
    )


if __name__ == "__main__":
    main()