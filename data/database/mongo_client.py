import psycopg2
from psycopg2.extras import Json, RealDictCursor
from config.settings import settings
from config.logging_config import log
from typing import Optional, Dict, Any, Tuple
from core.geolocation import infer_region_and_zone
from loguru import logger
class PostgresClient:
    def __init__(self):
        try:
            self.conn = psycopg2.connect(
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT,
                dbname=settings.POSTGRES_DB,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
            )
            self.conn.autocommit = False
            self._setup_schema()
            log.info("Connected to PostgreSQL")
        except Exception as e:
            log.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def _setup_schema(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS listings (
                    id SERIAL PRIMARY KEY,
                    property_id TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    type TEXT,
                    title TEXT NOT NULL,
                    description TEXT,
                    price DOUBLE PRECISION,
                    surface DOUBLE PRECISION,
                    rooms INTEGER,
                    region TEXT,
                    zone TEXT,
                    city TEXT,
                    municipalite TEXT,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    pdf_link TEXT,
                    images JSONB,
                    features JSONB,
                    scraped_at TIMESTAMP,
                    last_update TIMESTAMP,
                    transaction_type TEXT,
                    currency TEXT,
                    raw_data_path TEXT,
                    poi JSONB,
                    etl_status VARCHAR(50) DEFAULT 'pending',
                    etl_processed_at TIMESTAMP,
                    UNIQUE (source_name, property_id)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_metrics (
                    id SERIAL PRIMARY KEY,
                    run_started_at TIMESTAMP,
                    run_finished_at TIMESTAMP,
                    source_name TEXT,
                    strategy TEXT,
                    fetched INTEGER,
                    inserted INTEGER,
                    updated INTEGER,
                    unchanged INTEGER,
                    errors INTEGER,
                    consecutive_failures INTEGER,
                    disabled_until TIMESTAMP
                )
                """
            )
        self.conn.commit()

    def insert_listing(self, listing_data: Dict[str, Any]) -> bool:
        try:
            with self.conn.cursor() as cur:
                payload = self._prepare_payload(listing_data)
                cur.execute(
                    """
                    INSERT INTO listings (
                        property_id,
                        source_name,
                        url,
                        type,
                        title,
                        description,
                        price,
                        surface,
                        rooms,
                        region,
                        zone,
                        city,
                        municipalite,
                        latitude,
                        longitude,
                        pdf_link,
                        images,
                        features,
                        scraped_at,
                        last_update,
                        transaction_type,
                        currency,
                        raw_data_path,
                        poi
                    )
                    VALUES (
                        %(property_id)s,
                        %(source_name)s,
                        %(url)s,
                        %(type)s,
                        %(title)s,
                        %(description)s,
                        %(price)s,
                        %(surface)s,
                        %(rooms)s,
                        %(region)s,
                        %(zone)s,
                        %(city)s,
                        %(municipalite)s,
                        %(latitude)s,
                        %(longitude)s,
                        %(pdf_link)s,
                        %(images)s,
                        %(features)s,
                        %(scraped_at)s,
                        %(last_update)s,
                        %(transaction_type)s,
                        %(currency)s,
                        %(raw_data_path)s,
                        %(poi)s
                    )
                    """,
                    payload,
                )
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            log.error(f"Error inserting listing: {e}")
            return False

    def upsert_listing(self, listing) -> bool:
        """Insert or update a single listing."""
        try:
            with self.conn.cursor() as cur:
                # ✅ CHANGE: property_id → source_id
                cur.execute("""
                    SELECT 1 FROM listings 
                    WHERE source_name = %s AND source_id = %s
                """, (listing.source_name, listing.source_id))
                
                exists = cur.fetchone() is not None
                
                if exists:
                    # UPDATE
                    cur.execute("""
                        UPDATE listings SET
                            url = %s,
                            title = %s,
                            price = %s,
                            surface = %s,
                            rooms = %s,
                            city = %s,
                            region = %s,
                            transaction_type = %s,
                            property_type = %s,
                            last_updated = NOW()
                        WHERE source_name = %s AND source_id = %s
                    """, (
                        listing.url,
                        listing.title,
                        listing.price,
                        listing.surface_area_m2,
                        listing.rooms,
                        listing.location.city if listing.location else None,
                        listing.location.governorate if listing.location else None,
                        listing.transaction_type,
                        getattr(listing, 'property_type', 'Other'),
                        listing.source_name,
                        listing.source_id
                    ))
                else:
                    # INSERT
                    import hashlib
                    unique_id = hashlib.md5(f"{listing.source_name}:{listing.source_id}".encode()).hexdigest()[:16]
                    
                    cur.execute("""
                        INSERT INTO listings (
                            id, source_id, source_name, url, title, description,
                            price, currency, transaction_type, property_type,
                            rooms, city, region, surface,
                            scraped_at, last_updated
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            NOW(), NOW()
                        )
                    """, (
                        unique_id,
                        listing.source_id,
                        listing.source_name,
                        listing.url,
                        listing.title,
                        listing.description,
                        listing.price,
                        listing.currency,
                        listing.transaction_type,
                        getattr(listing, 'property_type', 'Other'),
                        listing.rooms,
                        listing.location.city if listing.location else None,
                        listing.location.governorate if listing.location else None,
                        listing.surface_area_m2
                    ))
                
                self.conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error upserting listing: {e}")
            self.conn.rollback()
            return False

    def log_agent_metrics(self, record: Dict[str, Any]) -> None:
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO agent_metrics (
                        run_started_at,
                        run_finished_at,
                        source_name,
                        strategy,
                        fetched,
                        inserted,
                        updated,
                        unchanged,
                        errors,
                        consecutive_failures,
                        disabled_until
                    )
                    VALUES (
                        %(run_started_at)s,
                        %(run_finished_at)s,
                        %(source_name)s,
                        %(strategy)s,
                        %(fetched)s,
                        %(inserted)s,
                        %(updated)s,
                        %(unchanged)s,
                        %(errors)s,
                        %(consecutive_failures)s,
                        %(disabled_until)s
                    )
                    """,
                    record,
                )
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            log.error(f"Error logging agent metrics: {e}")

    def listing_exists(self, source_name: str, source_id: str) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM listings
                    WHERE source_name = %s AND property_id = %s
                    LIMIT 1
                    """,
                    (source_name, source_id),
                )
                return cur.fetchone() is not None
        except Exception as e:
            log.error(f"Error checking listing existence: {e}")
            return False

    def check_duplicate(self, listing) -> bool:
        """Check if listing exists."""
        try:
            with self.conn.cursor() as cur:
                # ✅ CHANGE: property_id → source_id
                cur.execute("""
                    SELECT 1 FROM listings 
                    WHERE source_name = %s AND source_id = %s
                """, (listing.source_name, listing.source_id))
                return cur.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking duplicate: {e}")
            return False

    def iterate_listings(self, batch_size: int = 500):
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.itersize = batch_size
                cur.execute(
                    """
                    SELECT
                        id,
                        source_name,
                        property_id,
                        url,
                        title,
                        description,
                        features,
                        raw_data_path,
                        city,
                        municipalite,
                        latitude,
                        longitude,
                        poi
                    FROM listings
                    """
                )
                for row in cur:
                    yield row
        except Exception as e:
            log.error(f"Error iterating listings: {e}")

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def update_listing_features(self, listing_id: int, features) -> None:
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE listings
                    SET features = %s
                    WHERE id = %s
                    """,
                    (Json(features), listing_id),
                )
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            log.error(f"Error updating listing features for id={listing_id}: {e}")

    def update_listing_images_features(self, listing_id: int, images, features) -> None:
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE listings
                    SET images = %s,
                        features = %s
                    WHERE id = %s
                    """,
                    (Json(images), Json(features), listing_id),
                )
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            log.error(f"Error updating listing images/features for id={listing_id}: {e}")

    def update_listing_geolocation(self, listing_id: int, latitude, longitude, municipality, poi) -> None:
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE listings
                    SET latitude = %s,
                        longitude = %s,
                        municipalite = %s,
                        poi = %s
                    WHERE id = %s
                    """,
                    (latitude, longitude, municipality, Json(poi) if poi is not None else Json([]), listing_id),
                )
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            log.error(f"Error updating geolocation for id={listing_id}: {e}")

    def _prepare_payload(self, listing_data: Dict[str, Any]) -> Dict[str, Any]:
        location = listing_data.get("location") or {}
        pois = listing_data.get("pois") or []
        images = listing_data.get("images") or []
        features = listing_data.get("features") or []
        region, zone = self._infer_region_and_zone(location)
        last_update = listing_data.get("last_update") or listing_data.get("published_at") or listing_data.get("scraped_at")
        return {
            "property_id": listing_data.get("source_id"),
            "source_name": listing_data.get("source_name"),
            "url": listing_data.get("url"),
            "type": listing_data.get("property_type"),
            "title": listing_data.get("title"),
            "description": listing_data.get("description"),
            "price": listing_data.get("price"),
            "surface": listing_data.get("surface_area_m2"),
            "rooms": listing_data.get("rooms"),
            "region": region,
            "zone": zone,
            "city": location.get("city"),
            "municipalite": location.get("district"),
            "latitude": location.get("latitude"),
            "longitude": location.get("longitude"),
            "pdf_link": listing_data.get("pdf_link"),
            "images": Json(images),
            "features": Json(features),
            "scraped_at": listing_data.get("scraped_at"),
            "last_update": last_update,
            "transaction_type": listing_data.get("transaction_type"),
            "currency": listing_data.get("currency"),
            "raw_data_path": listing_data.get("raw_data_path"),
            "poi": Json(pois),
        }

    def _infer_region_and_zone(self, location: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        region_name, zone = infer_region_and_zone(location)
        return region_name, zone
