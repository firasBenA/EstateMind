import os
from typing import Dict, Any, Optional, Tuple

import psycopg2
from psycopg2.extras import Json


class PostgresClient:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            dbname=os.getenv("POSTGRES_DB", "estatemind"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "ameni"),
            options="-c client_encoding=UTF8"
        )
        self.conn.autocommit = False
        self._setup_schema()

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
                    UNIQUE (source_name, property_id)
                )
                """
            )
        self.conn.commit()

    def upsert_listing(self, listing_data: Dict[str, Any]) -> str:
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
                    ON CONFLICT (source_name, property_id)
                    DO UPDATE SET
                        url = EXCLUDED.url,
                        title = EXCLUDED.title,
                        description = EXCLUDED.description,
                        price = EXCLUDED.price,
                        surface = EXCLUDED.surface,
                        rooms = EXCLUDED.rooms,
                        region = EXCLUDED.region,
                        zone = EXCLUDED.zone,
                        city = EXCLUDED.city,
                        municipalite = EXCLUDED.municipalite,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        pdf_link = EXCLUDED.pdf_link,
                        images = EXCLUDED.images,
                        features = EXCLUDED.features,
                        scraped_at = EXCLUDED.scraped_at,
                        last_update = EXCLUDED.last_update,
                        transaction_type = EXCLUDED.transaction_type,
                        currency = EXCLUDED.currency,
                        raw_data_path = EXCLUDED.raw_data_path,
                        poi = EXCLUDED.poi
                    """,
                    payload,
                )
            self.conn.commit()
            return "updated"
        except Exception:
            self.conn.rollback()
            return "error"

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def _prepare_payload(self, listing_data: Dict[str, Any]) -> Dict[str, Any]:
        location = listing_data.get("location") or {}
        images = listing_data.get("images") or []
        features = listing_data.get("features") or []
        pois = listing_data.get("pois") or []
        region = location.get("governorate")
        zone = listing_data.get("zone")
        last_update = listing_data.get("last_update") or listing_data.get("scraped_at")
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

