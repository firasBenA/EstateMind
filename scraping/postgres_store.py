from __future__ import annotations
from typing import Iterable, Set
from psycopg import connect
from psycopg.rows import dict_row
from scraping.models import Listing
import json

class PostgresStore:
    def __init__(self, db_url: str):
        self.db_url = db_url

    def load_seen(self) -> Set[str]:
        seen: Set[str] = set()
        sql = "SELECT source, source_listing_id FROM listings;"
        with connect(self.db_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql)
                for row in cur.fetchall():
                    seen.add(f"{row['source']}:{row['source_listing_id']}")
        return seen

    def save_seen(self, seen: Set[str]) -> None:
        return

    def save(self, listings: Iterable[Listing]) -> None:
        rows = []
        for l in listings:
            d = l.model_dump(mode="json")
            # convert dict fields to JSON string
            d["poi"] = json.dumps(d.get("poi", []))  # met vide si None
            d["image_urls"] = json.dumps(d.get("image_urls", []))
            # ajouter lat/lon si elles existent dans l'objet Listing
            d["lat"] = d.get("lat")
            d["lon"] = d.get("lon")
            rows.append(d)

        with connect(self.db_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                for row in rows:
                    # 1️⃣ Check current price
                    cur.execute("""
                        SELECT price
                        FROM listings
                        WHERE source = %s
                        AND source_listing_id = %s
                    """, (row["source"], row["source_listing_id"]))

                    existing = cur.fetchone()
                    old_price = existing["price"] if existing else None

                    # 2️⃣ Upsert listing
                    cur.execute("""
                        INSERT INTO listings (
                            source,
                            source_listing_id,
                            transaction,
                            url,
                            image_urls,
                            title,
                            price,
                            currency,
                            governorate,
                            city,
                            zone,
                            property_type,
                            surface_m2,
                            rooms,
                            bathrooms,
                            poi,
                            lat,
                            lon,
                            posted_at,
                            scraped_at
                        )
                        VALUES (
                            %(source)s,
                            %(source_listing_id)s,
                            %(transaction)s,
                            %(url)s,
                            %(image_urls)s,
                            %(title)s,
                            %(price)s,
                            %(currency)s,
                            %(governorate)s,
                            %(city)s,
                            %(zone)s,
                            %(property_type)s,
                            %(surface_m2)s,
                            %(rooms)s,
                            %(bathrooms)s,
                            %(poi)s,
                            %(lat)s,
                            %(lon)s,
                            %(posted_at)s,
                            %(scraped_at)s
                        )
                        ON CONFLICT (source, source_listing_id)
                        DO UPDATE SET
                            transaction = EXCLUDED.transaction,
                            url = EXCLUDED.url,
                            image_urls = EXCLUDED.image_urls,
                            title = EXCLUDED.title,
                            price = EXCLUDED.price,
                            currency = EXCLUDED.currency,
                            governorate = EXCLUDED.governorate,
                            city = EXCLUDED.city,
                            zone = EXCLUDED.zone,
                            property_type = EXCLUDED.property_type,
                            surface_m2 = EXCLUDED.surface_m2,
                            rooms = EXCLUDED.rooms,
                            bathrooms = EXCLUDED.bathrooms,
                            poi = EXCLUDED.poi,
                            lat = EXCLUDED.lat,
                            lon = EXCLUDED.lon,
                            posted_at = EXCLUDED.posted_at,
                            scraped_at = EXCLUDED.scraped_at;
                    """, row)

                    # 3️⃣ If price changed → insert history
                    if row["price"] is not None and row["price"] != old_price:
                        cur.execute("""
                            INSERT INTO price_history (
                                source,
                                source_listing_id,
                                price,
                                currency
                            )
                            VALUES (%s, %s, %s, %s)
                        """, (
                            row["source"],
                            row["source_listing_id"],
                            row["price"],
                            row["currency"]
                        ))
            conn.commit()

    def save_lat_lon(self, listing_id: str, lat: float, lon: float) -> None:
        sql = """
            UPDATE listings
            SET lat = %s, lon = %s
            WHERE source_listing_id = %s;
        """
        with connect(self.db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (lat, lon, listing_id))
            conn.commit()
