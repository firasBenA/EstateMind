# database/vector_db.py
"""
EstateMind — pgvector handler.
Includes ALL columns from actual schema.
"""

from __future__ import annotations

import os
import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor
from loguru import logger

from core.models import PropertyListing  # ✅ IMPORT IS HERE


# ─── Embedding Configuration ──────────────────────────────────────────────────

EMBEDDING_CONFIG = {
    "huggingface": {
        "model_name": "paraphrase-multilingual-MiniLM-L12-v2",
        "dimension": 384,
    },
    "clip": {
        "model_name": "clip-ViT-B-32",
        "dimension": 512,
    },
}


class EmbeddingModel:
    """Singleton for embedding models."""
    
    _text_model = None
    _image_model = None
    
    @classmethod
    def get_text_model(cls):
        if cls._text_model is None:
            from sentence_transformers import SentenceTransformer
            cls._text_model = SentenceTransformer(EMBEDDING_CONFIG["huggingface"]["model_name"])
            logger.info("Text embedding model loaded")
        return cls._text_model
    
    @classmethod
    def get_image_model(cls):
        if cls._image_model is None:
            from sentence_transformers import SentenceTransformer
            cls._image_model = SentenceTransformer(EMBEDDING_CONFIG["clip"]["model_name"])
            logger.info("Image embedding model loaded")
        return cls._image_model
    
    @classmethod
    def embed_text(cls, texts: List[str]) -> List[List[float]]:
        model = cls.get_text_model()
        return model.encode(texts, convert_to_numpy=True).tolist()
    
    @classmethod
    def embed_images(cls, image_urls: List[str]) -> List[List[float]]:
        model = cls.get_image_model()
        return model.encode(image_urls, convert_to_numpy=True).tolist()


class VectorDBHandler:
    """pgvector handler with ALL columns from actual schema."""
    
    def __init__(self, strategy: str = "huggingface", **kwargs):
        self.strategy = strategy
        self.dimension = EMBEDDING_CONFIG.get(strategy, {}).get("dimension", 384)
        
        # Load environment
        try:
            from dotenv import load_dotenv
            data_dir = Path(__file__).resolve().parents[1]
            load_dotenv(data_dir / ".env")
        except Exception:
            pass
        
        self.conn = self._connect_pg()
        self._setup_schema()
        
        logger.info(f"pgvector handler ready (dim={self.dimension})")
    
    def _connect_pg(self):
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DATABASE", "estatemind"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
        )
    
    def _setup_schema(self):
        """Ensure schema is ready."""
        with self.conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            cur.execute(f"""
                ALTER TABLE listings 
                ADD COLUMN IF NOT EXISTS text_embedding vector({self.dimension});
            """)
            
            cur.execute("SELECT COUNT(*) FROM listings WHERE text_embedding IS NOT NULL")
            count = cur.fetchone()[0]
            if count >= 100:
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS listings_text_embedding_idx 
                    ON listings USING ivfflat (text_embedding vector_cosine_ops)
                    WITH (lists = 100);
                """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS image_embeddings (
                    id TEXT PRIMARY KEY,
                    listing_id TEXT,
                    image_url TEXT,
                    image_index INTEGER,
                    embedding vector(512),
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            
            self.conn.commit()
    
    def _embed_text(self, texts: List[str]) -> List[List[float]]:
        return EmbeddingModel.embed_text(texts)
    
    def _embed_images(self, image_urls: List[str]) -> List[List[float]]:
        return EmbeddingModel.embed_images(image_urls)
    
    def upsert_listing(self, listing: PropertyListing) -> bool:
        """Insert or update a single listing with ALL columns."""
        try:
            # Generate text embedding
            text = listing.to_embedding_text()
            embedding = self._embed_text([text])[0]
            
            # Build vector ID and unique ID
            vector_id = f"{listing.source_name}:{listing.source_id}"
            unique_id = hashlib.md5(vector_id.encode()).hexdigest()[:16]
            
            # Get location info
            loc = listing.location
            property_type = getattr(listing, 'property_type', None) or 'Other'
            
            # Compute price_per_m2
            price_per_m2 = None
            if listing.price and listing.surface_area_m2 and listing.surface_area_m2 > 0:
                price_per_m2 = listing.price / listing.surface_area_m2
            
            # Get POI data
            poi_data = None
            if hasattr(listing, 'pois') and listing.pois:
                poi_data = [{"name": p.name, "category": p.category, "distance": p.distance_m} 
                           for p in listing.pois]
            
            with self.conn.cursor() as cur:
                # Check if exists
                cur.execute("""
                    SELECT id FROM listings 
                    WHERE source_id = %s AND source_name = %s
                """, (listing.source_id, listing.source_name))
                
                row = cur.fetchone()
                
                if row:
                    # UPDATE
                    existing_id = row[0]
                    cur.execute("""
                        UPDATE listings SET
                            url = %s,
                            title = %s,
                            description = %s,
                            price = %s,
                            currency = %s,
                            transaction_type = %s,
                            property_type = %s,
                            rooms = %s,
                            region = %s,
                            zone = %s,
                            city = %s,
                            municipality = %s,
                            latitude = %s,
                            longitude = %s,
                            surface = %s,
                            images = %s,
                            images_count = %s,
                            features = %s,
                            poi = %s,
                            price_per_m2 = %s,
                            text_embedding = %s::vector,
                            last_updated = NOW()
                        WHERE id = %s
                    """, (
                        listing.url,
                        listing.title,
                        listing.description,
                        listing.price,
                        listing.currency,
                        listing.transaction_type,
                        property_type,
                        listing.rooms,
                        loc.governorate if loc else None,
                        loc.zone if loc else None,
                        loc.city if loc else None,
                        loc.municipalite if loc else None,
                        loc.latitude if loc else None,
                        loc.longitude if loc else None,
                        listing.surface_area_m2,
                        json.dumps(listing.images) if listing.images else None,
                        len(listing.images) if listing.images else 0,
                        json.dumps(listing.features) if listing.features else None,
                        json.dumps(poi_data) if poi_data else None,
                        price_per_m2,
                        embedding,
                        existing_id
                    ))
                    used_id = existing_id
                else:
                    # INSERT
                    cur.execute("""
                        INSERT INTO listings (
                            id, source_id, source_name, url, title, description,
                            price, currency, transaction_type, property_type,
                            rooms, region, zone, city, municipality,
                            latitude, longitude, surface,
                            images, images_count, features, poi, price_per_m2,
                            text_embedding,
                            scraped_at, last_updated
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s::vector,
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
                        property_type,
                        listing.rooms,
                        loc.governorate if loc else None,
                        loc.zone if loc else None,
                        loc.city if loc else None,
                        loc.municipalite if loc else None,
                        loc.latitude if loc else None,
                        loc.longitude if loc else None,
                        listing.surface_area_m2,
                        json.dumps(listing.images) if listing.images else None,
                        len(listing.images) if listing.images else 0,
                        json.dumps(listing.features) if listing.features else None,
                        json.dumps(poi_data) if poi_data else None,
                        price_per_m2,
                        embedding
                    ))
                    used_id = unique_id
                
                # Handle image embeddings
                if listing.images:
                    image_embeddings = self._embed_images(listing.images[:5])
                    for i, (img_url, img_emb) in enumerate(zip(listing.images[:5], image_embeddings)):
                        img_id = f"{used_id}_img_{i}"
                        cur.execute("""
                            INSERT INTO image_embeddings (id, listing_id, image_url, image_index, embedding)
                            VALUES (%s, %s, %s, %s, %s::vector)
                            ON CONFLICT (id) DO UPDATE SET
                                embedding = EXCLUDED.embedding,
                                image_url = EXCLUDED.image_url
                        """, (img_id, used_id, img_url, i, str(img_emb)))
                
                self.conn.commit()
            
            logger.debug(f"Upserted listing: {vector_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upsert {listing.source_id}: {e}")
            self.conn.rollback()
            return False
    
    def check_duplicate(self, listing: PropertyListing, threshold: float = 0.98) -> bool:
        """Check if listing already exists."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM listings 
                    WHERE source_id = %s AND source_name = %s
                """, (listing.source_id, listing.source_name))
                
                if cur.fetchone():
                    return True
                
                if threshold < 1.0:
                    text = listing.to_embedding_text()
                    embedding = self._embed_text([text])[0]
                    
                    cur.execute("""
                        SELECT 1 FROM listings
                        WHERE text_embedding IS NOT NULL
                        AND source_name = %s
                        AND 1 - (text_embedding <=> %s::vector) >= %s
                        LIMIT 1
                    """, (listing.source_name, str(embedding), threshold))
                    
                    if cur.fetchone():
                        return True
                
                return False
                
        except Exception as e:
            logger.error(f"Duplicate check failed: {e}")
            return False
    
    def fetch_all_metadata(self, limit: int = 10000) -> List[Dict[str, Any]]:
        """Fetch all listings."""
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        id, source_id, source_name, url, title, description,
                        price, currency, transaction_type, property_type,
                        rooms, region, zone, city, municipality,
                        latitude, longitude, surface,
                        images, images_count, features, poi, price_per_m2,
                        reliability_score, reliability_level,
                        is_outlier, suspected_duplicate, change_type,
                        scraped_at, last_updated
                    FROM listings
                    ORDER BY scraped_at DESC NULLS LAST
                    LIMIT %s
                """, (limit,))
                
                rows = cur.fetchall()
            
            records = []
            for row in rows:
                r = dict(row)
                r["property_id"] = r["source_id"]
                r["_id"] = r["id"]
                records.append(r)
            
            logger.info(f"Fetched {len(records)} records")
            return records
            
        except Exception as e:
            logger.error(f"Failed to fetch metadata: {e}")
            return []
    
    def get_stats(self) -> Dict[str, Any]:
        """Get database stats."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM listings")
            total = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM listings WHERE text_embedding IS NOT NULL")
            with_emb = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM image_embeddings")
            with_img = cur.fetchone()[0]
            
        return {
            "total_listings": total,
            "with_text_embeddings": with_emb,
            "with_image_embeddings": with_img
        }
    
    def close(self):
        self.conn.close()


# Legacy compatibility
VectorDBHandler.index = property(lambda self: self)