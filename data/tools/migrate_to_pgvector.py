# export_pinecone_to_postgres.py
"""
One-time migration: Export ALL data from Pinecone to PostgreSQL with pgvector.
Run this ONCE before switching to the new pgvector-based system.

Usage:
    python export_pinecone_to_postgres.py
    python export_pinecone_to_postgres.py --batch-size 50 --limit 1000  # test with limit
"""

from __future__ import annotations

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from dotenv import load_dotenv
load_dotenv()

try:
    import psycopg2
    from psycopg2.extras import execute_values, RealDictCursor
except ImportError:
    sys.exit("pip install psycopg2-binary")

try:
    from pinecone import Pinecone
except ImportError:
    sys.exit("pip install pinecone-client")

from loguru import logger


# ─── PostgreSQL Connection ────────────────────────────────────────────────────

def get_pg_connection():
    """Create PostgreSQL connection."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DATABASE", "estatemind"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )


# ─── Schema Setup ─────────────────────────────────────────────────────────────

SETUP_SQL = """
-- Enable pgvector
CREATE EXTENSION IF NOT EXISTS vector;

-- Main listings table (structured data + text embeddings)
CREATE TABLE IF NOT EXISTS listings (
    id                  TEXT PRIMARY KEY,
    source_name         TEXT,
    source_id           TEXT,
    url                 TEXT,
    title               TEXT,
    description         TEXT,
    price               NUMERIC,
    currency            TEXT,
    transaction_type    TEXT,
    property_type       TEXT,
    rooms               INTEGER,
    bedrooms            INTEGER,
    bathrooms           INTEGER,
    city                TEXT,
    municipality        TEXT,
    zone                TEXT,
    region              TEXT,
    address             TEXT,
    surface             NUMERIC,
    surface_land        NUMERIC,
    features            JSONB,
    amenities           JSONB,
    images              JSONB,
    images_count        INTEGER,
    price_per_m2        NUMERIC,
    latitude            NUMERIC,
    longitude           NUMERIC,
    poi                 JSONB,
    
    -- Pipeline fields
    fraud_score         NUMERIC,
    fraud_flag          BOOLEAN DEFAULT FALSE,
    fraud_reason        TEXT,
    reliability_score   NUMERIC,
    reliability_level   TEXT,
    is_outlier          BOOLEAN,
    outlier_flags       JSONB,
    suspected_duplicate BOOLEAN,
    canonical_id        TEXT,
    
    -- Change detection
    change_type         TEXT,
    price_delta         NUMERIC,
    price_delta_pct     NUMERIC,
    has_price_history   BOOLEAN,
    
    -- ETL tracking
    normalized          BOOLEAN DEFAULT FALSE,
    nlp_enriched        BOOLEAN DEFAULT FALSE,
    nlp_filled_fields   JSONB,
    model_weight        NUMERIC,
    should_drop         BOOLEAN DEFAULT FALSE,
    
    -- Text embedding (384-dim for MiniLM)
    text_embedding      vector(384),
    
    -- Timestamps
    scraped_at          TIMESTAMPTZ,
    last_updated        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    
    -- Extra catch-all
    extra_metadata      JSONB
);

-- Image embeddings table (512-dim for CLIP)
CREATE TABLE IF NOT EXISTS image_embeddings (
    id              TEXT PRIMARY KEY,
    listing_id      TEXT REFERENCES listings(id) ON DELETE CASCADE,
    image_url       TEXT,
    image_index     INTEGER,
    image_label     TEXT,
    embedding       vector(512),
    metadata        JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Text chunks table (for longer descriptions, optional)
CREATE TABLE IF NOT EXISTS text_chunks (
    id              SERIAL PRIMARY KEY,
    listing_id      TEXT REFERENCES listings(id) ON DELETE CASCADE,
    chunk_text      TEXT,
    chunk_type      TEXT,  -- 'description', 'title', 'features'
    embedding       vector(384),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for vector similarity search
CREATE INDEX IF NOT EXISTS listings_text_embedding_idx 
    ON listings USING ivfflat (text_embedding vector_cosine_ops)
    WITH (lists = 100);

CREATE INDEX IF NOT EXISTS image_embeddings_idx 
    ON image_embeddings USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

CREATE INDEX IF NOT EXISTS text_chunks_embedding_idx 
    ON text_chunks USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

-- Regular indexes for filtering
CREATE INDEX IF NOT EXISTS idx_listings_source ON listings(source_name);
CREATE INDEX IF NOT EXISTS idx_listings_city ON listings(city);
CREATE INDEX IF NOT EXISTS idx_listings_price ON listings(price);
CREATE INDEX IF NOT EXISTS idx_listings_transaction_type ON listings(transaction_type);
CREATE INDEX IF NOT EXISTS idx_listings_scraped_at ON listings(scraped_at DESC);

-- Views for easy querying
CREATE OR REPLACE VIEW listings_with_images AS
SELECT 
    l.*,
    COALESCE(
        (SELECT json_agg(
            json_build_object(
                'url', ie.image_url,
                'label', ie.image_label,
                'index', ie.image_index
            )
        )
        FROM image_embeddings ie 
        WHERE ie.listing_id = l.id
        ),
        '[]'::json
    ) as image_details
FROM listings l;
"""


def setup_schema(conn):
    """Create all tables and indexes."""
    with conn.cursor() as cur:
        cur.execute(SETUP_SQL)
    conn.commit()
    logger.info("PostgreSQL schema ready")


# ─── Pinecone Export ──────────────────────────────────────────────────────────

def connect_pinecone():
    """Connect to Pinecone index."""
    api_key = os.getenv("PINECONE_API_KEY")
    index_name = os.getenv("PINECONE_INDEX_NAME", "property-listings")
    
    if not api_key:
        raise ValueError("PINECONE_API_KEY not set")
    
    pc = Pinecone(api_key=api_key)
    index = pc.Index(index_name)
    
    stats = index.describe_index_stats()
    logger.info(f"Connected to Pinecone index '{index_name}'")
    logger.info(f"Total vectors: {stats.get('total_vector_count', '?')}")
    logger.info(f"Dimension: {stats.get('dimension', '?')}")
    
    return index


def fetch_all_vectors(index, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Fetch all vectors from Pinecone using pagination."""
    all_vectors = []
    
    # Get all IDs first
    logger.info("Fetching all vector IDs...")
    all_ids = []
    for page in index.list():
        if isinstance(page, list):
            all_ids.extend(page)
        else:
            all_ids.extend(getattr(page, "ids", []))
    
    logger.info(f"Found {len(all_ids)} vector IDs")
    
    if limit:
        all_ids = all_ids[:limit]
        logger.info(f"Limited to {limit} vectors")
    
    # Fetch in batches
    batch_size = 100
    for i in range(0, len(all_ids), batch_size):
        batch_ids = all_ids[i:i + batch_size]
        try:
            result = index.fetch(ids=batch_ids)
            vectors = result.get("vectors", {}) if isinstance(result, dict) else result.vectors
            
            for vec_id, vec_data in vectors.items():
                if isinstance(vec_data, dict):
                    values = vec_data.get("values", [])
                    metadata = vec_data.get("metadata", {})
                else:
                    values = list(vec_data.values) if hasattr(vec_data, "values") else []
                    metadata = dict(vec_data.metadata) if hasattr(vec_data, "metadata") else {}
                
                all_vectors.append({
                    "id": vec_id,
                    "values": values,
                    "metadata": metadata,
                    "dimension": len(values)
                })
            
            logger.info(f"Fetched batch {i//batch_size + 1}: {len(all_vectors)} vectors so far")
            
        except Exception as e:
            logger.error(f"Error fetching batch {batch_ids}: {e}")
    
    return all_vectors


# ─── Data Transformation ──────────────────────────────────────────────────────

def transform_vector(vector: Dict[str, Any]) -> Dict[str, Any]:
    """Transform Pinecone vector to PostgreSQL row format."""
    metadata = vector["metadata"]
    values = vector["values"]
    dim = len(values)
    
    # Extract listing ID (remove source prefix if present)
    vec_id = vector["id"]
    if ":" in vec_id:
        source_name, source_id = vec_id.split(":", 1)
    else:
        source_name = metadata.get("source_name", "unknown")
        source_id = vec_id
    
    # Base listing data
    listing = {
        "id": vec_id,
        "source_name": source_name,
        "source_id": source_id,
        "url": metadata.get("url"),
        "title": metadata.get("title"),
        "description": metadata.get("description"),
        "price": float(metadata.get("price", 0)) if metadata.get("price") else None,
        "currency": metadata.get("currency", "TND"),
        "transaction_type": metadata.get("transaction_type"),
        "property_type": metadata.get("type") or metadata.get("property_type"),
        "rooms": int(metadata.get("rooms", 0)) if metadata.get("rooms") else None,
        "bedrooms": int(metadata.get("bedrooms", 0)) if metadata.get("bedrooms") else None,
        "bathrooms": int(metadata.get("bathrooms", 0)) if metadata.get("bathrooms") else None,
        "city": metadata.get("city"),
        "municipality": metadata.get("municipality") or metadata.get("municipalite"),
        "zone": metadata.get("zone"),
        "region": metadata.get("region"),
        "surface": float(metadata.get("surface", 0)) if metadata.get("surface") else None,
        "surface_land": float(metadata.get("surface_land", 0)) if metadata.get("surface_land") else None,
        "latitude": float(metadata.get("latitude", 0)) if metadata.get("latitude") else None,
        "longitude": float(metadata.get("longitude", 0)) if metadata.get("longitude") else None,
        "images": metadata.get("images"),
        "images_count": len(metadata.get("images", [])) if isinstance(metadata.get("images"), list) else None,
        "price_per_m2": float(metadata.get("price_per_m2", 0)) if metadata.get("price_per_m2") else None,
        "scraped_at": metadata.get("scraped_at") or metadata.get("scraped_date"),
        "last_updated": metadata.get("last_updated") or datetime.now(timezone.utc).isoformat(),
        "extra_metadata": {}
    }
    
    # Handle features/amenities
    features = metadata.get("features", [])
    if isinstance(features, str):
        try:
            features = json.loads(features)
        except:
            features = [features]
    listing["features"] = features if features else None
    
    # Handle POI
    poi = metadata.get("poi", {})
    if isinstance(poi, str):
        try:
            poi = json.loads(poi)
        except:
            poi = {}
    listing["poi"] = poi if poi else None
    
    # Determine embedding type and store accordingly
    # 384-dim → text embedding (MiniLM)
    # 512-dim → image embedding (CLIP)
    # 768-dim → text embedding (nomic)
    
    if dim == 384:
        listing["text_embedding"] = values
        listing["embedding_type"] = "text"
    elif dim == 512:
        listing["embedding_type"] = "image"
        # For image embeddings, we store in separate table
        listing["text_embedding"] = None
    elif dim == 768:
        listing["text_embedding"] = values
        listing["embedding_type"] = "text_large"
    else:
        listing["embedding_type"] = "unknown"
        listing["text_embedding"] = None
    
    # Store any unhandled metadata in extra_metadata
    known_keys = set(listing.keys()) | {"embedding_type"}
    listing["extra_metadata"] = {
        k: v for k, v in metadata.items() 
        if k not in known_keys and v is not None
    }
    
    return listing


# ─── PostgreSQL Insert ────────────────────────────────────────────────────────

def insert_vectors_to_postgres(conn, vectors: List[Dict[str, Any]]) -> Dict[str, int]:
    """Insert transformed vectors into PostgreSQL."""
    stats = {
        "listings": 0,
        "image_embeddings": 0,
        "errors": 0
    }
    
    with conn.cursor() as cur:
        for vec in vectors:
            try:
                transformed = transform_vector(vec)
                
                # Insert/Update listings table
                columns = [k for k in transformed.keys() if k != "embedding_type"]
                placeholders = ", ".join(["%s"] * len(columns))
                col_names = ", ".join(columns)
                
                values = [transformed.get(col) for col in columns]
                
                # Convert embedding to string for pgvector
                for i, col in enumerate(columns):
                    if col == "text_embedding" and values[i] is not None:
                        values[i] = str(values[i])
                    elif col in ("features", "poi", "images", "extra_metadata") and values[i] is not None:
                        values[i] = json.dumps(values[i]) if not isinstance(values[i], str) else values[i]
                
                update_clause = ", ".join(
                    f"{col} = EXCLUDED.{col}"
                    for col in columns if col != "id"
                )
                
                cur.execute(f"""
                    INSERT INTO listings ({col_names})
                    VALUES ({placeholders})
                    ON CONFLICT (id) DO UPDATE SET {update_clause}
                """, values)
                
                stats["listings"] += 1
                
                # Handle image embeddings (512-dim)
                if transformed.get("embedding_type") == "image":
                    # Extract image URL from metadata
                    metadata = vec["metadata"]
                    image_url = metadata.get("image_url") or metadata.get("url")
                    image_label = metadata.get("image_label") or metadata.get("label")
                    
                    cur.execute("""
                        INSERT INTO image_embeddings (id, listing_id, image_url, embedding)
                        VALUES (%s, %s, %s, %s::vector)
                        ON CONFLICT (id) DO UPDATE SET
                            listing_id = EXCLUDED.listing_id,
                            image_url = EXCLUDED.image_url,
                            embedding = EXCLUDED.embedding
                    """, (
                        vec["id"],
                        transformed["id"],
                        image_url,
                        str(vec["values"])
                    ))
                    
                    stats["image_embeddings"] += 1
                
            except Exception as e:
                logger.error(f"Error inserting vector {vec.get('id')}: {e}")
                stats["errors"] += 1
                conn.rollback()
            else:
                conn.commit()
    
    return stats


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Export Pinecone to PostgreSQL")
    parser.add_argument("--batch-size", type=int, default=100, help="Fetch batch size")
    parser.add_argument("--limit", type=int, help="Limit number of vectors to export")
    parser.add_argument("--dry-run", action="store_true", help="Only fetch, don't insert")
    args = parser.parse_args()
    
    # Validate environment
    required_env = ["PINECONE_API_KEY", "PINECONE_INDEX_NAME", "POSTGRES_PASSWORD"]
    missing = [v for v in required_env if not os.getenv(v)]
    if missing:
        logger.error(f"Missing environment variables: {missing}")
        sys.exit(1)
    
    # Connect to Pinecone
    logger.info("Connecting to Pinecone...")
    index = connect_pinecone()
    
    # Fetch all vectors
    logger.info("Fetching vectors from Pinecone...")
    vectors = fetch_all_vectors(index, limit=args.limit)
    logger.info(f"Fetched {len(vectors)} vectors")
    
    if args.dry_run:
        logger.info("Dry run - no data inserted")
        # Show sample
        if vectors:
            sample = vectors[0]
            logger.info(f"Sample vector: id={sample['id']}, dim={sample['dimension']}")
            logger.info(f"Sample metadata keys: {list(sample['metadata'].keys())[:10]}")
        return
    
    # Connect to PostgreSQL
    logger.info("Connecting to PostgreSQL...")
    conn = get_pg_connection()
    
    # Setup schema
    logger.info("Setting up PostgreSQL schema...")
    setup_schema(conn)
    
    # Insert vectors
    logger.info("Inserting vectors into PostgreSQL...")
    stats = insert_vectors_to_postgres(conn, vectors)
    
    conn.close()
    
    # Summary
    logger.info("=" * 60)
    logger.info("Export complete!")
    logger.info(f"  Total vectors: {len(vectors)}")
    logger.info(f"  Listings inserted/updated: {stats['listings']}")
    logger.info(f"  Image embeddings inserted: {stats['image_embeddings']}")
    logger.info(f"  Errors: {stats['errors']}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()