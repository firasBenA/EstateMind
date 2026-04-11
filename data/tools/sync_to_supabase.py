# tools/sync_to_supabase.py - FIXED
"""
Sync to Supabase - handles all columns that exist locally.
"""

import os
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from dotenv import load_dotenv
load_dotenv()

import psycopg2
from psycopg2.extras import RealDictCursor
from supabase import create_client
from loguru import logger


def clean_for_json(obj: Any) -> Any:
    """Convert all non-JSON types to JSON-serializable."""
    if obj is None:
        return None
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [clean_for_json(x) for x in obj]
    if isinstance(obj, bytes):
        return obj.decode('utf-8', errors='ignore')
    return obj


class SupabaseSync:
    def __init__(self):
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_SERVICE_KEY")
        self.supabase = create_client(self.url, self.key)
        self.pg = self._connect_pg()
    
    def _connect_pg(self):
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DATABASE", "estatemind"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
        )
    
    def _get_local_columns(self, table: str) -> set:
        """Get all column names from local table."""
        with self.pg.cursor() as cur:
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s
            """, (table,))
            return {row[0] for row in cur.fetchall()}
    
    def sync_listings(self, batch_size: int = 50) -> dict:
        """Sync only columns that exist in BOTH local and Supabase."""
        stats = {"success": 0, "error": 0}
        
        # Get local columns
        local_cols = self._get_local_columns('listings')
        logger.info(f"Local listings has {len(local_cols)} columns")
        
        with self.pg.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) FROM listings")
            total = cur.fetchone()['count']
            logger.info(f"Syncing {total} listings...")
            
            # Select only the columns we need
            select_cols = [
                'id', 'source_name', 'source_id', 'url', 'title', 'description',
                'price', 'currency', 'transaction_type', 'property_type',
                'rooms', 'city', 'municipality', 'zone', 'region',
                'surface', 'latitude', 'longitude',
                'images', 'images_count', 'features', 'price_per_m2', 'poi',
                'reliability_score', 'reliability_level',
                'is_outlier', 'outlier_flags', 'suspected_duplicate',
                'change_type', 'price_delta', 'price_delta_pct',
                'has_price_history', 'normalized', 'nlp_enriched',
                'nlp_filled_fields', 'model_weight', 'should_drop',
                'text_embedding',
                'scraped_at', 'last_updated', 'created_at'
            ]
            
            # Only include columns that actually exist locally
            valid_cols = [c for c in select_cols if c in local_cols]
            cols_str = ', '.join(valid_cols)
            
            cur.execute(f"SELECT {cols_str} FROM listings ORDER BY scraped_at DESC")
            
            synced = 0
            batch = []
            
            for row in cur:
                clean_row = clean_for_json(dict(row))
                
                # Handle text_embedding (convert string to list)
                if clean_row.get('text_embedding') and isinstance(clean_row['text_embedding'], str):
                    try:
                        clean_row['text_embedding'] = json.loads(clean_row['text_embedding'])
                    except:
                        clean_row['text_embedding'] = None
                
                # Remove any None values for JSONB fields
                for field in ['poi', 'outlier_flags', 'nlp_filled_fields']:
                    if field in clean_row and clean_row[field] is None:
                        clean_row[field] = []
                
                batch.append(clean_row)
                
                if len(batch) >= batch_size:
                    try:
                        self.supabase.table("listings").upsert(
                            batch, on_conflict="id"
                        ).execute()
                        synced += len(batch)
                        logger.info(f"  Synced {synced}/{total} listings")
                    except Exception as e:
                        logger.error(f"  Batch failed: {e}")
                    batch = []
            
            # Final batch
            if batch:
                try:
                    self.supabase.table("listings").upsert(
                        batch, on_conflict="id"
                    ).execute()
                    synced += len(batch)
                except Exception as e:
                    logger.error(f"  Final batch failed: {e}")
            
            stats["success"] = synced
        
        return stats
    
    def sync_image_embeddings(self, batch_size: int = 100) -> dict:
        """Sync image embeddings."""
        stats = {"success": 0, "error": 0}
        
        with self.pg.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) FROM image_embeddings")
            total = cur.fetchone()['count']
            
            if total == 0:
                logger.info("No image embeddings to sync")
                return stats
            
            logger.info(f"Syncing {total} image embeddings...")
            cur.execute("SELECT * FROM image_embeddings")
            
            synced = 0
            batch = []
            
            for row in cur:
                clean_row = clean_for_json(dict(row))
                
                if clean_row.get('embedding') and isinstance(clean_row['embedding'], str):
                    try:
                        clean_row['embedding'] = json.loads(clean_row['embedding'])
                    except:
                        clean_row['embedding'] = None
                
                batch.append(clean_row)
                
                if len(batch) >= batch_size:
                    try:
                        self.supabase.table("image_embeddings").upsert(
                            batch, on_conflict="id"
                        ).execute()
                        synced += len(batch)
                        logger.info(f"  Synced {synced}/{total} images")
                    except Exception as e:
                        logger.error(f"  Image batch failed: {e}")
                    batch = []
            
            if batch:
                try:
                    self.supabase.table("image_embeddings").upsert(
                        batch, on_conflict="id"
                    ).execute()
                    synced += len(batch)
                except Exception as e:
                    logger.error(f"  Final image batch failed: {e}")
            
            stats["success"] = synced
        
        return stats
    
    def sync_all(self):
        """Sync all tables."""
        logger.info("=" * 60)
        logger.info("Starting Supabase sync")
        logger.info("=" * 60)
        
        listings_stats = self.sync_listings(batch_size=25)
        images_stats = self.sync_image_embeddings(batch_size=50)
        
        logger.info("=" * 60)
        logger.info("Sync complete!")
        logger.info(f"  Listings: {listings_stats['success']} synced")
        logger.info(f"  Images: {images_stats['success']} synced")
        logger.info("=" * 60)
    
    def close(self):
        self.pg.close()


def main():
    syncer = SupabaseSync()
    syncer.sync_all()
    syncer.close()


if __name__ == "__main__":
    main()