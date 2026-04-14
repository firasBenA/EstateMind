# tools/fix_metadata_to_columns.py - Updated version
"""
Extract preprocessing scores from extra_metadata to dedicated columns,
then optionally drop extra_metadata.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DATABASE", "estatemind"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )

def extract_from_extra_metadata(drop_extra: bool = False):
    """Extract scores from extra_metadata to dedicated columns."""
    conn = get_connection()
    
    with conn.cursor() as cur:
        # Check if columns exist, add if missing
        columns_to_ensure = [
            ("reliability_score", "NUMERIC"),
            ("reliability_level", "TEXT"),
            ("is_outlier", "BOOLEAN DEFAULT FALSE"),
            ("suspected_duplicate", "BOOLEAN DEFAULT FALSE"),
            ("change_type", "TEXT"),
            ("model_weight", "NUMERIC"),
            ("should_drop", "BOOLEAN DEFAULT FALSE"),
        ]
        
        for col_name, col_type in columns_to_ensure:
            cur.execute(f"""
                ALTER TABLE listings 
                ADD COLUMN IF NOT EXISTS {col_name} {col_type};
            """)
            logger.info(f"Ensured column: {col_name}")
        
        # Extract data from extra_metadata
        cur.execute("""
            UPDATE listings
            SET 
                reliability_score = CASE 
                    WHEN extra_metadata->>'reliability_score' IS NOT NULL 
                    THEN (extra_metadata->>'reliability_score')::NUMERIC 
                    ELSE reliability_score 
                END,
                reliability_level = COALESCE(
                    extra_metadata->>'reliability_level', 
                    reliability_level
                ),
                is_outlier = CASE 
                    WHEN extra_metadata->>'is_outlier' IS NOT NULL 
                    THEN (extra_metadata->>'is_outlier')::BOOLEAN 
                    ELSE COALESCE(is_outlier, FALSE)
                END,
                suspected_duplicate = CASE 
                    WHEN extra_metadata->>'suspected_duplicate' IS NOT NULL 
                    THEN (extra_metadata->>'suspected_duplicate')::BOOLEAN 
                    ELSE COALESCE(suspected_duplicate, FALSE)
                END,
                change_type = COALESCE(
                    extra_metadata->>'change_type',
                    change_type
                ),
                model_weight = CASE 
                    WHEN extra_metadata->>'model_weight' IS NOT NULL 
                    THEN (extra_metadata->>'model_weight')::NUMERIC 
                    ELSE COALESCE(model_weight, 0.5)
                END,
                should_drop = CASE 
                    WHEN extra_metadata->>'should_drop' IS NOT NULL 
                    THEN (extra_metadata->>'should_drop')::BOOLEAN 
                    ELSE COALESCE(should_drop, FALSE)
                END
            WHERE extra_metadata IS NOT NULL 
              AND extra_metadata::text != '{}';
        """)
        
        updated_count = cur.rowcount
        logger.info(f"Updated {updated_count} records from extra_metadata")
        
        # Verify
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(reliability_score) as with_score,
                COUNT(reliability_level) as with_level,
                COUNT(CASE WHEN is_outlier IS NOT NULL THEN 1 END) as with_outlier
            FROM listings
        """)
        stats = cur.fetchone()
        logger.info(f"Stats: total={stats[0]}, with_score={stats[1]}, with_level={stats[2]}")
        
        if drop_extra:
            logger.warning("Dropping dependent view and extra_metadata column...")
            
            # Drop the dependent view first
            cur.execute("DROP VIEW IF EXISTS listings_with_images CASCADE;")
            logger.info("Dropped listings_with_images view")
            
            # Now drop the column
            cur.execute("ALTER TABLE listings DROP COLUMN IF EXISTS extra_metadata;")
            logger.info("extra_metadata column dropped")
            
            # Recreate a simplified view
            cur.execute("""
                CREATE OR REPLACE VIEW listings_with_images AS
                SELECT 
                    l.*,
                    COALESCE(
                        (SELECT json_agg(
                            json_build_object(
                                'url', ie.image_url,
                                'index', ie.image_index
                            )
                        )
                        FROM image_embeddings ie 
                        WHERE ie.listing_id = l.id
                        ),
                        '[]'::json
                    ) as image_details
                FROM listings l;
            """)
            logger.info("Recreated listings_with_images view")
        
        conn.commit()
    
    conn.close()
    logger.info("Migration complete!")

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--drop-extra", action="store_true", 
                       help="Drop extra_metadata column after extraction")
    args = parser.parse_args()
    
    extract_from_extra_metadata(drop_extra=args.drop_extra)

if __name__ == "__main__":
    main()