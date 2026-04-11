"""
Script pour importer pinecone_metadata_20260409_134441.csv dans PostgreSQL.
Place ce fichier dans : EstateMind/data/tools/import_csv_to_pg.py
Place le CSV dans : EstateMind/data/

Usage:
    python tools/import_csv_to_pg.py
"""
import os, sys, json, ast
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import psycopg2
from psycopg2.extras import Json
from loguru import logger


def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DB", "estatemind"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "capTEEMO500"),
    )


def parse_list(val):
    if val is None or (isinstance(val, float) and str(val) == 'nan'):
        return []
    if isinstance(val, list):
        return val
    try:
        result = json.loads(str(val))
        if isinstance(result, list):
            return result
    except Exception:
        pass
    try:
        result = ast.literal_eval(str(val))
        if isinstance(result, list):
            return result
    except Exception:
        pass
    return []


def parse_float(val):
    if val is None or (isinstance(val, float) and str(val) == 'nan'):
        return None
    try:
        return float(val)
    except Exception:
        return None


def parse_int(val):
    if val is None or (isinstance(val, float) and str(val) == 'nan'):
        return None
    try:
        return int(float(val))
    except Exception:
        return None


def parse_bool(val):
    if val is None or (isinstance(val, float) and str(val) == 'nan'):
        return False
    if isinstance(val, bool):
        return val
    return str(val).lower() in ('true', '1', 'yes')


def parse_dt(val):
    if val is None or (isinstance(val, float) and str(val) == 'nan'):
        return None
    try:
        s = str(val).replace('Z', '+00:00')
        return datetime.fromisoformat(s)
    except Exception:
        return None


def parse_str(val):
    if val is None or (isinstance(val, float) and str(val) == 'nan'):
        return None
    return str(val).strip() or None


def setup_schema(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS listings (
                id SERIAL PRIMARY KEY,
                property_id TEXT NOT NULL,
                source_name TEXT NOT NULL,
                url TEXT,
                type TEXT,
                title TEXT,
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
                poi JSONB,
                scraped_at TIMESTAMP,
                last_update TIMESTAMP,
                transaction_type TEXT,
                currency TEXT,
                raw_data_path TEXT,
                image_count INTEGER DEFAULT 0,
                embedding vector(384),
                etl_status VARCHAR(50) DEFAULT 'pending',
                etl_processed_at TIMESTAMP,
                reliability_score DOUBLE PRECISION,
                reliability_level TEXT,
                model_weight DOUBLE PRECISION,
                is_outlier BOOLEAN DEFAULT FALSE,
                suspected_duplicate BOOLEAN DEFAULT FALSE,
                price_per_m2 DOUBLE PRECISION,
                price_delta DOUBLE PRECISION,
                price_delta_pct DOUBLE PRECISION,
                UNIQUE (source_name, property_id)
            );
        """)
    conn.commit()
    logger.info("Schema prêt")


def main():
    logger.info("=== Import CSV → PostgreSQL ===")

    csv_candidates = [
        Path(__file__).parent.parent / "pinecone_metadata_20260409_134441.csv",
    ]
    csv_path = None
    for p in csv_candidates:
        if p.exists():
            csv_path = p
            
            break

    if csv_path is None:
        logger.error( Path(__file__).parent.parent)
        logger.error("Fichier CSV introuvable. Place-le dans EstateMind/data/")
        sys.exit(1)

    logger.info(f"Fichier trouvé : {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"{len(df)} lignes à importer")

    conn = get_pg_conn()
    setup_schema(conn)

    inserted = errors = 0

    for i, row in df.iterrows():
        property_id = parse_str(row.get('property_id'))
        source_name = parse_str(row.get('source_name'))

        if not property_id or not source_name:
            errors += 1
            continue

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO listings (
                        property_id, source_name, url, type, title, description,
                        price, surface, rooms, region, zone, city, municipalite,
                        latitude, longitude, images, features, poi,
                        scraped_at, last_update, transaction_type, currency,
                        image_count, reliability_score, reliability_level,
                        model_weight, is_outlier, suspected_duplicate,
                        price_per_m2, price_delta, price_delta_pct
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s
                    )
                    ON CONFLICT (source_name, property_id) DO UPDATE SET
                        url=EXCLUDED.url, title=EXCLUDED.title,
                        description=EXCLUDED.description, price=EXCLUDED.price,
                        surface=EXCLUDED.surface, rooms=EXCLUDED.rooms,
                        region=EXCLUDED.region, zone=EXCLUDED.zone,
                        city=EXCLUDED.city, municipalite=EXCLUDED.municipalite,
                        latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude,
                        images=EXCLUDED.images, features=EXCLUDED.features,
                        poi=EXCLUDED.poi, scraped_at=EXCLUDED.scraped_at,
                        last_update=EXCLUDED.last_update,
                        transaction_type=EXCLUDED.transaction_type,
                        currency=EXCLUDED.currency,
                        image_count=EXCLUDED.image_count,
                        reliability_score=EXCLUDED.reliability_score,
                        reliability_level=EXCLUDED.reliability_level,
                        model_weight=EXCLUDED.model_weight,
                        is_outlier=EXCLUDED.is_outlier,
                        suspected_duplicate=EXCLUDED.suspected_duplicate,
                        price_per_m2=EXCLUDED.price_per_m2,
                        price_delta=EXCLUDED.price_delta,
                        price_delta_pct=EXCLUDED.price_delta_pct
                """, (
                    property_id, source_name,
                    parse_str(row.get('url')), parse_str(row.get('type')),
                    parse_str(row.get('title')), parse_str(row.get('description')),
                    parse_float(row.get('price')), parse_float(row.get('surface')),
                    parse_int(row.get('rooms')),
                    parse_str(row.get('region')), parse_str(row.get('zone')),
                    parse_str(row.get('city')), parse_str(row.get('municipalite')),
                    parse_float(row.get('latitude')), parse_float(row.get('longitude')),
                    Json(parse_list(row.get('images'))),
                    Json(parse_list(row.get('features'))),
                    Json(parse_list(row.get('poi'))),
                    parse_dt(row.get('scraped_at')), parse_dt(row.get('last_update')),
                    parse_str(row.get('transaction_type')), parse_str(row.get('currency')),
                    parse_int(row.get('image_count')),
                    parse_float(row.get('reliability_score')),
                    parse_str(row.get('reliability_level')),
                    parse_float(row.get('model_weight')),
                    parse_bool(row.get('is_outlier')),
                    parse_bool(row.get('suspected_duplicate')),
                    parse_float(row.get('price_per_m2')),
                    parse_float(row.get('price_delta')),
                    parse_float(row.get('price_delta_pct')),
                ))
            conn.commit()
            inserted += 1
            if inserted % 200 == 0:
                logger.info(f"  {inserted}/{len(df)} insérés...")
        except Exception as e:
            conn.rollback()
            logger.error(f"Erreur ligne {i} ({property_id}): {e}")
            errors += 1

    conn.close()
    logger.info(f"=== Terminé : {inserted} insérés, {errors} erreurs ===")


if __name__ == "__main__":
    main()