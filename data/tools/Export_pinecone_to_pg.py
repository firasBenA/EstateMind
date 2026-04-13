"""
Script pour exporter les données de Pinecone vers PostgreSQL (pgvector).
Lance ce script UNE SEULE FOIS pour migrer les données existantes.

Usage:
    python tools/export_pinecone_to_pg.py
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
load_dotenv()

from pinecone import Pinecone
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


def setup_pg_schema(conn):
    """Crée la table listings si elle n'existe pas."""
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
                images JSONB,
                features JSONB,
                poi JSONB,
                scraped_at TIMESTAMP,
                last_update TIMESTAMP,
                transaction_type TEXT,
                currency TEXT,
                embedding vector(384),
                etl_status VARCHAR(50) DEFAULT 'pending',
                etl_processed_at TIMESTAMP,
                UNIQUE (source_name, property_id)
            );
        """)
    conn.commit()
    logger.info("Schema PostgreSQL prêt")


def fetch_from_pinecone():
    """Récupère tous les vecteurs et métadonnées depuis Pinecone."""
    api_key = os.getenv("PINECONE_API_KEY")
    index_name = os.getenv("PINECONE_INDEX_NAME", "property-listings")

    if not api_key:
        logger.error("PINECONE_API_KEY manquante dans .env")
        sys.exit(1)

    pc = Pinecone(api_key=api_key)
    index = pc.Index(index_name)

    stats = index.describe_index_stats()
    total = stats.total_vector_count
    logger.info(f"Total vecteurs dans Pinecone : {total}")

    if total == 0:
        logger.warning("Aucune donnée dans Pinecone")
        return []

    all_records = []
    batch_size = 100

    for ids_page in index.list(prefix="", limit=batch_size):
        ids_list = list(ids_page) if not isinstance(ids_page, list) else ids_page
        if not ids_list:
            continue

        for i in range(0, len(ids_list), 10):
            batch_ids = ids_list[i:i+10]
            try:
                result = index.fetch(ids=batch_ids)
                for vector_id, vector_data in result.vectors.items():
                    md = dict(getattr(vector_data, "metadata", {}) or {})
                    md["_vector_id"] = vector_id
                    md["_embedding"] = list(vector_data.values) if vector_data.values else None
                    all_records.append(md)
            except Exception as e:
                logger.error(f"Erreur fetch batch {batch_ids}: {e}")

    logger.info(f"Récupéré {len(all_records)} enregistrements depuis Pinecone")
    return all_records


def insert_into_pg(conn, records):
    """Insère les enregistrements dans PostgreSQL."""
    inserted = updated = errors = 0

    for rec in records:
        vector_id = rec.get("_vector_id", "")
        parts = vector_id.split(":", 1) if ":" in vector_id else [None, vector_id]
        source_name = rec.get("source_name") or parts[0] or "unknown"
        property_id = rec.get("property_id") or parts[1] or vector_id
        embedding = rec.get("_embedding")

        try:
            price = float(rec["price"]) if rec.get("price") else None
        except (ValueError, TypeError):
            price = None

        try:
            surface = float(rec["surface"]) if rec.get("surface") else None
        except (ValueError, TypeError):
            surface = None

        try:
            rooms = int(rec["rooms"]) if rec.get("rooms") else None
        except (ValueError, TypeError):
            rooms = None

        try:
            lat = float(rec["latitude"]) if rec.get("latitude") else None
            lon = float(rec["longitude"]) if rec.get("longitude") else None
        except (ValueError, TypeError):
            lat = lon = None

        features = rec.get("features") or []
        if isinstance(features, str):
            features = [features]

        poi = rec.get("poi") or []
        if isinstance(poi, str):
            poi = [poi]

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO listings (
                        property_id, source_name, url, type, title, description,
                        price, surface, rooms, region, zone, city, municipalite,
                        latitude, longitude, features, poi,
                        transaction_type, currency, embedding
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (source_name, property_id) DO UPDATE SET
                        price = EXCLUDED.price,
                        surface = EXCLUDED.surface,
                        rooms = EXCLUDED.rooms,
                        embedding = EXCLUDED.embedding
                """, (
                    property_id, source_name,
                    rec.get("url"), rec.get("type"),
                    rec.get("title"), rec.get("description"),
                    price, surface, rooms,
                    rec.get("region"), rec.get("zone"),
                    rec.get("city"), rec.get("municipalite"),
                    lat, lon,
                    Json(features), Json(poi),
                    rec.get("transaction_type"), rec.get("currency"),
                    embedding,
                ))
            conn.commit()
            inserted += 1
        except Exception as e:
            conn.rollback()
            logger.error(f"Erreur insertion {property_id}: {e}")
            errors += 1

    logger.info(f"Résultat : {inserted} insérés, {errors} erreurs")
    return inserted, errors


def main():
    logger.info("=== Export Pinecone → PostgreSQL ===")

    logger.info("Connexion à PostgreSQL...")
    conn = get_pg_conn()
    setup_pg_schema(conn)

    logger.info("Récupération des données depuis Pinecone...")
    records = fetch_from_pinecone()

    if not records:
        logger.warning("Aucune donnée à migrer")
        return

    logger.info(f"Insertion de {len(records)} enregistrements dans PostgreSQL...")
    inserted, errors = insert_into_pg(conn, records)

    conn.close()
    logger.info(f"Migration terminée : {inserted} insérés, {errors} erreurs")


if __name__ == "__main__":
    main()