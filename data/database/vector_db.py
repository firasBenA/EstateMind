"""
EstateMind — pgvector database handler.

Remplace Pinecone par pgvector (extension PostgreSQL open source).
- Stocke les embeddings directement dans la table PostgreSQL 'listings'
- Même interface que l'ancien VectorDBHandler (Pinecone)
- Dimension: 384 (paraphrase-multilingual-MiniLM-L12-v2)
"""
from __future__ import annotations

import os
import time
from pathlib import Path

from typing import List, Optional, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor
from loguru import logger
from sympy import limit

from core.models import PropertyListing


EMBEDDING_DIM = 384
EMBEDDING_MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"


def _clean_metadata(raw: Dict[str, Any]) -> Dict[str, Any]:
    clean = {}
    for k, v in raw.items():
        if v is None:
            continue
        if isinstance(v, (str, int, float, bool)):
            clean[k] = v
        elif isinstance(v, list):
            clean[k] = [str(i) for i in v if i is not None]
    return clean


class EmbeddingModel:
    _model = None

    @classmethod
    def _get_model(cls):
        if cls._model is None:
            logger.info(f"Loading model: {EMBEDDING_MODEL_NAME} (one-time, CPU)")
            from sentence_transformers import SentenceTransformer
            cls._model = SentenceTransformer(EMBEDDING_MODEL_NAME)
            test = cls._model.encode(["test"])
            logger.info(f"Model ready, dim={len(test[0])}")
        return cls._model

    @classmethod
    def embed(cls, texts: List[str]) -> List[List[float]]:
        model = cls._get_model()
        return model.encode(texts, convert_to_numpy=True).tolist()


class VectorDBHandler:
    def __init__(
        self,
        api_key: Optional[str] = None,
        index_name: str = "property-listings",
        cloud: str = "aws",
        region: str = "us-east-1",
        strategy: str = "huggingface",
    ):
        try:
            from dotenv import load_dotenv
            data_dir = Path(__file__).resolve().parents[1]
            load_dotenv(data_dir / ".env")
        except Exception:
            pass

        self.conn = self._connect_pg()
        self._setup_schema()
        logger.info("pgvector handler ready")

    def _connect_pg(self):
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5433")),
            dbname=os.getenv("POSTGRES_DB", "estatemind"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
        )
        conn.autocommit = False
        return conn

    def _setup_schema(self):
        with self.conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            cur.execute("""
                ALTER TABLE listings
                ADD COLUMN IF NOT EXISTS embedding vector(%s);
            """ % EMBEDDING_DIM)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS listings_embedding_idx
                ON listings USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100);
            """)
        self.conn.commit()
        logger.info("pgvector schema ready")

    def _embed(self, texts: List[str]) -> List[List[float]]:
        return EmbeddingModel.embed(texts)

    def upsert_listing(self, listing: PropertyListing) -> bool:
        try:
            text = listing.to_embedding_text()
            embedding = self._embed([text])[0]
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE listings
                    SET embedding = %s::vector
                    WHERE source_name = %s AND property_id = %s
                """, (embedding, listing.source_name, listing.source_id))
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to upsert embedding {listing.source_id}: {e}")
            return False

    def upsert_listings(self, listings: List[PropertyListing], batch_size: int = 100) -> Dict[str, int]:
        stats = {"success": 0, "failed": 0}
        for i in range(0, len(listings), batch_size):
            batch = listings[i: i + batch_size]
            batch = listings[i: i + batch_size]
            texts = [l.to_embedding_text() for l in batch]
            try:
                embeddings = self._embed(texts)
            except Exception as e:
                logger.error(f"Embedding batch failed: {e}")
                stats["failed"] += len(batch)
                continue
            for listing, emb in zip(batch, embeddings):
                try:
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            UPDATE listings
                            SET embedding = %s::vector
                            WHERE source_name = %s AND property_id = %s
                        """, (emb, listing.source_name, listing.source_id))
                    self.conn.commit()
                    stats["success"] += 1
                except Exception as e:
                    self.conn.rollback()
                    stats["failed"] += 1
        return stats

    def check_duplicate(self, listing: PropertyListing, threshold: float = 0.98) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM listings
                    WHERE source_name = %s AND property_id = %s
                """, (listing.source_name, listing.source_id))
                if cur.fetchone():
                    return True
                if threshold < 1.0:
                    text = listing.to_embedding_text()
                    embedding = self._embed([text])[0]
                    cur.execute("""
                        SELECT 1 FROM listings
                        WHERE embedding IS NOT NULL
                        AND 1 - (embedding <=> %s::vector) >= %s
                        LIMIT 1
                    """, (embedding, threshold))
                    if cur.fetchone():
                        return True
            return False
        except Exception as e:
            logger.error(f"Duplicate check failed: {e}")
            return False

    def semantic_search(self, query: str, top_k: int = 10, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        try:
            embedding = self._embed([query])[0]
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        property_id, source_name, url, title, price,
                        surface, rooms, city, municipalite, transaction_type,
                        1 - (embedding <=> %s::vector) AS score
                    FROM listings
                    WHERE embedding IS NOT NULL
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s
                """, (embedding, embedding, top_k))
                results = cur.fetchall()
            return [dict(r) for r in results]
        except Exception as e:
            logger.error(f"Semantic search failed: {e}")
            return []

    def find_similar(self, listing: PropertyListing, top_k: int = 10) -> List[Dict[str, Any]]:
        return self.semantic_search(query=listing.to_embedding_text(), top_k=top_k)

    def fetch_all_metadata(self, limit: int = 10000) -> List[Dict[str, Any]]:
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        property_id, source_name, url, type, title, description,
                        price, surface, rooms, region, zone, city, municipalite,
                        latitude, longitude, features, poi,
                        scraped_at, last_update, transaction_type, currency
                    FROM listings
                    ORDER BY scraped_at DESC
                    LIMIT %s
                """, (limit,))
                rows = cur.fetchall()
            records = []
            for row in rows:
                r = dict(row)
                r["_id"] = f"{r['source_name']}:{r['property_id']}"
                records.append(r)
            logger.info(f"Fetched {len(records)} records from PostgreSQL")
            return records
        except Exception as e:
            logger.error(f"Failed to fetch all metadata: {e}")
            return []

    def fetch_by_source(self, source_name: str, limit: int = 10000) -> List[Dict[str, Any]]:
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT * FROM listings
                    WHERE source_name = %s
                    ORDER BY scraped_at DESC
                    LIMIT %s
                """, (source_name, limit))
                rows = cur.fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"Failed to fetch by source {source_name}: {e}")
            return []

    def get_stats(self) -> Dict[str, Any]:
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM listings")
                total = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM listings WHERE embedding IS NOT NULL")
                with_embeddings = cur.fetchone()[0]
            return {"total_vectors": with_embeddings, "total_listings": total, "dimension": EMBEDDING_DIM}
        except Exception as e:
            logger.error(f"Stats failed: {e}")
            return {}

    def delete_by_source(self, source_name: str) -> int:
        try:
            with self.conn.cursor() as cur:
                cur.execute("UPDATE listings SET embedding = NULL WHERE source_name = %s", (source_name,))
                count = cur.rowcount
            self.conn.commit()
            return count
        except Exception as e:
            self.conn.rollback()
            logger.error(f"delete_by_source failed: {e}")
            return 0

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass