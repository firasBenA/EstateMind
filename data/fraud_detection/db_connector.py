"""
EstateMind — Fraud Detection DB Connector
==========================================
Connexion unique Supabase (PostgreSQL) :
  - Lecture  : table listings (annonces + images)
  - Écriture : table fraud_detection_results (résultats DSO 2.2)

Variables d'environnement (data/.env) :
  SUPABASE_DB_HOST
  SUPABASE_DB_PORT
  SUPABASE_DB_NAME
  SUPABASE_DB_USER
  SUPABASE_DB_PASSWORD
  SUPABASE_DB_SSLMODE
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import List, Dict, Any, Optional

import psycopg2
from psycopg2.extras import RealDictCursor, Json
from loguru import logger


# Colonnes Supabase avec alias pour compatibilité avec le reste du code
LISTING_SELECT = """
    id,
    COALESCE(source_id, id) AS property_id,
    source_name,
    url,
    property_type    AS type,
    title,
    description,
    price,
    surface,
    rooms,
    region,
    zone,
    city,
    municipality     AS municipalite,
    latitude,
    longitude,
    images,
    features,
    poi,
    transaction_type,
    currency,
    scraped_at,
    last_updated     AS last_update
"""


def _load_env():
    try:
        env_path = Path(__file__).resolve().parents[1] / ".env"
        if env_path.exists():
            from dotenv import load_dotenv
            load_dotenv(env_path)
    except Exception:
        pass


def _connect() -> psycopg2.extensions.connection:
    """Connexion Supabase PostgreSQL."""
    _load_env()
    return psycopg2.connect(
        host=os.getenv("SUPABASE_DB_HOST"),
        port=int(os.getenv("SUPABASE_DB_PORT", "5432")),
        dbname=os.getenv("SUPABASE_DB_NAME", "postgres"),
        user=os.getenv("SUPABASE_DB_USER", "postgres"),
        password=os.getenv("SUPABASE_DB_PASSWORD", ""),
        sslmode=os.getenv("SUPABASE_DB_SSLMODE", "require"),
    )


class FraudDBConnector:
    """Connecteur DSO 2.2 — tout sur Supabase."""

    def __init__(self):
        self.conn = _connect()
        self.conn.autocommit = False
        self._setup_table()
        logger.info("[FraudDB] Tables prêtes — fraud_detection_results")
        logger.info("[FraudDB] Connexion Supabase établie")

    def _setup_table(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fraud_detection_results (
                    id                       SERIAL PRIMARY KEY,
                    property_id              TEXT NOT NULL,
                    source_name              TEXT NOT NULL,
                    multimodal_score         FLOAT,
                    image_text_similarity    FLOAT,
                    price_deviation_pct      FLOAT,
                    mismatch_types           JSONB,
                    images_analyzed          INTEGER,
                    analyzed_at              TIMESTAMP DEFAULT NOW(),
                    multimodal_model_version TEXT DEFAULT 'clip_vit_base_patch32_v1',
                    UNIQUE (source_name, property_id)
                )
            """)
        self.conn.commit()

    # ── Lecture listings (Supabase) ───────────────────────────────────────────

    def fetch_listings_with_images(
        self,
        min_images: int = 1,
        limit: int = 10000,
    ) -> List[Dict[str, Any]]:
        """Listings avec images depuis Supabase."""
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"""
                    SELECT {LISTING_SELECT}
                    FROM listings
                    WHERE images IS NOT NULL
                      AND jsonb_array_length(images) >= %s
                      AND price IS NOT NULL
                      AND region IS NOT NULL
                    ORDER BY scraped_at DESC
                    LIMIT %s
                """, (min_images, limit))
                rows = cur.fetchall()
            records = [dict(r) for r in rows]
            logger.info(f"[FraudDB] {len(records)} listings avec images chargés")
            return records
        except Exception as e:
            self.conn.rollback()
            logger.error(f"[FraudDB] Erreur fetch_listings_with_images: {e}")
            return []

    def get_regional_price_stats(self) -> Dict[str, Dict[str, float]]:
        """Statistiques de prix par région/type/transaction depuis Supabase."""
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        LOWER(region)           AS region,
                        LOWER(property_type)    AS prop_type,
                        transaction_type,
                        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price) AS q25,
                        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY price) AS median,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price) AS q75,
                        AVG(price)                                           AS mean,
                        STDDEV(price)                                        AS std,
                        COUNT(*)                                             AS count,
                        PERCENTILE_CONT(0.50) WITHIN GROUP (
                            ORDER BY price / NULLIF(surface, 0)
                        )                                                    AS median_price_m2
                    FROM listings
                    WHERE price > 0
                      AND region IS NOT NULL
                      AND property_type IS NOT NULL
                    GROUP BY LOWER(region), LOWER(property_type), transaction_type
                    HAVING COUNT(*) >= 5
                """)
                rows = cur.fetchall()

            stats = {}
            for row in rows:
                key = f"{row['region']}|{row['prop_type']}|{row['transaction_type']}"
                stats[key] = {
                    "q25":             float(row["q25"]    or 0),
                    "median":          float(row["median"] or 0),
                    "q75":             float(row["q75"]    or 0),
                    "mean":            float(row["mean"]   or 0),
                    "std":             float(row["std"]    or 1),
                    "count":           int(row["count"]),
                    "median_price_m2": float(row["median_price_m2"] or 0),
                }
            logger.info(f"[FraudDB] Stats régionales calculées pour {len(stats)} groupes")
            return stats
        except Exception as e:
            self.conn.rollback()
            logger.error(f"[FraudDB] Erreur get_regional_price_stats: {e}")
            return {}

    # ── Écriture résultats DSO 2.2 (Supabase) ────────────────────────────────

    def save_multimodal_results(self, results: List[Dict[str, Any]]) -> Dict[str, int]:
        """Sauvegarde les résultats DSO 2.2 dans fraud_detection_results."""
        saved = errors = 0

        for r in results:
            try:
                with self.conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO fraud_detection_results
                            (property_id, source_name,
                             multimodal_score, image_text_similarity,
                             price_deviation_pct, mismatch_types,
                             images_analyzed, multimodal_model_version)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source_name, property_id) DO UPDATE SET
                            multimodal_score         = EXCLUDED.multimodal_score,
                            image_text_similarity    = EXCLUDED.image_text_similarity,
                            price_deviation_pct      = EXCLUDED.price_deviation_pct,
                            mismatch_types           = EXCLUDED.mismatch_types,
                            images_analyzed          = EXCLUDED.images_analyzed,
                            multimodal_model_version = EXCLUDED.multimodal_model_version,
                            analyzed_at              = NOW()
                    """, (
                        r["property_id"],
                        r["source_name"],
                        r.get("multimodal_score"),
                        r.get("image_text_similarity"),
                        r.get("price_deviation_pct"),
                        Json(r.get("mismatch_types", [])),
                        r.get("images_analyzed", 0),
                        r.get("model_version", "clip_vit_base_patch32_v1"),
                    ))
                self.conn.commit()
                saved += 1
            except Exception as e:
                self.conn.rollback()
                logger.error(f"[FraudDB] Erreur save_multimodal {r.get('property_id')}: {e}")
                errors += 1

        logger.info(f"[FraudDB] Résultats multimodaux — {saved} sauvegardés, {errors} erreurs")
        return {"saved": saved, "errors": errors}

    # ── Résumé ────────────────────────────────────────────────────────────────

    def get_fraud_summary(self) -> Dict[str, Any]:
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        COUNT(*)                                                          AS total_analyzed,
                        SUM(CASE WHEN multimodal_score < 0.31 THEN 1 ELSE 0 END)         AS total_incoherent,
                        SUM(CASE WHEN multimodal_score BETWEEN 0.31 AND 0.55 THEN 1 ELSE 0 END) AS total_suspect,
                        SUM(CASE WHEN multimodal_score >= 0.56 THEN 1 ELSE 0 END)        AS total_coherent,
                        ROUND(AVG(multimodal_score)::numeric, 4)                         AS avg_multimodal_score,
                        ROUND(AVG(price_deviation_pct)::numeric, 2)                      AS avg_price_deviation
                    FROM fraud_detection_results
                """)
                row = cur.fetchone()
            return dict(row) if row else {}
        except Exception as e:
            self.conn.rollback()
            logger.error(f"[FraudDB] Erreur get_fraud_summary: {e}")
            return {}

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass
