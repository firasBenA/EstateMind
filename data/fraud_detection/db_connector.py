"""
EstateMind — Fraud Detection DB Connector
==========================================
Gère la connexion PostgreSQL pour le module de détection de fraude.

Responsabilités :
- Charger les listings depuis la table `listings` avec toutes les colonnes nécessaires
- Créer et maintenir la table `fraud_detection_results`
- Écrire les résultats DSO 2.1 (anomalie tabulaire) et DSO 2.2 (cohérence multimodale)
"""
from __future__ import annotations

import os
import json
from pathlib import Path
from typing import List, Dict, Any, Optional

import psycopg2
from psycopg2.extras import RealDictCursor, Json
from loguru import logger


# ── Colonnes extraites pour la détection de fraude ───────────────────────────

LISTING_COLUMNS = [
    "id",
    "property_id",
    "source_name",
    "url",
    "type",
    "title",
    "description",
    "price",
    "surface",
    "rooms",
    "region",
    "zone",
    "city",
    "municipalite",
    "latitude",
    "longitude",
    "images",       # JSONB → list of URLs
    "features",     # JSONB → list of amenity strings
    "poi",          # JSONB → list of POI dicts
    "transaction_type",
    "currency",
    "scraped_at",
    "last_update",
]


# ── Connexion ─────────────────────────────────────────────────────────────────

def _get_connection():
    """Ouvre une connexion PostgreSQL depuis les variables d'environnement."""
    try:
        data_dir = Path(__file__).resolve().parents[2]
        env_path = data_dir / ".env"
        if env_path.exists():
            from dotenv import load_dotenv
            load_dotenv(env_path)
    except Exception:
        pass

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DB", "estatemind"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "capTEEMO500"),
    )


class FraudDBConnector:
    """
    Connecteur dédié au module de détection de fraude.
    Encapsule toutes les requêtes PostgreSQL pour DSO 2.1 et DSO 2.2.
    """

    def __init__(self):
        self.conn = _get_connection()
        self.conn.autocommit = False
        self._setup_fraud_table()
        logger.info("[FraudDB] Connexion PostgreSQL établie")

    # ── Setup ─────────────────────────────────────────────────────────────────

    def _setup_fraud_table(self):
        """Crée la table fraud_detection_results si elle n'existe pas."""
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fraud_detection_results (
                    id                          SERIAL PRIMARY KEY,
                    property_id                 TEXT NOT NULL,
                    source_name                 TEXT NOT NULL,

                    -- DSO 2.1 : Détection d'anomalies tabulaires
                    anomaly_score               FLOAT,
                    is_anomaly                  BOOLEAN,
                    fraud_score                 FLOAT,       -- normalisé 0-100
                    fraud_flags                 JSONB,       -- liste de flags spécifiques
                    regional_context            JSONB,       -- stats régionales utilisées

                    -- DSO 2.2 : Cohérence multimodale
                    multimodal_score            FLOAT,       -- 0.0 (incohérent) → 1.0 (cohérent)
                    image_text_similarity       FLOAT,       -- cosine sim CLIP image vs texte
                    price_deviation_pct         FLOAT,       -- % écart vs médiane régionale
                    mismatch_types              JSONB,       -- ["price_high_for_images", ...]
                    images_analyzed             INTEGER,     -- nb images traitées

                    -- Méta
                    analyzed_at                 TIMESTAMP DEFAULT NOW(),
                    tabular_model_version       TEXT DEFAULT 'isolation_forest_v1',
                    multimodal_model_version    TEXT DEFAULT 'clip_vit_base_patch32_v1',

                    UNIQUE (source_name, property_id)
                )
            """)
        self.conn.commit()
        logger.info("[FraudDB] Table fraud_detection_results prête")

    # ── Lecture ───────────────────────────────────────────────────────────────

    def fetch_listings(
        self,
        limit: int = 50000,
        region: Optional[str] = None,
        transaction_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Charge les listings depuis PostgreSQL.

        Args:
            limit: Nombre maximum de lignes
            region: Filtrer par gouvernorat (ex: 'Tunis', 'Sfax')
            transaction_type: 'Sale' ou 'Rent'

        Returns:
            Liste de dicts avec toutes les colonnes LISTING_COLUMNS
        """
        cols = ", ".join(LISTING_COLUMNS)
        conditions = []
        params: List[Any] = []

        if region:
            conditions.append("LOWER(region) = LOWER(%s)")
            params.append(region)
        if transaction_type:
            conditions.append("transaction_type = %s")
            params.append(transaction_type)

        where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        params.append(limit)

        query = f"""
            SELECT {cols}
            FROM listings
            {where_clause}
            ORDER BY scraped_at DESC
            LIMIT %s
        """

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
            records = [dict(r) for r in rows]
            logger.info(f"[FraudDB] {len(records)} listings chargés" +
                        (f" (région={region})" if region else ""))
            return records
        except Exception as e:
            self.conn.rollback()
            logger.error(f"[FraudDB] Erreur fetch_listings: {e}")
            return []

    def fetch_listings_with_images(
        self,
        min_images: int = 1,
        limit: int = 10000,
    ) -> List[Dict[str, Any]]:
        """
        Charge uniquement les listings qui ont des images pour DSO 2.2.

        Args:
            min_images: Nombre minimum d'images requis
            limit: Nombre maximum de lignes
        """
        cols = ", ".join(LISTING_COLUMNS)
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"""
                    SELECT {cols}
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
        """
        Calcule les statistiques de prix par région + type + transaction.
        Utilisé comme référence pour la déviation de prix dans DSO 2.2.

        Returns:
            dict: {"{region}|{type}|{tx_type}": {median, q25, q75, mean, std, count}}
        """
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        LOWER(region)           AS region,
                        LOWER(type)             AS prop_type,
                        transaction_type,
                        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price)     AS q25,
                        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY price)     AS median,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price)     AS q75,
                        AVG(price)                                               AS mean,
                        STDDEV(price)                                            AS std,
                        COUNT(*)                                                 AS count,
                        PERCENTILE_CONT(0.50) WITHIN GROUP (
                            ORDER BY price / NULLIF(surface, 0)
                        )                                                        AS median_price_m2
                    FROM listings
                    WHERE price > 0
                      AND region IS NOT NULL
                      AND type   IS NOT NULL
                    GROUP BY LOWER(region), LOWER(type), transaction_type
                    HAVING COUNT(*) >= 5
                """)
                rows = cur.fetchall()

            stats = {}
            for row in rows:
                key = f"{row['region']}|{row['prop_type']}|{row['transaction_type']}"
                stats[key] = {
                    "q25":            float(row["q25"]   or 0),
                    "median":         float(row["median"] or 0),
                    "q75":            float(row["q75"]   or 0),
                    "mean":           float(row["mean"]  or 0),
                    "std":            float(row["std"]   or 1),
                    "count":          int(row["count"]),
                    "median_price_m2": float(row["median_price_m2"] or 0),
                }
            logger.info(f"[FraudDB] Stats régionales calculées pour {len(stats)} groupes")
            return stats
        except Exception as e:
            self.conn.rollback()
            logger.error(f"[FraudDB] Erreur get_regional_price_stats: {e}")
            return {}

    # ── Écriture DSO 2.1 ──────────────────────────────────────────────────────

    def save_tabular_results(self, results: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Sauvegarde les résultats DSO 2.1 (anomalie tabulaire) dans fraud_detection_results.

        Chaque result dict doit avoir :
            property_id, source_name,
            anomaly_score, is_anomaly, fraud_score,
            fraud_flags (list), regional_context (dict)
        """
        inserted = updated = errors = 0

        for r in results:
            try:
                with self.conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO fraud_detection_results
                            (property_id, source_name,
                             anomaly_score, is_anomaly, fraud_score,
                             fraud_flags, regional_context,
                             tabular_model_version)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source_name, property_id) DO UPDATE SET
                            anomaly_score           = EXCLUDED.anomaly_score,
                            is_anomaly              = EXCLUDED.is_anomaly,
                            fraud_score             = EXCLUDED.fraud_score,
                            fraud_flags             = EXCLUDED.fraud_flags,
                            regional_context        = EXCLUDED.regional_context,
                            tabular_model_version   = EXCLUDED.tabular_model_version,
                            analyzed_at             = NOW()
                    """, (
                        r["property_id"],
                        r["source_name"],
                        r.get("anomaly_score"),
                        r.get("is_anomaly", False),
                        r.get("fraud_score"),
                        Json(r.get("fraud_flags", [])),
                        Json(r.get("regional_context", {})),
                        r.get("model_version", "isolation_forest_v1"),
                    ))
                self.conn.commit()
                inserted += 1
            except Exception as e:
                self.conn.rollback()
                logger.error(f"[FraudDB] Erreur save_tabular {r.get('property_id')}: {e}")
                errors += 1

        logger.info(f"[FraudDB] Résultats tabulaires — {inserted} sauvegardés, {errors} erreurs")
        return {"saved": inserted, "errors": errors}

    # ── Écriture DSO 2.2 ──────────────────────────────────────────────────────

    def save_multimodal_results(self, results: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Sauvegarde les résultats DSO 2.2 (cohérence multimodale).

        Chaque result dict doit avoir :
            property_id, source_name,
            multimodal_score, image_text_similarity,
            price_deviation_pct, mismatch_types (list),
            images_analyzed (int)
        """
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
                            multimodal_score            = EXCLUDED.multimodal_score,
                            image_text_similarity       = EXCLUDED.image_text_similarity,
                            price_deviation_pct         = EXCLUDED.price_deviation_pct,
                            mismatch_types              = EXCLUDED.mismatch_types,
                            images_analyzed             = EXCLUDED.images_analyzed,
                            multimodal_model_version    = EXCLUDED.multimodal_model_version,
                            analyzed_at                 = NOW()
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

    # ── Lecture des résultats ─────────────────────────────────────────────────

    def fetch_fraud_results(
        self,
        only_anomalies: bool = False,
        min_fraud_score: Optional[float] = None,
        limit: int = 10000,
    ) -> List[Dict[str, Any]]:
        """Charge les résultats de détection de fraude pour analyse."""
        conditions = []
        params: List[Any] = []

        if only_anomalies:
            conditions.append("is_anomaly = TRUE")
        if min_fraud_score is not None:
            conditions.append("fraud_score >= %s")
            params.append(min_fraud_score)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        params.append(limit)

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"""
                    SELECT fdr.*, l.title, l.price, l.surface, l.region, l.city,
                           l.url, l.type, l.transaction_type
                    FROM fraud_detection_results fdr
                    LEFT JOIN listings l
                        ON l.source_name = fdr.source_name
                       AND l.property_id = fdr.property_id
                    {where}
                    ORDER BY fraud_score DESC NULLS LAST
                    LIMIT %s
                """, params)
                rows = cur.fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"[FraudDB] Erreur fetch_fraud_results: {e}")
            return []

    def get_fraud_summary(self) -> Dict[str, Any]:
        """Résumé statistique des résultats de détection de fraude."""
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        COUNT(*)                                    AS total_analyzed,
                        SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS total_anomalies,
                        AVG(fraud_score)                            AS avg_fraud_score,
                        AVG(multimodal_score)                       AS avg_multimodal_score,
                        MIN(analyzed_at)                            AS first_analysis,
                        MAX(analyzed_at)                            AS last_analysis
                    FROM fraud_detection_results
                """)
                row = dict(cur.fetchone() or {})
            return row
        except Exception as e:
            self.conn.rollback()
            logger.error(f"[FraudDB] Erreur get_fraud_summary: {e}")
            return {}

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass
