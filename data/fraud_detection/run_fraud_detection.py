"""
EstateMind — Détection de Fraude : DSO 2.2 (Multimodal CLIP)
=============================================================
Usage :
    # Depuis le dossier data/
    python fraud_detection/run_fraud_detection.py
    python fraud_detection/run_fraud_detection.py --limit 500
    python fraud_detection/run_fraud_detection.py --limit 200 --region Tunis
    python fraud_detection/run_fraud_detection.py --no-save
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from loguru import logger

from fraud_detection.db_connector import FraudDBConnector
from fraud_detection.multimodal.consistency_classifier import run_multimodal_pipeline


# ── Logging ───────────────────────────────────────────────────────────────────

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}",
    level="INFO",
)
log_dir = Path(__file__).resolve().parents[1] / "logs"
log_dir.mkdir(exist_ok=True)
logger.add(log_dir / "fraud_detection.log", rotation="10 MB", retention="7 days", level="DEBUG")


# ── Pipeline DSO 2.2 ──────────────────────────────────────────────────────────

def run_pipeline(
    db: FraudDBConnector,
    limit: int,
    region: Optional[str],
    save: bool,
) -> None:
    logger.info("=" * 60)
    logger.info("DSO 2.2 — Cohérence Multimodale CLIP")
    logger.info(f"  Limit  : {limit} listings")
    logger.info(f"  Région : {region or 'toutes'}")
    logger.info(f"  Save   : {save}")
    logger.info("=" * 60)
    t0 = time.time()

    logger.info(f"[DSO2.2] Chargement des listings avec images (limit={limit})")
    listings = db.fetch_listings_with_images(min_images=1, limit=limit)
    if not listings:
        logger.warning("[DSO2.2] Aucun listing avec images — abandon")
        return

    if region:
        region_lower = region.lower()
        listings = [l for l in listings if (l.get("region") or "").lower() == region_lower]
        logger.info(f"[DSO2.2] Après filtre région '{region}' : {len(listings)} listings")
        if not listings:
            logger.warning("[DSO2.2] Aucun listing pour cette région — abandon")
            return

    regional_stats = db.get_regional_price_stats()

    results = run_multimodal_pipeline(
        listings=listings,
        regional_stats=regional_stats,
        max_images_per_listing=3,
    )

    if not results:
        logger.error("[DSO2.2] Aucun résultat produit")
        return

    if save:
        logger.info("[DSO2.2] Sauvegarde des résultats sur Supabase...")
        db.save_multimodal_results(results)

    elapsed = round(time.time() - t0, 1)
    n_incoherent = sum(1 for r in results if r.get("multimodal_score", 1) < 0.31)
    n_suspect    = sum(1 for r in results if 0.31 <= r.get("multimodal_score", 1) < 0.56)
    n_coherent   = sum(1 for r in results if r.get("multimodal_score", 0) >= 0.56)
    logger.info(
        f"[DSO2.2] Terminé en {elapsed}s — {len(results)} listings | "
        f"Incohérents: {n_incoherent} | Suspects: {n_suspect} | Cohérents: {n_coherent}"
    )

    if save:
        summary = db.get_fraud_summary()
        logger.info("=" * 60)
        logger.info("RÉSUMÉ SUPABASE")
        logger.info(f"  Total en base       : {summary.get('total_analyzed', 0)}")
        logger.info(f"  Incohérents (< 0.31): {summary.get('total_incoherent', 0)}")
        logger.info(f"  Suspects            : {summary.get('total_suspect', 0)}")
        logger.info(f"  Cohérents           : {summary.get('total_coherent', 0)}")
        logger.info(f"  Avg multimodal score: {summary.get('avg_multimodal_score') or 0:.3f}")
        logger.info("=" * 60)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="EstateMind DSO 2.2 — Détection de fraude multimodale CLIP",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--limit",   type=int, default=1000, help="Nombre max de listings (défaut: 1000)")
    parser.add_argument("--region",  type=str, default=None, help="Filtrer par gouvernorat (ex: Tunis)")
    parser.add_argument("--no-save", action="store_true",    help="Ne pas sauvegarder sur Supabase")
    return parser.parse_args()


def main():
    args = parse_args()
    try:
        db = FraudDBConnector()
    except Exception as e:
        logger.error(f"Connexion DB échouée : {e}")
        sys.exit(1)

    try:
        run_pipeline(
            db     = db,
            limit  = args.limit,
            region = args.region,
            save   = not args.no_save,
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
