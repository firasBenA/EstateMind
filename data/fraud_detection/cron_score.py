"""
EstateMind — Cron Job : Scoring automatique DSO 2.2
====================================================
Ce script tourne automatiquement toutes les X heures.
Il score les nouvelles annonces Supabase et sauvegarde
les résultats dans fraud_detection_results.

Usage manuel :
    cd data/
    python fraud_detection/cron_score.py

Planification Linux (crontab -e) :
    0 */6 * * * cd /chemin/vers/EstateMind/data && python fraud_detection/cron_score.py >> logs/cron.log 2>&1

Planification Windows (Task Scheduler) :
    Programme : python
    Arguments : fraud_detection/cron_score.py
    Dossier   : C:\\chemin\\vers\\EstateMind\\data
    Intervalle: Toutes les 6 heures
"""
from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from loguru import logger
from fraud_detection.db_connector import FraudDBConnector
from fraud_detection.multimodal.consistency_classifier import run_multimodal_pipeline

# ── Configuration ─────────────────────────────────────────────────────────────

LIMIT        = 5000   # Nombre max d'annonces à scorer par exécution
MAX_IMAGES   = 3      # Images analysées par annonce

# ── Exécution ─────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 50)
    logger.info(f"Cron DSO 2.2 démarré — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    logger.info("=" * 50)

    try:
        db = FraudDBConnector()
    except Exception as e:
        logger.error(f"Connexion Supabase échouée : {e}")
        sys.exit(1)

    try:
        # 1. Charger les annonces avec images
        listings = db.fetch_listings_with_images(min_images=1, limit=LIMIT)
        if not listings:
            logger.warning("Aucune annonce avec images — fin")
            return

        # 2. Stats régionales de prix
        regional_stats = db.get_regional_price_stats()

        # 3. Pipeline CLIP complet
        results = run_multimodal_pipeline(
            listings=listings,
            regional_stats=regional_stats,
            max_images_per_listing=MAX_IMAGES,
        )

        # 4. Sauvegarde Supabase
        if results:
            db.save_multimodal_results(results)

        # 5. Résumé
        summary = db.get_fraud_summary()
        logger.info("── Résumé Supabase ──────────────────────────────")
        logger.info(f"  Total en base   : {summary.get('total_analyzed', 0)}")
        logger.info(f"  Incohérents     : {summary.get('total_incoherent', 0)}")
        logger.info(f"  Suspects        : {summary.get('total_suspect', 0)}")
        logger.info(f"  Cohérents       : {summary.get('total_coherent', 0)}")
        logger.info(f"  Avg score       : {summary.get('avg_multimodal_score') or 0:.3f}")
        logger.info("=" * 50)

    finally:
        db.close()


if __name__ == "__main__":
    main()
