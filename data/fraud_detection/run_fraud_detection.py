"""
EstateMind — Point d'Entrée Principal : Détection de Fraude
============================================================
Orchestre les deux pipelines DSO 2.1 et DSO 2.2.

Usage :
    # Depuis le dossier data/
    python fraud_detection/run_fraud_detection.py --mode tabular
    python fraud_detection/run_fraud_detection.py --mode multimodal
    python fraud_detection/run_fraud_detection.py --mode all
    python fraud_detection/run_fraud_detection.py --mode all --limit 5000

Options :
    --mode      tabular | multimodal | all   (défaut: all)
    --limit     Nombre max de listings à traiter (défaut: 50000)
    --region    Filtrer par gouvernorat (ex: Tunis, Sfax)
    --no-save   Ne pas sauvegarder les résultats en DB
    --no-report Ne pas générer les rapports
    --retrain   Forcer le ré-entraînement du modèle tabulaire

Exemple complet :
    python fraud_detection/run_fraud_detection.py \\
        --mode all \\
        --limit 10000 \\
        --region Tunis
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

# Ajouter le répertoire parent au PYTHONPATH (pour les imports relatifs)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from loguru import logger

from fraud_detection.db_connector import FraudDBConnector
from fraud_detection.tabular.anomaly_model import (
    train_regional_models,
    predict_anomalies,
    save_models,
    load_models,
)
from fraud_detection.tabular.evaluator import generate_report
from fraud_detection.tabular.feature_engineering import build_feature_matrix
from fraud_detection.multimodal.consistency_classifier import run_multimodal_pipeline


# ── Configuration logging ─────────────────────────────────────────────────────

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}",
    level="INFO",
)
logger.add(
    Path(__file__).resolve().parents[1] / "logs" / "fraud_detection.log",
    rotation="10 MB",
    retention="7 days",
    level="DEBUG",
)


# ── Pipeline DSO 2.1 (Tabulaire) ─────────────────────────────────────────────

def run_tabular_pipeline(
    db: FraudDBConnector,
    limit: int,
    region: Optional[str],
    save: bool,
    report: bool,
    retrain: bool,
) -> None:
    """
    Pipeline DSO 2.1 : Détection d'anomalies tabulaires.

    Étapes :
      1. Chargement des listings depuis PostgreSQL
      2. Construction de la matrice de features (13 features)
      3. Entraînement / chargement des modèles Isolation Forest par région
      4. Prédictions : anomaly_score, is_anomaly, fraud_score
      5. Génération des flags règle-métier
      6. Sauvegarde en DB + rapport JSON/CSV
    """
    logger.info("=" * 60)
    logger.info("DSO 2.1 — Détection d'Anomalies Tabulaires")
    logger.info("=" * 60)
    t0 = time.time()

    # 1. Charger les données
    logger.info(f"[Tabular] Chargement des listings (limit={limit})")
    listings = db.fetch_listings(limit=limit, region=region)
    if not listings:
        logger.error("[Tabular] Aucun listing chargé — abandon")
        return

    # 2. Stats régionales (pour flags règle-métier)
    regional_stats = db.get_regional_price_stats()

    # 3. Construire la matrice de features (nécessaire pour SHAP)
    import pandas as pd
    meta_df, feature_df = build_feature_matrix(listings)

    # 4. Entraîner ou charger les modèles
    models_dir = Path(__file__).resolve().parents[1] / "models" / "fraud_tabular"
    existing_models = load_models(models_dir) if not retrain else {}

    if existing_models and not retrain:
        logger.info(f"[Tabular] Modèles existants chargés ({len(existing_models)} régions)")
        results_list = predict_anomalies(listings, existing_models, regional_stats)
        results_df   = pd.DataFrame(results_list)
        models       = existing_models
    else:
        logger.info("[Tabular] Entraînement des modèles régionaux")
        models, results_df = train_regional_models(listings, regional_stats)
        if models:
            save_models(models, models_dir)
        results_list = results_df.to_dict(orient="records") if not results_df.empty else []

    if results_df.empty:
        logger.error("[Tabular] Aucun résultat produit")
        return

    # 5. Sauvegarder en DB
    if save:
        logger.info("[Tabular] Sauvegarde des résultats en base de données")
        db.save_tabular_results(results_list)

    # 6. Rapport + SHAP
    if report:
        run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        generate_report(
            results_df,
            run_id=run_id,
            save=True,
            models=models,
            meta_df=meta_df,
            feature_df=feature_df,
        )

    elapsed = round(time.time() - t0, 1)
    logger.info(f"[Tabular] Terminé en {elapsed}s — {len(results_list)} listings analysés")


# ── Pipeline DSO 2.2 (Multimodal) ────────────────────────────────────────────

def run_multimodal_pipeline_full(
    db: FraudDBConnector,
    limit: int,
    save: bool,
) -> None:
    """
    Pipeline DSO 2.2 : Cohérence multimodale image × texte × prix.

    Étapes :
      1. Chargement des listings avec images depuis PostgreSQL
      2. Calcul des stats régionales de prix
      3. Encodage images (CLIP) + textes (CLIP) + déviation prix
      4. Score de cohérence multimodale (0–1)
      5. Identification des types de mismatch
      6. Sauvegarde en DB
    """
    logger.info("=" * 60)
    logger.info("DSO 2.2 — Classificateur de Cohérence Multimodale")
    logger.info("=" * 60)
    t0 = time.time()

    # 1. Charger uniquement les listings avec des images
    logger.info(f"[Multimodal] Chargement des listings avec images (limit={limit})")
    listings = db.fetch_listings_with_images(min_images=1, limit=limit)
    if not listings:
        logger.warning("[Multimodal] Aucun listing avec images — abandon")
        return

    # 2. Stats régionales
    regional_stats = db.get_regional_price_stats()

    # 3. Pipeline complet
    results = run_multimodal_pipeline(
        listings=listings,
        regional_stats=regional_stats,
        max_images_per_listing=3,
    )

    if not results:
        logger.error("[Multimodal] Aucun résultat produit")
        return

    # 4. Sauvegarder
    if save:
        logger.info("[Multimodal] Sauvegarde des résultats en base de données")
        db.save_multimodal_results(results)

    elapsed = round(time.time() - t0, 1)
    logger.info(f"[Multimodal] Terminé en {elapsed}s — {len(results)} listings analysés")


# ── Point d'entrée CLI ────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="EstateMind — Détection de fraude immobilière (DSO 2.1 + DSO 2.2)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["tabular", "multimodal", "all"],
        default="all",
        help="Pipeline à exécuter (défaut: all)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50000,
        help="Nombre maximum de listings à traiter (défaut: 50000)",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=None,
        help="Filtrer par gouvernorat (ex: Tunis, Sfax)",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Ne pas sauvegarder les résultats en base de données",
    )
    parser.add_argument(
        "--no-report",
        action="store_true",
        help="Ne pas générer de rapport JSON/CSV",
    )
    parser.add_argument(
        "--retrain",
        action="store_true",
        help="Forcer le ré-entraînement du modèle tabulaire",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    logger.info("=" * 60)
    logger.info("EstateMind — Détection de Fraude Immobilière")
    logger.info(f"Mode       : {args.mode}")
    logger.info(f"Limit      : {args.limit}")
    logger.info(f"Région     : {args.region or 'toutes'}")
    logger.info(f"Save DB    : {not args.no_save}")
    logger.info(f"Rapport    : {not args.no_report}")
    logger.info(f"Retrain    : {args.retrain}")
    logger.info("=" * 60)

    # Connexion DB
    try:
        db = FraudDBConnector()
    except Exception as e:
        logger.error(f"Connexion PostgreSQL échouée: {e}")
        sys.exit(1)

    try:
        if args.mode in ("tabular", "all"):
            run_tabular_pipeline(
                db       = db,
                limit    = args.limit,
                region   = args.region,
                save     = not args.no_save,
                report   = not args.no_report,
                retrain  = args.retrain,
            )

        if args.mode in ("multimodal", "all"):
            run_multimodal_pipeline_full(
                db    = db,
                limit = min(args.limit, 10000),  # limiter pour les images
                save  = not args.no_save,
            )

        # Résumé final
        if not args.no_save:
            summary = db.get_fraud_summary()
            logger.info("=" * 60)
            logger.info("RÉSUMÉ FINAL")
            logger.info(f"  Total analysés    : {summary.get('total_analyzed', 0)}")
            logger.info(f"  Anomalies (DSO2.1): {summary.get('total_anomalies', 0)}")
            logger.info(f"  Avg fraud score   : {summary.get('avg_fraud_score') or 0:.1f}")
            logger.info(f"  Avg multimodal    : {summary.get('avg_multimodal_score') or 0:.3f}")
            logger.info("=" * 60)

    finally:
        db.close()


if __name__ == "__main__":
    main()
