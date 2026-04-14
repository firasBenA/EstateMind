"""
EstateMind — DSO 2.1 : Modèle de Détection d'Anomalies Tabulaires  (v2)
========================================================================
Améliorations v2 :
  - Normalisation fraud_score corrigée : sigmoid sur decision_function()
    → la valeur 0 du decision_function = frontière → fraud_score = 50
  - regional_stats transmis aux flags règle-métier pendant l'entraînement
  - SHAP exporté depuis le module évaluateur (voir evaluator.py)

Architecture du pipeline par région :
─────────────────────────────────────
  1. StandardScaler     — normalisation z-score des features
  2. IsolationForest    — détection d'anomalies (contamination=0.05)
  3. decision_function  — score centré en 0 (négatif = anomalie)
  4. sigmoid            — mapping [-∞,+∞] → fraud_score [0,100]

Interprétation du fraud_score (0–100) :
  0–20   → Listing normal
  21–50  → Suspect — à surveiller
  51–75  → Probablement frauduleux / trompeur
  76–100 → Très probablement frauduleux

Flags spécifiques générés :
  "extreme_price_per_m2"        : prix/m² > 3σ de la médiane régionale
  "suspected_test_price"        : prix rond suspect (999999, 1, etc.)
  "no_images_high_price"        : pas de photos + prix élevé
  "description_too_short"       : description < 20 caractères
  "rooms_surface_inconsistent"  : ratio pièces/surface aberrant
  "price_too_low_for_region"    : prix < Q1 − 2*IQR régional
  "price_too_high_for_region"   : prix > Q3 + 2*IQR régional
"""
from __future__ import annotations

import pickle
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import numpy as np
import pandas as pd
from loguru import logger
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

from fraud_detection.tabular.feature_engineering import (
    NUMERIC_FEATURES,
    build_feature_matrix,
    split_by_region,
)

# ── Hyperparamètres ───────────────────────────────────────────────────────────

CONTAMINATION      = 0.05    # 5 % de taux de fraude attendu
N_ESTIMATORS       = 200
MAX_SAMPLES        = "auto"  # min(256, n_samples)
RANDOM_STATE       = 42
MIN_REGION_SAMPLES = 30

# Seuils règle-métier
TEST_PRICES      = {0, 1, 999, 9999, 99999, 999999, 9999999, 11111, 22222, 33333, 12345}
MAX_ROOMS_PER_M2 = 0.20   # > 1 pièce / 5 m² → aberrant
MIN_DESC_LENGTH  = 20

# Facteur d'échelle de la sigmoid (calibré sur les plages IF typiques)
# decision_function ≈ [-0.3, +0.3] → sigmoid(±0.3*SIGMOID_SCALE) ≈ 0.95 / 0.05
SIGMOID_SCALE = 20.0

MODELS_DIR = Path(__file__).resolve().parents[2] / "models" / "fraud_tabular"


# ── Score normalisé — sigmoid sur decision_function ───────────────────────────

def _sigmoid(x: float) -> float:
    """Sigmoid stable numériquement."""
    if x >= 0:
        return 1.0 / (1.0 + np.exp(-x))
    ex = np.exp(x)
    return ex / (1.0 + ex)


def _decision_to_fraud_score(decision_score: float) -> float:
    """
    Convertit le decision_function d'IsolationForest en fraud_score [0, 100].

    decision_function(x) :
      > 0  → inlier (normal)
      < 0  → outlier (anomalie)
      = 0  → frontière exacte

    Mapping : fraud_score = sigmoid(-d * SIGMOID_SCALE) * 100
      d = 0.0  → 50  (borderline)
      d = -0.2 → ~98 (très anormal)
      d = +0.2 → ~2  (très normal)
    """
    raw = _sigmoid(-decision_score * SIGMOID_SCALE) * 100.0
    return round(max(0.0, min(100.0, raw)), 2)


def decision_scores_to_fraud_scores(decision_scores: np.ndarray) -> np.ndarray:
    """Vectorisé — applique _decision_to_fraud_score sur un array."""
    return np.vectorize(_decision_to_fraud_score)(decision_scores)


# ── Flags règle-métier ────────────────────────────────────────────────────────

def _compute_rule_flags(
    row: dict,
    regional_stats: Optional[Dict] = None,
) -> List[str]:
    """
    Génère des flags d'anomalies règle-métier lisibles.
    Les flags complètent le score IF avec des raisons explicables.
    """
    flags = []
    price      = float(row.get("price")              or 0)
    surface    = float(row.get("surface")            or 0)
    rooms      = float(row.get("rooms")              or 0)
    desc_len   = int(row.get("description_length")   or 0)
    img_count  = float(row.get("image_count")        or 0)
    pm2_zscore = float(row.get("price_m2_zscore")    or 0)

    if price and int(price) in TEST_PRICES:
        flags.append("suspected_test_price")

    if img_count == 0 and price > 200_000:
        flags.append("no_images_high_price")

    if desc_len < MIN_DESC_LENGTH:
        flags.append("description_too_short")

    if surface > 0 and rooms > 0 and (rooms / surface) > MAX_ROOMS_PER_M2:
        flags.append("rooms_surface_inconsistent")

    if abs(pm2_zscore) > 3.0:
        flags.append("extreme_price_per_m2")

    if regional_stats and price > 0:
        key = (
            f"{row.get('region_norm', '')}|"
            f"{row.get('type_norm', '')}|"
            f"{row.get('transaction_type', '')}"
        )
        stats = regional_stats.get(key)
        if stats and stats.get("count", 0) >= 10:
            iqr  = stats["q75"] - stats["q25"]
            low  = stats["q25"] - 2.0 * iqr
            high = stats["q75"] + 2.0 * iqr
            if price < low:
                flags.append("price_too_low_for_region")
            elif price > high:
                flags.append("price_too_high_for_region")

    return flags


# ── Construction du pipeline sklearn ─────────────────────────────────────────

def _build_region_pipeline() -> Pipeline:
    return Pipeline([
        ("scaler", StandardScaler()),
        ("iforest", IsolationForest(
            n_estimators=N_ESTIMATORS,
            max_samples=MAX_SAMPLES,
            contamination=CONTAMINATION,
            random_state=RANDOM_STATE,
            n_jobs=-1,
        )),
    ])


# ── Entraînement ──────────────────────────────────────────────────────────────

def train_regional_models(
    listings: List[Dict[str, Any]],
    regional_stats: Optional[Dict] = None,
) -> Tuple[Dict[str, Pipeline], pd.DataFrame]:
    """
    Entraîne un modèle Isolation Forest par région.

    Args:
        listings:       Liste de dicts listing
        regional_stats: Stats régionales de prix (pour les flags règle-métier)

    Returns:
        (models dict, results DataFrame)
    """
    logger.info(f"[AnomalyModel] Entraînement sur {len(listings)} listings")

    meta_df, feature_df = build_feature_matrix(listings)
    if meta_df.empty:
        logger.error("[AnomalyModel] Matrice vide — abandon")
        return {}, pd.DataFrame()

    region_splits = split_by_region(meta_df, feature_df, min_samples=MIN_REGION_SAMPLES)

    models: Dict[str, Pipeline] = {}
    all_results: List[Dict] = []

    for region_name, (r_meta, r_feat) in region_splits.items():
        n = len(r_feat)
        if n < 10:
            logger.warning(f"[AnomalyModel] '{region_name}' trop petite ({n}), ignorée")
            continue

        logger.info(f"[AnomalyModel] Entraînement '{region_name}' — {n} listings")

        X = r_feat[NUMERIC_FEATURES].values
        pipeline = _build_region_pipeline()
        pipeline.fit(X)
        models[region_name] = pipeline

        # decision_function : centré en 0, négatif = anomalie
        decision_scores = pipeline.decision_function(X)
        predictions     = pipeline.predict(X)   # +1 normal, -1 anomalie
        fraud_scores    = decision_scores_to_fraud_scores(decision_scores)

        for i in range(n):
            feat_row = {**r_feat.iloc[i].to_dict(), **r_meta.iloc[i].to_dict()}
            flags = _compute_rule_flags(feat_row, regional_stats)

            all_results.append({
                "property_id":      r_meta.iloc[i]["property_id"],
                "source_name":      r_meta.iloc[i]["source_name"],
                "region_norm":      r_meta.iloc[i]["region_norm"],
                "anomaly_score":    round(float(decision_scores[i]), 4),
                "is_anomaly":       bool(predictions[i] == -1),
                "fraud_score":      float(fraud_scores[i]),
                "fraud_flags":      flags,
                "regional_context": {"region": region_name, "n_trained": n},
                "model_version":    "isolation_forest_v2",
            })

        n_anom = int((predictions == -1).sum())
        logger.info(
            f"[AnomalyModel] '{region_name}' — {n_anom} anomalies ({n_anom/n*100:.1f}%) "
            f"| fraud_score moyen={fraud_scores.mean():.1f}"
        )

    results_df = pd.DataFrame(all_results) if all_results else pd.DataFrame()

    if not results_df.empty:
        logger.info(
            f"[AnomalyModel] Terminé — {len(models)} modèles — "
            f"{len(results_df)} résultats — "
            f"{results_df['is_anomaly'].sum()} anomalies — "
            f"fraud_score moyen={results_df['fraud_score'].mean():.1f}"
        )

    return models, results_df


# ── Inférence ─────────────────────────────────────────────────────────────────

def predict_anomalies(
    listings: List[Dict[str, Any]],
    models: Dict[str, Pipeline],
    regional_stats: Optional[Dict] = None,
) -> List[Dict[str, Any]]:
    """
    Prédit les anomalies sur de nouveaux listings.

    Args:
        listings:       Liste de dicts listing
        models:         Modèles entraînés par train_regional_models()
        regional_stats: Stats régionales pour les flags règle-métier

    Returns:
        Liste de dicts résultat
    """
    meta_df, feature_df = build_feature_matrix(listings)
    if meta_df.empty:
        return []

    results = []
    region_splits = split_by_region(meta_df, feature_df, min_samples=1)

    for region_name, (r_meta, r_feat) in region_splits.items():
        # Sélectionner le meilleur modèle disponible
        model_key = region_name if region_name in models else "sparse"
        if model_key not in models:
            model_key = next(iter(models), None)
        if model_key is None:
            logger.warning(f"[AnomalyModel] Aucun modèle pour '{region_name}'")
            continue

        pipeline = models[model_key]
        X = r_feat[NUMERIC_FEATURES].values

        try:
            decision_scores = pipeline.decision_function(X)
            predictions     = pipeline.predict(X)
            fraud_scores    = decision_scores_to_fraud_scores(decision_scores)
        except Exception as e:
            logger.error(f"[AnomalyModel] Prédiction '{region_name}': {e}")
            continue

        for i in range(len(r_meta)):
            feat_row = {**r_feat.iloc[i].to_dict(), **r_meta.iloc[i].to_dict()}
            flags = _compute_rule_flags(feat_row, regional_stats)

            results.append({
                "property_id":      r_meta.iloc[i]["property_id"],
                "source_name":      r_meta.iloc[i]["source_name"],
                "region_norm":      r_meta.iloc[i]["region_norm"],
                "anomaly_score":    round(float(decision_scores[i]), 4),
                "is_anomaly":       bool(predictions[i] == -1),
                "fraud_score":      float(fraud_scores[i]),
                "fraud_flags":      flags,
                "regional_context": {"model_used": model_key},
                "model_version":    "isolation_forest_v2",
            })

    return results


# ── Sauvegarde / chargement ───────────────────────────────────────────────────

def save_models(models: Dict[str, Pipeline], output_dir: Optional[Path] = None) -> Path:
    save_dir = output_dir or MODELS_DIR
    save_dir.mkdir(parents=True, exist_ok=True)
    for region_name, pipeline in models.items():
        safe = region_name.replace(" ", "_").replace("/", "_")
        path = save_dir / f"if_{safe}.pkl"
        with open(path, "wb") as f:
            pickle.dump(pipeline, f)
        logger.info(f"[AnomalyModel] Sauvegardé: {path.name}")
    logger.info(f"[AnomalyModel] {len(models)} modèles → {save_dir}")
    return save_dir


def load_models(models_dir: Optional[Path] = None) -> Dict[str, Pipeline]:
    load_dir = models_dir or MODELS_DIR
    models: Dict[str, Pipeline] = {}
    if not load_dir.exists():
        logger.warning(f"[AnomalyModel] Dossier modèles introuvable: {load_dir}")
        return models
    for pkl_file in load_dir.glob("if_*.pkl"):
        region_name = pkl_file.stem.replace("if_", "").replace("_", " ")
        try:
            with open(pkl_file, "rb") as f:
                models[region_name] = pickle.load(f)
            logger.info(f"[AnomalyModel] Chargé: {region_name}")
        except Exception as e:
            logger.error(f"[AnomalyModel] Erreur chargement {pkl_file.name}: {e}")
    logger.info(f"[AnomalyModel] {len(models)} modèles chargés")
    return models
