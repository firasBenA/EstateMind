"""
EstateMind — DSO 2.1 : Évaluateur et Rapport  (v2)
====================================================
Améliorations v2 :
  - SHAP (SHapley Additive exPlanations) pour l'explicabilité du modèle
  - Distribution corrigée des niveaux de risque (fraud_score calibré)
  - Rapport enrichi : SHAP summary + top suspects avec raisons explicites

Fonctionnalités :
  - Rapport JSON complet (métriques globales + par région)
  - Distribution des fraud_scores (histogramme)
  - Taux d'anomalie par région (bar chart)
  - SHAP summary plot : importance globale des features
  - SHAP waterfall : explication individuelle d'une annonce suspecte
  - Export CSV des résultats complets
  - Top-20 annonces suspectes avec fraud_flags
"""
from __future__ import annotations

import json
import os
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger
from sklearn.pipeline import Pipeline

from fraud_detection.tabular.feature_engineering import NUMERIC_FEATURES

REPORTS_DIR = Path(__file__).resolve().parents[2] / "reports" / "fraud_tabular"

# Seuils du fraud_score (version sigmoid calibrée)
RISK_THRESHOLDS = {
    "normal":       (0,  20),
    "suspect":      (21, 50),
    "fraud_likely": (51, 75),
    "fraud_high":   (76, 100),
}


# ── Métriques globales ────────────────────────────────────────────────────────

def compute_global_metrics(results_df: pd.DataFrame) -> Dict[str, Any]:
    """Calcule les métriques globales sur l'ensemble des résultats."""
    if results_df.empty:
        return {}

    total        = len(results_df)
    n_anomalies  = int(results_df["is_anomaly"].sum())
    fraud_scores = results_df["fraud_score"].dropna()

    risk_levels = {}
    for level, (lo, hi) in RISK_THRESHOLDS.items():
        risk_levels[level] = int(((fraud_scores >= lo) & (fraud_scores <= hi)).sum())

    all_flags = []
    for flags in results_df["fraud_flags"]:
        if isinstance(flags, list):
            all_flags.extend(flags)
        elif isinstance(flags, str):
            try:
                all_flags.extend(json.loads(flags))
            except Exception:
                pass

    return {
        "total_listings":    total,
        "n_anomalies":       n_anomalies,
        "anomaly_rate_pct":  round(n_anomalies / total * 100, 2),
        "fraud_score_stats": {
            "mean":   round(float(fraud_scores.mean()),             2),
            "std":    round(float(fraud_scores.std()),              2),
            "min":    round(float(fraud_scores.min()),              2),
            "p25":    round(float(fraud_scores.quantile(0.25)),     2),
            "median": round(float(fraud_scores.median()),           2),
            "p75":    round(float(fraud_scores.quantile(0.75)),     2),
            "max":    round(float(fraud_scores.max()),              2),
        },
        "risk_level_distribution": risk_levels,
        "top_fraud_flags":         dict(Counter(all_flags).most_common(10)),
    }


# ── Métriques par région ──────────────────────────────────────────────────────

def compute_regional_metrics(results_df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """Métriques de détection de fraude par gouvernorat."""
    if results_df.empty or "region_norm" not in results_df.columns:
        return {}

    regional = {}
    for region, group in results_df.groupby("region_norm"):
        n     = len(group)
        n_anom = int(group["is_anomaly"].sum())
        flags  = []
        for f in group["fraud_flags"]:
            if isinstance(f, list):
                flags.extend(f)

        regional[str(region)] = {
            "total":           n,
            "n_anomalies":     n_anom,
            "anomaly_rate":    round(n_anom / n * 100, 2),
            "avg_fraud_score": round(float(group["fraud_score"].mean()), 2),
            "top_flags":       dict(Counter(flags).most_common(5)),
        }

    return dict(sorted(regional.items(),
                       key=lambda x: x[1]["anomaly_rate"], reverse=True))


# ── Top-N suspects ────────────────────────────────────────────────────────────

def get_top_suspicious(results_df: pd.DataFrame, n: int = 20) -> List[Dict[str, Any]]:
    """Retourne les N annonces avec le fraud_score le plus élevé."""
    if results_df.empty:
        return []
    cols = [c for c in ["property_id", "source_name", "fraud_score",
                         "is_anomaly", "fraud_flags", "region_norm", "anomaly_score"]
            if c in results_df.columns]
    return (results_df[cols]
            .sort_values("fraud_score", ascending=False)
            .head(n)
            .to_dict(orient="records"))


# ── SHAP Explicabilité ────────────────────────────────────────────────────────

def compute_shap_values(
    models: Dict[str, Pipeline],
    meta_df: pd.DataFrame,
    feature_df: pd.DataFrame,
    max_samples: int = 500,
) -> Optional[Tuple[np.ndarray, np.ndarray, List[str]]]:
    """
    Calcule les valeurs SHAP pour le modèle Isolation Forest de la région
    la plus représentée (Tunis en général).

    Args:
        models:      Dict {region_name: Pipeline} depuis train_regional_models
        meta_df:     DataFrame de métadonnées (avec region_norm)
        feature_df:  DataFrame de features (colonnes = NUMERIC_FEATURES)
        max_samples: Nombre max de listings à analyser (SHAP est lent sur CPU)

    Returns:
        Tuple (shap_values array, X_sample array, feature_names list) ou None
    """
    try:
        import shap
    except ImportError:
        logger.warning("[SHAP] shap non installé — pip install shap")
        return None

    if not models:
        return None

    # Choisir la région la plus peuplée pour SHAP (la plus représentative)
    if "region_norm" in meta_df.columns:
        top_region = meta_df["region_norm"].value_counts().index[0]
    else:
        top_region = next(iter(models))

    # Chercher le modèle correspondant
    model_key = top_region if top_region in models else next(iter(models))
    pipeline  = models[model_key]

    # Sélectionner les features de cette région
    if "region_norm" in meta_df.columns:
        mask = meta_df["region_norm"] == top_region
        X_full = feature_df[mask][NUMERIC_FEATURES].values
    else:
        X_full = feature_df[NUMERIC_FEATURES].values

    # Limiter à max_samples pour la performance
    if len(X_full) > max_samples:
        idx = np.random.RandomState(42).choice(len(X_full), max_samples, replace=False)
        X_full = X_full[idx]

    # Normaliser avec le scaler du pipeline
    scaler  = pipeline.named_steps["scaler"]
    iforest = pipeline.named_steps["iforest"]
    X_scaled = scaler.transform(X_full)

    logger.info(
        f"[SHAP] Calcul des valeurs SHAP pour '{model_key}' "
        f"({len(X_scaled)} listings, {len(NUMERIC_FEATURES)} features)"
    )

    try:
        explainer   = shap.TreeExplainer(iforest)
        shap_values = explainer.shap_values(X_scaled)
        logger.info(f"[SHAP] SHAP values calculées — shape={shap_values.shape}")
        return shap_values, X_full, NUMERIC_FEATURES
    except Exception as e:
        logger.error(f"[SHAP] Erreur calcul: {e}")
        return None


def plot_shap_summary(
    shap_values: np.ndarray,
    X: np.ndarray,
    feature_names: List[str],
    output_dir: Optional[Path] = None,
    region_name: str = "global",
) -> Optional[Path]:
    """
    Génère le SHAP summary plot (beeswarm).

    Interprétation :
      - Axe X : impact sur le score d'anomalie (positif = plus anormal)
      - Couleur : valeur de la feature (rouge = haute, bleu = basse)
      - Axe Y : features triées par importance décroissante

    Returns:
        Chemin du fichier PNG sauvegardé, ou None
    """
    try:
        import shap
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        logger.warning("[SHAP] shap ou matplotlib non disponible")
        return None

    save_dir = output_dir or REPORTS_DIR
    save_dir.mkdir(parents=True, exist_ok=True)
    ts  = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out = save_dir / f"shap_summary_{region_name}_{ts}.png"

    try:
        plt.figure(figsize=(10, 6))
        shap.summary_plot(
            shap_values,
            X,
            feature_names=feature_names,
            plot_type="dot",
            show=False,
            max_display=len(feature_names),
        )
        plt.title(
            f"SHAP — Importance des features (Isolation Forest, région={region_name})",
            fontsize=12,
        )
        plt.tight_layout()
        plt.savefig(out, dpi=130, bbox_inches="tight")
        plt.close()
        logger.info(f"[SHAP] Summary plot sauvegardé: {out}")
        return out
    except Exception as e:
        logger.error(f"[SHAP] Erreur plot: {e}")
        plt.close()
        return None


def explain_top_suspect(
    models: Dict[str, Pipeline],
    meta_df: pd.DataFrame,
    feature_df: pd.DataFrame,
    results_df: pd.DataFrame,
    output_dir: Optional[Path] = None,
) -> Optional[Path]:
    """
    Génère un SHAP waterfall plot pour l'annonce la plus suspecte.
    Montre pourquoi cette annonce précise a été flaggée.

    Returns:
        Chemin du PNG sauvegardé, ou None
    """
    try:
        import shap
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return None

    if results_df.empty or not models:
        return None

    # Trouver l'annonce la plus suspecte
    top_row = results_df.sort_values("fraud_score", ascending=False).iloc[0]
    region  = top_row.get("region_norm", "")
    prop_id = top_row.get("property_id", "?")

    model_key = region if region in models else next(iter(models))
    pipeline  = models[model_key]

    # Trouver l'index de ce listing dans feature_df
    if "region_norm" in meta_df.columns:
        mask = meta_df["region_norm"] == region
        r_meta = meta_df[mask].reset_index(drop=True)
        r_feat = feature_df[mask].reset_index(drop=True)
    else:
        r_meta = meta_df
        r_feat = feature_df

    # Trouver l'index par property_id
    match = r_meta[r_meta["property_id"] == prop_id]
    if match.empty:
        return None
    idx = match.index[0]

    X_all    = r_feat[NUMERIC_FEATURES].values
    scaler   = pipeline.named_steps["scaler"]
    iforest  = pipeline.named_steps["iforest"]
    X_scaled = scaler.transform(X_all)

    try:
        explainer   = shap.TreeExplainer(iforest)
        shap_values = explainer.shap_values(X_scaled)
        shap_exp    = shap.Explanation(
            values=shap_values[idx],
            base_values=explainer.expected_value,
            data=X_all[idx],
            feature_names=NUMERIC_FEATURES,
        )

        save_dir = output_dir or REPORTS_DIR
        save_dir.mkdir(parents=True, exist_ok=True)
        ts  = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        out = save_dir / f"shap_waterfall_top_suspect_{ts}.png"

        plt.figure(figsize=(10, 5))
        shap.waterfall_plot(shap_exp, show=False, max_display=11)
        plt.title(
            f"SHAP Waterfall — Annonce la plus suspecte (ID={prop_id})",
            fontsize=11,
        )
        plt.tight_layout()
        plt.savefig(out, dpi=130, bbox_inches="tight")
        plt.close()
        logger.info(f"[SHAP] Waterfall plot sauvegardé: {out}")
        return out
    except Exception as e:
        logger.error(f"[SHAP] Erreur waterfall: {e}")
        plt.close()
        return None


# ── Rapport complet ───────────────────────────────────────────────────────────

def generate_report(
    results_df: pd.DataFrame,
    run_id: Optional[str] = None,
    save: bool = True,
    models: Optional[Dict[str, Pipeline]] = None,
    meta_df: Optional[pd.DataFrame] = None,
    feature_df: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """
    Génère le rapport complet DSO 2.1 avec SHAP si models fournis.

    Args:
        results_df:  DataFrame des résultats
        run_id:      Identifiant du run (auto si None)
        save:        Sauvegarder JSON + CSV
        models:      Pipelines entraînés (pour SHAP)
        meta_df:     Méta-données (pour SHAP)
        feature_df:  Features (pour SHAP)
    """
    if run_id is None:
        run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    logger.info(f"[Evaluator] Génération du rapport {run_id}")

    global_metrics   = compute_global_metrics(results_df)
    regional_metrics = compute_regional_metrics(results_df)
    top_suspicious   = get_top_suspicious(results_df, n=20)

    report = {
        "run_id":            run_id,
        "generated_at":      datetime.utcnow().isoformat(),
        "model":             "IsolationForest v2 (sigmoid fraud_score, regional)",
        "global_metrics":    global_metrics,
        "regional_metrics":  regional_metrics,
        "top_20_suspicious": top_suspicious,
    }

    if save:
        _save_report(report, results_df, run_id)

    # Visualisations
    plot_fraud_distribution(results_df)

    # SHAP (si models disponibles)
    if models and meta_df is not None and feature_df is not None:
        shap_result = compute_shap_values(models, meta_df, feature_df)
        if shap_result:
            shap_values, X_sample, feat_names = shap_result
            # Région la plus peuplée pour le titre du plot
            top_region = (meta_df["region_norm"].value_counts().index[0]
                          if "region_norm" in meta_df.columns else "global")
            plot_shap_summary(shap_values, X_sample, feat_names,
                              region_name=top_region)
            explain_top_suspect(models, meta_df, feature_df, results_df)

    _log_report_summary(global_metrics)
    return report


# ── Sauvegarde ────────────────────────────────────────────────────────────────

def _save_report(report: Dict, results_df: pd.DataFrame, run_id: str) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    json_path = REPORTS_DIR / f"report_{run_id}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    logger.info(f"[Evaluator] Rapport JSON: {json_path}")

    if not results_df.empty:
        csv_path = REPORTS_DIR / f"results_{run_id}.csv"
        df_exp = results_df.copy()
        for col in ["fraud_flags", "regional_context"]:
            if col in df_exp.columns:
                df_exp[col] = df_exp[col].apply(
                    lambda x: json.dumps(x, ensure_ascii=False)
                    if isinstance(x, (list, dict)) else str(x)
                )
        df_exp.to_csv(csv_path, index=False, encoding="utf-8")
        logger.info(f"[Evaluator] CSV: {csv_path}")


def _log_report_summary(metrics: Dict[str, Any]) -> None:
    if not metrics:
        return
    logger.info("=" * 60)
    logger.info("[Evaluator] RAPPORT DSO 2.1 — Détection d'Anomalies")
    logger.info(f"  Total listings analysés : {metrics.get('total_listings', 0)}")
    logger.info(f"  Anomalies détectées      : {metrics.get('n_anomalies', 0)} "
                f"({metrics.get('anomaly_rate_pct', 0):.1f}%)")
    dist = metrics.get("risk_level_distribution", {})
    for level in ["normal", "suspect", "fraud_likely", "fraud_high"]:
        label = {
            "normal":       "Normal       ",
            "suspect":      "Suspect      ",
            "fraud_likely": "Fraud likely ",
            "fraud_high":   "Fraud high   ",
        }[level]
        logger.info(f"  {label}: {dist.get(level, 0)}")
    top_flags = metrics.get("top_fraud_flags", {})
    if top_flags:
        logger.info("  Top flags     :")
        for flag, count in list(top_flags.items())[:5]:
            logger.info(f"    - {flag}: {count}")
    logger.info("=" * 60)


# ── Visualisation ─────────────────────────────────────────────────────────────

def plot_fraud_distribution(
    results_df: pd.DataFrame,
    output_path: Optional[Path] = None,
) -> Optional[Path]:
    """
    Génère histogramme des fraud_scores + taux d'anomalie par région.
    Retourne None si matplotlib n'est pas disponible.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        logger.warning("[Evaluator] matplotlib non disponible")
        return None

    if results_df.empty or "fraud_score" not in results_df.columns:
        return None

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    ts  = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out = output_path or (REPORTS_DIR / f"fraud_dist_{ts}.png")

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # ── Histogramme global ──
    ax1 = axes[0]
    scores = results_df["fraud_score"].dropna()
    ax1.hist(scores, bins=40, color="steelblue", edgecolor="white", alpha=0.85)
    for threshold, color, label in [
        (21,  "orange",  "Suspect (21)"),
        (51,  "red",     "Fraud likely (51)"),
        (76,  "darkred", "Fraud high (76)"),
    ]:
        ax1.axvline(threshold, color=color, linestyle="--", linewidth=1.5, label=label)
    ax1.set_title("Distribution des Fraud Scores (sigmoid calibré)", fontsize=11)
    ax1.set_xlabel("Fraud Score [0–100]")
    ax1.set_ylabel("Nombre d'annonces")
    ax1.legend(fontsize=8)

    # ── Taux d'anomalie par région ──
    ax2 = axes[1]
    if "region_norm" in results_df.columns:
        rates = (
            results_df.groupby("region_norm")["is_anomaly"]
            .mean()
            .sort_values(ascending=False)
            .head(10) * 100
        )
        if not rates.empty:
            colors = ["#d62728" if r > 7 else "#ff7f0e" if r > 5 else "#1f77b4"
                      for r in rates.values]
            rates.plot(kind="barh", ax=ax2, color=colors, edgecolor="white")
            ax2.set_title("Taux d'anomalie par région (Top 10)", fontsize=11)
            ax2.set_xlabel("% d'anomalies")
            ax2.invert_yaxis()

    plt.tight_layout()
    plt.savefig(out, dpi=130, bbox_inches="tight")
    plt.close()
    logger.info(f"[Evaluator] Distribution plot: {out}")
    return out
