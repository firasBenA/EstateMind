"""
EstateMind — Reliability Scorer (ML-Powered + XAI + Supabase Integration)

Assigns a reliability score (0-100) to each listing using:
- A trained XGBoost model (learns weights automatically from data)
- SHAP for explainability (why did this listing get this score?)
- Fallback to heuristic scoring if model is not yet trained

Score thresholds:
  < 25   → DROP from modeling (too many nulls or flagged as bad data)
  25-60  → LOW quality  — include with caution
  60-85  → GOOD quality — standard inclusion
  85-100 → HIGH quality — high confidence data, upweight in modeling

Usage:
  python -m preprocessing.steps.scorer
"""
from __future__ import annotations

import os
import sys
import json
import pickle
import warnings
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from loguru import logger

warnings.filterwarnings("ignore", category=UserWarning)

try:
    from .db_client import fetch_listings_for_scoring, update_listing_scores
except ImportError:
    # Fallback if running as script directly instead of module
    from db_client import fetch_listings_for_scoring, update_listing_scores

# ── Paths ─────────────────────────────────────────────────────────────────────

MODEL_DIR  = Path(os.environ.get("SCORER_MODEL_DIR", "models/scorer"))
MODEL_PATH = MODEL_DIR / "reliability_scorer.pkl"
META_PATH  = MODEL_DIR / "scorer_meta.json"


# ── Score thresholds (pipeline depends on these — do not rename) ──────────────

SCORE_LEVELS = {
    "HIGH":   (85, 100),
    "GOOD":   (60, 84),
    "LOW":    (25, 59),
    "DROP":   (0,  24),
}


# ── Feature definitions ───────────────────────────────────────────────────────
# These define WHAT to extract — the model learns HOW MUCH each one matters.

COMPLETENESS_FEATURES = [
    "price",
    "surface",
    "rooms",
    "city",
    "governorate",
    "coordinates",
    "description",
    "images",
    "features",
    "municipality",
]

BONUS_FEATURES = [
    "has_price_history",
    "price_changed",
    "cross_verified",
    "nlp_enriched",
]

PENALTY_FEATURES = [
    "price_outlier",
    "surface_outlier",
    "price_zero",
    "mostly_nulls",
    "suspected_duplicate",
    "price_per_m2_invalid",
]

ALL_FEATURES = COMPLETENESS_FEATURES + BONUS_FEATURES + PENALTY_FEATURES

# Human-readable labels for XAI explanations
FEATURE_LABELS = {
    "price":                "has a valid price",
    "surface":              "has surface area",
    "rooms":                "has room count",
    "city":                 "has city info",
    "governorate":          "has governorate",
    "coordinates":          "has GPS coordinates",
    "description":          "has a detailed description (50+ chars)",
    "images":               "has images",
    "features":             "has listed amenities/features",
    "municipality":         "has municipality/district",
    "has_price_history":    "seen across multiple scraping cycles",
    "price_changed":        "price changed (confirms active listing)",
    "cross_verified":       "verified on 2+ sources",
    "nlp_enriched":         "fields enriched by NLP extraction",
    "price_outlier":        "price flagged as statistical outlier",
    "surface_outlier":      "surface area is extreme",
    "price_zero":           "price is zero",
    "mostly_nulls":         "too many key fields are missing",
    "suspected_duplicate":  "suspected duplicate listing",
    "price_per_m2_invalid": "price per m² is outside valid range",
}


# ── Feature Extractor ─────────────────────────────────────────────────────────

def extract_features(
    metadata: Dict[str, Any],
    flags: Dict[str, bool],
) -> Dict[str, float]:
    """
    Convert raw metadata + flags into a numeric feature vector.
    Returns a dict of {feature_name: 0.0 or 1.0}.
    All features are binary (present/absent) for interpretability.
    Handles both 'region'/'governorate' and 'surface'/'surface_area_m2'.
    """
    price   = metadata.get("price")
    surface = metadata.get("surface") or metadata.get("surface_area_m2")
    rooms   = metadata.get("rooms")
    desc    = metadata.get("description") or ""
    lat     = metadata.get("latitude")
    lon     = metadata.get("longitude")

    def _f(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    price_val   = _f(price)
    surface_val = _f(surface)
    rooms_val   = _f(rooms)

    # ── Completeness features (0.0 or 1.0) ───────────────────────────────────
    completeness = {
        "price":        1.0 if price_val > 0 else 0.0,
        "surface":      1.0 if surface_val > 0 else 0.0,
        "rooms":        1.0 if rooms_val > 0 else 0.0,
        "city":         1.0 if metadata.get("city") else 0.0,
        # Handle both 'region' and 'governorate'
        "governorate":  1.0 if (metadata.get("region") or metadata.get("governorate")) else 0.0,
        "coordinates":  1.0 if (lat and lon) else 0.0,
        "description":  1.0 if len(str(desc)) > 50 else 0.0,
        "images":       1.0 if _f(metadata.get("images_count", 0)) > 0 else 0.0,
        "features":     1.0 if len(metadata.get("features") or []) > 0 else 0.0,
        # Handle both 'municipality' and 'municipalite'
        "municipality": 1.0 if (metadata.get("municipality") or metadata.get("municipalite")) else 0.0,
    }

    # ── Bonus features ────────────────────────────────────────────────────────
    bonuses = {
        "has_price_history": 1.0 if flags.get("has_price_history") else 0.0,
        "price_changed":     1.0 if flags.get("price_changed") else 0.0,
        "cross_verified":    1.0 if flags.get("cross_verified") else 0.0,
        "nlp_enriched":      1.0 if flags.get("nlp_enriched") else 0.0,
    }

    # ── Penalty features ──────────────────────────────────────────────────────
    key_fields = ["price", "surface", "rooms", "city", "governorate"]
    null_count = sum(1 for f in key_fields if not metadata.get(f))

    ppm2_invalid = 0.0
    if price_val > 0 and surface_val > 0:
        ppm2 = price_val / surface_val
        ppm2_invalid = 1.0 if (ppm2 < 100 or ppm2 > 20000) else 0.0

    penalties = {
        # Map 'is_outlier' from DB to 'price_outlier'
        "price_outlier":        1.0 if (flags.get("price_outlier") or metadata.get("is_outlier")) else 0.0,
        "surface_outlier":      1.0 if (flags.get("surface_outlier") or surface_val > 5000) else 0.0,
        "price_zero":           1.0 if (price_val == 0 and price is not None) else 0.0,
        "mostly_nulls":         1.0 if null_count >= 3 else 0.0,
        "suspected_duplicate":  1.0 if flags.get("suspected_duplicate") else 0.0,
        "price_per_m2_invalid": ppm2_invalid,
    }

    return {**completeness, **bonuses, **penalties}


# ── Heuristic scorer (fallback when model is not yet trained) ─────────────────

_HEURISTIC_WEIGHTS: Dict[str, float] = {
    # completeness
    "price": 20.0, "surface": 15.0, "rooms": 10.0, "city": 10.0,
    "governorate": 10.0, "coordinates": 10.0, "description": 10.0,
    "images": 5.0, "features": 5.0, "municipality": 5.0,
    # bonuses
    "has_price_history": 10.0, "price_changed": 5.0,
    "cross_verified": 15.0, "nlp_enriched": 5.0,
    # penalties
    "price_outlier": -20.0, "surface_outlier": -10.0, "price_zero": -15.0,
    "mostly_nulls": -25.0, "suspected_duplicate": -30.0, "price_per_m2_invalid": -10.0,
}


def _heuristic_score(features: Dict[str, float]) -> float:
    """Compute score using fixed heuristic weights (fallback/label generation only)."""
    raw = sum(features[f] * _HEURISTIC_WEIGHTS.get(f, 0.0) for f in ALL_FEATURES)
    return max(0.0, min(100.0, raw))


def _heuristic_contributions(features: Dict[str, float]) -> Dict[str, float]:
    """Return per-feature contributions under heuristic weights."""
    return {f: features[f] * _HEURISTIC_WEIGHTS.get(f, 0.0) for f in ALL_FEATURES}


# ── XAI Explainer ─────────────────────────────────────────────────────────────

def explain_score(
    shap_values: Dict[str, float],
    features: Dict[str, float],
    score: float,
    level: str,
    used_model: bool,
) -> str:
    """Generate a plain-language explanation of why a listing got this score."""
    positive = sorted(
        [(k, v) for k, v in shap_values.items() if v > 0.5],
        key=lambda x: -x[1],
    )
    negative = sorted(
        [(k, v) for k, v in shap_values.items() if v < -0.5],
        key=lambda x: x[1],
    )

    method = "ML model" if used_model else "heuristic rules"
    parts  = [f"Score {score:.0f}/100 — {level}. (via {method})"]

    if positive:
        top = [FEATURE_LABELS.get(k, k) for k, _ in positive[:3]]
        parts.append(f"✅ Strengths: {', '.join(top)}.")

    if negative:
        top = [FEATURE_LABELS.get(k, k) for k, _ in negative[:3]]
        parts.append(f"⚠️  Issues: {', '.join(top)}.")

    missing = [
        FEATURE_LABELS.get(f, f)
        for f in COMPLETENESS_FEATURES
        if features.get(f, 0.0) == 0.0
    ]
    if missing:
        parts.append(f"📭 Missing: {', '.join(missing[:4])}.")

    return " ".join(parts)


# ── ML Scorer Model ───────────────────────────────────────────────────────────

class ScorerModel:
    """XGBoost-based reliability scorer with SHAP explainability."""

    def __init__(self):
        self.model: Any = None
        self.explainer: Any = None
        self.feature_importances: Dict[str, float] = {}
        self.is_trained = False
        self._load()

    def _load(self):
        """Load trained model from disk if it exists."""
        if MODEL_PATH.exists():
            try:
                with open(MODEL_PATH, "rb") as f:
                    data = pickle.load(f)
                self.model               = data["model"]
                self.explainer           = data.get("explainer")
                self.feature_importances = data.get("feature_importances", {})
                self.is_trained          = True
                logger.info(f"[Scorer] ✓ Loaded ML model from {MODEL_PATH}")
            except Exception as e:
                logger.warning(f"[Scorer] Could not load model ({e}) — using heuristic fallback")

    def save(self):
        """Persist trained model + metadata to disk."""
        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        with open(MODEL_PATH, "wb") as f:
            pickle.dump({
                "model":               self.model,
                "explainer":           self.explainer,
                "feature_importances": self.feature_importances,
            }, f)
        with open(META_PATH, "w") as f:
            json.dump({
                "feature_importances": self.feature_importances,
                "features":            ALL_FEATURES,
            }, f, indent=2)
        logger.info(f"[Scorer] ✓ Model saved → {MODEL_PATH}")

    def train(self, records: List[Dict[str, Any]], force_retrain: bool = False):
        """Train the XGBoost scorer on a batch of records."""
        try:
            import numpy as np
            import xgboost as xgb
            import shap as shap_lib
        except ImportError:
            logger.error(
                "[Scorer] Missing ML dependencies.\n"
                "Install with: pip install xgboost shap numpy"
            )
            return

        if self.is_trained and not force_retrain:
            logger.info("[Scorer] Model already trained. Pass force_retrain=True to retrain.")
            return

        logger.info(f"[Scorer] Building training set from {len(records)} records...")

        X_rows, y_labels = [], []
        for rec in records:
            # In Supabase mode, records are flat dicts, not nested {"metadata": ...}
            metadata = rec if isinstance(rec, dict) and "price" in rec else rec.get("metadata", rec)
            
            flags = {
                "price_outlier":       metadata.get("is_outlier", False),
                "suspected_duplicate": metadata.get("suspected_duplicate", False),
                "nlp_enriched":        metadata.get("nlp_enriched", False),
                "has_price_history":   metadata.get("has_price_history", False),
                "price_changed":       bool(metadata.get("change_type") == "price_changed"),
                "cross_verified":      False # Add logic if you have multiple sources
            }
            feats = extract_features(metadata, flags)
            label = _heuristic_score(feats)   # proxy label
            X_rows.append([feats[f] for f in ALL_FEATURES])
            y_labels.append(label)

        if not X_rows:
            logger.warning("[Scorer] No valid records for training.")
            return

        X = np.array(X_rows, dtype=np.float32)
        y = np.array(y_labels, dtype=np.float32)

        logger.info(f"[Scorer] Training XGBoost on {len(X)} samples...")
        self.model = xgb.XGBRegressor(
            n_estimators=300,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            objective="reg:squarederror",
            random_state=42,
            verbosity=0,
        )
        self.model.fit(X, y)

        # SHAP explainer
        self.explainer = shap_lib.TreeExplainer(self.model)

        # Normalize feature importances to percentage
        raw_imp = self.model.feature_importances_
        total   = raw_imp.sum() or 1.0
        self.feature_importances = {
            f: round(float(raw_imp[i] / total * 100), 2)
            for i, f in enumerate(ALL_FEATURES)
        }

        self.is_trained = True
        top5 = sorted(self.feature_importances.items(), key=lambda x: -x[1])[:5]
        logger.info(f"[Scorer] ✓ Training complete. Top features: {top5}")

    def predict(
        self,
        features: Dict[str, float],
    ) -> Tuple[float, Dict[str, float]]:
        """Score a single listing and return (score, shap_contributions)."""
        if not self.is_trained or self.model is None:
            score = _heuristic_score(features)
            return score, _heuristic_contributions(features)

        try:
            import numpy as np
        except ImportError:
            score = _heuristic_score(features)
            return score, _heuristic_contributions(features)

        x         = np.array([[features[f] for f in ALL_FEATURES]], dtype=np.float32)
        raw_score = float(self.model.predict(x)[0])
        score     = max(0.0, min(100.0, raw_score))

        # SHAP values
        shap_vals: Dict[str, float] = {}
        if self.explainer is not None:
            try:
                sv        = self.explainer.shap_values(x)
                shap_vals = {f: float(sv[0][i]) for i, f in enumerate(ALL_FEATURES)}
            except Exception as e:
                logger.debug(f"[Scorer] SHAP computation failed: {e}")
                shap_vals = _heuristic_contributions(features)
        else:
            shap_vals = _heuristic_contributions(features)

        return score, shap_vals


# ── Module-level singleton ────────────────────────────────────────────────────

_scorer_model: Optional[ScorerModel] = None


def _get_model() -> ScorerModel:
    global _scorer_model
    if _scorer_model is None:
        _scorer_model = ScorerModel()
    return _scorer_model


# ── Public API ──────────────────────────────────────────────────────────────────

def compute_score(
    metadata: Dict[str, Any],
    flags: Dict[str, bool] = None,
) -> Dict[str, Any]:
    """Compute reliability score for a listing."""
    flags  = flags or {}
    model  = _get_model()

    features              = extract_features(metadata, flags)
    score, shap_vals      = model.predict(features)
    used_model            = model.is_trained

    score = round(score)

    level = "DROP"
    for lvl, (low, high) in SCORE_LEVELS.items():
        if low <= score <= high:
            level = lvl
            break

    breakdown   = {f: round(shap_vals.get(f, 0.0), 2) for f in ALL_FEATURES}
    explanation = explain_score(shap_vals, features, score, level, used_model)

    return {
        "score":       score,
        "level":       level,
        "should_drop": score < 25,
        "breakdown":   breakdown,
        "explanation": explanation,
        "shap_values": shap_vals,
        "used_model":  used_model,
    }


def compute_model_weight(score: int) -> float:
    """Weight multiplier for ML training."""
    if score >= 85: return 1.5
    if score >= 60: return 1.0
    if score >= 25: return 0.5
    return 0.0


def batch_score(records: list) -> list:
    """Score a list of Supabase records."""
    results = []
    for record in records:
        # Record is a flat dict from Supabase
        metadata = record
        
        # Extract flags from the record itself
        flags = {
            "price_outlier":       record.get("is_outlier", False),
            "suspected_duplicate": record.get("suspected_duplicate", False),
            "nlp_enriched":        record.get("nlp_enriched", False),
            "has_price_history":   record.get("has_price_history", False),
            "price_changed":       bool(record.get("change_type") == "price_changed"),
            "cross_verified":      False
        }
        
        score_result = compute_score(metadata, flags)
        
        # Prepare update payload for Supabase
        update_payload = {
            "id": record["id"], # Crucial for upsert
            "reliability_score": score_result["score"],
            "reliability_level": score_result["level"],
            "should_drop":       score_result["should_drop"],
            "model_weight":      compute_model_weight(score_result["score"]),
            # Optional: Save explanation for debugging (ensure column exists in DB if you want this)
            # "score_explanation": score_result["explanation"] 
        }
        results.append(update_payload)
        
    return results


def train_scorer(records: List[Dict[str, Any]], force: bool = False):
    """Train + save the ML scorer from anywhere in your codebase."""
    model = _get_model()
    model.train(records, force_retrain=force)
    model.save()


def get_feature_importances() -> Dict[str, float]:
    """Return what the trained model learned about feature importance (%)."""
    return _get_model().feature_importances


# ── Main entry point for CLI usage ────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv() 
    
    print("🚀 Starting EstateMind Reliability Scorer (Supabase Mode)")
    
    # 1. Fetch Data
    print("📥 Fetching listings from Supabase...")
    records = fetch_listings_for_scoring(limit=500)
    
    if not records:
        print("✅ No new listings to score (all have reliability_score).")
        sys.exit(0)
        
    print(f"🔍 Found {len(records)} listings to process.")
    
    # 2. Score Data
    print("\n🧠 Computing scores...")
    updates = batch_score(records)
    
    # 3. Print Sample Results
    print("\n📊 Sample Results:")
    for i, rec in enumerate(updates[:5]):
        print(f"  {i+1}. ID: {rec['id'][:8]}... | Score: {rec['reliability_score']}/100 | {rec['reliability_level']}")
    
    # 4. Update Supabase
    print("\n💾 Saving results to Supabase...")
    update_listing_scores(updates)
    
    print("\n✅ Scoring complete!")
    
    # Show feature importances if model was used
    if _get_model().is_trained:
        print(f"\n📊 Top Feature Importances (learned from data):")
        for feat, imp in sorted(get_feature_importances().items(), key=lambda x: -x[1])[:5]:
            bar = "█" * int(imp / 2)
            print(f"  {feat:25s} {imp:5.1f}%  {bar}")