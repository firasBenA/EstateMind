"""
EstateMind — Reliability Scorer

Assigns a reliability score (0-100) to each listing based on:
- Field completeness
- Data quality signals
- Outlier flags
- Cross-source verification bonus
- Price change history bonus

Score thresholds:
  < 25   → DROP from modeling (too many nulls or flagged as bad data)
  25-60  → LOW quality  — include with caution
  60-85  → GOOD quality — standard inclusion
  85-100 → HIGH quality — high confidence data, upweight in modeling
"""
from __future__ import annotations

from typing import Dict, Any, Optional


# ── Score weights ─────────────────────────────────────────────────────────────

COMPLETENESS_WEIGHTS = {
    "price":         20,   # most important field
    "surface":       15,
    "rooms":         10,
    "city":          10,
    "governorate":   10,
    "coordinates":   10,   # lat + lon both present
    "description":   10,   # description length > 50 chars
    "images":         5,   # at least one image
    "features":       5,   # features list not empty
    "municipalite":   5,   # municipality/district present
}

BONUS_WEIGHTS = {
    "has_price_history":    10,  # listing seen more than once
    "price_changed":         5,  # confirms it's a real active listing
    "cross_verified":       15,  # same property found on 2+ sources
    "nlp_enriched":          5,  # fields were successfully filled by NLP
}

PENALTY_WEIGHTS = {
    "price_outlier":        -20,
    "surface_outlier":      -10,
    "price_zero":           -15,
    "mostly_nulls":         -25,  # >60% of key fields are null
    "suspected_duplicate":  -30,
    "price_per_m2_invalid": -10,
}

SCORE_LEVELS = {
    "HIGH":   (85, 100),
    "GOOD":   (60, 84),
    "LOW":    (25, 59),
    "DROP":   (0,  24),
}


# ── Scorer ────────────────────────────────────────────────────────────────────

def compute_score(
    metadata: Dict[str, Any],
    flags: Dict[str, bool] = None,
) -> Dict[str, Any]:
    """
    Compute reliability score for a listing.

    Args:
        metadata: Pinecone metadata dict for the listing
        flags: optional dict of quality flags already computed
               e.g. {"price_outlier": True, "cross_verified": False}

    Returns:
        dict with keys: score (int), level (str), breakdown (dict)
    """
    flags = flags or {}
    breakdown = {}
    score = 0

    # ── Completeness ──────────────────────────────────────────────────────────

    # Price
    price = metadata.get("price")
    if price and float(price) > 0:
        score += COMPLETENESS_WEIGHTS["price"]
        breakdown["price"] = COMPLETENESS_WEIGHTS["price"]
    else:
        breakdown["price"] = 0

    # Surface
    surface = metadata.get("surface") or metadata.get("surface_area_m2")
    if surface and float(surface) > 0:
        score += COMPLETENESS_WEIGHTS["surface"]
        breakdown["surface"] = COMPLETENESS_WEIGHTS["surface"]
    else:
        breakdown["surface"] = 0

    # Rooms
    rooms = metadata.get("rooms")
    if rooms and int(rooms) > 0:
        score += COMPLETENESS_WEIGHTS["rooms"]
        breakdown["rooms"] = COMPLETENESS_WEIGHTS["rooms"]
    else:
        breakdown["rooms"] = 0

    # City
    if metadata.get("city"):
        score += COMPLETENESS_WEIGHTS["city"]
        breakdown["city"] = COMPLETENESS_WEIGHTS["city"]
    else:
        breakdown["city"] = 0

    # Governorate / region
    if metadata.get("region") or metadata.get("governorate"):
        score += COMPLETENESS_WEIGHTS["governorate"]
        breakdown["governorate"] = COMPLETENESS_WEIGHTS["governorate"]
    else:
        breakdown["governorate"] = 0

    # Coordinates
    lat = metadata.get("latitude")
    lon = metadata.get("longitude")
    if lat and lon:
        score += COMPLETENESS_WEIGHTS["coordinates"]
        breakdown["coordinates"] = COMPLETENESS_WEIGHTS["coordinates"]
    else:
        breakdown["coordinates"] = 0

    # Description quality
    desc = metadata.get("description") or ""
    if len(str(desc)) > 50:
        score += COMPLETENESS_WEIGHTS["description"]
        breakdown["description"] = COMPLETENESS_WEIGHTS["description"]
    else:
        breakdown["description"] = 0

    # Images
    image_count = metadata.get("image_count") or 0
    if int(image_count) > 0:
        score += COMPLETENESS_WEIGHTS["images"]
        breakdown["images"] = COMPLETENESS_WEIGHTS["images"]
    else:
        breakdown["images"] = 0

    # Features
    features = metadata.get("features") or []
    if features and len(features) > 0:
        score += COMPLETENESS_WEIGHTS["features"]
        breakdown["features"] = COMPLETENESS_WEIGHTS["features"]
    else:
        breakdown["features"] = 0

    # Municipality
    if metadata.get("municipalite"):
        score += COMPLETENESS_WEIGHTS["municipalite"]
        breakdown["municipalite"] = COMPLETENESS_WEIGHTS["municipalite"]
    else:
        breakdown["municipalite"] = 0

    # ── Bonuses ───────────────────────────────────────────────────────────────

    if flags.get("has_price_history"):
        score += BONUS_WEIGHTS["has_price_history"]
        breakdown["bonus_history"] = BONUS_WEIGHTS["has_price_history"]

    if flags.get("price_changed"):
        score += BONUS_WEIGHTS["price_changed"]
        breakdown["bonus_price_change"] = BONUS_WEIGHTS["price_changed"]

    if flags.get("cross_verified"):
        score += BONUS_WEIGHTS["cross_verified"]
        breakdown["bonus_cross_verified"] = BONUS_WEIGHTS["cross_verified"]

    if flags.get("nlp_enriched"):
        score += BONUS_WEIGHTS["nlp_enriched"]
        breakdown["bonus_nlp"] = BONUS_WEIGHTS["nlp_enriched"]

    # ── Penalties ─────────────────────────────────────────────────────────────

    # Price outlier check (price per m²)
    if flags.get("price_outlier"):
        score += PENALTY_WEIGHTS["price_outlier"]
        breakdown["penalty_price_outlier"] = PENALTY_WEIGHTS["price_outlier"]
    elif price and surface and float(surface) > 0:
        price_per_m2 = float(price) / float(surface)
        if price_per_m2 < 100 or price_per_m2 > 20000:
            score += PENALTY_WEIGHTS["price_per_m2_invalid"]
            breakdown["penalty_price_m2"] = PENALTY_WEIGHTS["price_per_m2_invalid"]

    # Surface outlier
    if flags.get("surface_outlier"):
        score += PENALTY_WEIGHTS["surface_outlier"]
        breakdown["penalty_surface"] = PENALTY_WEIGHTS["surface_outlier"]
    elif surface and float(surface) > 5000:
        score += PENALTY_WEIGHTS["surface_outlier"]
        breakdown["penalty_surface"] = PENALTY_WEIGHTS["surface_outlier"]

    # Zero price
    if price is not None and float(price) == 0:
        score += PENALTY_WEIGHTS["price_zero"]
        breakdown["penalty_zero_price"] = PENALTY_WEIGHTS["price_zero"]

    # Mostly nulls — count how many key fields are missing
    key_fields = ["price", "surface", "rooms", "city", "governorate"]
    null_count = sum(1 for f in key_fields if not metadata.get(f))
    if null_count >= 3:  # 60%+ of key fields missing
        score += PENALTY_WEIGHTS["mostly_nulls"]
        breakdown["penalty_mostly_nulls"] = PENALTY_WEIGHTS["mostly_nulls"]

    # Suspected duplicate
    if flags.get("suspected_duplicate"):
        score += PENALTY_WEIGHTS["suspected_duplicate"]
        breakdown["penalty_duplicate"] = PENALTY_WEIGHTS["suspected_duplicate"]

    # ── Clamp and classify ────────────────────────────────────────────────────

    score = max(0, min(100, score))

    level = "DROP"
    for lvl, (low, high) in SCORE_LEVELS.items():
        if low <= score <= high:
            level = lvl
            break

    return {
        "score": score,
        "level": level,
        "should_drop": score < 25,
        "breakdown": breakdown,
    }


def compute_model_weight(score: int) -> float:
    """
    Returns a weight multiplier for use in ML training.
    High quality listings count more in model training.
    """
    if score >= 85:
        return 1.5
    if score >= 60:
        return 1.0
    if score >= 25:
        return 0.5
    return 0.0  # drop


def batch_score(records: list) -> list:
    """
    Score a list of Pinecone metadata records.
    Returns list of dicts with original metadata + score fields added.
    """
    results = []
    for record in records:
        metadata = record.get("metadata", record)
        score_result = compute_score(metadata)
        enriched = dict(metadata)
        enriched["reliability_score"] = score_result["score"]
        enriched["reliability_level"] = score_result["level"]
        enriched["should_drop"] = score_result["should_drop"]
        enriched["model_weight"] = compute_model_weight(score_result["score"])
        results.append(enriched)
    return results


# ── Quick test ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_cases = [
        {
            "name": "Complete high-quality listing",
            "metadata": {
                "price": 450000, "surface": 120, "rooms": 4,
                "city": "La Marsa", "region": "Tunis",
                "latitude": 36.87, "longitude": 10.32,
                "description": "Bel appartement S+3 avec vue mer et piscine",
                "image_count": 8, "features": ["piscine", "vue mer"],
                "municipalite": "La Marsa",
            },
            "flags": {"has_price_history": True, "cross_verified": True}
        },
        {
            "name": "Missing most fields",
            "metadata": {
                "price": None, "surface": None, "rooms": None,
                "city": None, "region": "Tunis",
                "description": "Appartement",
                "image_count": 0, "features": [],
            },
            "flags": {}
        },
        {
            "name": "Price outlier",
            "metadata": {
                "price": 1000, "surface": 200, "rooms": 4,
                "city": "Tunis", "region": "Tunis",
                "latitude": 36.8, "longitude": 10.18,
                "description": "Villa luxueuse avec piscine et jardin",
                "image_count": 5, "features": ["piscine"],
            },
            "flags": {}
        },
    ]

    for case in test_cases:
        result = compute_score(case["metadata"], case["flags"])
        print(f"\n{case['name']}:")
        print(f"  Score: {result['score']}/100 — {result['level']}")
        print(f"  Should drop: {result['should_drop']}")
        print(f"  Model weight: {compute_model_weight(result['score'])}")