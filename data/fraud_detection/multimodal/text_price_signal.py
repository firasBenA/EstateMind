"""
DSO 2.2 — Extraction des Catégories Attendues + Signal Prix
============================================================
Deux rôles :

1. extract_expected_categories(listing)
   Détermine quelles catégories visuelles DEVRAIENT apparaître dans les photos
   en se basant sur le type de bien et les mots-clés de la description.

   Exemple :
     description = "villa avec piscine et jardin, vue mer"
     → expected = {"exterior", "pool", "garden", "view"}

   Ces catégories attendues seront comparées aux catégories détectées par CLIP
   dans les vraies images. Si une catégorie promise est absente → mismatch.

2. compute_price_deviation(listing, regional_stats)
   Calcule l'écart entre le prix de l'annonce et la médiane régionale.
   Signal de surévaluation ou de prix-appât.
"""
from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple

import numpy as np
from loguru import logger

# ── Catégories attendues selon le type de bien ───────────────────────────────

_TYPE_BASE_CATEGORIES: Dict[str, Set[str]] = {
    "villa":        {"exterior", "living_room", "bedroom"},
    "maison":       {"exterior", "living_room", "bedroom"},
    "appartement":  {"living_room", "bedroom"},
    "apartment":    {"living_room", "bedroom"},
    "studio":       {"living_room"},
    "duplex":       {"living_room", "bedroom"},
    "triplex":      {"living_room", "bedroom"},
    "penthouse":    {"living_room", "bedroom", "view"},
    "terrain":      {"land"},
    "bureau":       {"living_room"},
    "commercial":   {"living_room"},
    "local":        {"living_room"},
    "immeuble":     {"building", "exterior"},
}

# Mots-clés de description → catégorie visuelle attendue
_KEYWORD_CATEGORY_MAP: Dict[str, str] = {
    # Piscine
    "piscine":   "pool",
    "pool":      "pool",
    # Vue
    "vue mer":          "view",
    "vue panoramique":  "view",
    "vue sur mer":      "view",
    "panoramique":      "view",
    # Jardin
    "jardin":    "garden",
    "garden":    "garden",
    "parc":      "garden",
    # Terrasse / Balcon
    "terrasse":  "terrace",
    "balcon":    "terrace",
    "rooftop":   "terrace",
    # Parking / Garage
    "garage":    "parking",
    "parking":   "parking",
    "box":       "parking",
    # Cuisine (si mentionnée explicitement)
    "cuisine équipée": "kitchen",
    "cuisine aménagée": "kitchen",
    # Extérieur
    "façade":    "exterior",
    "extérieur": "exterior",
}

# Seuils de déviation prix
_PRICE_MOD  = 50.0    # > 50% → signal modéré
_PRICE_HIGH = 100.0   # > 100% → signal fort


# ── Extraction des catégories attendues ───────────────────────────────────────

def extract_expected_categories(listing: Dict[str, Any]) -> Set[str]:
    """
    Détermine les catégories visuelles attendues pour ce listing.

    Logique :
      1. Catégories de base selon le type de bien (villa, appartement, terrain...)
      2. Ajout des catégories liées aux mots-clés de la description
      3. Ajout des catégories liées aux features (amenities)

    Returns:
        Ensemble de catégories attendues (ex: {"exterior", "pool", "garden"})
    """
    categories: Set[str] = set()

    # 1. Catégories de base selon le type
    prop_type = (listing.get("type") or "").lower().strip()
    for type_key, base_cats in _TYPE_BASE_CATEGORIES.items():
        if type_key in prop_type:
            categories.update(base_cats)
            break

    if not categories:
        categories.add("living_room")   # défaut si type inconnu

    # 2. Analyse de la description
    description = (listing.get("description") or "").lower()
    for keyword, cat in _KEYWORD_CATEGORY_MAP.items():
        if keyword in description:
            categories.add(cat)

    # 3. Analyse des features (amenities)
    features = listing.get("features") or []
    if isinstance(features, str):
        import json
        try:
            features = json.loads(features)
        except Exception:
            features = []

    features_text = " ".join(str(f) for f in features).lower()
    for keyword, cat in _KEYWORD_CATEGORY_MAP.items():
        if keyword in features_text:
            categories.add(cat)

    return categories


# ── Déviation de prix régionale ───────────────────────────────────────────────

def compute_price_deviation(
    listing: Dict[str, Any],
    regional_stats: Dict[str, Dict[str, float]],
) -> Tuple[float, str, str]:
    """
    Calcule l'écart entre le prix de l'annonce et la médiane régionale.

    Returns:
        (price_deviation_pct, deviation_level, price_signal)
        - price_deviation_pct : % d'écart (positif = surpayé, négatif = sous-payé)
        - deviation_level     : "normal" | "moderate" | "high" | "no_data"
        - price_signal        : "overpriced" | "underpriced" | "normal"
    """
    price  = listing.get("price")
    region = (listing.get("region") or "unknown").lower().strip()
    ptype  = (listing.get("type")   or "other").lower().strip()
    tx     = listing.get("transaction_type") or "Sale"

    if not price or float(price) <= 0:
        return 0.0, "no_data", "normal"

    price = float(price)
    key   = f"{region}|{ptype}|{tx}"
    stats = regional_stats.get(key)

    # Fallback : médiane des médianes disponibles pour cette région
    if not stats:
        fallback = [k for k in regional_stats if k.startswith(f"{region}|")]
        if fallback:
            medians = [
                regional_stats[k]["median"]
                for k in fallback
                if regional_stats[k].get("median", 0) > 0
            ]
            if medians:
                stats = {"median": float(np.median(medians))}

    if not stats or stats.get("median", 0) <= 0:
        return 0.0, "no_data", "normal"

    median = stats["median"]
    dev_pct = (price - median) / median * 100.0
    abs_dev = abs(dev_pct)

    level = "high" if abs_dev > _PRICE_HIGH else ("moderate" if abs_dev > _PRICE_MOD else "normal")
    signal = "overpriced" if dev_pct > _PRICE_MOD else ("underpriced" if dev_pct < -_PRICE_MOD else "normal")

    return round(dev_pct, 2), level, signal


# ── Enrichissement batch ──────────────────────────────────────────────────────

def compute_description_signals(
    listings: List[Dict[str, Any]],
    regional_stats: Dict[str, Dict[str, float]],
) -> List[Dict[str, Any]]:
    """
    Enrichit chaque listing avec :
      - expected_categories  : Set[str] — catégories visuelles promises
      - price_deviation_pct  : float
      - deviation_level      : str
      - price_signal         : str
    """
    logger.info(f"[DescriptionParser] Extraction catégories attendues + prix — {len(listings)} listings")
    results = []

    n_with_pool    = 0
    n_with_view    = 0
    n_overpriced   = 0
    n_underpriced  = 0

    for listing in listings:
        expected = extract_expected_categories(listing)
        dev_pct, dev_level, price_signal = compute_price_deviation(listing, regional_stats)

        if "pool"   in expected: n_with_pool   += 1
        if "view"   in expected: n_with_view   += 1
        if price_signal == "overpriced":  n_overpriced  += 1
        if price_signal == "underpriced": n_underpriced += 1

        results.append({
            **listing,
            "expected_categories": expected,
            "price_deviation_pct": dev_pct,
            "deviation_level":     dev_level,
            "price_signal":        price_signal,
        })

    logger.info(
        f"[DescriptionParser] Résumé — "
        f"piscines promises: {n_with_pool} | "
        f"vues promises: {n_with_view} | "
        f"surpayés: {n_overpriced} | sous-payés: {n_underpriced}"
    )
    return results
