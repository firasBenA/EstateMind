"""
EstateMind — DSO 2.2 : Classificateur de Cohérence Multimodale
==============================================================
Combine les signaux image (CLIP), texte (CLIP) et prix (déviation régionale)
pour produire un score de cohérence global entre 0 et 1.

Principe :
──────────
Une annonce honnête doit être cohérente sur 3 dimensions :
  (A) Image ↔ Texte   — les photos correspondent à la description
  (B) Image ↔ Prix    — les photos reflètent un bien au prix annoncé
  (C) Texte ↔ Prix    — la description est alignée avec le niveau de prix

Score final = weighted_average(A, B, C)
  - Pondération : A×0.4 + B×0.3 + C×0.3

Interprétation du multimodal_score (0–1) :
  0.0 – 0.30 → Forte incohérence (fraude probable)
  0.31 – 0.55 → Incohérence modérée (suspect)
  0.56 – 0.75 → Cohérence acceptable
  0.76 – 1.0  → Très cohérent (annonce fiable)

Types de mismatch détectés :
  "image_text_low_similarity"     : photos ≠ description sémantiquement
  "overpriced_vs_images"          : prix élevé mais images modestes
  "underpriced_trap"              : prix trop bas (appât possible)
  "no_images_suspicious_price"    : pas de photos + prix anormal
  "luxury_description_poor_price" : description luxe mais prix bas
  "price_inflation_suspected"     : prix >> médiane régionale (> 100%)
"""
from __future__ import annotations

from typing import List, Dict, Any, Optional, Tuple
import json

import numpy as np
from loguru import logger

from fraud_detection.multimodal.image_encoder import (
    encode_listings_batch,
    cosine_similarity,
    CLIP_DIM,
)
from fraud_detection.multimodal.text_price_signal import (
    compute_text_price_signals,
)

# ── Poids du score final ──────────────────────────────────────────────────────

WEIGHT_IMAGE_TEXT  = 0.40   # Similarité image ↔ texte (CLIP)
WEIGHT_IMAGE_PRICE = 0.30   # Cohérence image ↔ prix
WEIGHT_TEXT_PRICE  = 0.30   # Cohérence texte ↔ prix

# Seuils de similarité cosine (espace CLIP, [-1, 1] → on mappe en [0, 1])
SIM_LOW_THRESHOLD  = 0.20   # < 0.20 → fort mismatch image-texte
SIM_HIGH_THRESHOLD = 0.50   # > 0.50 → bonne cohérence image-texte

# Seuils de déviation de prix
PRICE_OVERPRICED_MOD  = 50.0   # > 50% → overpriced modéré
PRICE_OVERPRICED_HIGH = 100.0  # > 100% → overpriced fort
PRICE_UNDERPRICED     = -50.0  # < -50% → underpriced suspect


# ── Mots indicateurs de luxe dans le texte ───────────────────────────────────

LUXURY_KEYWORDS = [
    "luxe", "luxueux", "haut standing", "prestige", "villa",
    "piscine", "vue mer", "vue panoramique", "marbre", "jacuzzi",
    "suite", "penthouse", "duplex", "triplex", "sécurisé",
    "gardien", "parking souterrain", "climatisation centralisée",
]


# ── Sous-scores ───────────────────────────────────────────────────────────────

def _score_image_text(
    image_emb: Optional[np.ndarray],
    text_emb:  Optional[np.ndarray],
) -> Tuple[float, Optional[str]]:
    """
    Score A : cohérence sémantique image ↔ texte.
    Retourne (score 0–1, flag ou None).
    """
    if image_emb is None or text_emb is None:
        return 0.5, None   # Inconnu → score neutre

    sim = cosine_similarity(image_emb, text_emb)

    # Mapper [-1, 1] → [0, 1]
    score = (sim + 1.0) / 2.0
    score = max(0.0, min(1.0, score))

    flag = None
    if sim < SIM_LOW_THRESHOLD:
        flag = "image_text_low_similarity"

    return round(score, 4), flag


def _score_image_price(
    image_emb:       Optional[np.ndarray],
    price_deviation: float,
    has_images:      bool,
) -> Tuple[float, List[str]]:
    """
    Score B : cohérence image ↔ prix.

    Logique :
    - Pas d'image + prix anormal → suspect
    - Image disponible + prix >> médiane → overpriced vs images
    - Image disponible + prix << médiane → underpriced trap
    """
    flags = []

    if not has_images:
        # Pas d'image
        if abs(price_deviation) > PRICE_OVERPRICED_MOD:
            flags.append("no_images_suspicious_price")
            score = 0.30
        else:
            score = 0.50   # neutre
        return round(score, 4), flags

    # Images disponibles
    if price_deviation > PRICE_OVERPRICED_HIGH:
        flags.append("overpriced_vs_images")
        score = 0.25
    elif price_deviation > PRICE_OVERPRICED_MOD:
        score = 0.45
    elif price_deviation < PRICE_UNDERPRICED:
        flags.append("underpriced_trap")
        score = 0.35
    else:
        score = 0.85   # prix dans la norme

    return round(score, 4), flags


def _score_text_price(
    description:     str,
    price_deviation: float,
    deviation_level: str,
) -> Tuple[float, List[str]]:
    """
    Score C : cohérence texte ↔ prix.

    Détecte :
    - Description de luxe + prix bas (description trompeuse)
    - Prix très au-dessus de la médiane (inflation)
    """
    flags = []
    desc_lower = description.lower() if description else ""

    # Compter les indicateurs de luxe dans la description
    luxury_count = sum(1 for kw in LUXURY_KEYWORDS if kw in desc_lower)
    is_luxury_desc = luxury_count >= 2

    if is_luxury_desc and price_deviation < -PRICE_OVERPRICED_MOD:
        # Description luxueuse mais prix très bas → suspect
        flags.append("luxury_description_poor_price")
        score = 0.25
    elif price_deviation > PRICE_OVERPRICED_HIGH:
        flags.append("price_inflation_suspected")
        score = 0.30
    elif deviation_level == "high" and price_deviation < 0:
        # Prix très bas sans raison textuelle → appât
        flags.append("underpriced_trap")
        score = 0.40
    elif deviation_level in ("normal", "no_data"):
        score = 0.85
    elif deviation_level == "moderate":
        score = 0.60
    else:
        score = 0.50

    return round(score, 4), flags


# ── Score de cohérence global ─────────────────────────────────────────────────

def compute_consistency_score(
    listing: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Calcule le score de cohérence multimodale pour un seul listing enrichi.

    Le listing doit avoir été enrichi par :
      - encode_listings_batch()        → image_embedding, images_analyzed, has_images
      - compute_text_price_signals()   → clip_text_embedding, price_deviation_pct,
                                         deviation_level, price_signal

    Returns:
        dict avec multimodal_score, image_text_similarity,
              price_deviation_pct, mismatch_types, images_analyzed
    """
    image_emb       = listing.get("image_embedding")
    text_emb        = listing.get("clip_text_embedding")
    price_deviation = listing.get("price_deviation_pct") or 0.0
    deviation_level = listing.get("deviation_level")    or "normal"
    has_images      = listing.get("has_images",   False)
    images_analyzed = listing.get("images_analyzed", 0)
    description     = str(listing.get("description") or "")

    # Sous-scores
    score_A, flag_A = _score_image_text(image_emb, text_emb)
    score_B, flags_B = _score_image_price(image_emb, price_deviation, has_images)
    score_C, flags_C = _score_text_price(description, price_deviation, deviation_level)

    # Score final pondéré
    multimodal_score = (
        WEIGHT_IMAGE_TEXT  * score_A +
        WEIGHT_IMAGE_PRICE * score_B +
        WEIGHT_TEXT_PRICE  * score_C
    )
    multimodal_score = round(max(0.0, min(1.0, multimodal_score)), 4)

    # Similarité image-texte brute
    if image_emb is not None and text_emb is not None:
        img_text_sim = round(cosine_similarity(image_emb, text_emb), 4)
    else:
        img_text_sim = None

    # Consolider tous les flags
    mismatch_types = []
    if flag_A:
        mismatch_types.append(flag_A)
    mismatch_types.extend(flags_B)
    mismatch_types.extend(flags_C)
    mismatch_types = list(set(mismatch_types))  # dédupliquer

    return {
        "property_id":           listing.get("property_id"),
        "source_name":           listing.get("source_name"),
        "multimodal_score":      multimodal_score,
        "image_text_similarity": img_text_sim,
        "price_deviation_pct":   price_deviation,
        "mismatch_types":        mismatch_types,
        "images_analyzed":       images_analyzed,
        "subscores": {
            "image_text":   score_A,
            "image_price":  score_B,
            "text_price":   score_C,
        },
        "model_version": "clip_vit_base_patch32_v1",
    }


# ── Pipeline complet DSO 2.2 ──────────────────────────────────────────────────

def run_multimodal_pipeline(
    listings: List[Dict[str, Any]],
    regional_stats: Dict[str, Dict[str, float]],
    max_images_per_listing: int = 3,
) -> List[Dict[str, Any]]:
    """
    Pipeline complet DSO 2.2 : image + texte + prix → score de cohérence.

    Args:
        listings:               Liste de dicts listing (avec images, description, price…)
        regional_stats:         Stats régionales de prix (depuis FraudDBConnector)
        max_images_per_listing: Max images à analyser par listing

    Returns:
        Liste de dicts résultats pour sauvegarde via FraudDBConnector.save_multimodal_results()
    """
    logger.info(f"[MultimodalPipeline] Démarrage DSO 2.2 sur {len(listings)} listings")

    # Étape 1 : Encoder les images
    logger.info("[MultimodalPipeline] Étape 1/3 — Encodage images (CLIP)")
    listings_with_images = encode_listings_batch(listings, max_images_per_listing)

    # Étape 2 : Encoder les textes + calculer déviation prix
    logger.info("[MultimodalPipeline] Étape 2/3 — Signaux texte + prix")
    listings_enriched = compute_text_price_signals(listings_with_images, regional_stats)

    # Étape 3 : Score de cohérence final
    logger.info("[MultimodalPipeline] Étape 3/3 — Score de cohérence multimodale")
    results = []
    for listing in listings_enriched:
        result = compute_consistency_score(listing)
        results.append(result)

    # Rapport rapide
    scores        = [r["multimodal_score"] for r in results]
    n_incoherent  = sum(1 for s in scores if s < 0.31)
    n_suspicious  = sum(1 for s in scores if 0.31 <= s < 0.56)
    n_coherent    = sum(1 for s in scores if s >= 0.56)

    logger.info(
        f"[MultimodalPipeline] Terminé — "
        f"{len(results)} listings analysés | "
        f"Incohérents: {n_incoherent} | "
        f"Suspects: {n_suspicious} | "
        f"Cohérents: {n_coherent}"
    )

    return results


# ── Utilitaire : interprétation du score ──────────────────────────────────────

def interpret_score(score: float) -> str:
    """Retourne une étiquette lisible pour un multimodal_score."""
    if score < 0.31:
        return "INCOHERENT (fraude probable)"
    elif score < 0.56:
        return "SUSPECT (incohérence modérée)"
    elif score < 0.76:
        return "ACCEPTABLE"
    else:
        return "COHERENT (annonce fiable)"
