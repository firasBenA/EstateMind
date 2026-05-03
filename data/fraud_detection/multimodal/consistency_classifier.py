"""
DSO 2.2 — Score de Cohérence Sémantique Multimodale
=====================================================
Combine deux signaux pour décider si une annonce est cohérente :

  A. Category Match Score (60%)
     Compare les catégories détectées par CLIP dans les images
     avec les catégories attendues d'après la description.

     Exemple :
       Description : "villa avec piscine et jardin"
       Catégories attendues : {exterior, living_room, bedroom, pool, garden}
       Catégories détectées : {bedroom, living_room, bathroom}
       → pool et garden manquants → match_score = 2/5 = 0.40 → suspect

  B. Price Deviation Score (40%)
     Mesure l'écart entre le prix annoncé et la médiane régionale.
     Un prix trop bas ou trop haut par rapport au marché est un signal.

Score final (multimodal_score) ∈ [0, 1] :
  0.00 – 0.30 → Forte incohérence  (fraude probable)
  0.31 – 0.55 → Incohérence modérée (suspect)
  0.56 – 0.75 → Cohérence acceptable
  0.76 – 1.00 → Très cohérent       (annonce fiable)

Flags sémantiques détectés :
  claimed_feature_not_visible : une caractéristique promise (piscine, vue...)
                                n'apparaît pas dans les images
  wrong_property_type         : les images ne correspondent pas au type déclaré
                                (ex: terrain décrit comme villa)
  no_real_estate_images       : les images ne semblent pas immobilières
  no_images_suspicious_price  : pas de photos + prix anormal
  overpriced_vs_images        : prix très au-dessus de la médiane régionale
  underpriced_trap            : prix trop bas (possible appât frauduleux)
"""
from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple

from loguru import logger

from fraud_detection.multimodal.image_encoder import (
    analyze_listings_batch,
    REAL_ESTATE_CATEGORIES,
)
from fraud_detection.multimodal.text_price_signal import (
    compute_description_signals,
)

# ── Poids du score final ──────────────────────────────────────────────────────
WEIGHT_CATEGORY_MATCH = 0.60   # Cohérence sémantique image ↔ description
WEIGHT_PRICE          = 0.40   # Cohérence prix ↔ marché régional

# Seuils de déviation prix
_PRICE_HIGH      = 100.0
_PRICE_MOD       = 50.0
_PRICE_UNDER     = -50.0

# Catégories "non immobilières" → suspicion si elles dominent
_NON_REALESTATE_CATS = {"other"}

# Catégories qui DEVRAIENT distinguer les types de biens
_LAND_CATEGORIES = {"land"}
_INTERIOR_CATEGORIES = {"bedroom", "living_room", "kitchen", "bathroom"}


# ── Score A : Cohérence catégories image ↔ description ───────────────────────

def _score_category_match(
    detected_categories: List[str],
    expected_categories: Set[str],
    listing: Dict[str, Any],
) -> Tuple[float, List[str]]:
    """
    Compare les catégories détectées par CLIP avec celles attendues.

    Règles :
      1. Pas d'images → score bas (0.20) + flag no_images_suspicious_price
         si prix anormal
      2. Catégories attendues toutes présentes → score élevé
      3. Catégories manquantes → score proportionnel
      4. Images non immobilières → flag + pénalité
      5. Type de bien incohérent → flag wrong_property_type

    Returns:
        (match_score [0-1], flags)
    """
    flags: List[str] = []
    detected_set = set(detected_categories)
    price        = float(listing.get("price") or 0)

    # Cas : pas d'images
    if not detected_categories:
        if price > 200_000:
            flags.append("no_images_suspicious_price")
            return 0.20, flags
        return 0.40, flags  # neutre si pas cher

    # Cas : images non immobilières (toutes classées "other")
    non_re_ratio = sum(1 for c in detected_categories if c in _NON_REALESTATE_CATS) / len(detected_categories)
    if non_re_ratio >= 0.6:
        flags.append("no_real_estate_images")
        return 0.15, flags

    # Cas : incohérence type de bien
    # Si la description dit "terrain" mais les images montrent des intérieurs
    prop_type = (listing.get("type") or "").lower()
    if "terrain" in prop_type:
        interior_count = sum(1 for c in detected_categories if c in _INTERIOR_CATEGORIES)
        if interior_count > len(detected_categories) * 0.5:
            flags.append("wrong_property_type")
            return 0.20, flags

    # Cas général : comparer expected vs detected
    if not expected_categories:
        return 0.65, flags  # pas d'attentes précises → score neutre positif

    found   = expected_categories & detected_set
    missing = expected_categories - detected_set

    # Flags pour chaque catégorie promise mais absente
    important_missing = {"pool", "view", "garden", "terrace", "parking", "land"}
    for cat in missing:
        if cat in important_missing:
            flags.append(f"claimed_{cat}_not_visible")

    # Score : proportion de catégories attendues trouvées
    match_score = len(found) / len(expected_categories)

    # Bonus léger si plus d'images que prévu (annonce détaillée)
    if len(detected_categories) > len(expected_categories):
        match_score = min(1.0, match_score + 0.05)

    return round(match_score, 4), flags


# ── Score B : Cohérence prix ──────────────────────────────────────────────────

def _score_price(
    price_deviation_pct: float,
    deviation_level: str,
    has_images: bool,
) -> Tuple[float, List[str]]:
    """
    Convertit la déviation de prix en score [0-1].
    Plus le prix est anormal, plus le score est bas.
    """
    flags: List[str] = []

    if deviation_level == "no_data":
        return 0.60, flags   # Pas de données → neutre

    abs_dev = abs(price_deviation_pct)

    if price_deviation_pct > _PRICE_HIGH:
        flags.append("overpriced_vs_images")
        score = 0.20
    elif price_deviation_pct > _PRICE_MOD:
        score = 0.45
    elif price_deviation_pct < _PRICE_UNDER:
        flags.append("underpriced_trap")
        score = 0.30
    elif abs_dev > 30:
        score = 0.55
    else:
        score = 0.85   # prix dans la norme régionale

    return round(score, 4), flags


# ── Score de cohérence global ─────────────────────────────────────────────────

def compute_consistency_score(listing: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calcule le score de cohérence multimodale sémantique pour un listing.

    Le listing doit avoir été enrichi par :
      - analyze_listings_batch()       → detected_categories, has_images, images_analyzed
      - compute_description_signals()  → expected_categories, price_deviation_pct,
                                         deviation_level, price_signal

    Returns:
        dict compatible avec FraudDBConnector.save_multimodal_results()
    """
    detected_cats    = listing.get("detected_categories", [])
    expected_cats    = listing.get("expected_categories", set())
    price_dev        = listing.get("price_deviation_pct", 0.0)
    dev_level        = listing.get("deviation_level", "no_data")
    has_images       = listing.get("has_images", False)
    images_analyzed  = listing.get("images_analyzed", 0)

    # Score A — Cohérence sémantique catégories
    score_A, flags_A = _score_category_match(detected_cats, expected_cats, listing)

    # Score B — Cohérence prix
    score_B, flags_B = _score_price(price_dev, dev_level, has_images)

    # Score final pondéré
    multimodal_score = round(
        WEIGHT_CATEGORY_MATCH * score_A + WEIGHT_PRICE * score_B,
        4,
    )
    multimodal_score = max(0.0, min(1.0, multimodal_score))

    # Consolider les flags
    all_flags = list(set(flags_A + flags_B))

    # image_text_similarity = category match score (pour compatibilité DB)
    return {
        "property_id":           listing.get("property_id"),
        "source_name":           listing.get("source_name"),
        "multimodal_score":      multimodal_score,
        "image_text_similarity": score_A,         # category match score
        "price_deviation_pct":   price_dev,
        "mismatch_types":        all_flags,
        "images_analyzed":       images_analyzed,
        "model_version":         "clip_zeroshot_semantic_v1",
        # Détail pour debug / rapport
        "_detected_categories":  detected_cats,
        "_expected_categories":  list(expected_cats),
        "_score_category":       score_A,
        "_score_price":          score_B,
    }


# ── Pipeline complet DSO 2.2 ──────────────────────────────────────────────────

def run_multimodal_pipeline(
    listings: List[Dict[str, Any]],
    regional_stats: Dict[str, Dict[str, float]],
    max_images_per_listing: int = 3,
) -> List[Dict[str, Any]]:
    """
    Pipeline DSO 2.2 — Cohérence Sémantique Multimodale (CLIP Zero-Shot).

    Étapes :
      1. Classification zero-shot des images par CLIP → catégories détectées
      2. Extraction des catégories attendues depuis la description
      3. Calcul de la déviation de prix régionale
      4. Score de cohérence = 60% category_match + 40% price_score
      5. Flags sémantiques (piscine non visible, type incohérent...)

    Args:
        listings               : listings avec images (depuis fetch_listings_with_images)
        regional_stats         : stats de prix régionaux
        max_images_per_listing : max images analysées par annonce

    Returns:
        Résultats prêts pour save_multimodal_results()
    """
    logger.info(f"[DSO2.2] Démarrage pipeline sémantique CLIP sur {len(listings)} listings")

    # Étape 1 : Classification zero-shot des images
    logger.info("[DSO2.2] Étape 1/3 — Classification zero-shot des images (CLIP)")
    listings_with_images = analyze_listings_batch(listings, max_images_per_listing)

    # Étape 2+3 : Catégories attendues + déviation prix
    logger.info("[DSO2.2] Étape 2/3 — Extraction catégories attendues + signal prix")
    listings_enriched = compute_description_signals(listings_with_images, regional_stats)

    # Étape 4 : Score de cohérence
    logger.info("[DSO2.2] Étape 3/3 — Calcul scores de cohérence sémantique")
    results = [compute_consistency_score(l) for l in listings_enriched]

    # Rapport
    scores       = [r["multimodal_score"] for r in results]
    n_incoherent = sum(1 for s in scores if s < 0.31)
    n_suspicious = sum(1 for s in scores if 0.31 <= s < 0.56)
    n_coherent   = sum(1 for s in scores if s >= 0.56)

    all_flags    = [f for r in results for f in r["mismatch_types"]]
    top_flag     = max(set(all_flags), key=all_flags.count) if all_flags else "aucun"

    logger.info(
        f"[DSO2.2] Terminé — {len(results)} listings | "
        f"Incohérents: {n_incoherent} | Suspects: {n_suspicious} | Cohérents: {n_coherent} | "
        f"Top flag: {top_flag}"
    )
    return results


# ── Interprétation du score ───────────────────────────────────────────────────

def interpret_score(score: float) -> str:
    if score < 0.31:
        return "INCOHERENT — fraude probable"
    elif score < 0.56:
        return "SUSPECT — incohérence modérée"
    elif score < 0.76:
        return "ACCEPTABLE"
    else:
        return "COHERENT — annonce fiable"
