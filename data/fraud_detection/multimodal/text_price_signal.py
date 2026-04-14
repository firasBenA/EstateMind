"""
EstateMind — DSO 2.2 : Signal Texte et Déviation de Prix
=========================================================
Encode le texte des annonces avec CLIP (espace partagé image/texte)
et calcule la déviation du prix par rapport à la médiane régionale.

Deux signaux produits :
──────────────────────
1. text_embedding (512-dim CLIP)
   Encode le texte de l'annonce dans l'espace CLIP pour permettre
   la comparaison directe avec l'embedding image.

   Texte encodé = titre + type + localisation + prix déclaré + description

2. price_deviation_pct
   % d'écart entre le prix réel et la médiane régionale (region×type×tx_type).
   Formule : (prix - médiane_région) / médiane_région × 100
   - Positif  → prix au-dessus de la médiane (potentiel surévaluation)
   - Négatif  → prix en-dessous (potentiel fraude/appât)

   Seuils d'alerte :
   - |déviation| > 50%  → signal modéré
   - |déviation| > 100% → signal fort (prix × 2 ou ÷ 2 vs médiane)

Note : On utilise CLIP pour le texte (au lieu des embeddings sentence-transformers
déjà en DB) car seul CLIP garantit la comparabilité image↔texte dans le même espace.
"""
from __future__ import annotations

from typing import List, Dict, Any, Optional, Tuple
import json

import numpy as np
from loguru import logger

from fraud_detection.multimodal.image_encoder import CLIPEncoder, CLIP_DIM

# ── Seuils de déviation de prix ───────────────────────────────────────────────

PRICE_DEVIATION_MODERATE = 50.0    # % d'écart — signal modéré
PRICE_DEVIATION_HIGH     = 100.0   # % d'écart — signal fort


# ── Construction du texte d'entrée CLIP ──────────────────────────────────────

def _build_clip_text(listing: Dict[str, Any]) -> str:
    """
    Construit le texte à encoder via CLIP pour une annonce.

    Le texte est conçu pour refléter le contenu visible de l'annonce :
    type de bien, localisation, prix, features, description.
    On l'optimise pour la comparabilité avec les images (style descriptif).

    Longueur : tronquée à 300 chars (limite CLIP ~77 tokens).
    """
    parts = []

    # Type et transaction
    prop_type = listing.get("type") or ""
    tx_type   = listing.get("transaction_type") or ""
    if prop_type:
        label = prop_type.lower()
        if tx_type.lower() == "rent":
            label += " à louer"
        else:
            label += " à vendre"
        parts.append(label)

    # Localisation
    city   = listing.get("city")   or ""
    region = listing.get("region") or ""
    if city:
        parts.append(f"à {city}")
    elif region:
        parts.append(f"région {region}")

    # Surface et pièces
    surface = listing.get("surface")
    rooms   = listing.get("rooms")
    if surface and float(surface) > 0:
        parts.append(f"surface {int(surface)} m²")
    if rooms and int(rooms) > 0:
        parts.append(f"{int(rooms)} pièces")

    # Prix
    price    = listing.get("price")
    currency = listing.get("currency") or "TND"
    if price and float(price) > 0:
        parts.append(f"prix {int(price):,} {currency}".replace(",", " "))

    # Features (amenities)
    features = listing.get("features") or []
    if isinstance(features, str):
        try:
            features = json.loads(features)
        except Exception:
            features = []
    if isinstance(features, list) and features:
        parts.append("avec " + ", ".join(str(f) for f in features[:4]))

    # Description (tronquée)
    desc = str(listing.get("description") or "").strip()
    if desc:
        parts.append(desc[:150])

    text = " — ".join(parts)
    return text[:300]


# ── Calcul de la déviation de prix ───────────────────────────────────────────

def compute_price_deviation(
    listing: Dict[str, Any],
    regional_stats: Dict[str, Dict[str, float]],
) -> Tuple[float, str, str]:
    """
    Calcule la déviation du prix d'une annonce par rapport à sa médiane régionale.

    Args:
        listing:        Dict listing
        regional_stats: Stats régionales depuis FraudDBConnector.get_regional_price_stats()

    Returns:
        Tuple (price_deviation_pct, deviation_level, price_signal)
          - price_deviation_pct : % d'écart (peut être négatif)
          - deviation_level     : "normal" | "moderate" | "high"
          - price_signal        : "overpriced" | "underpriced" | "normal"
    """
    price  = listing.get("price")
    region = (listing.get("region") or "unknown").lower().strip()
    ptype  = (listing.get("type")   or "other").lower().strip()
    tx     = listing.get("transaction_type") or "Sale"

    if not price or float(price) <= 0:
        return 0.0, "unknown", "unknown"

    price = float(price)

    # Chercher les stats pour ce groupe
    key    = f"{region}|{ptype}|{tx}"
    stats  = regional_stats.get(key)

    # Fallback : chercher avec seulement région
    if not stats:
        fallback_keys = [k for k in regional_stats if k.startswith(f"{region}|")]
        if fallback_keys:
            # Prendre la médiane des médianes disponibles
            medians = [regional_stats[k]["median"] for k in fallback_keys if regional_stats[k].get("median", 0) > 0]
            if medians:
                stats = {"median": float(np.median(medians)), "count": len(medians)}

    if not stats or stats.get("median", 0) <= 0:
        return 0.0, "no_data", "normal"

    median = stats["median"]
    deviation_pct = (price - median) / median * 100.0

    # Niveau de déviation
    abs_dev = abs(deviation_pct)
    if abs_dev > PRICE_DEVIATION_HIGH:
        level = "high"
    elif abs_dev > PRICE_DEVIATION_MODERATE:
        level = "moderate"
    else:
        level = "normal"

    # Signal directionnel
    if deviation_pct > PRICE_DEVIATION_MODERATE:
        signal = "overpriced"
    elif deviation_pct < -PRICE_DEVIATION_MODERATE:
        signal = "underpriced"
    else:
        signal = "normal"

    return round(deviation_pct, 2), level, signal


# ── Encodage texte en batch ───────────────────────────────────────────────────

def encode_listing_texts(
    listings: List[Dict[str, Any]],
) -> List[Optional[np.ndarray]]:
    """
    Encode les textes de tous les listings avec CLIP.

    Args:
        listings: Liste de dicts listing

    Returns:
        Liste d'embeddings np.array(512,) ou None par listing
    """
    texts = [_build_clip_text(lst) for lst in listings]
    logger.info(f"[TextSignal] Encodage CLIP de {len(texts)} textes")

    # Encoder en batches de 64 (limite mémoire CLIP)
    all_embeddings: List[Optional[np.ndarray]] = [None] * len(texts)
    batch_size = 64

    for start in range(0, len(texts), batch_size):
        batch_texts = texts[start:start + batch_size]
        batch_embs  = CLIPEncoder.encode_texts(batch_texts)  # shape (N, 512)

        for j, emb in enumerate(batch_embs):
            idx = start + j
            if np.linalg.norm(emb) > 1e-9:
                all_embeddings[idx] = emb
            # else: reste None

    n_encoded = sum(1 for e in all_embeddings if e is not None)
    logger.info(f"[TextSignal] {n_encoded}/{len(listings)} textes encodés avec succès")
    return all_embeddings


# ── Pipeline complet texte + prix par listing ─────────────────────────────────

def compute_text_price_signals(
    listings: List[Dict[str, Any]],
    regional_stats: Dict[str, Dict[str, float]],
) -> List[Dict[str, Any]]:
    """
    Calcule les signaux texte (CLIP embedding) et prix (déviation régionale)
    pour une liste de listings.

    Args:
        listings:       Liste de dicts listing
        regional_stats: Statistiques régionales de prix

    Returns:
        Listings enrichis avec :
          - clip_text_embedding:  np.array(512,) ou None
          - price_deviation_pct:  float
          - deviation_level:      "normal" | "moderate" | "high"
          - price_signal:         "overpriced" | "underpriced" | "normal"
          - clip_text_input:      str (texte utilisé pour l'encodage, pour debug)
    """
    logger.info(f"[TextSignal] Calcul des signaux texte+prix pour {len(listings)} listings")

    # Encoder tous les textes en une passe
    text_embeddings = encode_listing_texts(listings)

    # Enrichir chaque listing
    results = []
    for i, listing in enumerate(listings):
        dev_pct, dev_level, price_signal = compute_price_deviation(listing, regional_stats)
        clip_text = _build_clip_text(listing)

        results.append({
            **listing,
            "clip_text_embedding":  text_embeddings[i],
            "price_deviation_pct":  dev_pct,
            "deviation_level":      dev_level,
            "price_signal":         price_signal,
            "clip_text_input":      clip_text,
        })

    # Stats rapides
    n_overpriced   = sum(1 for r in results if r["price_signal"] == "overpriced")
    n_underpriced  = sum(1 for r in results if r["price_signal"] == "underpriced")
    n_high_dev     = sum(1 for r in results if r["deviation_level"] == "high")

    logger.info(
        f"[TextSignal] Résumé — "
        f"surpayés: {n_overpriced} | "
        f"sous-payés: {n_underpriced} | "
        f"déviation forte: {n_high_dev}"
    )
    return results
