"""
EstateMind — Microservice FastAPI : Scoring DSO 2.2 en temps réel
=================================================================
Lance avec :
    uvicorn fraud_detection.api:app --host 0.0.0.0 --port 8001 --reload

Endpoints :
    GET  /health              → Statut du service
    POST /score               → Scorer une annonce
    POST /score/batch         → Scorer plusieurs annonces
    GET  /summary             → Stats globales depuis Supabase
    POST /refresh-stats       → Recharge les stats régionales

Django appelle :
    POST http://localhost:8001/score
    Body : ListingRequest (JSON)
"""
from __future__ import annotations

import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

# Ajouter le dossier data/ au PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fraud_detection.api_schema import ListingRequest, ScoreResponse, SummaryResponse
from fraud_detection.db_connector import FraudDBConnector
from fraud_detection.multimodal.image_encoder import (
    CLIPClassifier,
    analyze_listing_images,
)
from fraud_detection.multimodal.text_price_signal import (
    extract_expected_categories,
    compute_price_deviation,
)
from fraud_detection.multimodal.consistency_classifier import (
    compute_consistency_score,
    interpret_score,
)


# ── État global du service ────────────────────────────────────────────────────

_regional_stats: Dict[str, Any] = {}


def _load_regional_stats() -> Dict[str, Any]:
    """Charge les stats de prix régionaux depuis Supabase."""
    try:
        db = FraudDBConnector()
        stats = db.get_regional_price_stats()
        db.close()
        logger.info(f"[API] Stats régionales chargées — {len(stats)} groupes")
        return stats
    except Exception as e:
        logger.error(f"[API] Impossible de charger les stats régionales : {e}")
        return {}


# ── Démarrage / Arrêt ─────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Exécuté au démarrage du service :
    1. Charge CLIP en mémoire (évite le cold start au premier /score)
    2. Charge les stats régionales depuis Supabase
    """
    global _regional_stats

    logger.info("[API] Démarrage — chargement CLIP...")
    try:
        CLIPClassifier._load()
        logger.info("[API] CLIP chargé et prêt")
    except Exception as e:
        logger.error(f"[API] Échec chargement CLIP : {e}")

    logger.info("[API] Chargement stats régionales...")
    _regional_stats = _load_regional_stats()

    yield  # Le service est opérationnel

    logger.info("[API] Arrêt du service")


# ── Application FastAPI ───────────────────────────────────────────────────────

app = FastAPI(
    title="EstateMind — Fraud Detection API",
    description="DSO 2.2 : Scoring de cohérence multimodale CLIP pour les annonces immobilières",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — permettre les appels depuis Django/React
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Restreindre en production : ["https://votre-domaine.com"]
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Helper : scorer un seul listing ──────────────────────────────────────────

def _score_one(listing_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Exécute les 3 étapes du pipeline DSO 2.2 sur un seul listing.
    Retourne le résultat brut de compute_consistency_score().
    """
    # Étape 1 — Classification CLIP des images
    image_analysis = analyze_listing_images(listing_dict, max_images=3)
    enriched = {**listing_dict, **image_analysis}

    # Étape 2 — Catégories attendues + déviation prix
    enriched["expected_categories"] = extract_expected_categories(enriched)
    dev_pct, dev_level, price_signal = compute_price_deviation(enriched, _regional_stats)
    enriched["price_deviation_pct"] = dev_pct
    enriched["deviation_level"]     = dev_level
    enriched["price_signal"]        = price_signal

    # Étape 3 — Score de cohérence
    return compute_consistency_score(enriched)


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health", tags=["Système"])
def health():
    """Vérifie que le service tourne et que CLIP est chargé."""
    clip_ready = CLIPClassifier._model is not None
    return {
        "status":          "ok" if clip_ready else "degraded",
        "clip_loaded":     clip_ready,
        "regional_groups": len(_regional_stats),
    }


@app.post("/score", response_model=ScoreResponse, tags=["Scoring"])
def score_listing(request: ListingRequest, save: bool = True):
    """
    Score une annonce immobilière en temps réel.

    - **save=true** (défaut) : sauvegarde le résultat dans Supabase
    - **save=false** : retourne juste le score, sans écriture DB

    Appelé par Django dès qu'une nouvelle annonce est publiée.
    """
    listing_dict = request.model_dump()

    try:
        result = _score_one(listing_dict)
    except Exception as e:
        logger.error(f"[API] Erreur scoring {request.property_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur scoring : {str(e)}")

    # Sauvegarde Supabase (optionnelle)
    saved = False
    if save:
        try:
            db = FraudDBConnector()
            db.save_multimodal_results([result])
            db.close()
            saved = True
        except Exception as e:
            logger.warning(f"[API] Score calculé mais non sauvegardé : {e}")

    return ScoreResponse(
        property_id           = result["property_id"],
        source_name           = result["source_name"],
        multimodal_score      = result["multimodal_score"],
        interpretation        = interpret_score(result["multimodal_score"]),
        image_text_similarity = result["image_text_similarity"],
        price_deviation_pct   = result["price_deviation_pct"],
        mismatch_types        = result["mismatch_types"],
        images_analyzed       = result["images_analyzed"],
        saved_to_db           = saved,
    )


@app.post("/score/batch", response_model=List[ScoreResponse], tags=["Scoring"])
def score_batch(requests: List[ListingRequest], save: bool = True):
    """
    Score plusieurs annonces en une seule requête (max 50).
    Utile pour l'import initial ou la resynchronisation.
    """
    if len(requests) > 50:
        raise HTTPException(status_code=400, detail="Maximum 50 annonces par batch")

    results_out = []
    results_to_save = []

    for req in requests:
        try:
            result = _score_one(req.model_dump())
            results_to_save.append(result)
            results_out.append(ScoreResponse(
                property_id           = result["property_id"],
                source_name           = result["source_name"],
                multimodal_score      = result["multimodal_score"],
                interpretation        = interpret_score(result["multimodal_score"]),
                image_text_similarity = result["image_text_similarity"],
                price_deviation_pct   = result["price_deviation_pct"],
                mismatch_types        = result["mismatch_types"],
                images_analyzed       = result["images_analyzed"],
                saved_to_db           = False,
            ))
        except Exception as e:
            logger.error(f"[API] Erreur batch scoring {req.property_id}: {e}")

    # Sauvegarde groupée
    saved = False
    if save and results_to_save:
        try:
            db = FraudDBConnector()
            db.save_multimodal_results(results_to_save)
            db.close()
            saved = True
            for r in results_out:
                r.saved_to_db = True
        except Exception as e:
            logger.warning(f"[API] Batch non sauvegardé : {e}")

    return results_out


@app.get("/summary", response_model=SummaryResponse, tags=["Dashboard"])
def get_summary():
    """
    Statistiques globales depuis Supabase.
    Utilisé par le dashboard admin pour les KPI cards.
    """
    try:
        db = FraudDBConnector()
        stats = db.get_fraud_summary()
        db.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return SummaryResponse(
        total_analyzed       = stats.get("total_analyzed", 0),
        total_incoherent     = stats.get("total_incoherent", 0),
        total_suspect        = stats.get("total_suspect", 0),
        total_coherent       = stats.get("total_coherent", 0),
        avg_multimodal_score = float(stats.get("avg_multimodal_score") or 0),
        avg_price_deviation  = float(stats.get("avg_price_deviation") or 0),
    )


@app.post("/refresh-stats", tags=["Système"])
def refresh_regional_stats():
    """
    Recharge les statistiques de prix régionaux depuis Supabase.
    À appeler si de nouvelles annonces ont été ajoutées en masse.
    """
    global _regional_stats
    _regional_stats = _load_regional_stats()
    return {"refreshed": True, "groups": len(_regional_stats)}
