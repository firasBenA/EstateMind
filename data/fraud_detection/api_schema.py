"""
EstateMind — Schémas Pydantic pour l'API de scoring DSO 2.2
============================================================
Contrat d'interface entre Django et le microservice FastAPI.

Ton collègue Django envoie un ListingRequest,
il reçoit un ScoreResponse.
"""
from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel, Field


# ── Requête ───────────────────────────────────────────────────────────────────

class ListingRequest(BaseModel):
    """
    Données d'une annonce à scorer.
    Tous les champs optionnels sauf property_id et source_name.
    """
    property_id:      str            = Field(..., description="Identifiant unique de l'annonce")
    source_name:      str            = Field(..., description="Source : tayara | mubawab | user_submission ...")

    # Champs descriptifs
    type:             Optional[str]  = Field(None, description="Type de bien : villa | appartement | terrain ...")
    description:      Optional[str]  = Field(None, description="Description textuelle de l'annonce")
    features:         Optional[List] = Field(None, description="Liste d'équipements : ['piscine', 'parking', ...]")

    # Prix et localisation
    price:            Optional[float]= Field(None, description="Prix en dinars tunisiens")
    region:           Optional[str]  = Field(None, description="Gouvernorat : Tunis | Sfax | Sousse ...")
    transaction_type: Optional[str]  = Field(None, description="Sale | Rent")

    # Images
    images:           Optional[List[str]] = Field(
        None,
        description="Liste d'URLs HTTP des photos de l'annonce"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "property_id":      "TYR-12345",
                "source_name":      "tayara",
                "type":             "villa",
                "description":      "Villa avec piscine et jardin, vue mer, garage double",
                "features":         ["piscine", "parking", "terrasse"],
                "price":            850000,
                "region":           "Tunis",
                "transaction_type": "Sale",
                "images": [
                    "https://example.com/img1.jpg",
                    "https://example.com/img2.jpg",
                ]
            }
        }


# ── Réponse ───────────────────────────────────────────────────────────────────

class ScoreResponse(BaseModel):
    """
    Résultat du scoring DSO 2.2 pour une annonce.
    """
    property_id:           str
    source_name:           str

    # Score principal
    multimodal_score:      float = Field(..., description="Score de cohérence [0-1]")
    interpretation:        str   = Field(..., description="INCOHERENT | SUSPECT | ACCEPTABLE | COHERENT")

    # Détails
    image_text_similarity: float = Field(..., description="Score correspondance image↔catégories attendues [0-1]")
    price_deviation_pct:   float = Field(..., description="Écart prix vs médiane régionale en %")
    mismatch_types:        List[str] = Field(..., description="Flags d'incohérence détectés")
    images_analyzed:       int   = Field(..., description="Nombre d'images analysées")

    # Méta
    model_version:         str   = "clip_zeroshot_semantic_v1"
    saved_to_db:           bool  = Field(..., description="True si sauvegardé dans Supabase")

    class Config:
        json_schema_extra = {
            "example": {
                "property_id":           "TYR-12345",
                "source_name":           "tayara",
                "multimodal_score":      0.21,
                "interpretation":        "INCOHERENT — fraude probable",
                "image_text_similarity": 0.15,
                "price_deviation_pct":   145.3,
                "mismatch_types":        ["overpriced_vs_images", "claimed_pool_not_visible"],
                "images_analyzed":       2,
                "model_version":         "clip_zeroshot_semantic_v1",
                "saved_to_db":           True
            }
        }


# ── Réponse résumé (dashboard) ────────────────────────────────────────────────

class SummaryResponse(BaseModel):
    total_analyzed:      int
    total_incoherent:    int
    total_suspect:       int
    total_coherent:      int
    avg_multimodal_score: float
    avg_price_deviation:  float
