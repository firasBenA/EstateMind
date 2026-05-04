"""
DSO 2.2 — Cohérence Sémantique Multimodale (CLIP Zero-Shot)
Détecte les incohérences entre images, description et prix d'une annonce.
"""
from fraud_detection.multimodal.consistency_classifier import (
    run_multimodal_pipeline,
    interpret_score,
)

__all__ = ["run_multimodal_pipeline", "interpret_score"]
