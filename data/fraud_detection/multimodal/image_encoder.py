"""
DSO 2.2 — Classification Zero-Shot Sémantique des Images (CLIP)
===============================================================
Principe :
  Au lieu de comparer image↔texte par cosinus brut, on classifie
  chaque image dans une catégorie immobilière sémantique (chambre,
  piscine, terrain...) en utilisant CLIP comme classificateur zero-shot.

  Comment ça marche :
    1. Encoder l'image → vecteur (512-dim)
    2. Encoder les descriptions de chaque catégorie → vecteurs (N_cat × 512)
    3. Softmax sur les similarités → probabilité d'appartenance à chaque catégorie
    4. La catégorie avec la plus haute probabilité = ce que montre l'image

  C'est du "zero-shot" : CLIP n'a pas été entraîné spécifiquement sur
  ces catégories, il les comprend grâce à son alignement image-texte.
"""
from __future__ import annotations

import io
import time
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────
IMAGE_CACHE_DIR = Path(__file__).resolve().parents[2] / "cache" / "image_embeddings"
IMAGE_CACHE_DIR.mkdir(parents=True, exist_ok=True)

DOWNLOAD_TIMEOUT = 10
MAX_IMAGE_SIZE   = 5       # MB
RETRY_ATTEMPTS   = 2
REQUEST_DELAY    = 0.3

CLIP_MODEL_NAME  = "openai/clip-vit-base-patch32"
CLIP_DIM         = 512

# ── Catégories sémantiques immobilières ──────────────────────────────────────
# Clé : identifiant | Valeur : prompt texte envoyé à CLIP pour cette catégorie
REAL_ESTATE_CATEGORIES: Dict[str, str] = {
    "bedroom":     "a photo of a bedroom with a bed and furniture",
    "living_room": "a photo of a living room or lounge area",
    "kitchen":     "a photo of a kitchen with appliances or cabinets",
    "bathroom":    "a photo of a bathroom or toilet",
    "exterior":    "a photo of the exterior facade of a house or building",
    "pool":        "a photo of a swimming pool",
    "terrace":     "a photo of a terrace balcony or outdoor space",
    "garden":      "a photo of a garden or yard with trees and plants",
    "parking":     "a photo of a garage or parking space",
    "view":        "a photo of a panoramic view or sea view from a window",
    "land":        "a photo of an empty land or plot of land",
    "building":    "a photo of an apartment building or residential complex",
    "other":       "a photo not related to real estate such as a person or document",
}

CATEGORY_NAMES = list(REAL_ESTATE_CATEGORIES.keys())
CATEGORY_TEXTS = list(REAL_ESTATE_CATEGORIES.values())


# ── Singleton CLIP ────────────────────────────────────────────────────────────
class CLIPClassifier:
    """
    Wrapper CLIP chargé une seule fois (singleton via classmethods).
    Utilisé pour :
      - encoder les images
      - encoder les prompts textuels des catégories (une seule fois, mis en cache)
      - classifier chaque image en zero-shot
    """
    _processor     = None
    _model         = None
    _device        = None
    _category_embs = None   # (N_cat, 512) — calculé une seule fois

    @classmethod
    def _load(cls):
        if cls._model is not None:
            return
        try:
            import torch
            from transformers import CLIPProcessor, CLIPModel

            cls._device    = "cuda" if torch.cuda.is_available() else "cpu"
            logger.info(f"[CLIP] Chargement {CLIP_MODEL_NAME} sur {cls._device}")
            cls._processor = CLIPProcessor.from_pretrained(CLIP_MODEL_NAME)
            cls._model     = CLIPModel.from_pretrained(CLIP_MODEL_NAME).to(cls._device)
            cls._model.eval()
            logger.info(f"[CLIP] Modèle prêt — {len(CATEGORY_NAMES)} catégories sémantiques")
        except ImportError as e:
            logger.error(f"[CLIP] Dépendances manquantes : {e}")
            raise

    @classmethod
    def _get_category_embeddings(cls) -> np.ndarray:
        """
        Encode les prompts textuels des catégories → (N_cat, 512).
        Calculé une seule fois puis mis en cache en mémoire.
        """
        if cls._category_embs is not None:
            return cls._category_embs

        cls._load()
        import torch

        inputs = cls._processor(
            text=CATEGORY_TEXTS,
            return_tensors="pt",
            padding=True,
            truncation=True,
            max_length=77,
        ).to(cls._device)

        with torch.no_grad():
            text_out   = cls._model.text_model(**inputs)
            pooled     = text_out.pooler_output
            text_feats = cls._model.text_projection(pooled)

        norms = text_feats.norm(dim=-1, keepdim=True).clamp(min=1e-9)
        cls._category_embs = (text_feats / norms).cpu().numpy()
        return cls._category_embs

    @classmethod
    def encode_images(cls, pil_images: list) -> np.ndarray:
        """Encode une liste d'images PIL → (N, 512) normalisé L2."""
        cls._load()
        import torch

        if not pil_images:
            return np.zeros((0, CLIP_DIM))

        try:
            inputs = cls._processor(
                images=pil_images,
                return_tensors="pt",
                padding=True,
            ).to(cls._device)

            with torch.no_grad():
                vision_out  = cls._model.vision_model(pixel_values=inputs["pixel_values"])
                pooled      = vision_out.pooler_output
                image_feats = cls._model.visual_projection(pooled)

            norms = image_feats.norm(dim=-1, keepdim=True).clamp(min=1e-9)
            return (image_feats / norms).cpu().numpy()

        except Exception as e:
            logger.error(f"[CLIP] Erreur encode_images : {e}")
            return np.zeros((len(pil_images), CLIP_DIM))

    @classmethod
    def classify_images(cls, pil_images: list) -> List[Dict[str, Any]]:
        """
        Classifie chaque image dans une catégorie sémantique (zero-shot).

        Algorithme :
          image_emb (512,) · category_embs (N_cat, 512)ᵀ → scores (N_cat,)
          softmax(scores × 100) → probabilités
          argmax → catégorie dominante

        Returns:
            [{category, confidence, all_scores}] par image
        """
        if not pil_images:
            return []

        image_embs = cls.encode_images(pil_images)          # (N_img, 512)
        cat_embs   = cls._get_category_embeddings()         # (N_cat, 512)

        # Produit scalaire = cosine sim (vecteurs normalisés)
        similarities = image_embs @ cat_embs.T              # (N_img, N_cat)

        # Softmax avec température CLIP (×100 pour accentuer les différences)
        exp_sims = np.exp(similarities * 100)
        probs    = exp_sims / exp_sims.sum(axis=1, keepdims=True)

        results = []
        for i in range(len(pil_images)):
            top_idx = int(np.argmax(probs[i]))
            results.append({
                "category":   CATEGORY_NAMES[top_idx],
                "confidence": round(float(probs[i, top_idx]), 4),
                "all_scores": {
                    cat: round(float(probs[i, j]), 4)
                    for j, cat in enumerate(CATEGORY_NAMES)
                },
            })
        return results


# ── Téléchargement d'images ───────────────────────────────────────────────────

def _download_image(url: str):
    """Télécharge une image → PIL.Image ou None."""
    try:
        from PIL import Image
    except ImportError:
        logger.error("[ImageEncoder] Pillow non installé — pip install Pillow")
        return None

    if not url or not url.startswith("http"):
        return None

    try:
        import requests
        headers = {"User-Agent": "Mozilla/5.0 Chrome/120.0"}
        for attempt in range(RETRY_ATTEMPTS):
            try:
                resp = requests.get(url, timeout=DOWNLOAD_TIMEOUT, headers=headers, stream=True)
                if resp.status_code != 200:
                    continue
                if int(resp.headers.get("Content-Length", 0)) > MAX_IMAGE_SIZE * 1024 * 1024:
                    return None
                img = Image.open(io.BytesIO(resp.content)).convert("RGB")
                time.sleep(REQUEST_DELAY)
                return img
            except Exception:
                time.sleep(0.5 * (attempt + 1))
    except ImportError:
        pass
    return None


def analyze_listing_images(
    listing: Dict[str, Any],
    max_images: int = 3,
) -> Dict[str, Any]:
    """
    Télécharge et classifie les images d'un listing via CLIP zero-shot.

    Returns:
        {
            detected_categories : liste des catégories détectées (une par image)
            category_confidences: score de confiance pour chaque image
            images_analyzed     : nombre d'images traitées
            has_images          : bool
            image_classifications: détail complet par image
        }
    """
    import json as _json

    # Vérifier le cache
    listing_id = f"{listing.get('source_name', '')}_{listing.get('property_id', '')}"
    cache_key  = hashlib.sha256(listing_id.encode()).hexdigest()[:16]
    cache_path = IMAGE_CACHE_DIR / f"cls_{cache_key}.npy"

    if cache_path.exists():
        try:
            cached_indices = np.load(cache_path)
            cats = [CATEGORY_NAMES[int(i)] for i in cached_indices]
            return {
                "detected_categories":   cats,
                "category_confidences":  [1.0] * len(cats),
                "images_analyzed":       len(cats),
                "has_images":            len(cats) > 0,
                "image_classifications": [],
            }
        except Exception:
            pass

    # Récupérer les URLs
    images_field = listing.get("images") or []
    if isinstance(images_field, str):
        try:
            images_field = _json.loads(images_field)
        except Exception:
            images_field = []

    urls = [u for u in images_field if isinstance(u, str) and u.startswith("http")][:max_images]

    # Télécharger
    pil_images = []
    for url in urls:
        img = _download_image(url)
        if img is not None:
            pil_images.append(img)

    if not pil_images:
        return {
            "detected_categories":   [],
            "category_confidences":  [],
            "images_analyzed":       0,
            "has_images":            False,
            "image_classifications": [],
        }

    # Classifier avec CLIP zero-shot
    classifications   = CLIPClassifier.classify_images(pil_images)
    detected_cats     = [c["category"]   for c in classifications]
    confidences       = [c["confidence"] for c in classifications]

    # Mettre en cache
    try:
        indices = np.array([CATEGORY_NAMES.index(c) for c in detected_cats])
        np.save(cache_path, indices)
    except Exception:
        pass

    return {
        "detected_categories":   detected_cats,
        "category_confidences":  confidences,
        "images_analyzed":       len(pil_images),
        "has_images":            True,
        "image_classifications": classifications,
    }


def analyze_listings_batch(
    listings: List[Dict[str, Any]],
    max_images_per_listing: int = 3,
) -> List[Dict[str, Any]]:
    """Analyse les images de tous les listings, enrichit chaque dict."""
    n = len(listings)
    logger.info(f"[ImageEncoder] CLIP zero-shot classification — {n} listings")
    results = []

    for i, listing in enumerate(listings):
        if i % 50 == 0:
            logger.info(f"[ImageEncoder] {i}/{n} listings traités")
        analysis = analyze_listing_images(listing, max_images_per_listing)
        results.append({**listing, **analysis})

    n_with = sum(1 for r in results if r["has_images"])
    logger.info(f"[ImageEncoder] Terminé — {n_with}/{n} listings avec images classifiées")
    return results
