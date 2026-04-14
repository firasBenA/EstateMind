"""
EstateMind — DSO 2.2 : Encodeur d'Images CLIP
==============================================
Télécharge les images des annonces depuis leurs URLs et les encode
via le modèle CLIP (openai/clip-vit-base-patch32).

CLIP (Contrastive Language-Image Pretraining) produit des embeddings
dans un espace vectoriel partagé image/texte, permettant de mesurer
la similarité sémantique entre une photo et une description textuelle.

Pipeline :
  URL image → téléchargement → décodage PIL → prétraitement CLIP → vecteur 512-dim

Choix de CLIP vs alternatives :
  - CLIP    → meilleur alignement image-texte, idéal pour DSO 2.2
  - ViT     → meilleur pour qualité d'image pure mais pas d'alignement texte
  - ResNet  → plus rapide mais moins expressif
"""
from __future__ import annotations

import io
import time
import hashlib
import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import numpy as np
from loguru import logger

# Cache local des embeddings images (évite de re-télécharger)
IMAGE_CACHE_DIR = Path(__file__).resolve().parents[2] / "cache" / "image_embeddings"
IMAGE_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Timeout et configuration de téléchargement
DOWNLOAD_TIMEOUT  = 10    # secondes
MAX_IMAGE_SIZE    = 5     # MB
RETRY_ATTEMPTS    = 2
REQUEST_DELAY     = 0.3   # secondes entre requêtes (politesse)

# Modèle CLIP
CLIP_MODEL_NAME = "openai/clip-vit-base-patch32"
CLIP_DIM        = 512


# ── Chargement du modèle CLIP (singleton) ────────────────────────────────────

class CLIPEncoder:
    """Singleton CLIP — chargé une seule fois en mémoire."""

    _processor = None
    _model     = None
    _device    = None

    @classmethod
    def _load(cls):
        if cls._model is not None:
            return

        try:
            import torch
            from transformers import CLIPProcessor, CLIPModel

            cls._device = "cuda" if torch.cuda.is_available() else "cpu"
            logger.info(f"[CLIP] Chargement du modèle {CLIP_MODEL_NAME} sur {cls._device}")

            cls._processor = CLIPProcessor.from_pretrained(CLIP_MODEL_NAME)
            cls._model     = CLIPModel.from_pretrained(CLIP_MODEL_NAME).to(cls._device)
            cls._model.eval()

            logger.info(f"[CLIP] Modèle prêt — dim={CLIP_DIM}")
        except ImportError as e:
            logger.error(f"[CLIP] Dépendances manquantes: {e} — pip install transformers torch")
            raise

    @classmethod
    def encode_images(cls, pil_images: List) -> np.ndarray:
        """
        Encode une liste d'images PIL en embeddings CLIP normalisés.

        Args:
            pil_images: Liste d'objets PIL.Image

        Returns:
            Array numpy shape (N, 512), normalisé L2
        """
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
                # Compatibilité transformers 4.x et 5.x :
                # get_image_features() retourne un tenseur en 4.x,
                # mais un objet structuré en 5.x → utiliser vision_model directement.
                vision_out = cls._model.vision_model(
                    pixel_values=inputs["pixel_values"]
                )
                pooled = vision_out.pooler_output          # (N, 768)
                image_features = cls._model.visual_projection(pooled)  # (N, 512)

            # Normalisation L2
            norms = image_features.norm(dim=-1, keepdim=True).clamp(min=1e-9)
            image_features = image_features / norms
            return image_features.cpu().numpy()

        except Exception as e:
            logger.error(f"[CLIP] Erreur encode_images: {e}")
            return np.zeros((len(pil_images), CLIP_DIM))

    @classmethod
    def encode_texts(cls, texts: List[str]) -> np.ndarray:
        """
        Encode une liste de textes en embeddings CLIP normalisés.

        Args:
            texts: Liste de chaînes de caractères

        Returns:
            Array numpy shape (N, 512), normalisé L2
        """
        cls._load()
        import torch

        if not texts:
            return np.zeros((0, CLIP_DIM))

        try:
            # Tronquer à 77 tokens (limite CLIP)
            truncated = [t[:300] for t in texts]

            inputs = cls._processor(
                text=truncated,
                return_tensors="pt",
                padding=True,
                truncation=True,
                max_length=77,
            ).to(cls._device)

            with torch.no_grad():
                # Compatibilité transformers 5.x — utiliser text_model directement.
                text_out = cls._model.text_model(**inputs)
                pooled = text_out.pooler_output                          # (N, 512)
                text_features = cls._model.text_projection(pooled)       # (N, 512)

            norms = text_features.norm(dim=-1, keepdim=True).clamp(min=1e-9)
            text_features = text_features / norms
            return text_features.cpu().numpy()

        except Exception as e:
            logger.error(f"[CLIP] Erreur encode_texts: {e}")
            return np.zeros((len(texts), CLIP_DIM))


# ── Téléchargement d'images ───────────────────────────────────────────────────

def _url_to_cache_key(url: str) -> str:
    """Hash SHA-256 de l'URL comme clé de cache."""
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def _download_image(url: str) -> Optional[object]:
    """
    Télécharge une image depuis une URL et retourne un objet PIL.Image.
    Utilise un cache local pour éviter les re-téléchargements.

    Returns:
        PIL.Image ou None en cas d'échec
    """
    try:
        from PIL import Image
    except ImportError:
        logger.error("[ImageEncoder] PIL/Pillow non installé — pip install Pillow")
        return None

    if not url or not url.startswith("http"):
        return None

    # Vérification du cache
    cache_key  = _url_to_cache_key(url)
    cache_path = IMAGE_CACHE_DIR / f"{cache_key}.npy"

    if cache_path.exists():
        # Cache hit — on retourne None ici car le cache est au niveau embedding
        # (géré dans encode_listing_images)
        pass

    # Téléchargement
    try:
        import requests
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            )
        }
        for attempt in range(RETRY_ATTEMPTS):
            try:
                resp = requests.get(
                    url, timeout=DOWNLOAD_TIMEOUT,
                    headers=headers, stream=True
                )
                if resp.status_code != 200:
                    continue

                # Vérifier la taille
                content_length = int(resp.headers.get("Content-Length", 0))
                if content_length > MAX_IMAGE_SIZE * 1024 * 1024:
                    logger.debug(f"[ImageEncoder] Image trop grande ({content_length/1e6:.1f}MB): {url}")
                    return None

                img_data = resp.content
                img = Image.open(io.BytesIO(img_data)).convert("RGB")
                time.sleep(REQUEST_DELAY)
                return img

            except Exception as e:
                if attempt == RETRY_ATTEMPTS - 1:
                    logger.debug(f"[ImageEncoder] Échec téléchargement {url}: {e}")
                time.sleep(0.5 * (attempt + 1))

    except ImportError:
        logger.error("[ImageEncoder] requests non installé")

    return None


# ── Encodage d'un listing ─────────────────────────────────────────────────────

def encode_listing_images(
    listing: Dict[str, Any],
    max_images: int = 3,
) -> Tuple[Optional[np.ndarray], int]:
    """
    Encode les images d'un listing en un embedding moyen CLIP.

    Stratégie d'agrégation :
      - On encode max_images images (évite trop de requêtes)
      - L'embedding final = moyenne des embeddings individuels
      - La moyenne est re-normalisée L2

    Args:
        listing:    Dict listing avec champ 'images' (list d'URLs)
        max_images: Nombre maximum d'images à traiter

    Returns:
        Tuple (embedding array shape (512,) ou None, nb_images_analysées)
    """
    # Récupérer les URLs d'images
    images_field = listing.get("images") or []
    if isinstance(images_field, str):
        import json
        try:
            images_field = json.loads(images_field)
        except Exception:
            images_field = []

    urls = [u for u in images_field if isinstance(u, str) and u.startswith("http")]
    if not urls:
        return None, 0

    # Vérifier le cache au niveau listing
    listing_id  = f"{listing.get('source_name', '')}_{listing.get('property_id', '')}"
    cache_key   = hashlib.sha256(listing_id.encode()).hexdigest()[:16]
    cache_path  = IMAGE_CACHE_DIR / f"listing_{cache_key}.npy"

    if cache_path.exists():
        try:
            cached = np.load(cache_path)
            return cached, len(urls[:max_images])
        except Exception:
            pass

    # Télécharger et encoder
    pil_images = []
    for url in urls[:max_images]:
        img = _download_image(url)
        if img is not None:
            pil_images.append(img)

    if not pil_images:
        return None, 0

    # Encoder avec CLIP
    embeddings = CLIPEncoder.encode_images(pil_images)  # shape (N, 512)

    if embeddings.shape[0] == 0:
        return None, 0

    # Moyenne des embeddings
    avg_emb = embeddings.mean(axis=0)

    # Re-normalisation L2
    norm = np.linalg.norm(avg_emb)
    if norm > 1e-9:
        avg_emb = avg_emb / norm

    # Mise en cache
    try:
        np.save(cache_path, avg_emb)
    except Exception:
        pass

    return avg_emb, len(pil_images)


# ── Encodage en batch ─────────────────────────────────────────────────────────

def encode_listings_batch(
    listings: List[Dict[str, Any]],
    max_images_per_listing: int = 3,
) -> List[Dict[str, Any]]:
    """
    Encode les images de tous les listings en batch.

    Args:
        listings:               Liste de dicts listing
        max_images_per_listing: Max images par listing

    Returns:
        Liste de dicts avec champs ajoutés :
          - image_embedding:  np.array(512,) ou None
          - images_analyzed:  int
          - has_images:       bool
    """
    results = []
    n = len(listings)

    logger.info(f"[ImageEncoder] Encodage de {n} listings (max {max_images_per_listing} images/listing)")

    for i, listing in enumerate(listings):
        if i % 100 == 0:
            logger.info(f"[ImageEncoder] Progression: {i}/{n}")

        embedding, n_images = encode_listing_images(listing, max_images_per_listing)

        results.append({
            **listing,
            "image_embedding":  embedding,
            "images_analyzed":  n_images,
            "has_images":       (embedding is not None),
        })

    n_with_images = sum(1 for r in results if r["has_images"])
    logger.info(
        f"[ImageEncoder] Terminé — {n_with_images}/{n} listings avec images encodées"
    )
    return results


# ── Utilitaire : similarité cosine ───────────────────────────────────────────

def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """
    Calcule la similarité cosine entre deux vecteurs normalisés.
    Retourne 0.0 si l'un des vecteurs est None ou zero.
    """
    if a is None or b is None:
        return 0.0

    a = np.asarray(a, dtype=float).flatten()
    b = np.asarray(b, dtype=float).flatten()

    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)

    if norm_a < 1e-9 or norm_b < 1e-9:
        return 0.0

    return float(np.dot(a, b) / (norm_a * norm_b))
