"""
Estate Mind — Pipeline Similarite Image/Texte avec CLIP
========================================================
Utilise CLIP pour calculer la similarite semantique
entre la description d'une annonce et ses images.

Source  : Supabase (table listings)
Sortie  : Supabase (table image_similarity)

Colonnes produites :
    - listing_id       : id de l'annonce
    - nb_images        : nombre d'images analysees
    - similarity_score : score moyen texte/images (0-1)
    - similarity_min   : score minimum
    - similarity_max   : score maximum
    - similarity_signal: signal incoherence = 1 - similarity_score
    - coherence_label  : coherent / suspect / incoherent
    - analysed_at      : timestamp

Usage :
    python pipeline_clip.py           # mode batch
    python pipeline_clip.py --test    # test sur 10 annonces

Prerequis :
    pip install supabase transformers torch pillow requests
"""

import os
import io
import sys
import json
import time
import argparse
import requests
import numpy as np
from PIL import Image
from tqdm import tqdm
from dotenv import load_dotenv
from transformers import CLIPProcessor, CLIPModel
import torch
from supabase import create_client

load_dotenv()

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

CLIP_MODEL_NAME = "openai/clip-vit-base-patch32"

TABLE_SOURCE = "listings"
TABLE_DEST   = "image_similarity"

MAX_IMAGES_PER_LISTING = 5    # on prend max 5 images par annonce
IMAGE_TIMEOUT          = 10   # secondes timeout téléchargement
BATCH_SIZE             = 10   # annonces traitees par batch

# Seuils coherence
SEUIL_COHERENT  = 0.25   # score > 0.25 = coherent
SEUIL_SUSPECT   = 0.20   # score 0.20-0.25 = suspect
# score < 0.20 = incoherent


# ─────────────────────────────────────────────
# CONNEXION
# ─────────────────────────────────────────────
def get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("SUPABASE_URL et SUPABASE_SERVICE_KEY requis dans .env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ─────────────────────────────────────────────
# CREATION TABLE
# ─────────────────────────────────────────────
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_DEST} (
    listing_id        INTEGER PRIMARY KEY,
    nb_images         INTEGER DEFAULT 0,
    nb_images_ok      INTEGER DEFAULT 0,
    similarity_score  FLOAT,
    similarity_min    FLOAT,
    similarity_max    FLOAT,
    similarity_signal FLOAT,
    coherence_label   VARCHAR(20),
    analysed_at       TIMESTAMP DEFAULT NOW()
);
"""


# ─────────────────────────────────────────────
# CHARGEMENT CLIP
# ─────────────────────────────────────────────
def load_clip():
    print(f"\n[...] Chargement du modele CLIP ({CLIP_MODEL_NAME})...")
    model     = CLIPModel.from_pretrained(CLIP_MODEL_NAME)
    processor = CLIPProcessor.from_pretrained(CLIP_MODEL_NAME)
    model.eval()
    print("[OK] CLIP charge")
    return model, processor


# ─────────────────────────────────────────────
# TELECHARGEMENT IMAGE
# ─────────────────────────────────────────────
def download_image(url: str):
    """Telecharge une image depuis une URL et retourne un objet PIL Image."""
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, timeout=IMAGE_TIMEOUT, headers=headers)
        response.raise_for_status()
        img = Image.open(io.BytesIO(response.content)).convert("RGB")
        return img
    except Exception:
        return None


# ─────────────────────────────────────────────
# CALCUL SIMILARITE CLIP
# ─────────────────────────────────────────────
def compute_similarity(model, processor, text: str, images: list) -> dict:
    """
    Calcule la similarite semantique entre un texte et une liste d'images.
    Retourne un dict avec les scores.
    """
    if not images:
        return None

    try:
        # Encoder le texte
        text_inputs = processor(
            text=[text[:500]],
            return_tensors="pt",
            padding=True,
            truncation=True,
            max_length=77   # limite CLIP
        )

        # Encoder les images
        image_inputs = processor(
            images=images,
            return_tensors="pt"
        )

        with torch.no_grad():
            text_features  = model.get_text_features(**text_inputs)
            image_features = model.get_image_features(**image_inputs)

        # Normaliser les embeddings
        text_features  = text_features  / text_features.norm(dim=-1, keepdim=True)
        image_features = image_features / image_features.norm(dim=-1, keepdim=True)

        # Similarite cosine entre le texte et chaque image
        similarities = (text_features @ image_features.T).squeeze(0).numpy()

        # Convertir en float Python
        if isinstance(similarities, np.ndarray):
            scores = similarities.tolist()
            if not isinstance(scores, list):
                scores = [float(scores)]
        else:
            scores = [float(similarities)]

        avg_score = float(np.mean(scores))
        min_score = float(np.min(scores))
        max_score = float(np.max(scores))

        return {
            "similarity_score":  round(avg_score, 4),
            "similarity_min":    round(min_score, 4),
            "similarity_max":    round(max_score, 4),
            "similarity_signal": round(1 - avg_score, 4),
        }

    except Exception as e:
        print(f"\n[ERREUR CLIP] {str(e)[:100]}")
        return None


def score_to_label(score: float) -> str:
    if score >= SEUIL_COHERENT:
        return "coherent"
    elif score >= SEUIL_SUSPECT:
        return "suspect"
    else:
        return "incoherent"


# ─────────────────────────────────────────────
# RECUPERATION DES ANNONCES
# ─────────────────────────────────────────────
def get_listings_with_images(supabase, limit=None, already_done=None):
    """Recupere les annonces avec images non encore analysees."""
    query = (
        supabase.table(TABLE_SOURCE)
        .select("id, description, images")
        .not_.is_("images", "null")
    )

    if limit:
        query = query.limit(limit)

    response = query.execute()
    listings = response.data

    # Filtrer celles deja analysees
    if already_done:
        listings = [l for l in listings if l["id"] not in already_done]

    # Filtrer celles avec images non vides
    listings = [
        l for l in listings
        if l.get("images") and len(l["images"]) > 0
    ]

    return listings


def get_already_analysed(supabase):
    """Retourne les IDs deja analyses."""
    try:
        response = supabase.table(TABLE_DEST).select("listing_id").execute()
        return {row["listing_id"] for row in response.data}
    except Exception:
        return set()


# ─────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ─────────────────────────────────────────────
def process_listing(listing, model, processor):
    """Traite une annonce — telecharge les images et calcule la similarite."""
    listing_id  = listing["id"]
    description = listing.get("description", "") or ""
    images_data = listing.get("images", [])

    # Extraire les URLs
    if isinstance(images_data, str):
        try:
            images_data = json.loads(images_data)
        except Exception:
            images_data = []

    if not isinstance(images_data, list):
        images_data = []

    # Deduplication et limite
    urls = list(dict.fromkeys(images_data))[:MAX_IMAGES_PER_LISTING]
    nb_images = len(urls)

    if nb_images == 0:
        return None

    # Telecharger les images
    pil_images = []
    for url in urls:
        img = download_image(url)
        if img:
            pil_images.append(img)

    nb_ok = len(pil_images)

    if nb_ok == 0:
        return {
            "listing_id":        listing_id,
            "nb_images":         nb_images,
            "nb_images_ok":      0,
            "similarity_score":  None,
            "similarity_min":    None,
            "similarity_max":    None,
            "similarity_signal": None,
            "coherence_label":   "no_images",
        }

    # Calculer la similarite
    scores = compute_similarity(model, processor, description, pil_images)

    if not scores:
        return None

    return {
        "listing_id":        listing_id,
        "nb_images":         nb_images,
        "nb_images_ok":      nb_ok,
        "similarity_score":  scores["similarity_score"],
        "similarity_min":    scores["similarity_min"],
        "similarity_max":    scores["similarity_max"],
        "similarity_signal": scores["similarity_signal"],
        "coherence_label":   score_to_label(scores["similarity_score"]),
    }


def save_batch(supabase, results):
    """Sauvegarde les resultats dans Supabase."""
    if not results:
        return
    try:
        supabase.table(TABLE_DEST).upsert(results).execute()
    except Exception as e:
        print(f"\n[ERREUR SAVE] {str(e)[:150]}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Test sur 10 annonces")
    args = parser.parse_args()

    # Connexion Supabase
    supabase = get_supabase()
    print("[OK] Supabase connecte")

    # Charger CLIP
    model, processor = load_clip()

    # Recuperer les IDs deja analyses
    already_done = get_already_analysed(supabase)
    print(f"[INFO] {len(already_done)} annonces deja analysees")

    # Recuperer les annonces
    limit = 10 if args.test else None
    listings = get_listings_with_images(supabase, limit=limit, already_done=already_done)
    print(f"[INFO] {len(listings)} annonces a analyser")

    if not listings:
        print("[OK] Toutes les annonces sont deja analysees")
        return

    # Traitement
    results  = []
    errors   = 0
    coherent = 0
    suspect  = 0
    incoherent = 0

    for listing in tqdm(listings, desc="CLIP Similarity"):
        result = process_listing(listing, model, processor)

        if result:
            results.append(result)
            label = result.get("coherence_label", "")
            if label == "coherent":
                coherent += 1
            elif label == "suspect":
                suspect += 1
            elif label == "incoherent":
                incoherent += 1
        else:
            errors += 1

        # Sauvegarder par batch
        if len(results) >= BATCH_SIZE:
            save_batch(supabase, results)
            results = []

    # Sauvegarder le reste
    if results:
        save_batch(supabase, results)

    # Rapport
    total = coherent + suspect + incoherent
    print(f"\n{'='*50}")
    print("RAPPORT CLIP SIMILARITY")
    print(f"{'='*50}")
    print(f"Total analyse     : {total}")
    print(f"Erreurs           : {errors}")
    print(f"\nDistribution :")
    print(f"  coherent    : {coherent} ({coherent/max(total,1)*100:.1f}%)")
    print(f"  suspect     : {suspect} ({suspect/max(total,1)*100:.1f}%)")
    print(f"  incoherent  : {incoherent} ({incoherent/max(total,1)*100:.1f}%)")
    print(f"\n[OK] Resultats sauvegardes dans Supabase table '{TABLE_DEST}'")


if __name__ == "__main__":
    main()