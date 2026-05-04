# EstateMind — Rapport de Validation Technique
## DSO 2.2 : Détection de Fraude Multimodale CLIP

**Projet :** EstateMind — Plateforme d'analyse immobilière intelligente
**Module :** BO2 — Détection de fraude et d'annonces trompeuses
**Pipeline :** DSO 2.2 — Cohérence Sémantique Multimodale (CLIP Zero-Shot)
**Base de données :** Supabase (PostgreSQL cloud)
**Date :** Mai 2026

---

## Table des matières
1. [Analyse des Résultats d'Exécution](#1-analyse-des-résultats-dexécution)
2. [DSO — Data Source Objects](#2-dso--data-source-objects)
3. [DBO — Data Business Objects](#3-dbo--data-business-objects)
4. [Data Sources — Sources de données](#4-data-sources--sources-de-données)
5. [Data Pipeline — Flux de données](#5-data-pipeline--flux-de-données)
6. [Data Modeling — Modélisation & Architecture ML](#6-data-modeling--modélisation--architecture-ml)
7. [Architecture Logique](#7-architecture-logique)
8. [Technologies utilisées](#8-technologies-utilisées)
9. [Index des fichiers](#9-index-des-fichiers)

---

## 1. Analyse des Résultats d'Exécution

### 1.1 Contexte d'exécution

```
Commande : python fraud_detection/run_fraud_detection.py --limit 5000
Source   : Supabase — table listings
Résultats: Supabase — table fraud_detection_results
```

### 1.2 Résultats DSO 2.2 — 3 333 listings analysés

| Métrique | Valeur | Interprétation |
|----------|--------|----------------|
| **Listings chargés** | 3 333 | Listings avec images disponibles sur Supabase |
| **Incohérents** | 1 387 (41.6%) | Score < 0.31 → fraude probable |
| **Suspects** | 1 495 (44.8%) | Score 0.31–0.55 → incohérence modérée |
| **Cohérents** | 451 (13.5%) | Score ≥ 0.56 → annonce fiable |
| **Top flag** | `claimed_parking_not_visible` | Parking promis non visible dans les images |

**Signaux textuels détectés :**

| Signal | Nombre | Taux | Signification |
|--------|--------|------|---------------|
| Piscines promises | 348 | 10.4% | Annonces mentionnant une piscine |
| Vues promises | 149 | 4.5% | Annonces promettant vue mer / panoramique |
| Surpayés | 1 117 | 33.5% | Prix > 50% au-dessus de la médiane régionale |
| Sous-payés | 728 | 21.8% | Prix < 50% en-dessous de la médiane → appât |

**Interprétation globale :**

- **86.4% des annonces sont incohérentes ou suspectes** — le marché tunisien présente une forte proportion d'annonces photographiquement insuffisantes ou mensongères.
- Le **top flag `claimed_parking_not_visible`** révèle que le parking est l'équipement le plus souvent promis mais absent des photos — les vendeurs l'annoncent dans la description sans le montrer.
- **33.5% de surpayement** confirme une tendance structurelle à l'overpricing, indépendante de la qualité photographique.
- **21.8% de sous-payement** (vs 7% précédemment sur 200 listings) — à l'échelle de 3 333 annonces, les prix-appâts sont bien plus répandus que les premières analyses ne le laissaient penser.
- Seulement **13.5% d'annonces cohérentes** — les annonces réellement fiables (images correspondant à la description, prix dans la norme) sont minoritaires.

---

## 2. DSO — Data Source Objects

Le DSO (Data Source Object) représente la couche d'accès aux données. Il définit comment le système se connecte aux sources, extrait les données brutes et les prépare pour le traitement.

### 2.1 `db_connector.py` — Connecteur Supabase

**Rôle :** Couche d'accès unique à Supabase. Ce fichier est le seul point de contact avec la base de données dans tout le pipeline DSO 2.2. Il gère la connexion, la lecture des annonces et l'écriture des résultats.

**Connexion :**
```python
def _connect() -> psycopg2.extensions.connection:
    # Lit les variables d'environnement depuis data/.env
    return psycopg2.connect(
        host     = os.getenv("SUPABASE_DB_HOST"),   # aws-1-eu-central-1.pooler.supabase.com
        port     = os.getenv("SUPABASE_DB_PORT"),   # 5432
        dbname   = os.getenv("SUPABASE_DB_NAME"),   # postgres
        user     = os.getenv("SUPABASE_DB_USER"),   # postgres.amxnojlfczwffvtwutrb
        password = os.getenv("SUPABASE_DB_PASSWORD"),
        sslmode  = "require",                       # Obligatoire pour Supabase
    )
```

**Ce que ce fichier fait concrètement :**

| Méthode | Direction | Description |
|---------|-----------|-------------|
| `fetch_listings_with_images()` | Lecture | Charge les annonces ayant ≥1 image depuis `listings` |
| `get_regional_price_stats()` | Lecture | Calcule médiane, Q25, Q75 par région/type via SQL |
| `save_multimodal_results()` | Écriture | Sauvegarde les scores DSO 2.2 dans `fraud_detection_results` |
| `get_fraud_summary()` | Lecture | Résumé statistique (total, incohérents, suspects, cohérents) |
| `_setup_table()` | Admin | Crée `fraud_detection_results` si elle n'existe pas encore |

**Particularité — Adaptation au schéma Supabase :**
La table `listings` sur Supabase utilise des noms de colonnes différents du schéma local. Le fichier adapte via des alias SQL :
```sql
source_id        AS property_id,   -- identifiant unique de l'annonce
property_type    AS type,           -- type de bien (villa, appartement...)
municipality     AS municipalite,   -- commune
last_updated     AS last_update,    -- date de mise à jour
COALESCE(source_id, id) AS property_id  -- fallback si source_id est NULL
```

**Statistiques régionales (pour le signal prix) :**
```sql
SELECT
    LOWER(region), LOWER(property_type), transaction_type,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY price) AS median,
    PERCENTILE_CONT(0.25) AS q25, PERCENTILE_CONT(0.75) AS q75,
    AVG(price) AS mean, STDDEV(price) AS std
FROM listings
WHERE price > 0 AND region IS NOT NULL AND property_type IS NOT NULL
GROUP BY LOWER(region), LOWER(property_type), transaction_type
HAVING COUNT(*) >= 5
```
→ Produit 28+ groupes régionaux utilisés pour détecter les prix anormaux.

---

### 2.2 `run_fraud_detection.py` — Orchestrateur CLI

**Rôle :** Point d'entrée unique du pipeline. Ce fichier coordonne toutes les étapes dans l'ordre correct et expose une interface en ligne de commande.

```python
# Usage
python fraud_detection/run_fraud_detection.py --limit 5000
python fraud_detection/run_fraud_detection.py --limit 500 --region Tunis
python fraud_detection/run_fraud_detection.py --no-save   # test sans écriture
```

**Ce que ce fichier fait concrètement :**
1. Parse les arguments CLI (`--limit`, `--region`, `--no-save`)
2. Instancie `FraudDBConnector` → connexion Supabase
3. Appelle `fetch_listings_with_images()` → listings source
4. Appelle `get_regional_price_stats()` → référence prix
5. Appelle `run_multimodal_pipeline()` → exécute les 3 étapes DSO 2.2
6. Appelle `save_multimodal_results()` → écrit sur Supabase
7. Appelle `get_fraud_summary()` → affiche le résumé final

---

## 3. DBO — Data Business Objects

Le DBO (Data Business Object) représente l'information métier construite à partir des données brutes. C'est le résultat transformé, enrichi, et interprétable par un analyste ou une application.

### 3.1 Objet Métier : MultimodalScore

C'est le DBO central de DSO 2.2. Il représente l'évaluation de cohérence d'une annonce immobilière sur trois dimensions : visuelle, sémantique et tarifaire.

```
MultimodalScore {
    property_id           : str          — Identifiant unique de l'annonce
    source_name           : str          — Source (tayara, mubawab, user_submission...)
    multimodal_score      : float [0–1]  — Score de cohérence global (DBO principal)
    image_text_similarity : float [0–1]  — Score A : correspondance catégories image/texte
    price_deviation_pct   : float        — Écart prix vs médiane régionale en %
    mismatch_types        : List[str]    — Flags d'incohérence détectés
    images_analyzed       : int          — Nombre d'images traitées
    model_version         : str          — "clip_zeroshot_semantic_v1"
    analyzed_at           : timestamp    — Date d'analyse
}
```

**Interprétation du score :**

| Score | Niveau | Signification métier |
|-------|--------|----------------------|
| 0.00 – 0.30 | **INCOHERENT** | Fraude probable — images hors sujet ou features manquantes |
| 0.31 – 0.55 | **SUSPECT** | Incohérence modérée — annonce douteuse |
| 0.56 – 0.75 | **ACCEPTABLE** | Cohérence partielle — quelques écarts |
| 0.76 – 1.00 | **COHERENT** | Annonce fiable — images et description concordantes |

**Flags métier générés :**

| Flag | Déclencheur | Signification |
|------|-------------|---------------|
| `no_real_estate_images` | ≥ 60% images classées "other" | Images hors contexte (personnes, docs, logos) |
| `no_images_suspicious_price` | 0 images + prix > 200k | Annonce chère sans preuve visuelle |
| `wrong_property_type` | Type "terrain" mais images d'intérieurs | Description contradictoire avec les photos |
| `claimed_pool_not_visible` | "piscine" promis mais absent des photos | Feature non prouvée par l'image |
| `claimed_parking_not_visible` | "parking/garage" promis mais absent | Feature non prouvée (top flag actuel) |
| `claimed_view_not_visible` | "vue mer" promis mais absent | Feature non prouvée |
| `overpriced_vs_images` | Prix > +100% médiane régionale | Surpayement excessif |
| `underpriced_trap` | Prix < -50% médiane | Prix anormalement bas → appât |

### 3.2 Schéma de la table `fraud_detection_results` (Supabase)

```sql
CREATE TABLE fraud_detection_results (
    id                       SERIAL PRIMARY KEY,
    property_id              TEXT NOT NULL,
    source_name              TEXT NOT NULL,
    multimodal_score         FLOAT,         -- [0-1] : score de cohérence global
    image_text_similarity    FLOAT,         -- [0-1] : score A (category match)
    price_deviation_pct      FLOAT,         -- % d'écart vs médiane régionale
    mismatch_types           JSONB,         -- liste des flags déclenchés
    images_analyzed          INTEGER,       -- nombre d'images traitées
    analyzed_at              TIMESTAMP DEFAULT NOW(),
    multimodal_model_version TEXT DEFAULT 'clip_vit_base_patch32_v1',
    UNIQUE (source_name, property_id)       -- idempotence : ON CONFLICT DO UPDATE
);
```

---

## 4. Data Sources — Sources de données

### 4.1 Source principale : Table `listings` (Supabase)

**Nature :** Mixte — structurée + semi-structurée

| Champ | Type Supabase | Nature | Utilisation DSO 2.2 |
|-------|--------------|--------|---------------------|
| `source_id` | TEXT | Structurée | Identifiant de l'annonce |
| `price` | NUMERIC | Structurée | Signal prix — déviation régionale |
| `property_type` | TEXT | Structurée | Catégories visuelles attendues |
| `region`, `city` | TEXT | Structurée | Lookup stats régionales |
| `transaction_type` | TEXT | Structurée | Lookup stats régionales |
| `description` | TEXT | Semi-structurée | Extraction mots-clés (piscine, vue...) |
| `images` | JSONB | Semi-structurée | URLs des photos à télécharger |
| `features` | JSONB | Semi-structurée | Équipements déclarés (garage, terrasse...) |
| `surface` | NUMERIC | Structurée | Prix au m² pour stats |

**Volume traité :** 3 333 listings avec images (sur ~5 000 total)

### 4.2 Source secondaire : Images immobilières (URLs HTTP)

**Nature :** Non structurée

- Stockées en JSONB dans `listings.images` → liste d'URLs HTTP
- Téléchargées à la volée par `image_encoder.py`
- Format : JPEG/PNG/WebP, taille max 5 MB
- Redimensionnées à 224×224 par le processeur CLIP
- **Cache local** : `data/cache/image_embeddings/` — chaque classification est sauvegardée en `.npy` (hash SHA-256 de l'identifiant listing) pour éviter les re-téléchargements

### 4.3 Source calculée : Statistiques régionales

**Nature :** Calculée dynamiquement depuis `listings`

- Requête SQL avec `PERCENTILE_CONT` par groupe `(region, property_type, transaction_type)`
- 28+ groupes régionaux actifs dans le corpus
- Clé de lookup : `"tunis|villa|Sale"` → `{median: 450000, q25: ..., q75: ...}`
- Utilisée pour calculer la déviation de prix de chaque annonce

---

## 5. Data Pipeline — Flux de données

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DSO 2.2 — PIPELINE COMPLET                               │
│              run_fraud_detection.py (orchestrateur CLI)                     │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────┐
│   SUPABASE           │
│   table: listings    │
│   3 333 annonces     │
│   avec images JSONB  │
└──────────┬───────────┘
           │  db_connector.py
           │  fetch_listings_with_images()
           │  get_regional_price_stats()
           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  ÉTAPE 1 — image_encoder.py                                                 │
│  Classification Zero-Shot CLIP                                              │
│                                                                             │
│  Pour chaque listing (max 3 images) :                                       │
│  ① Vérifier cache disque (cache/image_embeddings/cls_<hash>.npy)           │
│  ② Si cache miss → télécharger image HTTP → PIL.Image RGB                  │
│  ③ CLIPClassifier.encode_images() → embedding visuel (512D)                │
│  ④ Cosine similarity image↔13 prompts de catégories                        │
│  ⑤ Softmax(sim × 100) → probabilités par catégorie                        │
│  ⑥ argmax → catégorie dominante + confiance                               │
│  ⑦ Sauvegarder en cache (.npy)                                             │
│                                                                             │
│  Sortie par listing : detected_categories, category_confidences,            │
│                       images_analyzed, has_images                           │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  ÉTAPE 2 — text_price_signal.py                                             │
│  Extraction Catégories Attendues + Signal Prix                              │
│                                                                             │
│  Pour chaque listing :                                                      │
│  A. extract_expected_categories() :                                         │
│     • Catégories de base selon property_type                                │
│       ex: "villa" → {exterior, living_room, bedroom}                       │
│     • + mots-clés dans description                                          │
│       ex: "piscine" → +pool | "jardin" → +garden | "vue mer" → +view       │
│     • + mots-clés dans features JSONB                                       │
│       ex: "garage" → +parking | "terrasse" → +terrace                      │
│                                                                             │
│  B. compute_price_deviation() :                                             │
│     • Lookup : regional_stats["region|type|tx"]["median"]                  │
│     • deviation = (price - median) / median × 100                          │
│     • Fallback : médiane des médianes de la région si groupe absent        │
│                                                                             │
│  Sortie : expected_categories, price_deviation_pct, deviation_level,        │
│           price_signal (overpriced/underpriced/normal)                      │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  ÉTAPE 3 — consistency_classifier.py                                        │
│  Calcul du Score de Cohérence Sémantique                                    │
│                                                                             │
│  Score A — Category Match (poids 60%) :                                     │
│    match = |detected ∩ expected| / |expected|                              │
│    Pénalités :                                                              │
│    • ≥ 60% images "other"  → score = 0.15 + flag no_real_estate_images     │
│    • Type "terrain" + images d'intérieurs → score = 0.20 + wrong_type      │
│    • 0 images + prix > 200k → score = 0.20 + no_images_suspicious_price    │
│    Flags pour chaque feature promise mais absente :                         │
│    → claimed_pool_not_visible, claimed_parking_not_visible...               │
│                                                                             │
│  Score B — Price Score (poids 40%) :                                        │
│    deviation > +100% → 0.20 + overpriced_vs_images                         │
│    deviation +50% à +100% → 0.45                                           │
│    deviation < -50% → 0.30 + underpriced_trap                              │
│    |deviation| > 30% → 0.55                                                │
│    normal → 0.85                                                            │
│                                                                             │
│  multimodal_score = 0.60 × ScoreA + 0.40 × ScoreB  ∈ [0, 1]              │
│                                                                             │
│  Sortie : MultimodalScore (property_id, multimodal_score, mismatch_types...) │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │  db_connector.py
                                   │  save_multimodal_results()
                                   │  ON CONFLICT DO UPDATE (idempotent)
                                   ▼
                        ┌──────────────────────┐
                        │   SUPABASE           │
                        │   fraud_detection    │
                        │   _results           │
                        │   3 333 lignes       │
                        └──────────────────────┘
```

---

## 6. Data Modeling — Modélisation & Architecture ML

### 6.1 Modèle CLIP Zero-Shot — `image_encoder.py`

**Modèle :** `openai/clip-vit-base-patch32` (HuggingFace)

CLIP (Contrastive Language–Image Pre-training) est un modèle pré-entraîné par OpenAI sur **400 millions de paires (image, texte)** extraites d'internet. Son principe : entraîner simultanément un encodeur visuel et un encodeur textuel pour que les représentations d'une image et de sa description soient proches dans un espace vectoriel commun (512 dimensions).

**Architecture interne :**

```
Image 224×224
      │
      ▼
  ViT-B/32 (Vision Transformer)
  • 12 couches Transformer
  • 12 têtes d'attention multi-tête
  • Patch size : 32×32 → 49 patches par image
  • Dimension cachée : 768D
  • Projection finale : 512D (normalisé L2)
      │
      ▼
  Embedding image (512D) ──────────────────┐
                                           │
"a photo of a swimming pool"               │ Cosine similarity
      │                                    │ (= produit scalaire
      ▼                                    │  car vecteurs normalisés)
  CLIP Text Encoder                        │
  • Transformer causal BPE                 │
  • Vocabulaire ~50k tokens                │
  • Max 77 tokens                          │
  • Projection : 512D (normalisé L2)       │
      │                                    │
      ▼                                    │
  Embedding texte (512D) ─────────────────┘
                                           │
                                           ▼
                              similarities (13 valeurs)
                              softmax(sim × 100) → probabilités
                              argmax → "pool" (87% confiance)
```

**Pourquoi `softmax(sim × 100)` ?**
Les similarités cosinus sont des valeurs dans [-1, 1], très proches entre elles (ex: 0.23 vs 0.19). Sans amplification, le softmax produirait une distribution quasi-uniforme. Le facteur ×100 "étire" les différences et produit une classification décisive.

**13 catégories sémantiques immobilières :**

| ID | Prompt CLIP (anglais) | Raison du choix |
|----|----------------------|-----------------|
| `bedroom` | "a photo of a bedroom with a bed and furniture" | Pièce principale habitation |
| `living_room` | "a photo of a living room or lounge area" | Pièce la plus photographiée |
| `kitchen` | "a photo of a kitchen with appliances or cabinets" | Équipement valorisant |
| `bathroom` | "a photo of a bathroom or toilet" | Rénovation fréquente |
| `exterior` | "a photo of the exterior facade of a house or building" | Vue d'ensemble du bien |
| `pool` | "a photo of a swimming pool" | Feature premium souvent mensongère |
| `terrace` | "a photo of a terrace balcony or outdoor space" | Espace extérieur |
| `garden` | "a photo of a garden or yard with trees and plants" | Terrain/verdure |
| `parking` | "a photo of a garage or parking space" | Feature souvent manquante |
| `view` | "a photo of a panoramic view or sea view from a window" | Vue vendue mais rare |
| `land` | "a photo of an empty land or plot of land" | Terrain nu |
| `building` | "a photo of an apartment building or residential complex" | Immeuble entier |
| `other` | "a photo not related to real estate such as a person or document" | Détecteur de spam |

> **Pourquoi les prompts en anglais ?** CLIP a été entraîné majoritairement sur des données anglaises. Un prompt anglais descriptif ("a photo of a swimming pool") obtient de meilleures performances qu'un équivalent français.

**Cache disque :**
Chaque résultat de classification est sauvegardé en `.npy` (indice de catégorie, hash SHA-256 de l'identifiant listing). Lors d'une 2ème exécution, les 3 333 listings déjà traités sont récupérés du cache en quelques secondes.

---

### 6.2 Extraction des Catégories Attendues — `text_price_signal.py`

Ce fichier répond à la question : **"Quelles images devrait-on voir dans cette annonce ?"**

Il combine deux sources d'information :

**Source 1 — Type de bien :**
```python
_TYPE_BASE_CATEGORIES = {
    "villa":       {"exterior", "living_room", "bedroom"},
    "appartement": {"living_room", "bedroom"},
    "studio":      {"living_room"},
    "terrain":     {"land"},
    "penthouse":   {"living_room", "bedroom", "view"},
    ...
}
```

**Source 2 — Mots-clés dans la description et les features :**
```python
_KEYWORD_CATEGORY_MAP = {
    "piscine":         "pool",
    "vue mer":         "view",
    "jardin":          "garden",
    "garage":          "parking",
    "parking":         "parking",
    "terrasse":        "terrace",
    "balcon":          "terrace",
    "cuisine équipée": "kitchen",
    ...
}
```

**Exemple concret :**
```
listing = {
    type: "villa",
    description: "villa avec piscine et jardin, cuisine équipée",
    features: ["parking", "terrasse"]
}

→ Base type "villa"      : {exterior, living_room, bedroom}
→ "piscine" → +pool      : {exterior, living_room, bedroom, pool}
→ "jardin" → +garden     : {exterior, living_room, bedroom, pool, garden}
→ "cuisine équipée" → +kitchen : {exterior, living_room, bedroom, pool, garden, kitchen}
→ feature "parking" → +parking : {exterior, living_room, bedroom, pool, garden, kitchen, parking}
→ feature "terrasse" → +terrace : {..., terrace}

expected_categories = {exterior, living_room, bedroom, pool, garden, kitchen, parking, terrace}
```

**Calcul de la déviation de prix :**
```python
deviation = (price - regional_median) / regional_median × 100

# Exemples de sortie
+150% → level="high",     signal="overpriced"  → score_B = 0.20
+70%  → level="moderate", signal="overpriced"  → score_B = 0.45
-60%  → level="moderate", signal="underpriced" → score_B = 0.30
+5%   → level="normal",   signal="normal"      → score_B = 0.85
```

---

### 6.3 Score de Cohérence Final — `consistency_classifier.py`

Ce fichier répond à la question : **"Cette annonce est-elle cohérente entre ses images, sa description et son prix ?"**

**Formule finale :**
```
multimodal_score = 0.60 × score_category_match + 0.40 × score_price
```

**Score A — Category Match (60%) :**
```python
found   = expected_categories ∩ detected_categories
missing = expected_categories - detected_categories

match_score = len(found) / len(expected_categories)

# Exemples :
# expected={exterior, pool, garden, bedroom, living_room}
# detected={bedroom, living_room, bathroom}
# → found={bedroom, living_room}, missing={exterior, pool, garden}
# → match_score = 2/5 = 0.40
# → flags : claimed_pool_not_visible, claimed_garden_not_visible (pas claimed_exterior car non "important")
```

Cas spéciaux :
```python
# Images hors contexte (spam)
if ratio "other" >= 60% → score_A = 0.15, flag no_real_estate_images

# Type incohérent
if type="terrain" AND >50% images d'intérieurs → score_A = 0.20, flag wrong_property_type

# Pas d'images
if images=0 AND prix>200k → score_A = 0.20, flag no_images_suspicious_price
if images=0 AND prix<=200k → score_A = 0.40 (neutre)
```

**Score B — Price (40%) :**
```python
deviation > +100% → 0.20   # Sur-évaluation extrême
deviation +50–100% → 0.45  # Sur-évaluation modérée
deviation < -50%  → 0.30   # Prix-appât
|deviation| > 30% → 0.55   # Écart notable
normal            → 0.85   # Prix dans la norme
```

**Justification des poids 60/40 :**
- **60% image-texte** : L'incohérence visuelle est le signal le plus direct et vérifiable d'une annonce trompeuse. Une piscine promise mais absente des photos est une preuve concrète.
- **40% prix** : Le signal de prix est fiable mais dépend de la qualité des statistiques régionales. Certaines régions ont peu d'annonces → médiane moins précise.

#### Métriques d'évaluation observées

| Métrique | Valeur | Signification |
|----------|--------|---------------|
| Avg multimodal score | ~0.36 | Majorité dans la bande SUSPECT |
| Taux incohérents (< 0.31) | 41.6% | Fraude probable — sur 3 333 listings |
| Taux suspects (0.31–0.55) | 44.8% | Zone grise — annonce douteuse |
| Taux cohérents (≥ 0.56) | 13.5% | Annonces réellement fiables |
| Top flag | `claimed_parking_not_visible` | Feature la plus souvent non prouvée |

---

## 7. Architecture Logique

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         ARCHITECTURE LOGIQUE ESTATEMIND                          │
│                          Module BO2 — Détection de Fraude                        │
└──────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────┐
│         COUCHE DONNÉES              │
│                                     │
│  ┌─────────────────────────────┐    │
│  │  SUPABASE (PostgreSQL cloud)│    │
│  │  ┌──────────────────────┐   │    │
│  │  │  table: listings     │   │    │
│  │  │  • 5000+ annonces    │   │    │
│  │  │  • images (JSONB)    │   │    │
│  │  │  • price, region...  │   │    │
│  │  └──────────────────────┘   │    │
│  └─────────────────────────────┘    │
│                                     │
│  Images HTTP (URLs dans JSONB)      │
│  Cache local .npy (SHA-256)         │
└───────────────┬─────────────────────┘
                │
                │ db_connector.py
                │ fetch_listings_with_images()
                │ get_regional_price_stats()
                ▼
┌─────────────────────────────────────┐
│         COUCHE TRAITEMENT           │
│                                     │
│  ┌─────────────────────────────┐    │
│  │  image_encoder.py           │    │
│  │  CLIP Zero-Shot             │    │
│  │  → detected_categories      │    │
│  └─────────────┬───────────────┘    │
│                │                    │
│  ┌─────────────▼───────────────┐    │
│  │  text_price_signal.py       │    │
│  │  → expected_categories      │    │
│  │  → price_deviation_pct      │    │
│  └─────────────┬───────────────┘    │
│                │                    │
│  ┌─────────────▼───────────────┐    │
│  │  consistency_classifier.py  │    │
│  │  → multimodal_score [0-1]   │    │
│  │  → mismatch_types (flags)   │    │
│  └─────────────┬───────────────┘    │
└───────────────┬─────────────────────┘
                │
                │ db_connector.py
                │ save_multimodal_results()
                ▼
┌─────────────────────────────────────┐
│         COUCHE STOCKAGE             │
│                                     │
│  ┌─────────────────────────────┐    │
│  │  SUPABASE                   │    │
│  │  table: fraud_detection     │    │
│  │  _results                   │    │
│  │  • multimodal_score         │    │
│  │  • mismatch_types (JSONB)   │    │
│  │  • price_deviation_pct      │    │
│  └─────────────────────────────┘    │
└───────────────┬─────────────────────┘
                │
                ▼
┌─────────────────────────────────────┐
│         COUCHE EXPLOITATION         │
│                                     │
│  Dashboard frontend                 │
│  ← API REST Supabase (auto-générée) │
│  ← backend/dashboard/views.py       │
└─────────────────────────────────────┘
```

---

## 8. Technologies utilisées

| Couche | Technologie | Version | Justification |
|--------|-------------|---------|---------------|
| **Base de données** | Supabase (PostgreSQL) | 15+ cloud | PostgreSQL managé + API REST auto-générée pour le dashboard |
| **Connecteur DB** | psycopg2 | 2.9+ | Pilote PostgreSQL natif Python, supporte SSL (requis Supabase) |
| **Vision ML** | openai/clip-vit-base-patch32 | HuggingFace | Zero-shot sans données labellisées, généralisation robuste |
| **Deep Learning** | PyTorch | 2.0+ | Backend HuggingFace, support CUDA/CPU |
| **Transformers** | HuggingFace Transformers | 4.30+ | Chargement standardisé de CLIP |
| **Images** | Pillow (PIL) | 10+ | Décodage et conversion RGB des images téléchargées |
| **HTTP** | requests | 2.31+ | Téléchargement images avec retry, timeout, streaming |
| **Calcul numérique** | NumPy | 1.24+ | Cosine similarity, softmax, cache `.npy` |
| **Variables env** | python-dotenv | 1.0+ | Chargement sécurisé des credentials depuis `.env` |
| **Logging** | loguru | 0.7+ | Logs colorés avec timestamps, rotation fichier |
| **Langage** | Python | 3.10+ | Écosystème ML/NLP de référence |

**Justifications des choix clés :**

> **Pourquoi CLIP Zero-Shot au lieu d'un modèle entraîné ?**
> Aucun dataset labellisé d'images immobilières n'est disponible pour la Tunisie. CLIP permet une classification en 13 catégories sémantiques sans fine-tuning, en utilisant uniquement des prompts textuels descriptifs. La taxonomie peut être étendue ou modifiée sans ré-entraînement.

> **Pourquoi Supabase et non PostgreSQL local ?**
> Supabase offre une API REST auto-générée que le dashboard frontend peut consommer directement, sans couche backend supplémentaire. Les résultats de fraude sont immédiatement accessibles via l'interface Supabase pour monitoring en temps réel.

> **Pourquoi un score hybride 60% image + 40% prix ?**
> L'incohérence visuelle est le signal le plus objectif et vérifiable. Le prix est un signal complémentaire mais dépend de la densité des données régionales — certaines zones ont peu d'annonces, rendant la médiane moins représentative.

---

## 9. Index des fichiers

| Fichier | Rôle | Inputs | Outputs |
|---------|------|--------|---------|
| [run_fraud_detection.py](../data/fraud_detection/run_fraud_detection.py) | Orchestrateur CLI | Args `--limit`, `--region` | Logs + appels pipeline |
| [db_connector.py](../data/fraud_detection/db_connector.py) | Couche Supabase | `.env` credentials | Listings, stats, save résultats |
| [multimodal/image_encoder.py](../data/fraud_detection/multimodal/image_encoder.py) | CLIP Zero-Shot | Listings + URLs images | `detected_categories` par listing |
| [multimodal/text_price_signal.py](../data/fraud_detection/multimodal/text_price_signal.py) | Analyse description + prix | Listings + stats régionales | `expected_categories` + `price_deviation_pct` |
| [multimodal/consistency_classifier.py](../data/fraud_detection/multimodal/consistency_classifier.py) | Score final | Listings enrichis | `multimodal_score` + `mismatch_types` |
| [multimodal/\_\_init\_\_.py](../data/fraud_detection/multimodal/__init__.py) | Export module | — | `run_multimodal_pipeline`, `interpret_score` |

---

*Rapport généré depuis l'analyse du code source EstateMind — DSO 2.2 uniquement, Supabase*
