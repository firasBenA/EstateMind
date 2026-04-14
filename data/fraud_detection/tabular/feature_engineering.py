"""
EstateMind — DSO 2.1 : Feature Engineering Tabulaire
======================================================
Transforme les listings PostgreSQL en matrice de features numériques
prête pour Isolation Forest.

Features extraites (11 features numériques) :
───────────────────────────────────────────────
Brutes :
  1.  price                 — prix de l'annonce (TND)
  2.  surface               — surface en m²
  3.  rooms                 — nombre de pièces
  4.  image_count           — nombre de photos

Dérivées :
  5.  price_per_m2          — prix / surface (indicateur de valorisation)
  6.  rooms_per_m2          — rooms / surface (densité de pièces)
  7.  description_length    — longueur de la description (qualité rédactionnelle)
  8.  title_length          — longueur du titre
  9.  feature_count         — nb d'équipements déclarés (piscine, jardin…)
  10. poi_count             — nb de POI à proximité

Contexte régional :
  11. price_m2_zscore       — z-score du prix/m² dans région+type+transaction
      (mesure l'écart par rapport à la norme régionale)

Stratégie de gestion des valeurs manquantes :
  - Valeurs nulles imputées par la médiane régionale (par groupe region/type/tx)
  - Si médiane régionale indisponible → médiane globale
  - Colonnes price_per_m2 et rooms_per_m2 → NaN si surface=0 → imputation médiane
"""
from __future__ import annotations

import math
from typing import List, Dict, Any, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger

# ── Table de correspondance ville → gouvernorat ───────────────────────────────
# Couvre les principales villes tunisiennes pour résoudre les listings "unknown"

CITY_TO_REGION: Dict[str, str] = {
    # Tunis
    "tunis": "tunis", "la marsa": "tunis", "marsa": "tunis",
    "carthage": "tunis", "sidi bou said": "tunis", "bardo": "tunis",
    "le bardo": "tunis", "el menzah": "tunis", "el manar": "tunis",
    "el omrane": "tunis", "ettahrir": "tunis", "cite olympique": "tunis",
    "montplaisir": "tunis", "mutuelleville": "tunis", "lac": "tunis",
    "les berges du lac": "tunis", "lac 2": "tunis", "ain zaghouan": "tunis",
    "ezzouhour": "tunis", "sijoumi": "tunis", "cite ennasr": "tunis",
    "ennasr": "tunis", "el mourouj": "tunis",
    # Ariana
    "ariana": "ariana", "raoued": "ariana", "sokra": "ariana",
    "ettadhamen": "ariana", "mnihla": "ariana", "ghazela": "ariana",
    "cite el ghazela": "ariana", "kalaat landlous": "ariana",
    "sidi thabet": "ariana", "borj louzir": "ariana",
    # Ben Arous
    "ben arous": "ben arous", "rades": "ben arous", "hammam lif": "ben arous",
    "megrine": "ben arous", "bou mhel": "ben arous", "ezzahra": "ben arous",
    "hammam chott": "ben arous", "mohamedia": "ben arous",
    "fouchana": "ben arous", "mornag": "ben arous", "nouvelle medina": "ben arous",
    # Manouba
    "manouba": "manouba", "oued ellil": "manouba", "tebourba": "manouba",
    "djedeida": "manouba", "mornaguia": "manouba", "borj el amri": "manouba",
    # Nabeul / Cap Bon
    "nabeul": "nabeul", "hammamet": "nabeul", "kelibia": "nabeul",
    "korba": "nabeul", "dar chaabane": "nabeul", "beni khiar": "nabeul",
    "menzel temime": "nabeul", "grombalia": "nabeul", "soliman": "nabeul",
    "takelsa": "nabeul", "cap bon": "nabeul", "el haouaria": "nabeul",
    "korbous": "nabeul",
    # Bizerte
    "bizerte": "bizerte", "menzel bourguiba": "bizerte", "mateur": "bizerte",
    "ras jebel": "bizerte", "menzel jemil": "bizerte", "zarzouna": "bizerte",
    # Béja
    "beja": "beja", "testour": "beja", "medjez el bab": "beja",
    # Jendouba
    "jendouba": "jendouba", "tabarka": "jendouba", "ain draham": "jendouba",
    # Kef
    "kef": "kef", "le kef": "kef", "sakiet sidi youssef": "kef",
    # Siliana
    "siliana": "siliana", "makthar": "siliana",
    # Sousse
    "sousse": "sousse", "hammam sousse": "sousse", "kantaoui": "sousse",
    "sahloul": "sousse", "khezama": "sousse", "akouda": "sousse",
    "kalaa kebira": "sousse", "msaken": "sousse", "port el kantaoui": "sousse",
    "kondar": "sousse",
    # Monastir
    "monastir": "monastir", "skanes": "monastir", "ksar hellal": "monastir",
    "ksibet el mediouni": "monastir", "jemmal": "monastir",
    "moknine": "monastir", "teboulba": "monastir",
    # Mahdia
    "mahdia": "mahdia", "el jem": "mahdia", "ksour essef": "mahdia",
    "chebba": "mahdia",
    # Sfax
    "sfax": "sfax", "sakiet ezzit": "sfax", "sakiet eddaier": "sfax",
    "agareb": "sfax", "el ain": "sfax", "thyna": "sfax",
    "la skira": "sfax",
    # Kairouan
    "kairouan": "kairouan", "haffouz": "kairouan", "sbikha": "kairouan",
    # Kasserine
    "kasserine": "kasserine", "sbeitla": "kasserine",
    # Sidi Bouzid
    "sidi bouzid": "sidi bouzid", "meknassy": "sidi bouzid",
    # Gafsa
    "gafsa": "gafsa", "metlaoui": "gafsa", "redeyef": "gafsa",
    # Tozeur
    "tozeur": "tozeur", "nefta": "tozeur",
    # Kebili
    "kebili": "kebili", "douz": "kebili",
    # Gabes
    "gabes": "gabes", "mareth": "gabes", "matmata": "gabes",
    # Medenine / Djerba
    "medenine": "medenine", "djerba": "medenine", "houmt souk": "medenine",
    "midoun": "medenine", "aghir": "medenine", "zarzis": "medenine",
    "ben gardane": "medenine",
    # Tataouine
    "tataouine": "tataouine", "remada": "tataouine",
}


def _infer_region(region_raw: Optional[str], city_raw: Optional[str]) -> str:
    """
    Résout la région d'un listing :
    1. Utilise region_raw si non vide
    2. Sinon tente une correspondance city → région via CITY_TO_REGION
    3. Retourne 'unknown' en dernier recours
    """
    if region_raw:
        clean = region_raw.strip().lower()
        if clean and clean != "unknown":
            return clean

    if city_raw:
        city_lower = city_raw.strip().lower()
        # Correspondance directe
        if city_lower in CITY_TO_REGION:
            return CITY_TO_REGION[city_lower]
        # Correspondance partielle (ex : "Hammamet Nord" → "nabeul")
        for key, region in CITY_TO_REGION.items():
            if key in city_lower or city_lower in key:
                return region

    return "unknown"


# ── Définition des features ───────────────────────────────────────────────────

NUMERIC_FEATURES = [
    "price",
    "surface",
    "rooms",
    "image_count",
    "price_per_m2",
    "rooms_per_m2",
    "description_length",
    "title_length",
    "feature_count",
    "poi_count",
    "price_m2_zscore",
]

# Features utilisées pour la stratification régionale
GROUPBY_KEYS = ["region_norm", "type_norm", "transaction_type"]


# ── Extraction brute depuis un listing dict ───────────────────────────────────

def _safe_float(val: Any, default: float = 0.0) -> float:
    """Convertit en float, retourne default si impossible."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _extract_raw_features(listing: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extrait les features brutes d'un listing.
    Retourne un dict avec toutes les colonnes + features dérivées.
    """
    price   = _safe_float(listing.get("price"))
    surface = _safe_float(listing.get("surface"))
    rooms   = _safe_float(listing.get("rooms"))

    # Clé de stratification — région inférée depuis city si manquante
    region_norm = _infer_region(listing.get("region"), listing.get("city"))
    type_norm   = (listing.get("type") or "other").lower().strip()
    tx_type     = listing.get("transaction_type") or "Sale"

    # Features dérivées
    price_per_m2 = (price / surface) if surface > 0 else None
    rooms_per_m2 = (rooms / surface) if surface > 0 else None

    # Longueur description / titre
    desc  = str(listing.get("description") or "")
    title = str(listing.get("title")       or "")
    description_length = len(desc.strip())
    title_length       = len(title.strip())

    # Nb équipements et POI
    features_list = listing.get("features") or []
    poi_list      = listing.get("poi")      or []
    if isinstance(features_list, str):
        import json
        try:
            features_list = json.loads(features_list)
        except Exception:
            features_list = []
    if isinstance(poi_list, str):
        import json
        try:
            poi_list = json.loads(poi_list)
        except Exception:
            poi_list = []

    feature_count = len(features_list) if isinstance(features_list, list) else 0
    poi_count     = len(poi_list)      if isinstance(poi_list, list)      else 0

    # Gestion image_count
    images = listing.get("images") or []
    if isinstance(images, str):
        try:
            import json
            images = json.loads(images)
        except Exception:
            images = []
    image_count = len(images) if isinstance(images, list) else _safe_float(listing.get("image_count"))

    return {
        # Identifiants (non utilisés comme features)
        "property_id":    listing.get("property_id"),
        "source_name":    listing.get("source_name"),

        # Clés de stratification
        "region_norm":    region_norm,
        "type_norm":      type_norm,
        "transaction_type": tx_type,

        # Features numériques brutes
        "price":              price   if price   > 0 else None,
        "surface":            surface if surface > 0 else None,
        "rooms":              rooms   if rooms   > 0 else None,
        "image_count":        image_count,

        # Features dérivées
        "price_per_m2":       price_per_m2,
        "rooms_per_m2":       rooms_per_m2,
        "description_length": description_length,
        "title_length":       title_length,
        "feature_count":      feature_count,
        "poi_count":          poi_count,

        # Z-score calculé plus tard
        "price_m2_zscore":    None,
    }


# ── Calcul du z-score régional ────────────────────────────────────────────────

def _compute_regional_zscores(df: pd.DataFrame) -> pd.Series:
    """
    Calcule le z-score du prix/m² pour chaque listing par groupe
    (region_norm × type_norm × transaction_type).

    z = (x - μ_région) / σ_région

    Pour les groupes avec σ = 0 ou < 5 enregistrements, retourne 0.0.
    """
    zscores = pd.Series(0.0, index=df.index)

    for group_keys, group_df in df.groupby(GROUPBY_KEYS, dropna=False):
        idx = group_df.index
        vals = group_df["price_per_m2"].dropna()

        if len(vals) < 5:
            zscores[idx] = 0.0
            continue

        mean = vals.mean()
        std  = vals.std(ddof=1)

        if std < 1e-9:
            zscores[idx] = 0.0
            continue

        # z-score pour chaque ligne du groupe
        for i in idx:
            pm2 = df.at[i, "price_per_m2"]
            zscores[i] = (pm2 - mean) / std if pd.notna(pm2) else 0.0

    return zscores


# ── Imputation des valeurs manquantes ─────────────────────────────────────────

def _impute(df: pd.DataFrame) -> pd.DataFrame:
    """
    Imputation des NaN par médiane régionale, puis médiane globale.
    Ne modifie que les colonnes NUMERIC_FEATURES.
    """
    df = df.copy()

    for col in NUMERIC_FEATURES:
        if col not in df.columns:
            df[col] = 0.0
            continue

        # Médiane par groupe regional
        group_medians = (
            df.groupby(GROUPBY_KEYS, dropna=False)[col]
            .transform("median")
        )
        # Médiane globale comme fallback
        global_median = df[col].median()
        if pd.isna(global_median):
            global_median = 0.0

        df[col] = df[col].fillna(group_medians).fillna(global_median)

    return df


# ── Pipeline principal ────────────────────────────────────────────────────────

def build_feature_matrix(
    listings: List[Dict[str, Any]],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Construit la matrice de features à partir d'une liste de listings.

    Args:
        listings: Liste de dicts tels que retournés par FraudDBConnector.fetch_listings()

    Returns:
        Tuple (meta_df, feature_df) où :
          - meta_df    : colonnes d'identification (property_id, source_name, region_norm, …)
          - feature_df : matrice numpy-ready, colonnes = NUMERIC_FEATURES, index aligné
    """
    if not listings:
        logger.warning("[FeatEng] Aucun listing fourni")
        return pd.DataFrame(), pd.DataFrame()

    logger.info(f"[FeatEng] Extraction features pour {len(listings)} listings")

    # 1. Extraction brute
    rows = [_extract_raw_features(lst) for lst in listings]
    df = pd.DataFrame(rows)

    # 2. Z-score régional du prix/m²
    df["price_m2_zscore"] = _compute_regional_zscores(df)

    # 3. Imputation
    df = _impute(df)

    # 4. Clip les z-scores extrêmes (évite que Isolation Forest soit dominé par outliers extrêmes)
    df["price_m2_zscore"] = df["price_m2_zscore"].clip(-10, 10)

    # 5. Séparer méta vs features
    meta_cols = ["property_id", "source_name", "region_norm", "type_norm", "transaction_type"]
    meta_df    = df[meta_cols].reset_index(drop=True)
    feature_df = df[NUMERIC_FEATURES].astype(float).reset_index(drop=True)

    logger.info(
        f"[FeatEng] Matrice construite: {feature_df.shape[0]} lignes × {feature_df.shape[1]} features"
    )
    _log_feature_stats(feature_df)

    return meta_df, feature_df


# ── Logging de qualité des features ──────────────────────────────────────────

def _log_feature_stats(df: pd.DataFrame) -> None:
    """Log les statistiques basiques de chaque feature pour vérification."""
    logger.debug("[FeatEng] Statistiques des features :")
    for col in df.columns:
        series = df[col]
        null_pct = series.isna().mean() * 100
        logger.debug(
            f"  {col:25s} | "
            f"mean={series.mean():10.2f} | "
            f"std={series.std():9.2f} | "
            f"min={series.min():10.2f} | "
            f"max={series.max():10.2f} | "
            f"null={null_pct:.1f}%"
        )


# ── Utilitaire : split par région pour entraînement par région ───────────────

def split_by_region(
    meta_df: pd.DataFrame,
    feature_df: pd.DataFrame,
    min_samples: int = 30,
) -> Dict[str, Tuple[pd.DataFrame, pd.DataFrame]]:
    """
    Divise la matrice de features par région.
    Les régions avec < min_samples enregistrements sont agrégées dans un groupe
    'sparse' qui sera traité avec un modèle global.

    Returns:
        dict {region_name: (meta_subset, feature_subset)}
    """
    splits: Dict[str, Tuple[pd.DataFrame, pd.DataFrame]] = {}
    region_counts = meta_df["region_norm"].value_counts()

    for region, count in region_counts.items():
        mask = meta_df["region_norm"] == region
        if count >= min_samples:
            splits[region] = (
                meta_df[mask].reset_index(drop=True),
                feature_df[mask].reset_index(drop=True),
            )
        else:
            # Regrouper dans 'sparse'
            if "sparse" not in splits:
                splits["sparse"] = (
                    meta_df[mask].reset_index(drop=True),
                    feature_df[mask].reset_index(drop=True),
                )
            else:
                sparse_meta, sparse_feat = splits["sparse"]
                splits["sparse"] = (
                    pd.concat([sparse_meta, meta_df[mask]]).reset_index(drop=True),
                    pd.concat([sparse_feat, feature_df[mask]]).reset_index(drop=True),
                )

    logger.info(
        f"[FeatEng] Régions : {len(splits)} groupes — "
        f"{[f'{k}({v[0].shape[0]})' for k, v in splits.items()]}"
    )
    return splits
