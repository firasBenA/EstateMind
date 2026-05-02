"""
Estate Mind — Phase 0.5b : Enrichissement exemples negatif
===========================================================
Ce script cible les annonces les plus susceptibles d'être frauduleuses
pour trouver 3-4 exemples negatif solides pour le prompt few-shot.

Critères de ciblage :
  1. nb_alert_emojis >= 1
  2. Plusieurs numéros WhatsApp dans le texte
  3. Mots-clés suspects (étranger, occasion, urgent, garantie...)
  4. Prix anormalement bas détecté dans le texte

Usage :
    python phase05b_enrichissement.py
"""

import re
import pandas as pd

INPUT_CSV  = "./phase0_output/dataset_clean.csv"
OUTPUT_CSV = "./phase0_output/sample_20_negatif_candidates.csv"

# ─────────────────────────────────────────────
# Critères de détection des annonces suspectes
# ─────────────────────────────────────────────

# Mots-clés qui apparaissent souvent dans les annonces frauduleuses
KEYWORDS_SUSPECT = [
    "étranger", "etranger", "expatri",
    "urgent", "urgence",
    "occasion à ne pas rater", "occasion a ne pas rater",
    "ne pas rater",
    "pour les sérieux", "pour les serieux",
    "garantie", "garanti",
    "investissement rentable", "rendement garanti",
    "prix sacrifié", "prix sacrifice",
    "départ définitif", "depart definitif",
    "départ urgent", "depart urgent",
    "cause décès", "cause deces",
    "cause divorce",
    "frais de visite",
    "avance sur loyer",
    "virement bancaire",
    "western union", "money gram",
]

# Pattern pour détecter plusieurs numéros WhatsApp
WHATSAPP_PATTERN = re.compile(
    r'whatsapp', re.IGNORECASE
)

# Pattern pour détecter les numéros de téléphone
PHONE_PATTERN = re.compile(
    r'(?:\+?216\s?)?(?:\d{2}\s?){4}', re.IGNORECASE
)


def count_whatsapp_mentions(text: str) -> int:
    if not isinstance(text, str):
        return 0
    return len(WHATSAPP_PATTERN.findall(text))


def count_phone_numbers(text: str) -> int:
    if not isinstance(text, str):
        return 0
    return len(PHONE_PATTERN.findall(text))


def count_suspect_keywords(text: str) -> int:
    if not isinstance(text, str):
        return 0
    text_lower = text.lower()
    return sum(1 for kw in KEYWORDS_SUSPECT if kw in text_lower)


def compute_suspicion_score(row) -> float:
    """
    Score de suspicion composite entre 0 et 1.
    Plus le score est élevé, plus l'annonce est suspecte.
    """
    score = 0.0
    text = row.get("description_clean", "")

    # Emojis d'alerte (poids fort)
    nb_alert = row.get("nb_alert_emojis", 0)
    if nb_alert >= 3:
        score += 0.35
    elif nb_alert >= 1:
        score += 0.20

    # Emojis total (poids moyen)
    nb_emojis = row.get("nb_emojis", 0)
    if nb_emojis >= 10:
        score += 0.15
    elif nb_emojis >= 5:
        score += 0.08

    # Plusieurs mentions WhatsApp (poids fort)
    nb_wa = count_whatsapp_mentions(text)
    if nb_wa >= 3:
        score += 0.30
    elif nb_wa >= 2:
        score += 0.15

    # Beaucoup de numéros de téléphone (poids moyen)
    nb_phones = count_phone_numbers(text)
    if nb_phones >= 4:
        score += 0.15
    elif nb_phones >= 3:
        score += 0.08

    # Mots-clés suspects (poids fort)
    nb_kw = count_suspect_keywords(text)
    if nb_kw >= 2:
        score += 0.30
    elif nb_kw == 1:
        score += 0.15

    # Description très courte pour un bien de valeur (poids faible)
    nb_mots = len(text.split()) if isinstance(text, str) else 0
    if nb_mots < 20:
        score += 0.05

    return round(min(score, 1.0), 3)


def main():
    # Charger le dataset propre
    df = pd.read_csv(INPUT_CSV)
    print(f"[OK] {len(df)} descriptions chargées")

    # Exclure les 40 déjà annotées
    already_annotated = [
        252, 4915, 1651, 1882, 836, 611, 214, 4204, 3613, 3894,
        149, 3156, 4346, 4621, 2785, 3622, 170, 4099, 212, 2618,
        3824, 3065, 3810, 1431, 740, 1422, 2224, 2124, 1471, 4142,
        4439, 5040, 2551, 870, 4813, 3420, 342, 3618, 3244, 680
    ]
    df_remaining = df[~df["id"].isin(already_annotated)].copy()
    print(f"[OK] {len(df_remaining)} descriptions restantes (hors sample déjà annoté)")

    # Calculer les features de suspicion
    print("\n[...] Calcul des scores de suspicion...")
    df_remaining["nb_whatsapp"]       = df_remaining["description_clean"].apply(count_whatsapp_mentions)
    df_remaining["nb_phones"]         = df_remaining["description_clean"].apply(count_phone_numbers)
    df_remaining["nb_suspect_kw"]     = df_remaining["description_clean"].apply(count_suspect_keywords)
    df_remaining["suspicion_score"]   = df_remaining.apply(compute_suspicion_score, axis=1)

    # Trier par score décroissant
    df_sorted = df_remaining.sort_values("suspicion_score", ascending=False)

    # Stats
    print(f"\nDistribution des scores de suspicion :")
    print(f"  Score >= 0.5 (très suspect) : {(df_sorted['suspicion_score'] >= 0.5).sum()}")
    print(f"  Score >= 0.3 (suspect)      : {(df_sorted['suspicion_score'] >= 0.3).sum()}")
    print(f"  Score >= 0.1 (légèrement)   : {(df_sorted['suspicion_score'] >= 0.1).sum()}")
    print(f"  Score = 0 (neutre)          : {(df_sorted['suspicion_score'] == 0).sum()}")

    # Prendre les 20 plus suspects
    sample = df_sorted.head(20)[[
        "id", "description_clean",
        "nb_emojis", "emojis_list", "nb_alert_emojis",
        "nb_whatsapp", "nb_phones", "nb_suspect_kw",
        "suspicion_score"
    ]].copy()

    sample["label_manuel"] = ""
    sample["notes"]        = ""

    sample.to_csv(OUTPUT_CSV, index=False)
    print(f"\n[OK] 20 candidats negatif sauvegardés : {OUTPUT_CSV}")

    print("\n" + "="*50)
    print("TOP 5 ANNONCES LES PLUS SUSPECTES")
    print("="*50)
    for _, row in sample.head(5).iterrows():
        print(f"\nID {row['id']} | score={row['suspicion_score']} | "
              f"alert_emojis={row['nb_alert_emojis']} | "
              f"whatsapp={row['nb_whatsapp']} | "
              f"kw_suspects={row['nb_suspect_kw']}")
        print(f"  {str(row['description_clean'])[:120]}...")

    print("\n" + "="*50)
    print("PROCHAINE ÉTAPE")
    print("="*50)
    print(f"Ouvre : {OUTPUT_CSV}")
    print("Annote la colonne label_manuel avec :")
    print("  positif / neutre_positif / neutre_negatif / negatif")
    print("Objectif : trouver au moins 3-4 exemples 'negatif'")


if __name__ == "__main__":
    main()