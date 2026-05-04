"""
Estate Mind — Combinaison des scores et classification finale
=============================================================
Combine les signaux des 3 couches en un score final de fraude.

Entree : dataset_rules.csv (sortie couche 3)
Sortie : dataset_final.csv

Colonnes ajoutees :
    - score_final  : score de fraude combine entre 0 et 1
    - label_final  : positif / neutre_positif / neutre_negatif / negatif
    - confiance    : niveau de confiance de la classification

Poids des couches :
    - Couche 1 (sentiment BERT)     : 30%
    - Couche 2 (zero-shot MiniLM)   : 30%
    - Couche 3 (regles linguistiques): 40%

Usage :
    python combinaison.py
"""

import pandas as pd

# CONFIG
INPUT_CSV  = "./phase0_output/dataset_rules.csv"
OUTPUT_CSV = "./phase0_output/dataset_final_seuil50.csv"
TEST_MODE  = False   # mettre False pour le dataset complet

# Poids des 3 couches
POIDS_SENTIMENT = 0.20
POIDS_ZEROSHOT  = 0.20
POIDS_REGLES    = 0.60

# Seuils de classification du score final
SEUILS = {
    "positif":        (0.00, 0.20),
    "neutre_positif": (0.20, 0.40),
    "neutre_negatif": (0.40, 0.50),
    "negatif":        (0.50, 1.00),
}


def score_to_label(score: float) -> str:
    """Convertit le score final en label."""
    for label, (low, high) in SEUILS.items():
        if low <= score < high:
            return label
    return "negatif"


def score_to_confiance(score: float, label: str) -> str:
    """
    Evalue le niveau de confiance selon la position
    du score dans la plage de sa classe.
    """
    low, high = SEUILS[label]
    plage = high - low
    position = (score - low) / plage  # 0 = bord bas, 1 = bord haut

    if label in ["positif", "neutre_positif"]:
        # Plus on est haut dans la plage plus c'est sur
        if position > 0.7:
            return "eleve"
        elif position > 0.3:
            return "moyen"
        else:
            return "faible"
    else:
        # Pour negatif et neutre_negatif idem
        if position > 0.7:
            return "eleve"
        elif position > 0.3:
            return "moyen"
        else:
            return "faible"


def main():
    df = pd.read_csv(INPUT_CSV)
    print(f"[OK] {len(df)} descriptions chargees")

    if TEST_MODE:
        df = df.head(50)
        print("[TEST] Mode test : 50 premieres descriptions")

    # Verification des colonnes necessaires
    colonnes_requises = ["sentiment_signal", "zeroshot_signal", "rules_signal"]
    for col in colonnes_requises:
        if col not in df.columns:
            print(f"[ERREUR] Colonne manquante : {col}")
            return

    # Calcul du score final
    df["score_final"] = (
        POIDS_SENTIMENT * df["sentiment_signal"] +
        POIDS_ZEROSHOT  * df["zeroshot_signal"]  +
        POIDS_REGLES    * df["rules_signal"]
    ).round(3)

    # Classification finale
    df["label_final"] = df["score_final"].apply(score_to_label)
    df["confiance"]   = df.apply(
        lambda row: score_to_confiance(row["score_final"], row["label_final"]),
        axis=1
    )

    df.to_csv(OUTPUT_CSV, index=False)

    print(f"\n{'='*50}")
    print("RAPPORT FINAL — PIPELINE COMPLET")
    print(f"{'='*50}")

    print(f"\nPoids utilises :")
    print(f"  Couche 1 (BERT sentiment)    : {POIDS_SENTIMENT*100:.0f}%")
    print(f"  Couche 2 (MiniLM zero-shot)  : {POIDS_ZEROSHOT*100:.0f}%")
    print(f"  Couche 3 (regles)            : {POIDS_REGLES*100:.0f}%")

    print(f"\nDistribution des labels finaux :")
    print(df["label_final"].value_counts().to_string())

    print(f"\nDistribution des scores finaux :")
    print(f"  Score moyen    : {df['score_final'].mean():.3f}")
    print(f"  Score median   : {df['score_final'].median():.3f}")
    print(f"  Score max      : {df['score_final'].max():.3f}")
    print(f"  Score min      : {df['score_final'].min():.3f}")

    print(f"\nDistribution de la confiance :")
    print(df["confiance"].value_counts().to_string())

    print(f"\n[OK] Dataset final sauvegarde : {OUTPUT_CSV}")

    print(f"\n--- TOP 10 ANNONCES LES PLUS SUSPECTES ---")
    cols = ["id", "score_final", "label_final", "confiance",
            "sentiment_signal", "zeroshot_signal", "rules_signal", "rules_details"]
    cols = [c for c in cols if c in df.columns]
    top10 = df.nlargest(10, "score_final")[cols]
    print(top10.to_string(index=False))

    print(f"\n--- REPARTITION PAR COUCHE ---")
    print(f"  Signal sentiment moyen  : {df['sentiment_signal'].mean():.3f}")
    print(f"  Signal zeroshot moyen   : {df['zeroshot_signal'].mean():.3f}")
    print(f"  Signal regles moyen     : {df['rules_signal'].mean():.3f}")
    print(f"  Score final moyen       : {df['score_final'].mean():.3f}")


if __name__ == "__main__":
    main()