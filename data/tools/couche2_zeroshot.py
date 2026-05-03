"""
Estate Mind — Couche 2 : Classification zero-shot avec MiniLM
=============================================================
Modele retenu : cross-encoder/nli-MiniLM2-L6-H768
Entree : dataset_sentiment.csv (sortie couche 1)
Sortie : dataset_zeroshot.csv

Colonnes ajoutees :
    - zeroshot_label  : classe predite (trompeuse/fiable/vague/pressante)
    - zeroshot_score  : score de confiance entre 0 et 1
    - zeroshot_signal : signal de fraude entre 0 et 1

Prerequis :
    pip install transformers torch pandas tqdm

Usage :
    python couche2_zeroshot.py
"""

import pandas as pd
from tqdm import tqdm
from transformers import pipeline

# CONFIG
INPUT_CSV  = "./phase0_output/dataset_sentiment.csv"
OUTPUT_CSV = "./phase0_output/dataset_zeroshot.csv"
MODEL_NAME = "cross-encoder/nli-MiniLM2-L6-H768"
MAX_CHARS  = 400    # longueur max de la description envoyee au modele
TEST_MODE  = False   # mettre False pour le dataset complet

# Hypotheses soumises au modele
# Ordre important : du plus frauduleux au moins frauduleux
HYPOTHESES = [
    "annonce immobiliere trompeuse et frauduleuse",
    "annonce immobiliere avec ton pressant et urgent",
    "annonce immobiliere vague ou incomplete",
    "annonce immobiliere fiable et professionnelle",
]

# Mapping hypothese -> signal de fraude
HYPOTHESIS_SIGNAL = {
    "annonce immobiliere trompeuse et frauduleuse":    0.95,
    "annonce immobiliere avec ton pressant et urgent": 0.70,
    "annonce immobiliere vague ou incomplete":         0.45,
    "annonce immobiliere fiable et professionnelle":   0.05,
}

# Mapping hypothese -> label lisible
HYPOTHESIS_LABEL = {
    "annonce immobiliere trompeuse et frauduleuse":    "trompeuse",
    "annonce immobiliere avec ton pressant et urgent": "pressante",
    "annonce immobiliere vague ou incomplete":         "vague",
    "annonce immobiliere fiable et professionnelle":   "fiable",
}


def truncate_text(text: str, max_chars: int = MAX_CHARS) -> str:
    if not isinstance(text, str):
        return ""
    return text[:max_chars]


def process_result(result: dict) -> dict:
    """
    Extrait le label, score et signal de fraude du resultat zero-shot.
    Le modele retourne les hypotheses triees par score decroissant.
    On prend la premiere (la plus probable).
    """
    best_label = result["labels"][0]
    best_score = result["scores"][0]

    signal = HYPOTHESIS_SIGNAL.get(best_label, 0.45)
    label  = HYPOTHESIS_LABEL.get(best_label, "vague")

    return {
        "zeroshot_label":  label,
        "zeroshot_score":  round(best_score, 3),
        "zeroshot_signal": round(signal * best_score, 3),
    }


def main():
    df = pd.read_csv(INPUT_CSV)
    print(f"[OK] {len(df)} descriptions chargees")

    if TEST_MODE:
        df = df.head(50)
        print("[TEST] Mode test : 50 premieres descriptions")

    print(f"\n[...] Chargement du modele {MODEL_NAME}...")
    classifier = pipeline(
        "zero-shot-classification",
        model=MODEL_NAME,
        device=-1
    )
    print("[OK] Modele charge")

    texts = df["description_clean"].apply(truncate_text).tolist()
    all_results = []

    print(f"\n[...] Classification zero-shot en cours...\n")
    for text in tqdm(texts, desc="MiniLM Zero-shot"):
        try:
            result = classifier(text, candidate_labels=HYPOTHESES)
            all_results.append(process_result(result))
        except Exception as e:
            print(f"\n[ERREUR] {str(e)[:100]}")
            all_results.append({
                "zeroshot_label":  "vague",
                "zeroshot_score":  0.5,
                "zeroshot_signal": 0.20,
            })

    df["zeroshot_label"]  = [r["zeroshot_label"]  for r in all_results]
    df["zeroshot_score"]  = [r["zeroshot_score"]  for r in all_results]
    df["zeroshot_signal"] = [r["zeroshot_signal"] for r in all_results]

    df.to_csv(OUTPUT_CSV, index=False)

    print(f"\n{'='*50}")
    print("RAPPORT COUCHE 2 - ZERO-SHOT")
    print(f"{'='*50}")
    print(f"\nDistribution des labels zero-shot :")
    print(df["zeroshot_label"].value_counts().to_string())
    print(f"\nSignal de fraude moyen par label :")
    print(df.groupby("zeroshot_label")["zeroshot_signal"].mean().round(3).to_string())
    print(f"\nDistribution du signal :")
    print(f"  Signal > 0.5 (suspect)     : {(df['zeroshot_signal'] > 0.5).sum()}")
    print(f"  Signal 0.3-0.5 (modere)    : {((df['zeroshot_signal'] >= 0.3) & (df['zeroshot_signal'] <= 0.5)).sum()}")
    print(f"  Signal < 0.3 (peu suspect) : {(df['zeroshot_signal'] < 0.3).sum()}")
    print(f"\n[OK] Resultats sauvegardes : {OUTPUT_CSV}")

    print(f"\n--- TOP 5 DESCRIPTIONS LES PLUS SUSPECTES ---")
    top5 = df.nlargest(5, "zeroshot_signal")[
        ["id", "zeroshot_label", "zeroshot_score", "zeroshot_signal"]
    ]
    print(top5.to_string(index=False))


if __name__ == "__main__":
    main()