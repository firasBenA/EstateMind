"""
Estate Mind — Couche 1 : Analyse de sentiment avec BERT multilingue
====================================================================
Modele : nlptown/bert-base-multilingual-uncased-sentiment
Retourne 1 a 5 etoiles converti en signal de fraude

Colonnes ajoutees :
    - sentiment_stars  : 1 (negatif) a 5 (positif)
    - sentiment_score  : confiance du modele
    - sentiment_signal : signal de fraude entre 0 et 1
"""

import pandas as pd
from tqdm import tqdm
from transformers import pipeline

INPUT_CSV  = "./phase0_output/dataset_clean.csv"
OUTPUT_CSV = "./phase0_output/dataset_sentiment.csv"
MODEL_NAME = "nlptown/bert-base-multilingual-uncased-sentiment"
BATCH_SIZE = 16
TEST_MODE  = False   # mettre False pour le dataset complet


def label_to_stars(label: str) -> int:
    return int(label.split()[0])


def stars_to_signal(stars: int, score: float) -> float:
    """
    Convertit les etoiles en signal de fraude.
    1 etoile  -> 0.90 (tres suspect)
    2 etoiles -> 0.70 (suspect)
    3 etoiles -> 0.40 (neutre)
    4 etoiles -> 0.20 (peu suspect)
    5 etoiles -> 0.05 (pas suspect)
    """
    mapping = {1: 0.90, 2: 0.70, 3: 0.40, 4: 0.20, 5: 0.05}
    base = mapping.get(stars, 0.40)
    return round(base * score, 3)


def truncate_text(text: str, max_chars: int = 512) -> str:
    if not isinstance(text, str):
        return ""
    return text[:max_chars]


def main():
    df = pd.read_csv(INPUT_CSV)
    print(f"[OK] {len(df)} descriptions chargees")

    if TEST_MODE:
        df = df.head(50)
        print("[TEST] Mode test : 50 premieres descriptions")

    print(f"\n[...] Chargement du modele {MODEL_NAME}...")
    sentiment_pipeline = pipeline(
        "sentiment-analysis",
        model=MODEL_NAME,
        tokenizer=MODEL_NAME,
        max_length=512,
        truncation=True,
        device=-1
    )
    print("[OK] Modele charge")

    texts = df["description_clean"].apply(truncate_text).tolist()
    all_results = []

    print(f"\n[...] Analyse en cours...\n")
    for i in tqdm(range(0, len(texts), BATCH_SIZE), desc="BERT Sentiment"):
        batch = texts[i:i + BATCH_SIZE]
        try:
            results = sentiment_pipeline(batch)
            all_results.extend(results)
        except Exception as e:
            print(f"\n[ERREUR] batch {i} : {e}")
            for _ in batch:
                all_results.append({"label": "3 stars", "score": 0.5})

    df["sentiment_stars"]  = [label_to_stars(r["label"]) for r in all_results]
    df["sentiment_score"]  = [round(r["score"], 3) for r in all_results]
    df["sentiment_signal"] = [
        stars_to_signal(label_to_stars(r["label"]), r["score"])
        for r in all_results
    ]

    df.to_csv(OUTPUT_CSV, index=False)

    print(f"\n{'='*50}")
    print("RAPPORT COUCHE 1 - SENTIMENT")
    print(f"{'='*50}")
    print(f"\nDistribution des etoiles :")
    print(df["sentiment_stars"].value_counts().sort_index().to_string())
    print(f"\nSignal de fraude moyen par etoiles :")
    print(df.groupby("sentiment_stars")["sentiment_signal"].mean().round(3).to_string())
    print(f"\nDistribution du signal :")
    print(f"  Signal > 0.5 (suspect)     : {(df['sentiment_signal'] > 0.5).sum()}")
    print(f"  Signal 0.3-0.5 (modere)    : {((df['sentiment_signal'] >= 0.3) & (df['sentiment_signal'] <= 0.5)).sum()}")
    print(f"  Signal < 0.3 (peu suspect) : {(df['sentiment_signal'] < 0.3).sum()}")
    print(f"\n[OK] Resultats sauvegardes : {OUTPUT_CSV}")

    print(f"\n--- TOP 5 DESCRIPTIONS LES PLUS SUSPECTES ---")
    top5 = df.nlargest(5, "sentiment_signal")[
        ["id", "sentiment_stars", "sentiment_score", "sentiment_signal"]
    ]
    print(top5.to_string(index=False))


if __name__ == "__main__":
    main()