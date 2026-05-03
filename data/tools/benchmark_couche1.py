"""
Estate Mind — Benchmark Couches 1 et 2
=======================================
Compare plusieurs modeles de sentiment et zero-shot
sur les 60 descriptions annotees manuellement.

Metriques : F1 macro, Accuracy, Cohen Kappa, temps inference

Usage :
    python benchmark.py
"""

import time
import pandas as pd
import numpy as np
from sklearn.metrics import f1_score, accuracy_score, cohen_kappa_score, confusion_matrix
from transformers import pipeline

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
SAMPLE_40  = "./phase0_output/sample_40_to_annotate.csv"
SAMPLE_20  = "./phase0_output/sample_20_negatif_candidates.csv"
OUTPUT_CSV = "./phase0_output/benchmark_results.csv"

# Hypotheses pour le zero-shot
HYPOTHESES = [
    "annonce immobiliere trompeuse et frauduleuse",
    "annonce immobiliere fiable et professionnelle",
    "annonce immobiliere vague ou incomplete",
    "annonce immobiliere avec ton pressant et urgent",
]

# Mapping label manuel -> classe numerique
LABEL_MAP = {
    "positif":        0,
    "neutre_positif": 1,
    "neutre_negatif": 2,
    "negatif":        3,
}

# ─────────────────────────────────────────────
# MODELES A COMPARER
# ─────────────────────────────────────────────
SENTIMENT_MODELS = [
    "nlptown/bert-base-multilingual-uncased-sentiment",
    "lxyuan/distilbert-base-multilingual-cased-sentiments-student",
]

ZEROSHOT_MODELS = [
    "MoritzLaurer/mDeBERTa-v3-base-mnli-xnli",
    "cross-encoder/nli-MiniLM2-L6-H768",
    "facebook/bart-large-mnli",
]
# ─────────────────────────────────────────────


def normalize_label(x):
    if not isinstance(x, str):
        return None
    x = x.strip().lower()
    mapping = {
        "positif":         "positif",
        "postif":          "positif",
        "neutre_positif":  "neutre_positif",
        "neutre positif":  "neutre_positif",
        "neutre_negatif":  "neutre_negatif",
        "neutre negatif":  "neutre_negatif",
        "neutre_négatif":  "neutre_negatif",
        "neutre négatif":  "neutre_negatif",
        "negatif":         "negatif",
        "négatif":         "negatif",
    }
    return mapping.get(x, None)


def load_annotations():
    """Charge et nettoie les 60 annotations manuelles."""
    df40 = pd.read_csv(SAMPLE_40)
    df20 = pd.read_csv(SAMPLE_20)

    df40["label_norm"] = df40["label_manuel"].apply(normalize_label)
    df20["label_norm"] = df20["label_manuel"].apply(normalize_label)

    df = pd.concat([
        df40[["id", "description_clean", "label_norm"]],
        df20[["id", "description_clean", "label_norm"]]
    ], ignore_index=True)

    df = df[df["label_norm"].notna()].copy()
    print(f"[OK] {len(df)} annotations chargees")
    print(f"Distribution : {df['label_norm'].value_counts().to_dict()}")
    return df


def stars_to_class(label: str) -> int:
    """Convertit les etoiles BERT en classe 0-3."""
    stars = int(label.split()[0])
    if stars <= 2:
        return 3   # negatif
    elif stars == 3:
        return 2   # neutre_negatif
    elif stars == 4:
        return 1   # neutre_positif
    else:
        return 0   # positif


def distilbert_to_class(label: str) -> int:
    """Convertit le label DistilBERT en classe 0-3."""
    label = label.lower()
    if label == "negative":
        return 3
    elif label == "neutral":
        return 2
    else:
        return 0


def zeroshot_to_class(result: dict) -> int:
    """
    Convertit le resultat zero-shot en classe 0-3
    selon l'hypothese la plus probable.
    """
    labels = result["labels"]
    scores = result["scores"]
    best   = labels[0]  # hypothese la plus probable

    if "trompeuse" in best or "frauduleuse" in best:
        return 3   # negatif
    elif "pressant" in best or "urgent" in best:
        return 2   # neutre_negatif
    elif "vague" in best or "incomplete" in best:
        return 1   # neutre_positif
    else:
        return 0   # positif


def evaluate_model(y_true, y_pred, model_name, duration, layer):
    """Calcule les metriques de performance."""
    acc   = accuracy_score(y_true, y_pred)
    f1    = f1_score(y_true, y_pred, average="macro", zero_division=0)
    kappa = cohen_kappa_score(y_true, y_pred)
    cm    = confusion_matrix(y_true, y_pred)

    print(f"\n  Accuracy  : {acc:.3f}")
    print(f"  F1 macro  : {f1:.3f}")
    print(f"  Kappa     : {kappa:.3f}")
    print(f"  Temps/desc: {duration:.2f}s")
    print(f"  Matrice de confusion :\n{cm}")

    return {
        "layer":    layer,
        "model":    model_name,
        "accuracy": round(acc, 3),
        "f1_macro": round(f1, 3),
        "kappa":    round(kappa, 3),
        "time_per_desc": round(duration, 3),
    }


def benchmark_sentiment(df, results):
    """Benchmark des modeles de sentiment."""
    print(f"\n{'='*60}")
    print("BENCHMARK COUCHE 1 — SENTIMENT")
    print(f"{'='*60}")

    y_true = [LABEL_MAP[l] for l in df["label_norm"]]
    texts  = df["description_clean"].fillna("").tolist()

    for model_name in SENTIMENT_MODELS:
        print(f"\n--- Modele : {model_name} ---")
        try:
            clf = pipeline(
                "sentiment-analysis",
                model=model_name,
                tokenizer=model_name,
                max_length=512,
                truncation=True,
                device=-1
            )

            start  = time.time()
            preds  = clf(texts)
            elapsed = (time.time() - start) / len(texts)

            if "star" in preds[0]["label"].lower():
                y_pred = [stars_to_class(p["label"]) for p in preds]
            else:
                y_pred = [distilbert_to_class(p["label"]) for p in preds]

            result = evaluate_model(y_true, y_pred, model_name, elapsed, "couche1")
            results.append(result)

        except Exception as e:
            print(f"  [ERREUR] {str(e)[:100]}")


def benchmark_zeroshot(df, results):
    """Benchmark des modeles zero-shot."""
    print(f"\n{'='*60}")
    print("BENCHMARK COUCHE 2 — ZERO-SHOT")
    print(f"{'='*60}")

    y_true = [LABEL_MAP[l] for l in df["label_norm"]]
    texts  = df["description_clean"].fillna("").str[:400].tolist()

    for model_name in ZEROSHOT_MODELS:
        print(f"\n--- Modele : {model_name} ---")
        try:
            clf = pipeline(
                "zero-shot-classification",
                model=model_name,
                device=-1
            )

            start  = time.time()
            preds  = [clf(text, candidate_labels=HYPOTHESES) for text in texts]
            elapsed = (time.time() - start) / len(texts)

            y_pred = [zeroshot_to_class(p) for p in preds]

            result = evaluate_model(y_true, y_pred, model_name, elapsed, "couche2")
            results.append(result)

        except Exception as e:
            print(f"  [ERREUR] {str(e)[:100]}")


def main():
    df = load_annotations()

    results = []

    benchmark_sentiment(df, results)
    benchmark_zeroshot(df, results)

    # Tableau comparatif final
    df_results = pd.DataFrame(results)
    df_results.to_csv(OUTPUT_CSV, index=False)

    print(f"\n{'='*60}")
    print("TABLEAU COMPARATIF FINAL")
    print(f"{'='*60}")
    print(df_results.to_string(index=False))

    print(f"\n[OK] Resultats sauvegardes : {OUTPUT_CSV}")

    # Meilleurs modeles
    best_c1 = df_results[df_results["layer"] == "couche1"].nlargest(1, "f1_macro").iloc[0]
    best_c2 = df_results[df_results["layer"] == "couche2"].nlargest(1, "f1_macro").iloc[0]

    print(f"\n{'='*60}")
    print("MEILLEURS MODELES")
    print(f"{'='*60}")
    print(f"Couche 1 : {best_c1['model']} (F1={best_c1['f1_macro']})")
    print(f"Couche 2 : {best_c2['model']} (F1={best_c2['f1_macro']})")


if __name__ == "__main__":
    main()