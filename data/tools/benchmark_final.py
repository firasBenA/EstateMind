"""
Estate Mind — Benchmark final du pipeline
==========================================
Evalue les performances du pipeline complet
sur le ground truth de 120 annotations manuelles.

Metriques : F1 macro, Accuracy, Cohen Kappa
Matrice de confusion par classe

Usage :
    python benchmark_final.py
"""

import pandas as pd
import numpy as np
from sklearn.metrics import (
    f1_score, accuracy_score, cohen_kappa_score,
    confusion_matrix, classification_report
)

GROUND_TRUTH = "./phase0_output/ground_truth_150.csv"
DATASET_FINAL = "./phase0_output/dataset_final.csv"
OUTPUT_CSV = "./phase0_output/benchmark_final_results.csv"

LABEL_MAP = {
    "positif":        0,
    "neutre_positif": 1,
    "neutre_negatif": 2,
    "negatif":        3,
}
LABEL_NAMES = ["positif", "neutre_positif", "neutre_negatif", "negatif"]


def normalize_label(x):
    if not isinstance(x, str):
        return None
    x = x.strip().lower()
    mapping = {
        "positif":        "positif",
        "neutre_positif": "neutre_positif",
        "neutre_negatif": "neutre_negatif",
        "negatif":        "negatif",
    }
    return mapping.get(x, None)


def main():
    # Charger le ground truth
    df_gt = pd.read_csv(GROUND_TRUTH)
    df_gt["label_humain"] = df_gt["label_humain"].apply(normalize_label)
    df_gt = df_gt[df_gt["label_humain"].notna()].copy()
    print(f"[OK] {len(df_gt)} annotations humaines chargees")
    print(f"Distribution ground truth :")
    print(df_gt["label_humain"].value_counts().to_string())

    # Charger les predictions du pipeline
    df_final = pd.read_csv(DATASET_FINAL)
    df_final["label_final"] = df_final["label_final"].apply(normalize_label)
    print(f"\n[OK] {len(df_final)} predictions pipeline chargees")

    # Merger sur l'id
    df = df_gt.merge(
        df_final[["id", "label_final", "score_final",
                  "sentiment_signal", "zeroshot_signal", "rules_signal"]],
        on="id", how="left"
    )

    print(f"\n[OK] {len(df)} descriptions matchees")

    # Verifier les NaN
    missing = df["label_final"].isna().sum()
    if missing > 0:
        print(f"[WARN] {missing} descriptions sans prediction pipeline")
        df = df[df["label_final"].notna()].copy()

    # Preparer les vecteurs
    y_true = [LABEL_MAP[l] for l in df["label_humain"]]
    y_pred = [LABEL_MAP[l] for l in df["label_final"]]

    # Metriques globales
    acc   = accuracy_score(y_true, y_pred)
    f1    = f1_score(y_true, y_pred, average="macro", zero_division=0)
    kappa = cohen_kappa_score(y_true, y_pred)

    print(f"\n{'='*60}")
    print("BENCHMARK FINAL — PIPELINE COMPLET")
    print(f"{'='*60}")
    print(f"\nMetriques globales :")
    print(f"  Accuracy  : {acc:.3f}")
    print(f"  F1 macro  : {f1:.3f}")
    print(f"  Kappa     : {kappa:.3f}")

    # Rapport par classe
    print(f"\nRapport par classe :")
    print(classification_report(
        y_true, y_pred,
        target_names=LABEL_NAMES,
        zero_division=0
    ))

    # Matrice de confusion
    cm = confusion_matrix(y_true, y_pred)
    print(f"Matrice de confusion :")
    print(f"{'':20}", end="")
    for name in LABEL_NAMES:
        print(f"{name:15}", end="")
    print()
    for i, name in enumerate(LABEL_NAMES):
        print(f"{name:20}", end="")
        for j in range(len(LABEL_NAMES)):
            print(f"{cm[i][j]:15}", end="")
        print()

    # Analyse des erreurs
    df["correct"] = df["label_humain"] == df["label_final"]
    print(f"\nTaux de bonne classification par classe :")
    for label in LABEL_NAMES:
        subset = df[df["label_humain"] == label]
        if len(subset) > 0:
            taux = subset["correct"].mean()
            print(f"  {label:20} : {taux:.1%} ({subset['correct'].sum()}/{len(subset)})")

    # Cas les plus mal classifies
    erreurs = df[~df["correct"]].copy()
    print(f"\nNombre d'erreurs : {len(erreurs)}/{len(df)}")
    print(f"\nTop 10 erreurs (humain vs pipeline) :")
    cols = ["id", "label_humain", "label_final", "score_final", "rules_details"]
    cols = [c for c in cols if c in erreurs.columns]
    print(erreurs[cols].head(10).to_string(index=False))

    # Sauvegarder
    results = {
        "accuracy": round(acc, 3),
        "f1_macro": round(f1, 3),
        "kappa":    round(kappa, 3),
        "n_total":  len(df),
        "n_correct": sum(1 for a, b in zip(y_true, y_pred) if a == b),
        "n_errors":  sum(1 for a, b in zip(y_true, y_pred) if a != b),
    }
    pd.DataFrame([results]).to_csv(OUTPUT_CSV, index=False)
    print(f"\n[OK] Resultats sauvegardes : {OUTPUT_CSV}")

    # Conclusion
    print(f"\n{'='*60}")
    print("CONCLUSION")
    print(f"{'='*60}")
    if kappa >= 0.6:
        print("Kappa >= 0.6 : accord substantiel — pipeline VALIDE")
    elif kappa >= 0.4:
        print("Kappa 0.4-0.6 : accord modere — pipeline ACCEPTABLE")
    elif kappa >= 0.2:
        print("Kappa 0.2-0.4 : accord faible — pipeline A AMELIORER")
    else:
        print("Kappa < 0.2 : accord tres faible — pipeline NON VALIDE")


if __name__ == "__main__":
    main()