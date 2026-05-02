"""
Estate Mind — Generation du ground truth (150 descriptions)
============================================================
Tirage stratifie depuis le dataset_final.csv
pour avoir une representation equilibree des classes predites.

Sortie : ground_truth_150.csv
    - id
    - description_clean
    - score_final        : score predit par le pipeline
    - label_predit       : label predit par le pipeline
    - rules_details      : signaux detectes par les regles
    - nb_emojis
    - label_humain       : A REMPLIR MANUELLEMENT

Usage :
    python generer_ground_truth.py
"""

import pandas as pd

INPUT_CSV  = "./phase0_output/dataset_final.csv"
OUTPUT_CSV = "./phase0_output/ground_truth_150.csv"

# Nombre de descriptions par classe predite
N_PAR_CLASSE = {
    "positif":        30,
    "neutre_positif": 50,
    "neutre_negatif": 50,
    "negatif":        20,
}
# Total : 150


def main():
    df = pd.read_csv(INPUT_CSV)
    print(f"[OK] {len(df)} descriptions chargees")

    # Distribution actuelle
    print(f"\nDistribution des labels predits :")
    print(df["label_final"].value_counts().to_string())

    # Tirage stratifie
    samples = []
    for label, n in N_PAR_CLASSE.items():
        subset = df[df["label_final"] == label]
        available = len(subset)

        if available == 0:
            print(f"[WARN] Aucune description pour la classe '{label}'")
            continue

        if available < n:
            print(f"[WARN] Seulement {available} descriptions pour '{label}' (demande {n})")
            n = available

        # Trier par score_final decroissant pour prendre les cas les plus representatifs
        if label in ["negatif", "neutre_negatif"]:
            # Pour les classes suspectes, prendre les scores les plus eleves
            sampled = subset.nlargest(n, "score_final")
        else:
            # Pour les classes saines, prendre un echantillon aleatoire
            sampled = subset.sample(n=n, random_state=42)

        samples.append(sampled)
        print(f"  {label:20s} : {len(sampled)} descriptions selectionnees")

    df_sample = pd.concat(samples, ignore_index=True)

    # Exclure les 60 descriptions deja annotees
    already_annotated = [
        252, 4915, 1651, 1882, 836, 611, 214, 4204, 3613, 3894,
        149, 3156, 4346, 4621, 2785, 3622, 170, 4099, 212, 2618,
        3824, 3065, 3810, 1431, 740, 1422, 2224, 2124, 1471, 4142,
        4439, 5040, 2551, 870, 4813, 3420, 342, 3618, 3244, 680,
        78, 12, 5492, 101, 158, 5502, 152, 140, 139, 138,
        137, 134, 133, 132, 115, 5516, 157, 3622, 1905, 160
    ]
    df_sample = df_sample[~df_sample["id"].isin(already_annotated)]
    print(f"\nApres exclusion des 60 deja annotees : {len(df_sample)} descriptions")

    # Colonnes utiles pour l'annotation
    cols = [
        "id",
        "description_clean",
        "score_final",
        "label_predit",
        "rules_details",
        "nb_emojis",
        "nb_alert_emojis",
    ]

    # Garder seulement les colonnes qui existent
    cols_existantes = [c for c in cols if c in df_sample.columns]

    # Renommer label_final en label_predit
    if "label_final" in df_sample.columns:
        df_sample = df_sample.rename(columns={"label_final": "label_predit"})

    cols_existantes = [c for c in cols if c in df_sample.columns]
    df_out = df_sample[cols_existantes].copy()

    # Ajouter colonne a remplir
    df_out["label_humain"] = ""
    df_out["notes"]        = ""

    # Melanger pour eviter biais d'ordre
    df_out = df_out.sample(frac=1, random_state=42).reset_index(drop=True)

    df_out.to_csv(OUTPUT_CSV, index=False)

    print(f"\n[OK] {len(df_out)} descriptions sauvegardees : {OUTPUT_CSV}")
    print(f"\nDistribution finale :")
    if "label_predit" in df_out.columns:
        print(df_out["label_predit"].value_counts().to_string())

    print(f"\n{'='*50}")
    print("INSTRUCTIONS ANNOTATION")
    print(f"{'='*50}")
    print(f"Ouvre : {OUTPUT_CSV} avec Edit CSV dans VSCode")
    print(f"Remplis la colonne 'label_humain' avec :")
    print(f"  positif        -> annonce transparente, agence, prix, statut juridique")
    print(f"  neutre_positif -> honnete mais incomplete")
    print(f"  neutre_negatif -> signaux suspects")
    print(f"  negatif        -> activement trompeur")
    print(f"\nLa colonne 'label_predit' montre ce que le pipeline a predit.")
    print(f"Ne te laisse pas influencer par le label predit — annote independamment.")


if __name__ == "__main__":
    main()