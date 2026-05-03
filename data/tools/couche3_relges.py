"""
Estate Mind — Couche 3 : Regles linguistiques
==============================================
Pas de modele ML — regles codees manuellement
basees sur les patterns observes lors de l'annotation.

Entree : dataset_zeroshot.csv (sortie couche 2)
Sortie : dataset_rules.csv

Colonnes ajoutees :
    - rules_signal      : signal de fraude entre 0 et 1
    - rules_details     : liste des signaux detectes
    - rules_count       : nombre de signaux detectes

Usage :
    python couche3_regles.py
"""

import re
import pandas as pd
from tqdm import tqdm

# CONFIG
INPUT_CSV  = "./phase0_output/dataset_zeroshot.csv"
OUTPUT_CSV = "./phase0_output/dataset_rules.csv"
TEST_MODE  = False   # mettre False pour le dataset complet

# ─────────────────────────────────────────────
# REGLES LINGUISTIQUES
# Chaque regle a un poids entre 0 et 1
# Base sur nos observations des 60 annotations
# ─────────────────────────────────────────────

REGLES = [

    # SIGNAUX FORTS (poids 0.8 - 1.0)
    {
        "nom":    "ciblage_etranger",
        "poids":  1.0,
        "pattern": re.compile(
            r"\b(pour\s+[eé]tranger|pour\s+expatri|offre\s+[eé]tranger|"
            r"location\s+[eé]tranger|destin[eé]\s+aux\s+[eé]trangers)\b",
            re.IGNORECASE
        ),
    },
    {
        "nom":    "urgence_forte",
        "poids":  0.9,
        "pattern": re.compile(
            r"\b(d[eé]part\s+d[eé]finitif|d[eé]part\s+urgent|cause\s+d[eé]c[eè]s|"
            r"cause\s+divorce|vente\s+urgente|liquidation|"
            r"occasion\s+[àa]\s+ne\s+jamais\s+rater|"
            r"occasion\s+[àa]\s+ne\s+pas\s+rater)\b",
            re.IGNORECASE
        ),
    },
    {
        "nom":    "frais_visite",
        "poids":  0.85,
        "pattern": re.compile(
            r"\b(frais\s+de\s+visite|visite\s+payante|\d+\s*dt\s+visite)\b",
            re.IGNORECASE
        ),
    },
    {
        "nom":    "hashtags",
        "poids":  0.80,
        "pattern": re.compile(r"#\w+", re.IGNORECASE),
    },

    # SIGNAUX MOYENS (poids 0.5 - 0.7)
    {
        "nom":    "multiple_whatsapp",
        "poids":  0.70,
        "pattern": re.compile(
            r"(whatsapp.*whatsapp|whatsapp.*whatsapp.*whatsapp)",
            re.IGNORECASE | re.DOTALL
        ),
    },
    {
        "nom":    "pression_achat",
        "poids":  0.65,
        "pattern": re.compile(
            r"\b(ne\s+pas\s+rater|[àa]\s+saisir|opportunit[eé]\s+rare|"
            r"pour\s+les\s+s[eé]rieux|offre\s+limit[eé]e|"
            r"derni[eè]re\s+chance|tr[eè]s\s+recherch[eé])\b",
            re.IGNORECASE
        ),
    },
    {
        "nom":    "promesse_rentabilite",
        "poids":  0.65,
        "pattern": re.compile(
            r"\b(rendement\s+garanti|investissement\s+rentable|"
            r"rentabilit[eé]\s+locatif|rapport\s+locatif\s+garanti|"
            r"garantie\s+locative)\b",
            re.IGNORECASE
        ),
    },
    {
        "nom":    "emojis_excessifs",
        "poids":  0.60,
        "check_column": "nb_emojis",
        "threshold": 5,
    },
    {
        "nom":    "emojis_alerte",
        "poids":  0.70,
        "check_column": "nb_alert_emojis",
        "threshold": 1,
    },

    # SIGNAUX FAIBLES (poids 0.2 - 0.4)
    {
        "nom":    "absence_prix_bien_cher",
        "poids":  0.40,
        "pattern": re.compile(
            r"\b(piscine|villa|standing|luxe|prestige|panoramique)\b",
            re.IGNORECASE
        ),
        "absence_pattern": re.compile(
            r"\b(\d+\s*(dt|dinar|000))\b",
            re.IGNORECASE
        ),
    },
    {
        "nom":    "contacts_multiples",
        "poids":  0.35,
        "pattern": re.compile(
            r"(\d{2}\s*\d{3}\s*\d{3}.*\d{2}\s*\d{3}\s*\d{3}.*\d{2}\s*\d{3}\s*\d{3})",
            re.DOTALL
        ),
    },
    {
        "nom":    "description_trop_courte",
        "poids":  0.25,
        "check_column": "nb_mots",
        "threshold_max": 15,
    },
]


def apply_rules(row) -> dict:
    """
    Applique toutes les regles sur une description.
    Retourne le signal agregé et la liste des signaux detectes.
    """
    text      = str(row.get("description_clean", ""))
    signaux   = []
    score_max = 0.0

    for regle in REGLES:
        detected = False

        # Regle basee sur une colonne numerique
        if "check_column" in regle:
            col = regle["check_column"]
            val = row.get(col, 0)

            if "threshold" in regle and val >= regle["threshold"]:
                detected = True
            elif "threshold_max" in regle and val <= regle["threshold_max"]:
                detected = True

        # Regle avec absence de pattern (bien cher sans prix)
        elif "absence_pattern" in regle:
            if regle["pattern"].search(text) and not regle["absence_pattern"].search(text):
                detected = True

        # Regle basee sur un pattern regex
        elif "pattern" in regle:
            if regle["pattern"].search(text):
                detected = True

        if detected:
            signaux.append(regle["nom"])
            score_max = max(score_max, regle["poids"])

    # Score final : max des signaux + bonus pour accumulation
    nb_signaux = len(signaux)
    if nb_signaux == 0:
        signal_final = 0.0
    elif nb_signaux == 1:
        signal_final = score_max * 0.8
    elif nb_signaux == 2:
        signal_final = score_max * 0.9
    else:
        signal_final = min(score_max * 1.0 + (nb_signaux - 2) * 0.05, 1.0)

    return {
        "rules_signal":  round(signal_final, 3),
        "rules_details": "|".join(signaux) if signaux else "aucun",
        "rules_count":   nb_signaux,
    }


def main():
    df = pd.read_csv(INPUT_CSV)
    print(f"[OK] {len(df)} descriptions chargees")

    if TEST_MODE:
        df = df.head(50)
        print("[TEST] Mode test : 50 premieres descriptions")

    # Calculer nb_mots si absent
    if "nb_mots" not in df.columns:
        df["nb_mots"] = df["description_clean"].apply(
            lambda x: len(str(x).split()) if isinstance(x, str) else 0
        )

    print(f"\n[...] Application des regles linguistiques...")
    results = []
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Regles"):
        results.append(apply_rules(row))

    df["rules_signal"]  = [r["rules_signal"]  for r in results]
    df["rules_details"] = [r["rules_details"] for r in results]
    df["rules_count"]   = [r["rules_count"]   for r in results]

    df.to_csv(OUTPUT_CSV, index=False)

    print(f"\n{'='*50}")
    print("RAPPORT COUCHE 3 - REGLES LINGUISTIQUES")
    print(f"{'='*50}")

    print(f"\nDistribution des signaux detectes :")
    print(f"  0 signal        : {(df['rules_count'] == 0).sum()}")
    print(f"  1 signal        : {(df['rules_count'] == 1).sum()}")
    print(f"  2 signaux       : {(df['rules_count'] == 2).sum()}")
    print(f"  3+ signaux      : {(df['rules_count'] >= 3).sum()}")

    print(f"\nSignaux les plus frequents :")
    all_signals = []
    for details in df["rules_details"]:
        if details != "aucun":
            all_signals.extend(details.split("|"))
    if all_signals:
        from collections import Counter
        for signal, count in Counter(all_signals).most_common():
            print(f"  {signal:35s} : {count}")

    print(f"\nDistribution du signal regles :")
    print(f"  Signal > 0.5 (suspect)     : {(df['rules_signal'] > 0.5).sum()}")
    print(f"  Signal 0.3-0.5 (modere)    : {((df['rules_signal'] >= 0.3) & (df['rules_signal'] <= 0.5)).sum()}")
    print(f"  Signal < 0.3 (peu suspect) : {(df['rules_signal'] < 0.3).sum()}")

    print(f"\n[OK] Resultats sauvegardes : {OUTPUT_CSV}")

    print(f"\n--- TOP 5 DESCRIPTIONS LES PLUS SUSPECTES ---")
    top5 = df.nlargest(5, "rules_signal")[
        ["id", "rules_signal", "rules_count", "rules_details"]
    ]
    print(top5.to_string(index=False))


if __name__ == "__main__":
    main()