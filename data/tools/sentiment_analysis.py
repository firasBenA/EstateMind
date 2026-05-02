"""
Estate Mind — Phase 0 : Nettoyage et exploration des données
============================================================
Prérequis :
    pip install psycopg2-binary pandas matplotlib seaborn langdetect

Usage :
    Modifier la section CONFIG ci-dessous puis lancer :
    python phase0_exploration.py
"""

import os
import re
import ftfy
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text

# ─────────────────────────────────────────────
# CONFIG — à adapter à ton environnement
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5433,
    "dbname":   "estatemind",       # nom de ta base
    "user":     "postgres",          # ton user
    "password": "capTEEMO500",  # ton mot de passe
}

TABLE_NAME   = "listings"            # nom de ta table
DESC_COLUMN  = "description"         # colonne des descriptions
SOURCE_COLUMN = "source_name"             # colonne source (tecnocasa, mubaweb...) — None si inexistante
ID_COLUMN    = "id"                  # colonne identifiant unique

OUTPUT_DIR   = "./phase0_output"
SAMPLE_CSV   = f"{OUTPUT_DIR}/sample_40_to_annotate.csv"
CLEAN_CSV    = f"{OUTPUT_DIR}/dataset_clean.csv"
# ─────────────────────────────────────────────


def get_engine():
    from urllib.parse import quote_plus
    # quote_plus encode les caractères spéciaux du mot de passe (accents, @, #...)
    pwd = quote_plus(DB_CONFIG["password"])
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{pwd}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    return create_engine(url)


def load_data(engine):
    cols = [ID_COLUMN, DESC_COLUMN]
    if SOURCE_COLUMN:
        cols.append(SOURCE_COLUMN)

    query = f"SELECT {', '.join(cols)} FROM {TABLE_NAME};"
    df = pd.read_sql(text(query), engine)
    print(f"[OK] {len(df)} lignes chargées depuis PostgreSQL")
    return df


"""
Plages Unicode couvrant les emojis courants.
On les extrait AVANT de nettoyer le texte, puis on les retire du texte propre
pour ne pas perturber le LLM — mais on garde les features dans des colonnes séparées.
"""
EMOJI_PATTERN = re.compile(
    "["
    "\U0001F600-\U0001F64F"   # emoticons visages
    "\U0001F300-\U0001F5FF"   # symboles & pictogrammes
    "\U0001F680-\U0001F6FF"   # transports & cartes
    "\U0001F1E0-\U0001F1FF"   # drapeaux
    "\U00002700-\U000027BF"   # Dingbats
    "\U0001F900-\U0001F9FF"   # emojis supplémentaires
    "\U00002600-\U000026FF"   # symboles divers (étoiles, cœurs...)
    "\U0001FA00-\U0001FA6F"   # Chess & autres
    "\U0001FA70-\U0001FAFF"   # emojis récents
    "\U00002500-\U00002BEF"   # symboles techniques et flèches
    "]+",
    flags=re.UNICODE
)

# Emojis considérés comme signaux d'alerte (urgence, argent, cadenas, feu...)
ALERT_EMOJIS = {
    "\U0001F525",  # feu
    "\U0001F4B0",  # sac d'argent
    "\U0001F4B8",  # billet avec ailes
    "\U0001F4B5",  # billet dollar
    "\U0001F4B6",  # billet euro
    "\U0001F512",  # cadenas fermé
    "\U0001F513",  # cadenas ouvert
    "\U000026A0",  # panneau attention
    "\U0001F6A8",  # gyrophare
    "\U0001F3C6",  # trophée
    "\U00002B50",  # étoile
    "\U0001F31F",  # étoile brillante
    "\U0001F4E3",  # mégaphone
    "\U0001F4E2",  # haut-parleur
    "\U0001F4AF",  # 100
    "\U0001F449",  # doigt pointant droite
    "\U0001F447",  # doigt pointant bas
}


def extract_emoji_features(text: str) -> dict:
    """
    Extrait les features emoji d'une description brute (avant nettoyage).
    Retourne un dict avec :
      - nb_emojis       : nombre total d'emojis
      - emojis_list     : liste des emojis trouvés (dédoublonnée)
      - nb_alert_emojis : nombre d'emojis d'alerte
      - ratio_emojis    : emojis / nb mots (densité)
    """
    if not isinstance(text, str):
        return {"nb_emojis": 0, "emojis_list": "", "nb_alert_emojis": 0, "ratio_emojis": 0.0}

    found = EMOJI_PATTERN.findall(text)
    # Décomposer les chaînes multi-emojis en emojis individuels
    individual = []
    for chunk in found:
        individual.extend(list(chunk))

    nb_total  = len(individual)
    unique    = list(dict.fromkeys(individual))   # dédoublonné, ordre conservé
    nb_alert  = sum(1 for e in individual if e in ALERT_EMOJIS)
    nb_mots   = max(len(text.split()), 1)
    ratio     = round(nb_total / nb_mots, 4)

    return {
        "nb_emojis":       nb_total,
        "emojis_list":     " ".join(unique),
        "nb_alert_emojis": nb_alert,
        "ratio_emojis":    ratio,
    }


def clean_description(text: str) -> str | None:
    """
    Nettoie une description et retourne None si inutilisable.
    Les emojis sont retirés du texte propre (déjà capturés dans extract_emoji_features).
    """
    if not isinstance(text, str):
        return None
    # Supprimer les balises HTML résiduelles
    text = re.sub(r"<[^>]+>", " ", text)
    # Retirer les emojis du texte propre (features déjà extraites séparément)
    text = EMOJI_PATTERN.sub(" ", text)
    # Supprimer les caractères spéciaux non textuels
    text = re.sub(r"[^\w\sàâäéèêëîïôùûüçœæ.,;:!?()\-'/]", " ", text)
    # Normaliser les espaces
    text = re.sub(r"\s+", " ", text).strip()
    # Seuil minimal : 10 mots
    if len(text.split()) < 10:
        return None
    return text


def fix_encoding(text: str) -> str:
    """
    Corrige le mojibake (double encodage Latin-1/UTF-8) introduit lors du scraping.
    Utilise ftfy qui gère tous les cas : Ã© → é, Â² → ², Ã  → à, etc.
    """
    if not isinstance(text, str):
        return text
    return ftfy.fix_text(text)


def detect_language_simple(text: str) -> str:
    """
    Détection de langue simplifiée sans dépendance externe.
    Retourne 'fr', 'ar', ou 'unknown'.
    """
    arabic_chars = len(re.findall(r'[\u0600-\u06FF]', text))
    total_chars = len(text.replace(" ", ""))
    if total_chars == 0:
        return "unknown"
    if arabic_chars / total_chars > 0.3:
        return "ar"
    # Indicateurs simples du français
    fr_markers = ["le ", "la ", "les ", "un ", "une ", "des ", "est ", "avec ", "pour ", "dans ", "sur "]
    fr_count = sum(text.lower().count(m) for m in fr_markers)
    if fr_count >= 2:
        return "fr"
    return "unknown"


def run_exploration(df_clean: pd.DataFrame):
    """Génère les statistiques et graphiques d'exploration."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("\n" + "="*50)
    print("STATISTIQUES D'EXPLORATION")
    print("="*50)

    # Longueur des descriptions
    df_clean["nb_mots"] = df_clean["description_clean"].apply(lambda x: len(x.split()))
    print(f"\nNombre de descriptions utilisables : {len(df_clean)}")
    print(f"Longueur moyenne              : {df_clean['nb_mots'].mean():.0f} mots")
    print(f"Longueur médiane              : {df_clean['nb_mots'].median():.0f} mots")
    print(f"Longueur min                  : {df_clean['nb_mots'].min()} mots")
    print(f"Longueur max                  : {df_clean['nb_mots'].max()} mots")

    # Distribution par source
    if SOURCE_COLUMN and SOURCE_COLUMN in df_clean.columns:
        print(f"\nDistribution par source :")
        print(df_clean[SOURCE_COLUMN].value_counts().to_string())

    # Distribution par langue
    print(f"\nDistribution par langue :")
    print(df_clean["langue"].value_counts().to_string())

    # Stats emojis
    if "nb_emojis" in df_clean.columns:
        print(f"\nStats emojis (descriptions françaises) :")
        print(f"  Avec au moins 1 emoji        : {(df_clean['nb_emojis'] > 0).sum()}")
        print(f"  Avec au moins 1 emoji alerte : {(df_clean['nb_alert_emojis'] > 0).sum()}")
        print(f"  Nb emojis max dans 1 desc    : {df_clean['nb_emojis'].max()}")
        print(f"  Ratio emojis/mots moyen      : {df_clean['ratio_emojis'].mean():.4f}")

    # ── Graphiques ──────────────────────────────
    fig, axes = plt.subplots(1, 3, figsize=(16, 4))
    fig.suptitle("Estate Mind — Exploration Phase 0", fontsize=13)

    # Histogramme longueurs
    axes[0].hist(df_clean["nb_mots"], bins=30, color="#4A90D9", edgecolor="white")
    axes[0].set_title("Distribution longueur des descriptions")
    axes[0].set_xlabel("Nombre de mots")
    axes[0].set_ylabel("Fréquence")
    axes[0].axvline(df_clean["nb_mots"].median(), color="red", linestyle="--", label="médiane")
    axes[0].legend()

    # Barplot sources
    if SOURCE_COLUMN and SOURCE_COLUMN in df_clean.columns:
        src_counts = df_clean[SOURCE_COLUMN].value_counts()
        axes[1].barh(src_counts.index, src_counts.values, color="#5BA05A")
        axes[1].set_title("Descriptions par source")
        axes[1].set_xlabel("Nombre")
    else:
        axes[1].text(0.5, 0.5, "Colonne source non configurée",
                     ha="center", va="center", transform=axes[1].transAxes)
        axes[1].set_title("Sources")

    # Histogramme nb emojis
    if "nb_emojis" in df_clean.columns:
        emoji_counts = df_clean["nb_emojis"].clip(upper=20)
        axes[2].hist(emoji_counts, bins=20, color="#E07B54", edgecolor="white")
        axes[2].set_title("Distribution nb emojis par description")
        axes[2].set_xlabel("Nb emojis (plafonné à 20)")
        axes[2].set_ylabel("Fréquence")
        # Marquer le seuil d'alerte
        axes[2].axvline(5, color="red", linestyle="--", label="seuil alerte (5)")
        axes[2].legend()
    else:
        axes[2].set_visible(False)

    plt.tight_layout()
    plot_path = f"{OUTPUT_DIR}/exploration.png"
    plt.savefig(plot_path, dpi=120)
    print(f"\n[OK] Graphique sauvegardé : {plot_path}")
    plt.close()


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Chargement
    engine = get_engine()
    df = load_data(engine)

    # 2. Extraction des features emoji (AVANT nettoyage — sur le texte brut)
    print("\n[...] Extraction des features emoji...")
    emoji_features = df[DESC_COLUMN].apply(extract_emoji_features)
    df["nb_emojis"]       = emoji_features.apply(lambda x: x["nb_emojis"])
    df["emojis_list"]     = emoji_features.apply(lambda x: x["emojis_list"])
    df["nb_alert_emojis"] = emoji_features.apply(lambda x: x["nb_alert_emojis"])
    df["ratio_emojis"]    = emoji_features.apply(lambda x: x["ratio_emojis"])

    desc_avec_emojis = (df["nb_emojis"] > 0).sum()
    desc_avec_alerts = (df["nb_alert_emojis"] > 0).sum()
    print(f"  Descriptions avec au moins 1 emoji        : {desc_avec_emojis}")
    print(f"  Descriptions avec au moins 1 emoji alerte : {desc_avec_alerts}")

    # 3. Nettoyage du texte
    print("\n[...] Nettoyage du texte en cours...")
    # Corriger l'encodage sur le texte brut AVANT le nettoyage
    df[DESC_COLUMN] = df[DESC_COLUMN].apply(fix_encoding)
    df["description_clean"] = df[DESC_COLUMN].apply(clean_description)
    df["langue"] = df["description_clean"].apply(
        lambda x: detect_language_simple(x) if isinstance(x, str) else "null"
    )

    # Statistiques de nettoyage
    total = len(df)
    nulls_avant = df[DESC_COLUMN].isna().sum()
    trop_court  = (df["description_clean"].isna() & df[DESC_COLUMN].notna()).sum()
    df_clean    = df[df["description_clean"].notna()].copy()
    df_fr       = df_clean[df_clean["langue"] == "fr"].copy()

    print(f"\nRapport de nettoyage :")
    print(f"  Total initial              : {total}")
    print(f"  Descriptions nulles        : {nulls_avant}")
    print(f"  Trop courtes (< 10 mots)   : {trop_court}")
    print(f"  Après nettoyage (toutes langues) : {len(df_clean)}")
    print(f"  Gardées (français)         : {len(df_fr)}")
    print(f"  Ignorées (arabe/other)     : {len(df_clean) - len(df_fr)}")

    # 3. Exploration
    run_exploration(df_fr)

    # 4. Sauvegarde du dataset propre
    df_fr.to_csv(CLEAN_CSV, index=False)
    print(f"\n[OK] Dataset propre sauvegardé : {CLEAN_CSV}")

    # 5. Échantillon de 40 descriptions pour annotation manuelle (Phase 0.5)
    n_sample    = min(40, len(df_fr))
    sample_cols = [c for c in [ID_COLUMN, "description_clean",
                                "nb_emojis", "emojis_list", "nb_alert_emojis"]
                   if c in df_fr.columns]
    sample = df_fr[sample_cols].sample(n=n_sample, random_state=42).copy()
    sample["label_manuel"] = ""   # colonne à remplir à la main
    sample["notes"]        = ""   # colonne pour tes observations
    sample.to_csv(SAMPLE_CSV, index=False)

    print(f"\n[OK] Échantillon de {n_sample} descriptions sauvegardé : {SAMPLE_CSV}")
    print("\n" + "="*50)
    print("PROCHAINE ÉTAPE — Phase 0.5")
    print("="*50)
    print(f"Ouvre le fichier : {SAMPLE_CSV}")
    print("Pour chaque description, remplis la colonne 'label_manuel' avec :")
    print("  positif        → description enthousiaste, transparente, détaillée")
    print("  neutre_positif → description correcte mais générique")
    print("  neutre_negatif → description vague, ambiguë, éléments suspects")
    print("  negatif        → incohérente, urgence suspecte, promesses irréalistes")
    print("\nUtilise la colonne 'notes' pour noter tes observations.")
    print("Ces 40 exemples serviront de few-shot pour Mistral en Phase 1.")


if __name__ == "__main__":
    main()