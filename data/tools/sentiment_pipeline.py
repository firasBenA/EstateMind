"""
Estate Mind — Pipeline Complet PostgreSQL
==========================================
Lit depuis la table 'listings', analyse le sentiment
et ecrit les resultats dans 'sentiment_analysis'.

Fonctionne en deux modes :
  - Mode batch  : traite toutes les annonces non encore analysees
  - Mode watch  : surveille les nouvelles annonces en continu

Usage :
    python pipeline_postgres.py             # mode batch
    python pipeline_postgres.py --watch     # mode temps reel
    python pipeline_postgres.py --reset     # reanalyse tout

Prerequis :
    pip install psycopg2-binary sqlalchemy transformers torch pandas tqdm python-dotenv
"""

import os
import re
import sys
import time
import ftfy
import argparse
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from transformers import pipeline as hf_pipeline

load_dotenv()

# ─────────────────────────────────────────────
# CONFIG BASE DE DONNEES
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "estatemind"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

TABLE_SOURCE = "listings"
TABLE_DEST   = "sentiment_analysis"
BATCH_SIZE   = 32
WATCH_INTERVAL = 60 

SENTIMENT_MODEL = "nlptown/bert-base-multilingual-uncased-sentiment"
ZEROSHOT_MODEL  = "cross-encoder/nli-MiniLM2-L6-H768"

HYPOTHESES = [
    "annonce immobiliere trompeuse et frauduleuse",
    "annonce immobiliere avec ton pressant et urgent",
    "annonce immobiliere vague ou incomplete",
    "annonce immobiliere fiable et professionnelle",
]

HYPOTHESIS_SIGNAL = {
    "annonce immobiliere trompeuse et frauduleuse":    0.95,
    "annonce immobiliere avec ton pressant et urgent": 0.70,
    "annonce immobiliere vague ou incomplete":         0.45,
    "annonce immobiliere fiable et professionnelle":   0.05,
}

HYPOTHESIS_LABEL = {
    "annonce immobiliere trompeuse et frauduleuse":    "trompeuse",
    "annonce immobiliere avec ton pressant et urgent": "pressante",
    "annonce immobiliere vague ou incomplete":         "vague",
    "annonce immobiliere fiable et professionnelle":   "fiable",
}


POIDS_SENTIMENT = 0.20
POIDS_ZEROSHOT  = 0.20
POIDS_REGLES    = 0.60


SEUILS = {
    "positif":        (0.00, 0.20),
    "neutre_positif": (0.20, 0.40),
    "neutre_negatif": (0.40, 0.65),
    "negatif":        (0.65, 1.00),
}

# ─────────────────────────────────────────────
# REGLES LINGUISTIQUES
# ─────────────────────────────────────────────
EMOJI_PATTERN = re.compile(
    "["
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF"
    "\U00002700-\U000027BF"
    "\U0001F900-\U0001F9FF"
    "\U00002600-\U000026FF"
    "\U0001FA70-\U0001FAFF"
    "]+",
    flags=re.UNICODE
)

ALERT_EMOJIS = {
    "\U0001F525", "\U0001F4B0", "\U0001F4B8", "\U0001F512",
    "\U000026A0", "\U0001F6A8", "\U0001F4E3", "\U0001F4AF",
}

REGLES = [
    {"nom": "ciblage_etranger", "poids": 1.0,
     "pattern": re.compile(r"\b(pour\s+[eé]tranger|pour\s+expatri|offre\s+[eé]tranger)\b", re.I)},
    {"nom": "urgence_forte", "poids": 0.9,
     "pattern": re.compile(r"\b(d[eé]part\s+d[eé]finitif|cause\s+d[eé]c[eè]s|cause\s+divorce|occasion\s+[àa]\s+ne\s+jamais\s+rater)\b", re.I)},
    {"nom": "frais_visite", "poids": 0.85,
     "pattern": re.compile(r"\b(frais\s+de\s+visite|visite\s+payante)\b", re.I)},
    {"nom": "hashtags", "poids": 0.80,
     "pattern": re.compile(r"#\w+", re.I)},
    {"nom": "multiple_whatsapp", "poids": 0.70,
     "pattern": re.compile(r"(whatsapp.*whatsapp)", re.I | re.DOTALL)},
    {"nom": "pression_achat", "poids": 0.65,
     "pattern": re.compile(r"\b(ne\s+pas\s+rater|[àa]\s+saisir|opportunit[eé]\s+rare|pour\s+les\s+s[eé]rieux)\b", re.I)},
    {"nom": "promesse_rentabilite", "poids": 0.65,
     "pattern": re.compile(r"\b(rendement\s+garanti|investissement\s+rentable|rentabilit[eé]\s+locatif)\b", re.I)},
    {"nom": "contacts_multiples", "poids": 0.35,
     "pattern": re.compile(r"(\d{2}\s*\d{3}\s*\d{3}.*\d{2}\s*\d{3}\s*\d{3}.*\d{2}\s*\d{3}\s*\d{3})", re.DOTALL)},
]


# ─────────────────────────────────────────────
# FONCTIONS UTILITAIRES
# ─────────────────────────────────────────────

def get_engine():
    pwd = quote_plus(DB_CONFIG["password"])
    url = (f"postgresql+psycopg2://{DB_CONFIG['user']}:{pwd}"
           f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    return create_engine(url)


def create_table(engine):
    """Cree la table sentiment_analysis si elle n'existe pas."""
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_DEST} (
        id                INTEGER PRIMARY KEY,
        description_clean TEXT,
        nb_emojis         INTEGER DEFAULT 0,
        nb_alert_emojis   INTEGER DEFAULT 0,
        sentiment_stars   INTEGER,
        sentiment_score   FLOAT,
        sentiment_signal  FLOAT,
        zeroshot_label    VARCHAR(20),
        zeroshot_score    FLOAT,
        zeroshot_signal   FLOAT,
        rules_signal      FLOAT,
        rules_details     TEXT,
        rules_count       INTEGER DEFAULT 0,
        score_final       FLOAT,
        label_final       VARCHAR(20),
        confiance         VARCHAR(10),
        analysed_at       TIMESTAMP DEFAULT NOW()
    );
    """
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    print(f"[OK] Table '{TABLE_DEST}' prete")


def get_unanalysed(engine):
    """Retourne les annonces pas encore analysees."""
    sql = f"""
    SELECT l.id, l.description, l.source_name
    FROM {TABLE_SOURCE} l
    LEFT JOIN {TABLE_DEST} s ON l.id = s.id
    WHERE s.id IS NULL
      AND l.description IS NOT NULL
      AND LENGTH(l.description) > 50
    ORDER BY l.id;
    """
    with engine.connect() as conn:
        result = pd.read_sql(text(sql), conn)
    return result


def fix_encoding(text):
    if not isinstance(text, str):
        return ""
    return ftfy.fix_text(text)


def clean_text(text):
    if not isinstance(text, str):
        return ""
    text = ftfy.fix_text(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = EMOJI_PATTERN.sub(" ", text)
    text = re.sub(r"[^\w\sàâäéèêëîïôùûüçœæ.,;:!?()\-'/]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_emoji_features(text):
    if not isinstance(text, str):
        return 0, 0
    found = EMOJI_PATTERN.findall(text)
    individual = []
    for chunk in found:
        individual.extend(list(chunk))
    nb_total = len(individual)
    nb_alert = sum(1 for e in individual if e in ALERT_EMOJIS)
    return nb_total, nb_alert


def detect_language(text):
    if not isinstance(text, str):
        return "unknown"
    arabic = len(re.findall(r'[\u0600-\u06FF]', text))
    total  = max(len(text.replace(" ", "")), 1)
    if arabic / total > 0.3:
        return "ar"
    fr_markers = ["le ", "la ", "les ", "un ", "une ", "des ", "est ", "avec ", "pour "]
    if sum(text.lower().count(m) for m in fr_markers) >= 2:
        return "fr"
    return "unknown"


def apply_rules(text, nb_emojis, nb_alert_emojis):
    signaux = []
    for regle in REGLES:
        if regle["nom"] == "emojis_excessifs" and nb_emojis >= 5:
            signaux.append(regle["nom"])
        elif regle["nom"] == "emojis_alerte" and nb_alert_emojis >= 1:
            signaux.append(regle["nom"])
        elif "pattern" in regle and regle["pattern"].search(text):
            signaux.append(regle["nom"])

   
    if nb_emojis >= 5:
        signaux.append("emojis_excessifs")
    if nb_alert_emojis >= 1:
        signaux.append("emojis_alerte")

    
    signaux = list(dict.fromkeys(signaux))

    nb = len(signaux)
    if nb == 0:
        signal = 0.0
    else:
        max_poids = max(
            next((r["poids"] for r in REGLES if r["nom"] == s), 0.3)
            for s in signaux
        )
        if nb == 1:
            signal = max_poids * 0.8
        elif nb == 2:
            signal = max_poids * 0.9
        else:
            signal = min(max_poids + (nb - 2) * 0.05, 1.0)

    return round(signal, 3), "|".join(signaux) if signaux else "aucun", nb


def stars_to_signal(stars, score):
    mapping = {1: 0.90, 2: 0.70, 3: 0.40, 4: 0.20, 5: 0.05}
    return round(mapping.get(stars, 0.40) * score, 3)


def score_to_label(score):
    for label, (low, high) in SEUILS.items():
        if low <= score < high:
            return label
    return "negatif"


def score_to_confiance(score, label):
    low, high = SEUILS[label]
    position = (score - low) / (high - low)
    if position > 0.7:
        return "eleve"
    elif position > 0.3:
        return "moyen"
    return "faible"


def save_results(engine, results):
    """Insere les resultats dans PostgreSQL."""
    if not results:
        return
    df = pd.DataFrame(results)
    with engine.connect() as conn:
        for _, row in df.iterrows():
            sql = f"""
            INSERT INTO {TABLE_DEST} (
                id, description_clean, nb_emojis, nb_alert_emojis,
                sentiment_stars, sentiment_score, sentiment_signal,
                zeroshot_label, zeroshot_score, zeroshot_signal,
                rules_signal, rules_details, rules_count,
                score_final, label_final, confiance
            ) VALUES (
                :id, :description_clean, :nb_emojis, :nb_alert_emojis,
                :sentiment_stars, :sentiment_score, :sentiment_signal,
                :zeroshot_label, :zeroshot_score, :zeroshot_signal,
                :rules_signal, :rules_details, :rules_count,
                :score_final, :label_final, :confiance
            )
            ON CONFLICT (id) DO UPDATE SET
                score_final  = EXCLUDED.score_final,
                label_final  = EXCLUDED.label_final,
                analysed_at  = NOW();
            """
            conn.execute(text(sql), row.to_dict())
        conn.commit()


# ─────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ─────────────────────────────────────────────

def load_models():
    print("\n[...] Chargement des modeles...")
    sentiment_clf = hf_pipeline(
        "sentiment-analysis",
        model=SENTIMENT_MODEL,
        tokenizer=SENTIMENT_MODEL,
        max_length=512,
        truncation=True,
        device=-1
    )
    zeroshot_clf = hf_pipeline(
        "zero-shot-classification",
        model=ZEROSHOT_MODEL,
        device=-1
    )
    print("[OK] Modeles charges")
    return sentiment_clf, zeroshot_clf


def process_batch(df_batch, sentiment_clf, zeroshot_clf):
    results = []

    for _, row in df_batch.iterrows():
        raw_text = str(row.get("description", ""))
        listing_id = row["id"]

        # Extraction emojis AVANT nettoyage
        nb_emojis, nb_alert = extract_emoji_features(raw_text)

        # Nettoyage
        clean = clean_text(raw_text)

        # Filtre langue
        if detect_language(clean) not in ["fr", "unknown"]:
            continue

        if len(clean.split()) < 10:
            continue

        # Couche 1 — Sentiment
        try:
            sent_result = sentiment_clf([clean[:512]])[0]
            stars = int(sent_result["label"].split()[0])
            sent_score = round(sent_result["score"], 3)
            sent_signal = stars_to_signal(stars, sent_result["score"])
        except Exception:
            stars, sent_score, sent_signal = 3, 0.5, 0.20

        # Couche 2 — Zero-shot
        try:
            zs_result = zeroshot_clf(clean[:400], candidate_labels=HYPOTHESES)
            best_label = zs_result["labels"][0]
            best_score = zs_result["scores"][0]
            zs_label  = HYPOTHESIS_LABEL.get(best_label, "vague")
            zs_signal = round(HYPOTHESIS_SIGNAL.get(best_label, 0.45) * best_score, 3)
            zs_score  = round(best_score, 3)
        except Exception:
            zs_label, zs_score, zs_signal = "vague", 0.5, 0.20

        # Couche 3 — Regles
        rules_signal, rules_details, rules_count = apply_rules(clean, nb_emojis, nb_alert)

        # Score final
        score_final = round(
            POIDS_SENTIMENT * sent_signal +
            POIDS_ZEROSHOT  * zs_signal   +
            POIDS_REGLES    * rules_signal,
            3
        )
        label_final = score_to_label(score_final)
        confiance   = score_to_confiance(score_final, label_final)

        results.append({
            "id":                listing_id,
            "description_clean": clean,
            "nb_emojis":         nb_emojis,
            "nb_alert_emojis":   nb_alert,
            "sentiment_stars":   stars,
            "sentiment_score":   sent_score,
            "sentiment_signal":  sent_signal,
            "zeroshot_label":    zs_label,
            "zeroshot_score":    zs_score,
            "zeroshot_signal":   zs_signal,
            "rules_signal":      rules_signal,
            "rules_details":     rules_details,
            "rules_count":       rules_count,
            "score_final":       score_final,
            "label_final":       label_final,
            "confiance":         confiance,
        })

    return results


def run_batch(engine, sentiment_clf, zeroshot_clf, reset=False):
    """Mode batch — traite toutes les annonces non analysees."""
    if reset:
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {TABLE_DEST}"))
            conn.commit()
        print("[OK] Table resetee")

    df = get_unanalysed(engine)
    total = len(df)

    if total == 0:
        print("[OK] Toutes les annonces sont deja analysees")
        return

    print(f"\n[INFO] {total} annonces a analyser")
    all_results = []

    for i in tqdm(range(0, total, BATCH_SIZE), desc="Pipeline"):
        batch = df.iloc[i:i + BATCH_SIZE]
        results = process_batch(batch, sentiment_clf, zeroshot_clf)
        all_results.extend(results)

        # Sauvegarder par batch
        if len(all_results) >= BATCH_SIZE:
            save_results(engine, all_results)
            all_results = []

    # Sauvegarder le reste
    if all_results:
        save_results(engine, all_results)

    print(f"\n[OK] {total} annonces analysees et sauvegardees dans '{TABLE_DEST}'")

    # Rapport
    with engine.connect() as conn:
        result = pd.read_sql(
            text(f"SELECT label_final, COUNT(*) as n FROM {TABLE_DEST} GROUP BY label_final ORDER BY n DESC"),
            conn
        )
    print(f"\nDistribution des labels dans la base :")
    print(result.to_string(index=False))


def run_watch(engine, sentiment_clf, zeroshot_clf):
    """Mode watch — surveille les nouvelles annonces toutes les N secondes."""
    print(f"\n[WATCH] Surveillance active — check toutes les {WATCH_INTERVAL}s")
    print("[WATCH] Ctrl+C pour arreter\n")

    while True:
        try:
            df = get_unanalysed(engine)
            if len(df) > 0:
                print(f"\n[WATCH] {len(df)} nouvelle(s) annonce(s) detectee(s)")
                results = process_batch(df, sentiment_clf, zeroshot_clf)
                save_results(engine, results)
                print(f"[WATCH] {len(results)} annonces analysees")
                for r in results:
                    print(f"  ID {r['id']:5} → {r['label_final']:15} (score={r['score_final']})")
            else:
                print(f"[WATCH] Aucune nouvelle annonce — prochain check dans {WATCH_INTERVAL}s")

            time.sleep(WATCH_INTERVAL)

        except KeyboardInterrupt:
            print("\n[WATCH] Arret demande")
            break


def main():
    parser = argparse.ArgumentParser(description="Estate Mind — Pipeline Sentiment")
    parser.add_argument("--watch", action="store_true", help="Mode surveillance temps reel")
    parser.add_argument("--reset", action="store_true", help="Reanalyse toutes les annonces")
    args = parser.parse_args()

    # Connexion
    engine = get_engine()
    print("[OK] Connexion PostgreSQL etablie")

    # Creer la table si necessaire
    create_table(engine)

    # Charger les modeles
    sentiment_clf, zeroshot_clf = load_models()

    # Lancer le pipeline
    if args.watch:
        run_watch(engine, sentiment_clf, zeroshot_clf)
    else:
        run_batch(engine, sentiment_clf, zeroshot_clf, reset=args.reset)


if __name__ == "__main__":
    main()