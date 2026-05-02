"""
Estate Mind — Pipeline Sentiment Production (Supabase)
=======================================================
Lit depuis Supabase (table listings)
Analyse le sentiment en 3 couches
Ecrit les resultats dans Supabase (table sentiment_analysis)
Retourne un warning de fraude pour chaque annonce

Modes :
    python pipeline_supabase.py             # batch toutes les annonces
    python pipeline_supabase.py --watch     # surveillance temps reel
    python pipeline_supabase.py --reset     # reanalyse tout

Prerequis :
    pip install supabase transformers torch pandas tqdm python-dotenv ftfy
"""

import os
import re
import time
import ftfy
import argparse
from tqdm import tqdm
from dotenv import load_dotenv
from transformers import pipeline as hf_pipeline
from supabase import create_client

load_dotenv()

# ─────────────────────────────────────────────
# CONFIG SUPABASE
# ─────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

TABLE_SOURCE   = "listings"
TABLE_DEST     = "sentiment_analysis"
BATCH_SIZE     = 32
WATCH_INTERVAL = 60  # secondes

# ─────────────────────────────────────────────
# CONFIG MODELES
# ─────────────────────────────────────────────
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

# Poids des couches
POIDS_SENTIMENT = 0.20
POIDS_ZEROSHOT  = 0.20
POIDS_REGLES    = 0.60

# Seuils de classification
SEUILS = {
    "positif":        (0.00, 0.20),
    "neutre_positif": (0.20, 0.40),
    "neutre_negatif": (0.40, 0.65),
    "negatif":        (0.65, 1.00),
}

# Warning affiché dans l'application
WARNINGS = {
    "negatif":        "POSSIBLY FRAUDULENT",
    "neutre_negatif": "SUSPICIOUS — Review recommended",
    "neutre_positif": "Listing appears legitimate",
    "positif":        "Trustworthy listing",
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

def get_supabase():
    return create_client(SUPABASE_URL, SUPABASE_KEY)


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


def clean_text(text):
    if not isinstance(text, str):
        return ""
    text = ftfy.fix_text(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = EMOJI_PATTERN.sub(" ", text)
    text = re.sub(r"[^\w\sàâäéèêëîïôùûüçœæ.,;:!?()\-'/]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


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
        if "pattern" in regle and regle["pattern"].search(text):
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


# ─────────────────────────────────────────────
# SUPABASE — LECTURE / ECRITURE
# ─────────────────────────────────────────────

def get_analysed_ids(supabase):
    """Retourne les IDs deja analyses dans sentiment_analysis."""
    try:
        response = supabase.table(TABLE_DEST).select("listing_id").execute()
        return {row["listing_id"] for row in response.data}
    except Exception:
        return set()


def get_unanalysed_listings(supabase, already_done):
    """Recupere les annonces non encore analysees depuis Supabase."""
    response = (
        supabase.table(TABLE_SOURCE)
        .select("id, description")
        .not_.is_("description", "null")
        .execute()
    )
    listings = [
        l for l in response.data
        if l["id"] not in already_done
        and isinstance(l.get("description"), str)
        and len(l["description"]) > 50
    ]
    return listings


def save_to_supabase(supabase, results):
    """Sauvegarde les resultats dans Supabase."""
    if not results:
        return
    try:
        supabase.table(TABLE_DEST).upsert(results).execute()
    except Exception as e:
        print(f"[ERREUR SAVE] {str(e)[:150]}")


# ─────────────────────────────────────────────
# ANALYSE D'UNE DESCRIPTION
# ─────────────────────────────────────────────

def analyse_listing(listing, sentiment_clf, zeroshot_clf):
    """Analyse une annonce et retourne le resultat complet avec warning."""
    listing_id = listing["id"]
    raw_text   = listing.get("description", "")

    # Extraction emojis AVANT nettoyage
    nb_emojis, nb_alert = extract_emoji_features(raw_text)

    # Nettoyage
    clean = clean_text(raw_text)

    # Filtre langue
    if detect_language(clean) not in ["fr", "unknown"]:
        return None
    if len(clean.split()) < 10:
        return None

    # Couche 1 — Sentiment BERT
    try:
        sent_result  = sentiment_clf([clean[:512]])[0]
        stars        = int(sent_result["label"].split()[0])
        sent_score   = round(sent_result["score"], 3)
        sent_signal  = stars_to_signal(stars, sent_result["score"])
    except Exception:
        stars, sent_score, sent_signal = 3, 0.5, 0.20

    # Couche 2 — Zero-shot MiniLM
    try:
        zs_result  = zeroshot_clf(clean[:400], candidate_labels=HYPOTHESES)
        best_label = zs_result["labels"][0]
        best_score = zs_result["scores"][0]
        zs_label   = HYPOTHESIS_LABEL.get(best_label, "vague")
        zs_signal  = round(HYPOTHESIS_SIGNAL.get(best_label, 0.45) * best_score, 3)
        zs_score   = round(best_score, 3)
    except Exception:
        zs_label, zs_score, zs_signal = "vague", 0.5, 0.20

    # Couche 3 — Regles linguistiques
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
    warning     = WARNINGS[label_final]

    return {
        "listing_id":        listing_id,
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
        "warning":           warning,
    }


# ─────────────────────────────────────────────
# MODES BATCH ET WATCH
# ─────────────────────────────────────────────

def run_batch(supabase, sentiment_clf, zeroshot_clf, reset=False):
    """Traite toutes les annonces non encore analysees."""
    if reset:
        try:
            supabase.table(TABLE_DEST).delete().neq("listing_id", 0).execute()
            print("[OK] Table resetee")
        except Exception as e:
            print(f"[ERREUR RESET] {e}")

    already_done = get_analysed_ids(supabase)
    listings     = get_unanalysed_listings(supabase, already_done)
    total        = len(listings)

    if total == 0:
        print("[OK] Toutes les annonces sont deja analysees")
        return

    print(f"\n[INFO] {total} annonces a analyser")
    results = []

    for listing in tqdm(listings, desc="Pipeline Supabase"):
        result = analyse_listing(listing, sentiment_clf, zeroshot_clf)
        if result:
            results.append(result)

        if len(results) >= BATCH_SIZE:
            save_to_supabase(supabase, results)
            results = []

    if results:
        save_to_supabase(supabase, results)

    print(f"\n[OK] {total} annonces analysees")
    print("\nDistribution des warnings :")
    from collections import Counter
    warnings_count = Counter(r["warning"] for r in results if r)
    for w, n in warnings_count.most_common():
        print(f"  {w:40s} : {n}")


def run_watch(supabase, sentiment_clf, zeroshot_clf):
    """Surveille les nouvelles annonces toutes les N secondes."""
    print(f"\n[WATCH] Surveillance active — check toutes les {WATCH_INTERVAL}s")
    print("[WATCH] Ctrl+C pour arreter\n")

    while True:
        try:
            already_done = get_analysed_ids(supabase)
            listings     = get_unanalysed_listings(supabase, already_done)

            if listings:
                print(f"\n[WATCH] {len(listings)} nouvelle(s) annonce(s) detectee(s)")
                results = []
                for listing in listings:
                    result = analyse_listing(listing, sentiment_clf, zeroshot_clf)
                    if result:
                        results.append(result)
                        print(f"  ID {result['listing_id']:5} | {result['label_final']:15} | "
                              f"score={result['score_final']} | {result['warning']}")

                save_to_supabase(supabase, results)
                print(f"[WATCH] {len(results)} annonces analysees et sauvegardees")
            else:
                print(f"[WATCH] Aucune nouvelle annonce — prochain check dans {WATCH_INTERVAL}s")

            time.sleep(WATCH_INTERVAL)

        except KeyboardInterrupt:
            print("\n[WATCH] Arret demande")
            break


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Estate Mind — Pipeline Sentiment Supabase")
    parser.add_argument("--watch", action="store_true", help="Mode surveillance temps reel")
    parser.add_argument("--reset", action="store_true", help="Reanalyse toutes les annonces")
    args = parser.parse_args()

    # Connexion Supabase
    supabase = get_supabase()
    print("[OK] Supabase connecte")

    # Charger les modeles
    print("\n[...] Chargement des modeles NLP...")
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

    # Lancer le pipeline
    if args.watch:
        run_watch(supabase, sentiment_clf, zeroshot_clf)
    else:
        run_batch(supabase, sentiment_clf, zeroshot_clf, reset=args.reset)


if __name__ == "__main__":
    main()