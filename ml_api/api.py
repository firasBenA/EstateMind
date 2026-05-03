"""
Estate Mind — API FastAPI pour l'analyse de sentiment
=====================================================
Expose le pipeline sentiment comme un service HTTP.

Usage :
    uvicorn api:app --reload --port 8001

Endpoint :
    POST /analyze
    Body : { "listing_id": 123, "description": "..." }
"""

import os
import re
import ftfy
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from transformers import pipeline as hf_pipeline

# ─────────────────────────────────────────────
# CHEMINS DES MODELES
# ─────────────────────────────────────────────
BASE_DIR       = Path(__file__).parent.parent
BERT_PATH      = BASE_DIR / "data" / "models" / "bert_sentiment"
MINILM_PATH    = BASE_DIR / "data" / "models" / "minilm_zeroshot"

# ─────────────────────────────────────────────
# CONFIG PIPELINE (repris de pipeline_sentiment.py)
# ─────────────────────────────────────────────
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

WARNINGS = {
    "negatif":        "POSSIBLY FRAUDULENT",
    "neutre_negatif": "SUSPICIOUS — Review recommended",
    "neutre_positif": "Listing appears legitimate",
    "positif":        "Trustworthy listing",
}

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
# CHARGEMENT DES MODELES AU DEMARRAGE
# ─────────────────────────────────────────────
print("[...] Chargement des modeles...")
sentiment_clf = hf_pipeline(
    "sentiment-analysis",
    model=str(BERT_PATH),
    tokenizer=str(BERT_PATH),
    max_length=512,
    truncation=True,
    device=-1
)
zeroshot_clf = hf_pipeline(
    "zero-shot-classification",
    model=str(MINILM_PATH),
    device=-1
)
print("[OK] Modeles charges")

# ─────────────────────────────────────────────
# FASTAPI
# ─────────────────────────────────────────────
app = FastAPI(title="Estate Mind — Sentiment API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class AnalyzeRequest(BaseModel):
    listing_id: int
    description: str

# ─────────────────────────────────────────────
# FONCTIONS UTILITAIRES
# ─────────────────────────────────────────────
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
# ENDPOINT
# ─────────────────────────────────────────────
@app.post("/analyze")
def analyze(req: AnalyzeRequest):
    raw_text = req.description
    nb_emojis, nb_alert = extract_emoji_features(raw_text)
    clean = clean_text(raw_text)

    # Filtre langue
    if detect_language(clean) not in ["fr", "unknown"]:
        return {"listing_id": req.listing_id, "warning": "Langue non supportee", "skipped": True}
    if len(clean.split()) < 10:
        return {"listing_id": req.listing_id, "warning": "Description trop courte", "skipped": True}

    # Couche 1 — BERT
    try:
        sent_result = sentiment_clf([clean[:512]])[0]
        stars       = int(sent_result["label"].split()[0])
        sent_score  = round(sent_result["score"], 3)
        sent_signal = stars_to_signal(stars, sent_result["score"])
    except Exception:
        stars, sent_score, sent_signal = 3, 0.5, 0.20

    # Couche 2 — MiniLM zero-shot
    try:
        zs_result  = zeroshot_clf(clean[:400], candidate_labels=HYPOTHESES)
        best_label = zs_result["labels"][0]
        best_score = zs_result["scores"][0]
        zs_label   = HYPOTHESIS_LABEL.get(best_label, "vague")
        zs_signal  = round(HYPOTHESIS_SIGNAL.get(best_label, 0.45) * best_score, 3)
        zs_score   = round(best_score, 3)
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
    warning     = WARNINGS[label_final]

    return {
        "listing_id":       req.listing_id,
        "warning":          warning,
        "label_final":      label_final,
        "score_final":      score_final,
        "confiance":        confiance,
        "rules_details":    rules_details,
        "rules_count":      rules_count,
        "sentiment_stars":  stars,
        "sentiment_score":  sent_score,
        "zeroshot_label":   zs_label,
        "zeroshot_score":   zs_score,
        "skipped":          False,
    }

@app.get("/health")
def health():
    return {"status": "ok"}