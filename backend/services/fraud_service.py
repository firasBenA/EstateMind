# services/fraud_service.py

import json
import re
import ftfy
from pathlib import Path
from typing import Dict, Any
from collections import defaultdict
from django.db import connection
from django.core.cache import cache
from transformers import pipeline as hf_pipeline
import numpy as np
from typing import Dict, Any, List
# ── CHEMINS DES MODELES ──
BASE_DIR = Path(__file__).resolve().parent.parent.parent
BERT_PATH = BASE_DIR / "data" / "models" / "bert_sentiment"
MINILM_PATH = BASE_DIR / "data" / "models" / "minilm_zeroshot"

# ── CONFIG PIPELINE ──
HYPOTHESES = [
    "annonce immobiliere trompeuse et frauduleuse",
    "annonce immobiliere avec ton pressant et urgent",
    "annonce immobiliere vague ou incomplete",
    "annonce immobiliere fiable et professionnelle",
]

HYPOTHESIS_SIGNAL = {
    "annonce immobiliere trompeuse et frauduleuse": 0.95,
    "annonce immobiliere avec ton pressant et urgent": 0.70,
    "annonce immobiliere vague ou incomplete": 0.45,
    "annonce immobiliere fiable et professionnelle": 0.05,
}

HYPOTHESIS_LABEL = {
    "annonce immobiliere trompeuse et frauduleuse": "trompeuse",
    "annonce immobiliere avec ton pressant et urgent": "pressante",
    "annonce immobiliere vague ou incomplete": "vague",
    "annonce immobiliere fiable et professionnelle": "fiable",
}

POIDS_SENTIMENT = 0.20
POIDS_ZEROSHOT = 0.20
POIDS_REGLES = 0.60

SEUILS = {
    "positif": (0.00, 0.20),
    "neutre_positif": (0.20, 0.40),
    "neutre_negatif": (0.40, 0.65),
    "negatif": (0.65, 1.00),
}

WARNINGS = {
    "negatif": "⚠️ POSSIBLY FRAUDULENT",
    "neutre_negatif": "⚠️ SUSPICIOUS — Review recommended",
    "neutre_positif": "✅ Listing appears legitimate",
    "positif": "✅ Trustworthy listing",
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

# ── CHARGEMENT DES MODELES (lazy loading) ──
_sentiment_clf = None
_zeroshot_clf = None


def get_sentiment_classifier():
    global _sentiment_clf
    if _sentiment_clf is None:
        print("[...] Loading BERT sentiment model...")
        _sentiment_clf = hf_pipeline(
            "sentiment-analysis",
            model=str(BERT_PATH),
            tokenizer=str(BERT_PATH),
            max_length=512,
            truncation=True,
            device=-1
        )
        print("[OK] BERT loaded")
    return _sentiment_clf


def get_zeroshot_classifier():
    global _zeroshot_clf
    if _zeroshot_clf is None:
        print("[...] Loading MiniLM zero-shot model...")
        _zeroshot_clf = hf_pipeline(
            "zero-shot-classification",
            model=str(MINILM_PATH),
            device=-1
        )
        print("[OK] MiniLM loaded")
    return _zeroshot_clf


# ── FONCTIONS UTILITAIRES ──
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
    total = max(len(text.replace(" ", "")), 1)
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


def analyze_listing_description(description: str) -> Dict[str, Any]:
    """
    Analyse une description d'annonce et retourne un score de risque.
    """
    raw_text = description
    nb_emojis, nb_alert = extract_emoji_features(raw_text)
    clean = clean_text(raw_text)

    # Filtre langue
    if detect_language(clean) not in ["fr", "unknown"] or len(clean.split()) < 10:
        return {
            "skipped": True,
            "warning": "Description too short or non-French",
            "score_final": 0.5,
            "label_final": "neutre_positif",
            "confiance": "moyen",
        }

    # Couche 1 — BERT
    try:
        sentiment_clf = get_sentiment_classifier()
        sent_result = sentiment_clf([clean[:512]])[0]
        stars = int(sent_result["label"].split()[0])
        sent_score = round(sent_result["score"], 3)
        sent_signal = stars_to_signal(stars, sent_result["score"])
    except Exception:
        stars, sent_score, sent_signal = 3, 0.5, 0.20

    # Couche 2 — MiniLM zero-shot
    try:
        zeroshot_clf = get_zeroshot_classifier()
        zs_result = zeroshot_clf(clean[:400], candidate_labels=HYPOTHESES)
        best_label = zs_result["labels"][0]
        best_score = zs_result["scores"][0]
        zs_label = HYPOTHESIS_LABEL.get(best_label, "vague")
        zs_signal = round(HYPOTHESIS_SIGNAL.get(best_label, 0.45) * best_score, 3)
        zs_score = round(best_score, 3)
    except Exception:
        zs_label, zs_score, zs_signal = "vague", 0.5, 0.20

    # Couche 3 — Règles
    rules_signal, rules_details, rules_count = apply_rules(clean, nb_emojis, nb_alert)

    # Score final
    score_final = round(
        POIDS_SENTIMENT * sent_signal +
        POIDS_ZEROSHOT * zs_signal +
        POIDS_REGLES * rules_signal,
        3
    )
    label_final = score_to_label(score_final)
    confiance = score_to_confiance(score_final, label_final)
    warning = WARNINGS[label_final]

    return {
        "listing_id": None,
        "warning": warning,
        "label_final": label_final,
        "score_final": score_final,
        "confiance": confiance,
        "rules_details": rules_details,
        "rules_count": rules_count,
        "sentiment_stars": stars,
        "sentiment_score": sent_score,
        "zeroshot_label": zs_label,
        "zeroshot_score": zs_score,
        "skipped": False,
    }



def analyze_listing_description_by_id(listing_id: str) -> Dict[str, Any]:
    """
    Analyse la description d'une annonce par son ID
    """
    from services.models import Listing  # Import ici pour éviter circular import
    
    try:
        listing = Listing.objects.get(id=listing_id)
        description = listing.description or ""
        
        result = analyze_listing_description(description)
        result["listing_id"] = listing_id
        result["title"] = listing.title
        
        return result
    except Listing.DoesNotExist:
        return {
            "error": "Listing not found",
            "skipped": True
        }
    except Exception as e:
        return {
            "error": str(e),
            "skipped": True
        }


def get_fraud_score_for_listing(listing_id: str) -> Dict[str, Any]:
    """
    Récupère le score de fraude pour une annonce depuis la base ou le calcule.
    """
    cache_key = f"fraud_score_{listing_id}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    try:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT multimodal_score, mismatch_types, price_deviation_pct, images_analyzed
                FROM fraud_detection_results
                WHERE property_id = %s
                ORDER BY analyzed_at DESC
                LIMIT 1
            """, [listing_id])
            row = cur.fetchone()

        if row:
            score = float(row[0]) if row[0] else 0.5
            result = {
                "score": score,
                "risk_level": "incoherent" if score < 0.31 else ("suspect" if score < 0.56 else "coherent"),
                "mismatch_types": row[1] if row[1] else [],
                "price_deviation": float(row[2]) if row[2] else 0,
                "images_analyzed": row[3] or 0,
                "cached": True,
            }
        else:
            result = {
                "score": 0.5,
                "risk_level": "suspect",
                "mismatch_types": [],
                "price_deviation": 0,
                "images_analyzed": 0,
                "cached": False,
            }
    except Exception:
        result = {"score": 0.5, "risk_level": "suspect", "mismatch_types": [], "price_deviation": 0, "images_analyzed": 0}

    cache.set(cache_key, result, 3600)  # Cache 1 heure
    return result


# Ajoutez ces fonctions à la fin du fichier

def get_text_analysis_summary():
    """Récupère le résumé des analyses textuelles depuis sentiment_analysis"""
    from django.db import connection
    
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN label_final = 'negatif' THEN 1 ELSE 0 END) as negatif,
                    SUM(CASE WHEN label_final = 'neutre_negatif' THEN 1 ELSE 0 END) as neutre_negatif,
                    SUM(CASE WHEN label_final = 'neutre_positif' THEN 1 ELSE 0 END) as neutre_positif,
                    SUM(CASE WHEN label_final = 'positif' THEN 1 ELSE 0 END) as positif,
                    COALESCE(AVG(score_final), 0) as avg_score,
                    COALESCE(AVG(sentiment_score), 0) as avg_sentiment_score,
                    COALESCE(AVG(zeroshot_score), 0) as avg_zeroshot_score,
                    SUM(CASE WHEN description_clean IS NULL OR description_clean = '' THEN 1 ELSE 0 END) as skipped_count
                FROM sentiment_analysis
            """)
            row = cursor.fetchone()
            
        return {
            "total_analyzed": row[0] or 0,
            "negatif": row[1] or 0,
            "neutre_negatif": row[2] or 0,
            "neutre_positif": row[3] or 0,
            "positif": row[4] or 0,
            "avg_score": float(row[5]) if row[5] else 0,
            "avg_sentiment_score": float(row[6]) if row[6] else 0,
            "avg_zeroshot_score": float(row[7]) if row[7] else 0,
            "skipped_count": row[8] or 0,
        }
    except Exception as e:
        print(f"Error in get_text_analysis_summary: {e}")
        return {
            "total_analyzed": 0,
            "negatif": 0,
            "neutre_negatif": 0,
            "neutre_positif": 0,
            "positif": 0,
            "avg_score": 0,
            "avg_sentiment_score": 0,
            "avg_zeroshot_score": 0,
            "skipped_count": 0,
        }


def get_text_analysis_listings(risk: str, page: int, page_size: int):
    """Récupère la liste paginée des analyses textuelles depuis sentiment_analysis"""
    from django.db import connection
    
    try:
        offset = (page - 1) * page_size
        
        with connection.cursor() as cursor:
            # Récupérer le total
            cursor.execute("""
                SELECT COUNT(*)
                FROM sentiment_analysis s
                WHERE s.label_final = %s
            """, [risk])
            total = cursor.fetchone()[0] or 0
            
            # Récupérer les données paginées
            cursor.execute("""
                SELECT 
                    s.listing_id,
                    COALESCE(l.title, 'Sans titre') as title,
                    COALESCE(l.source_name, 'unknown') as source_name,
                    COALESCE(l.city, '') as city,
                    COALESCE(l.property_type, '') as property_type,
                    s.score_final,
                    s.label_final,
                    COALESCE(s.sentiment_stars, 0) as sentiment_stars,
                    COALESCE(s.sentiment_score, 0) as sentiment_score,
                    COALESCE(s.zeroshot_label, 'N/A') as zeroshot_label,
                    COALESCE(s.zeroshot_score, 0) as zeroshot_score,
                    COALESCE(s.rules_details, '') as rules_details,
                    COALESCE(s.rules_count, 0) as rules_count,
                    COALESCE(s.nb_emojis, 0) as nb_emojis,
                    s.analysed_at,
                    l.url
                FROM sentiment_analysis s
                LEFT JOIN listings l ON s.listing_id = l.id::text
                WHERE s.label_final = %s
                ORDER BY s.analysed_at DESC
                LIMIT %s OFFSET %s
            """, [risk, page_size, offset])
            
            rows = cursor.fetchall()
        
        results = []
        for row in rows:
            results.append({
                "listing_id": row[0],
                "title": row[1],
                "source_name": row[2],
                "city": row[3] or "",
                "property_type": row[4] or "",
                "score_final": float(row[5]) if row[5] else 0,
                "label_final": row[6],
                "sentiment_stars": row[7] or 0,
                "sentiment_score": float(row[8]) if row[8] else 0,
                "zeroshot_label": row[9] or "N/A",
                "zeroshot_score": float(row[10]) if row[10] else 0,
                "rules_details": row[11] or "",
                "rules_count": row[12] or 0,
                "nb_emojis": row[13] or 0,
                "analyzed_at": row[14].isoformat() if row[14] else None,
                "url": row[15] or "",
            })
        
        return {
            "count": total,
            "pages": (total + page_size - 1) // page_size,
            "page": page,
            "results": results,
        }
    except Exception as e:
        print(f"Error in get_text_analysis_listings: {e}")
        return {
            "count": 0,
            "pages": 0,
            "page": page,
            "results": [],
        }


def get_text_rule_stats():
    """Récupère les statistiques des règles déclenchées depuis sentiment_analysis"""
    from django.db import connection
    from collections import defaultdict
    
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT rules_details, COUNT(*) as count
                FROM sentiment_analysis
                WHERE rules_details IS NOT NULL AND rules_details != 'aucun'
                GROUP BY rules_details
                ORDER BY count DESC
                LIMIT 100
            """)
            rows = cursor.fetchall()
        
        # Compter chaque règle individuellement
        rule_counts = defaultdict(int)
        for row in rows:
            rules_details = row[0]
            count = row[1]
            if rules_details:
                for rule in rules_details.split('|'):
                    rule = rule.strip()
                    if rule and rule != 'aucun':
                        rule_counts[rule] += count
        
        # Convertir en liste triée
        rules_list = [{"rule": k, "count": v} for k, v in rule_counts.items()]
        rules_list.sort(key=lambda x: x["count"], reverse=True)
        
        return rules_list[:20]
    except Exception as e:
        print(f"Error in get_text_rule_stats: {e}")
        return []


def save_text_analysis_result(listing_id: str, analysis_result: dict):
    """Sauvegarde le résultat de l'analyse textuelle dans sentiment_analysis"""
    from django.db import connection
    from django.utils import timezone
    
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO sentiment_analysis (
                    listing_id, score_final, label_final, 
                    sentiment_stars, sentiment_score, zeroshot_label, 
                    zeroshot_score, rules_details, rules_count, 
                    nb_emojis, nb_alert_emojis, warning, analysed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (listing_id) DO UPDATE SET
                    score_final = EXCLUDED.score_final,
                    label_final = EXCLUDED.label_final,
                    sentiment_stars = EXCLUDED.sentiment_stars,
                    sentiment_score = EXCLUDED.sentiment_score,
                    zeroshot_label = EXCLUDED.zeroshot_label,
                    zeroshot_score = EXCLUDED.zeroshot_score,
                    rules_details = EXCLUDED.rules_details,
                    rules_count = EXCLUDED.rules_count,
                    nb_emojis = EXCLUDED.nb_emojis,
                    nb_alert_emojis = EXCLUDED.nb_alert_emojis,
                    warning = EXCLUDED.warning,
                    analysed_at = EXCLUDED.analysed_at
            """, [
                listing_id,
                analysis_result.get("score_final", 0),
                analysis_result.get("label_final", "neutre_positif"),
                analysis_result.get("sentiment_stars"),
                analysis_result.get("sentiment_score"),
                analysis_result.get("zeroshot_label"),
                analysis_result.get("zeroshot_score"),
                analysis_result.get("rules_details"),
                analysis_result.get("rules_count", 0),
                analysis_result.get("nb_emojis", 0),
                analysis_result.get("nb_alert_emojis", 0),
                analysis_result.get("warning", ""),
                timezone.now()
            ])
    except Exception as e:
        print(f"Error saving text analysis for {listing_id}: {e}")