"""
Estate Mind — Export des modèles NLP (save_pretrained)
=======================================================
Sauvegarde BERT et MiniLM dans leur format natif Hugging Face.

Usage :
    python export_models.py

Sortie :
    models/bert_sentiment/
    models/minilm_zeroshot/
"""

import os
from transformers import pipeline, AutoModelForSequenceClassification, AutoTokenizer

os.makedirs("models/bert_sentiment", exist_ok=True)
os.makedirs("models/minilm_zeroshot", exist_ok=True)

# ─────────────────────────────────────────────
# BERT multilingue — Couche 1
# ─────────────────────────────────────────────
print("[...] Chargement BERT multilingue...")
BERT_MODEL = "nlptown/bert-base-multilingual-uncased-sentiment"

bert_model     = AutoModelForSequenceClassification.from_pretrained(BERT_MODEL)
bert_tokenizer = AutoTokenizer.from_pretrained(BERT_MODEL)

bert_model.save_pretrained("models/bert_sentiment")
bert_tokenizer.save_pretrained("models/bert_sentiment")
print("[OK] BERT sauvegarde : models/bert_sentiment/")

# ─────────────────────────────────────────────
# MiniLM zero-shot — Couche 2
# ─────────────────────────────────────────────
print("\n[...] Chargement MiniLM zero-shot...")
MINILM_MODEL = "cross-encoder/nli-MiniLM2-L6-H768"

minilm_model     = AutoModelForSequenceClassification.from_pretrained(MINILM_MODEL)
minilm_tokenizer = AutoTokenizer.from_pretrained(MINILM_MODEL)

minilm_model.save_pretrained("models/minilm_zeroshot")
minilm_tokenizer.save_pretrained("models/minilm_zeroshot")
print("[OK] MiniLM sauvegarde : models/minilm_zeroshot/")

print("\n[OK] Les deux modeles sont prets pour le backend")