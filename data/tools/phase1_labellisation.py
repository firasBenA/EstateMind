"""
Estate Mind — Phase 1 : Labellisation automatique avec Groq API
================================================================
Prérequis :
    pip install groq pandas tqdm python-dotenv

Usage :
    python phase1_labellisation.py

Durée estimée : 20 à 30 minutes pour 4381 descriptions
"""

import json
import time
import os
import pandas as pd
from tqdm import tqdm
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

# CONFIG
MODEL_NAME     = "llama-3.3-70b-versatile"
INPUT_CSV      = "./phase0_output/dataset_clean.csv"
OUTPUT_CSV     = "./phase0_output/dataset_labelled.csv"
CHECKPOINT_CSV = "./phase0_output/checkpoint_labelled_v5.csv"
BATCH_SIZE     = 50
TEMPERATURE    = 0.1
MAX_TOKENS     = 150
TEST_MODE      = True   # mettre False pour le dataset complet

SYSTEM_PROMPT = (
    "Tu es un expert en detection de fraude immobiliere en Tunisie. Sois strict et conservateur.\n\n"
    "Classes (du moins au plus suspect) :\n\n"
    "- positif : TOUS ces elements reunis : agence identifiee avec email/site ET prix affiche ET statut juridique explicite (titre foncier ou attestation) ET composition detaillee. TRES RARE, moins de 20% des annonces.\n"
    "- neutre_positif : annonce honnete MAIS manque au moins un element cle parmi : prix, statut juridique, email agence. CLASSE PAR DEFAUT si rien de suspect.\n"
    "- neutre_negatif : presence d'au moins UN signal suspect : 2+ numeros WhatsApp, emojis > 5, frais de visite, pression achat (occasion rare, ne pas rater), prix absent sur bien > 500k DT, template copier-coller, agence non verifiable.\n"
    "- negatif : activement trompeur : ciblage explicite etrangers/expatries, prix anormalement bas, urgence excessive repetee, hashtags reseaux sociaux, promesses rentabilite sans chiffres, fautes grossieres sur bien de grande valeur.\n\n"
    "REGLES IMPORTANTES :\n"
    "- Si tu hesites entre positif et neutre_positif -> choisis neutre_positif\n"
    "- Si tu hesites entre neutre_positif et neutre_negatif -> cherche les signaux suspects\n"
    "- Une agence connue (Century21, NewKey) ne suffit pas pour positif si prix ou statut manquent\n\n"
    'Reponds UNIQUEMENT en JSON : {"label": "positif|neutre_positif|neutre_negatif|negatif", "score": 0.0-1.0, "raison": "max 15 mots"}'
)

FEW_SHOT_EXAMPLES = [
    {"role": "user", "content": "Description : local commercial 130m2 gold carthage immobiliere, loyer 6000 dinars/mois hors retenue, local a l etat brut sauf restauration, contact: 52083333"},
    {"role": "assistant", "content": '{"label": "positif", "score": 0.92, "raison": "Agence identifiee, prix clair, limitations mentionnees"}'},
    {"role": "user", "content": "Description : a louer appartement s2 derriere carrefour el wahat, salon kitchenette equipee, chauffage central, contacter au 90531973"},
    {"role": "assistant", "content": '{"label": "neutre_positif", "score": 0.75, "raison": "Honnete mais sans prix ni superficie ni agence"}'},
    {"role": "user", "content": "Description : TERRAIN KANTAOUI Opportunite rare ! WhatsApp: 55660538 // 28657936 // 50802150 WhatsApp: 216 55660538"},
    {"role": "assistant", "content": '{"label": "neutre_negatif", "score": 0.75, "raison": "3 numeros WhatsApp, pression achat, pas agence"}'},
    {"role": "user", "content": "Description : offre exclusif pour etranger, appartement luxueusement meuble, 2 mois caution, gsm proprietaire 216 55690000"},
    {"role": "assistant", "content": '{"label": "negatif", "score": 0.91, "raison": "Ciblage explicite etrangers, contact unique sans agence"}'},
]


def call_groq(client, description, retries=3):
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    messages += FEW_SHOT_EXAMPLES
    messages.append({"role": "user", "content": f"Description : {description[:400]}"})

    for attempt in range(retries):
        try:
            response = client.chat.completions.create(
                model=MODEL_NAME,
                messages=messages,
                temperature=TEMPERATURE,
                max_tokens=MAX_TOKENS,
            )
            raw = response.choices[0].message.content.strip()
            start = raw.find("{")
            end   = raw.rfind("}") + 1
            if start == -1 or end == 0:
                raise ValueError(f"Pas de JSON : {raw[:100]}")

            result = json.loads(raw[start:end])
            label  = result.get("label", "").strip().lower()

            if label not in ["positif", "neutre_positif", "neutre_negatif", "negatif"]:
                raise ValueError(f"Label invalide : {label}")

            score = float(result.get("score", 0.5))
            score = max(0.0, min(1.0, score))

            return {
                "label":  label,
                "score":  round(score, 3),
                "raison": str(result.get("raison", ""))[:200],
                "erreur": None
            }

        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1)
            else:
                print(f"ERREUR : {str(e)[:300]}")
                return {"label": "erreur", "score": 0.0, "raison": "", "erreur": str(e)[:200]}

def main():
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        print("[ERREUR] GROQ_API_KEY non trouvee dans .env")
        return

    client = Groq(api_key=api_key)
    print("[OK] Groq API initialisee")

    df = pd.read_csv(INPUT_CSV)
    print(f"[OK] {len(df)} descriptions chargees")

    if TEST_MODE:
        df = df.head(50)
        print("[TEST] Mode test : 50 premieres descriptions")

    try:
        df_done  = pd.read_csv(CHECKPOINT_CSV)
        done_ids = set(df_done["id"].tolist())
        print(f"[OK] Checkpoint : {len(done_ids)} descriptions deja traitees")
    except FileNotFoundError:
        df_done  = pd.DataFrame()
        done_ids = set()
        print("[INFO] Pas de checkpoint, demarrage depuis le debut")

    df_todo = df[~df["id"].isin(done_ids)].copy()
    print(f"[INFO] {len(df_todo)} descriptions a traiter")

    if len(df_todo) == 0:
        print("[OK] Tout est deja traite")
        return

    results = []
    errors  = 0

    print(f"\n[...] Labellisation via Groq ({MODEL_NAME})...\n")

    for _, row in tqdm(df_todo.iterrows(), total=len(df_todo), desc="Groq"):
        desc   = str(row.get("description_clean", ""))
        result = call_groq(client, desc)
        result["id"] = row["id"]

        if result["erreur"]:
            errors += 1

        results.append(result)

        if len(results) % BATCH_SIZE == 0:
            df_batch  = pd.DataFrame(results)
            df_merged = pd.concat([df_done, df_batch], ignore_index=True)
            df_merged.to_csv(CHECKPOINT_CSV, index=False)

    df_results = pd.DataFrame(results)
    df_all     = pd.concat([df_done, df_results], ignore_index=True)

    df_final = df.merge(
        df_all[["id", "label", "score", "raison", "erreur"]],
        on="id", how="left"
    )
    df_final.to_csv(OUTPUT_CSV, index=False)

    print(f"\n{'='*50}")
    print("RAPPORT DE LABELLISATION")
    print(f"{'='*50}")
    print(f"Total traite : {len(df_all)}")
    print(f"Erreurs      : {errors}")
    print(f"\nDistribution des labels :")
    print(df_final["label"].value_counts().to_string())
    print(f"\nDistribution des scores :")
    print(f"  Score >= 0.85 (fiables LoRA) : {(df_final['score'] >= 0.85).sum()}")
    print(f"  Score >= 0.70               : {(df_final['score'] >= 0.70).sum()}")
    print(f"  Score <  0.60 (incertains)  : {(df_final['score'] < 0.60).sum()}")
    print(f"\n[OK] Dataset labellise : {OUTPUT_CSV}")


if __name__ == "__main__":
    main()