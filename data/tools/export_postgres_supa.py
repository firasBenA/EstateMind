"""
Estate Mind — Export PostgreSQL local vers Supabase
====================================================
Lit depuis sentiment_analysis (PostgreSQL local)
Exporte vers sentiment_analysis (Supabase)

Usage :
    python export_postgres_supa.py

Prerequis :
    pip install supabase sqlalchemy psycopg2-binary pandas python-dotenv
"""

import os
import pandas as pd
from dotenv import load_dotenv
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from supabase import create_client

load_dotenv()

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5433)),
    "dbname":   os.getenv("POSTGRES_DB", "estatemind"),
    "user":     os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "capTEEMO500"),
}

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

BATCH_SIZE = 100

WARNINGS = {
    "negatif":        "POSSIBLY FRAUDULENT",
    "neutre_negatif": "SUSPICIOUS — Review recommended",
    "neutre_positif": "Listing appears legitimate",
    "positif":        "Trustworthy listing",
}


def get_engine():
    pwd = quote_plus(DB_CONFIG["password"])
    url = (f"postgresql+psycopg2://{DB_CONFIG['user']}:{pwd}"
           f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    return create_engine(url)


def main():
    # Connexion PostgreSQL local
    engine = get_engine()
    print("[OK] PostgreSQL local connecte")

    # Lire toutes les donnees
    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT * FROM sentiment_analysis"), conn)
    print(f"[OK] {len(df)} lignes chargees depuis PostgreSQL local")

    # Renommer la colonne id -> listing_id
    df = df.rename(columns={"id": "listing_id"})

    # Convertir les colonnes Timestamp en string
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
        df[col] = df[col].astype(str)

    # Convertir aussi analysed_at si elle existe
    if "analysed_at" in df.columns:
        df["analysed_at"] = df["analysed_at"].astype(str)

    # Ajouter la colonne warning
    df["warning"] = df["label_final"].map(WARNINGS).fillna("Unknown")

    # Nettoyer les NaN
    df = df.where(pd.notna(df), None)

    # Connexion Supabase
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("[OK] Supabase connecte")

    # Export par batch
    total    = len(df)
    inserted = 0
    errors   = 0

    print(f"\n[...] Export de {total} lignes vers Supabase...\n")

    for i in range(0, total, BATCH_SIZE):
        batch   = df.iloc[i:i + BATCH_SIZE]
        records = batch.to_dict(orient="records")

        # Nettoyer les valeurs et types
        clean_records = []
        for record in records:
            clean = {}
            for k, v in record.items():
                try:
                    if pd.isna(v):
                        clean[k] = None
                    elif hasattr(v, 'item'):
                        clean[k] = v.item()
                    else:
                        clean[k] = v
                except Exception:
                    clean[k] = str(v) if v is not None else None
            clean_records.append(clean)

        try:
            supabase.table("sentiment_analysis").upsert(clean_records).execute()
            inserted += len(clean_records)
            print(f"  {min(i + BATCH_SIZE, total)}/{total} lignes exportees...")
        except Exception as e:
            errors += len(clean_records)
            print(f"  [ERREUR] batch {i} : {str(e)[:200]}")

    print(f"\n{'='*50}")
    print("RAPPORT EXPORT")
    print(f"{'='*50}")
    print(f"  Exportees avec succes : {inserted}")
    print(f"  Erreurs               : {errors}")

    # Verification dans Supabase
    try:
        response = supabase.table("sentiment_analysis").select("label_final", count="exact").execute()
        print(f"\n[OK] Total dans Supabase : {response.count} lignes")
    except Exception as e:
        print(f"\n[WARN] Verification impossible : {e}")

    print(f"\nDistribution des warnings :")
    print(df["warning"].value_counts().to_string())


if __name__ == "__main__":
    main()