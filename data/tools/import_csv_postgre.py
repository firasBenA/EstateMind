"""
Estate Mind — Import CSV vers PostgreSQL
=========================================
Importe le dataset_final.csv dans la table sentiment_analysis.

Usage :
    python import_csv_to_postgres.py
"""

import os
import pandas as pd
from dotenv import load_dotenv
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

load_dotenv()

# CONFIG
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5433)),
    "dbname":   os.getenv("DB_NAME", "estatemind"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "capTEEMO500"),
}

INPUT_CSV  = "./phase0_output/dataset_final.csv"
TABLE_DEST = "sentiment_analysis"


def get_engine():
    pwd = quote_plus(DB_CONFIG["password"])
    url = (f"postgresql+psycopg2://{DB_CONFIG['user']}:{pwd}"
           f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    return create_engine(url)


def create_table(engine):
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


def main():
    engine = get_engine()
    print("[OK] Connexion PostgreSQL etablie")

    create_table(engine)

    # Charger le CSV
    df = pd.read_csv(INPUT_CSV)
    print(f"[OK] {len(df)} lignes chargees depuis {INPUT_CSV}")

    # Colonnes a importer
    cols_mapping = {
        "id":                "id",
        "description_clean": "description_clean",
        "nb_emojis":         "nb_emojis",
        "nb_alert_emojis":   "nb_alert_emojis",
        "sentiment_stars":   "sentiment_stars",
        "sentiment_score":   "sentiment_score",
        "sentiment_signal":  "sentiment_signal",
        "zeroshot_label":    "zeroshot_label",
        "zeroshot_score":    "zeroshot_score",
        "zeroshot_signal":   "zeroshot_signal",
        "rules_signal":      "rules_signal",
        "rules_details":     "rules_details",
        "rules_count":       "rules_count",
        "score_final":       "score_final",
        "label_final":       "label_final",
        "confiance":         "confiance",
    }

    # Garder seulement les colonnes disponibles
    cols_available = {k: v for k, v in cols_mapping.items() if k in df.columns}
    df_import = df[list(cols_available.keys())].copy()
    df_import = df_import.rename(columns=cols_available)

    # Nettoyer les NaN
    df_import = df_import.where(pd.notna(df_import), None)

    print(f"[INFO] Import de {len(df_import)} lignes...")

    # Insertion par batch
    inserted = 0
    errors   = 0
    BATCH    = 100

    with engine.connect() as conn:
        for i in range(0, len(df_import), BATCH):
            batch = df_import.iloc[i:i + BATCH]
            for _, row in batch.iterrows():
                try:
                    row_dict = row.to_dict()
                    cols = ", ".join(row_dict.keys())
                    vals = ", ".join([f":{k}" for k in row_dict.keys()])
                    update = ", ".join([f"{k} = EXCLUDED.{k}" for k in row_dict.keys() if k != "id"])

                    sql = f"""
                    INSERT INTO {TABLE_DEST} ({cols})
                    VALUES ({vals})
                    ON CONFLICT (id) DO UPDATE SET {update}, analysed_at = NOW();
                    """
                    conn.execute(text(sql), row_dict)
                    inserted += 1
                except Exception as e:
                    errors += 1
                    if errors <= 3:
                        print(f"[ERREUR] ID {row.get('id')} : {str(e)[:100]}")

            conn.commit()
            print(f"  {min(i + BATCH, len(df_import))}/{len(df_import)} lignes importees...")

    print(f"\n{'='*50}")
    print("RAPPORT IMPORT")
    print(f"{'='*50}")
    print(f"  Inserees avec succes : {inserted}")
    print(f"  Erreurs              : {errors}")

    # Verification
    with engine.connect() as conn:
        result = pd.read_sql(
            text(f"SELECT label_final, COUNT(*) as n FROM {TABLE_DEST} GROUP BY label_final ORDER BY n DESC"),
            conn
        )
    print(f"\nDistribution dans PostgreSQL :")
    print(result.to_string(index=False))
    print(f"\n[OK] Import termine dans la table '{TABLE_DEST}'")


if __name__ == "__main__":
    main()