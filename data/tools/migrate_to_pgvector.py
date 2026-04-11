"""
Script de migration : active pgvector sur la base estatemind existante.
Lance ce script UNE SEULE FOIS après avoir remplacé les fichiers.

Usage:
    python tools/migrate_to_pgvector.py
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
load_dotenv()

import psycopg2

def migrate():
    print("Connexion à PostgreSQL...")
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DB", "estatemind"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "capTEEMO500"),
    )
    conn.autocommit = False

    with conn.cursor() as cur:
        print("1. Activation de l'extension pgvector...")
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")

        print("2. Ajout de la colonne embedding (vector 384)...")
        cur.execute("""
            ALTER TABLE listings
            ADD COLUMN IF NOT EXISTS embedding vector(384);
        """)

        print("3. Ajout des colonnes etl_status et etl_processed_at si manquantes...")
        cur.execute("""
            ALTER TABLE listings
            ADD COLUMN IF NOT EXISTS etl_status VARCHAR(50) DEFAULT 'pending';
        """)
        cur.execute("""
            ALTER TABLE listings
            ADD COLUMN IF NOT EXISTS etl_processed_at TIMESTAMP;
        """)

        print("4. Création de l'index ivfflat...")
        cur.execute("SELECT COUNT(*) FROM listings")
        count = cur.fetchone()[0]
        if count >= 100:
            cur.execute("""
                CREATE INDEX IF NOT EXISTS listings_embedding_idx
                ON listings USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100);
            """)
            print(f"   Index créé ({count} lignes)")
        else:
            print(f"   Index ignoré — seulement {count} lignes (minimum 100 requis)")

    conn.commit()
    conn.close()
    print("\nMigration terminée avec succès !")
    print("Tu peux maintenant lancer : python main.py run")

if __name__ == "__main__":
    migrate()