# tools/show_local_schema.py
"""Show exact local PostgreSQL schema to replicate in Supabase."""

import os
import psycopg2
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    dbname=os.getenv("POSTGRES_DATABASE", "estatemind"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", ""),
)

with conn.cursor() as cur:
    # Get listings columns
    cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'listings'
        ORDER BY ordinal_position
    """)
    
    print("-- LISTINGS TABLE --")
    for row in cur.fetchall():
        print(f"{row[0]}: {row[1]} (nullable={row[2]})")
    
    # Get image_embeddings columns
    cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'image_embeddings'
        ORDER BY ordinal_position
    """)
    
    print("\n-- IMAGE_EMBEDDINGS TABLE --")
    for row in cur.fetchall():
        print(f"{row[0]}: {row[1]} (nullable={row[2]})")

conn.close()