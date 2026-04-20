# data/preprocessing/steps/db_client.py
import os
import sys
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv

# 📍 FIX: Point to the .env file in the 'data' folder
# __file__ is .../data/preprocessing/steps/db_client.py
# .parent.parent goes to .../data/
env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(env_path)

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") # Use Service Role for writing

if not url or not key:
    raise ValueError(f"Missing SUPABASE_URL or KEY. Checked path: {env_path}")

supabase: Client = create_client(url, key)

def fetch_listings_for_scoring(limit: int = 500):
    """Fetch listings from Supabase that need scoring."""
    try:
        response = supabase.table("listings") \
            .select("*") \
            .is_("reliability_score", None) \
            .limit(limit) \
            .execute()
        return response.data
    except Exception as e:
        print(f"❌ Error fetching from Supabase: {e}")
        return []

def update_listing_scores(updates: list):
    """Batch update reliability scores in Supabase."""
    if not updates:
        return
    
    try:
        result = supabase.table("listings").upsert(
            updates, 
            on_conflict="id" 
        ).execute()
        print(f"✅ Successfully updated {len(updates)} listings in Supabase")
    except Exception as e:
        print(f"❌ Error updating Supabase: {e}")