"""
EstateMind — Load Excel + Score Listings

Usage:
    python load_and_score.py Classeur2.xlsx
"""
import sys
import pandas as pd
from pathlib import Path

# Add project root to path if needed
sys.path.insert(0, str(Path(__file__).parent.parent))

from preprocessing.steps.scorer import (
    compute_score, 
    batch_score, 
    train_scorer, 
    get_feature_importances
)


def load_excel_to_records(filepath: str) -> list:
    """
    Load your Excel file and convert each row to the format scorer expects.
    
    Your Excel has columns like:
      price, surface, rooms, city, region, latitude, longitude, 
      image_count, features, description, municipalite,
      is_outlier, has_price_history, cross_verified, suspected_duplicate,
      reliability_score, reliability_level, should_drop, model_weight, ...
    """
    df = pd.read_excel(filepath)
    
    records = []
    for _, row in df.iterrows():
        # Extract metadata (all property fields)
        metadata = {
            # Core fields
            "price": row.get("price"),
            "surface": row.get("surface"),
            "surface_area_m2": row.get("surface_area_m2"),  # fallback
            "rooms": row.get("rooms"),
            "city": row.get("city"),
            "region": row.get("region"),      # your column name
            "governorate": row.get("governorate"),  # fallback
            "latitude": row.get("latitude"),
            "longitude": row.get("longitude"),
            "description": row.get("description"),
            "image_count": row.get("image_count"),
            "features": row.get("features") if pd.notna(row.get("features")) else [],
            "municipality": row.get("municipality"),
            "municipalite": row.get("municipalite"),  # your column name
            
            # Flags embedded in metadata (your data structure)
            "is_outlier": bool(row.get("is_outlier", False)),
            "has_price_history": bool(row.get("has_price_history", False)),
            "cross_verified": bool(row.get("cross_verified", False)),
            "suspected_duplicate": bool(row.get("suspected_duplicate", False)),
            "nlp_enriched": bool(row.get("nlp_enriched", False)),
            "price_changed": bool(row.get("price_changed", False)),
            
            # Keep original IDs for reference
            "property_id": row.get("property_id"),
            "_id": row.get("_id"),
            "source_name": row.get("source_name"),
        }
        
        # Extract explicit flags dict (what scorer.compute_score expects)
        flags = {
            "price_outlier": bool(row.get("is_outlier", False)),
            "suspected_duplicate": bool(row.get("suspected_duplicate", False)),
            "nlp_enriched": bool(row.get("nlp_enriched", False)),
            "has_price_history": bool(row.get("has_price_history", False)),
            "price_changed": bool(row.get("price_changed", False)),
            "cross_verified": bool(row.get("cross_verified", False)),
            "surface_outlier": bool(row.get("outlier_flags", [])),  # if applicable
        }
        
        records.append({
            "metadata": metadata,
            "flags": flags,
            # Keep original reliability fields for comparison
            "original_score": row.get("reliability_score"),
            "original_level": row.get("reliability_level"),
        })
    
    return records


def main():
    if len(sys.argv) < 2:
        print("Usage: python load_and_score.py <excel_file.xlsx>")
        print("Example: python load_and_score.py Classeur2.xlsx")
        return
    
    filepath = sys.argv[1]
    print(f"📥 Loading {filepath}...")
    
    records = load_excel_to_records(filepath)
    print(f"✅ Loaded {len(records)} listings")
    
    # ── Option 1: Score all listings (uses existing model or heuristic) ─────
    print("\n🔍 Scoring listings...")
    scored = batch_score(records)
    
    # Show sample results
    print("\n📊 Sample Results:")
    for i, rec in enumerate(scored[:5]):
        print(f"  {i+1}. {rec.get('city', '?')} | Score: {rec['reliability_score']}/100 | {rec['reliability_level']}")
        if rec.get('score_explanation'):
            print(f"     → {rec['score_explanation'][:100]}...")
    
    # ── Option 2: Train the model on your data ──────────────────────────────
    # Uncomment below to train (requires 500+ records for good results)
    # print("\n🎓 Training ML model on your data...")
    # train_scorer(records, force=True)
    # 
    # print("\n📈 Learned Feature Importances:")
    # for feat, imp in sorted(get_feature_importances().items(), key=lambda x: -x[1])[:10]:
    #     print(f"  {feat:25s} {imp:5.1f}%")
    
    # ── Option 3: Export scored results ─────────────────────────────────────
    output_path = "scored_listings.xlsx"
    export_df = pd.DataFrame(scored)
    export_df.to_excel(output_path, index=False)
    print(f"\n💾 Exported scored results to {output_path}")
    
    # Quick stats
    print(f"\n📈 Quality Distribution:")
    print(export_df['reliability_level'].value_counts())
    
    drop_count = export_df['should_drop'].sum()
    print(f"\n⚠️  {drop_count} listings marked for DROP (score < 25)")


if __name__ == "__main__":
    main()