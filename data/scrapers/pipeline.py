"""
Pipeline principal — Tunisia Macro Features
Fusionne : BVMT (ZIP) + World Bank API
Output : data/exports/macro_features_final.csv

Usage :
    python pipeline.py
"""

import time
import pandas as pd
from pathlib import Path

from source_bvmt import run_all as bvmt_run
from source_worldbank import run_all as wb_run

# ==============================
# PATHS DYNAMIQUES
# ==============================
BASE_DIR = Path(__file__).resolve().parent       # data/scrapers
DATA_DIR = BASE_DIR.parent                       # data/
EXPORT_DIR = DATA_DIR / "exports"

START_DATE = "2015-01-01"


# ==============================
# COLLECTE
# ==============================
def collect(start_date=START_DATE, output_dir=EXPORT_DIR) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 62)
    print("🚀 Tunisia Macro Feature Pipeline")
    print(f"   Période : {start_date} → aujourd'hui")
    print("=" * 62)

    results = {}

    # BVMT
    print("\n📊 [1/2] BVMT")
    try:
        results["bvmt"] = bvmt_run(start_date=start_date, output_dir=output_dir)
    except Exception as e:
        print(f"❌ BVMT: {e}")
        results["bvmt"] = pd.DataFrame()

    time.sleep(1)

    # World Bank
    print("\n🌍 [2/2] World Bank")
    try:
        results["wb"] = wb_run(start_date=start_date, output_dir=output_dir)
    except Exception as e:
        print(f"❌ WorldBank: {e}")
        results["wb"] = pd.DataFrame()

    time.sleep(1)

    return results


# ==============================
# FUSION
# ==============================
def merge_all(results: dict, start_date=START_DATE) -> pd.DataFrame:

    valid = {k: v for k, v in results.items() if not v.empty and "date" in v.columns}

    if not valid:
        print("\n❌ Aucune donnée")
        return pd.DataFrame()

    print(f"\n🔄 Fusion : {list(valid.keys())}")

    merged = None

    for name, df in valid.items():
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.drop_duplicates("date").sort_values("date")

        if merged is None:
            merged = df
        else:
            merged = merged.merge(df, on="date", how="outer")

    merged = merged[merged["date"] >= start_date].sort_values("date").reset_index(drop=True)

    # Nettoyage colonnes vides
    thresh = int(len(merged) * 0.4)
    merged = merged.dropna(axis=1, thresh=thresh)

    # Remplissage
    num_cols = merged.select_dtypes(include="number").columns
    merged[num_cols] = merged[num_cols].ffill().bfill()

    # ==============================
    # FEATURES
    # ==============================
    if "tunindex" in merged.columns:
        merged["tunindex_ret_1m"] = merged["tunindex"].pct_change()
        merged["tunindex_ma3"] = merged["tunindex"].rolling(3).mean()
        merged["tunindex_ma12"] = merged["tunindex"].rolling(12).mean()
        merged["tunindex_vol_12m"] = merged["tunindex"].pct_change().rolling(12).std()

    if "inflation_pct" in merged.columns:
        merged["inflation_lag1"] = merged["inflation_pct"].shift(1)
        merged["inflation_ma6"] = merged["inflation_pct"].rolling(6).mean()

    return merged


# ==============================
# RAPPORT
# ==============================
def print_report(df: pd.DataFrame):

    print("\n" + "=" * 62)
    print("📋 RAPPORT FINAL")
    print("=" * 62)

    print(f"Période : {df['date'].min().date()} → {df['date'].max().date()}")
    print(f"Lignes  : {len(df)}")
    print(f"Features: {df.shape[1]-1}")

    print("\nCouverture :")

    total = len(df)

    for col in df.columns:
        if col == "date":
            continue

        pct = df[col].notna().sum() / total * 100
        status = "✅" if pct >= 90 else ("⚠️" if pct >= 60 else "❌")

        print(f"{status} {col:<35} {pct:.1f}%")


# ==============================
# MAIN
# ==============================
if __name__ == "__main__":

    results = collect()
    df = merge_all(results)

    if df.empty:
        print("\n⚠️ Aucune donnée générée")
    else:
        print_report(df)

        output_path = EXPORT_DIR / "macro_features_final.csv"
        df.to_csv(output_path, index=False)

        print(f"\n💾 Sauvegardé : {output_path}")