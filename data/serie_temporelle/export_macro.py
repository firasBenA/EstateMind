"""
EstateMind — export_macro.py
=============================
Exports all time-series data to CSV files ready for modeling.

WHERE THIS FILE LIVES:
    EstateMind/data/serie_temporelle/export_macro.py

HOW TO RUN (from EstateMind/data/):
    python serie_temporelle/export_macro.py

FILES PRODUCED in serie_temporelle/timeseries_exports/:
    price_history_full_YYYYMMDD.csv     — every raw listing
    price_history_monthly_YYYYMMDD.csv  — monthly aggregates (your target variable)
    macro_indicators_YYYYMMDD.csv       — BCT + INS + BVMT indicators (exogenous features)
    macro_wide_YYYYMMDD.csv             — macro pivoted: one row/month, one col/indicator
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# =============================================================================
# PATHS
# =============================================================================
# This file is at:  EstateMind/data/serie_temporelle/export_macro.py
# _HERE  = EstateMind/data/serie_temporelle/
# _ROOT  = EstateMind/data/                 ← your working directory
_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent

# The main DB with price_history (2087 rows) AND macro_indicators
# config/settings.py sets TIMESERIES_DB_PATH = "data/estatemind_timeseries.db"
# which resolves to EstateMind/data/data/estatemind_timeseries.db
_DB_PATH = _ROOT / "data" / "estatemind_timeseries.db"

# Fallback: DB at root level
if not _DB_PATH.exists():
    _DB_PATH = _ROOT / "estatemind_timeseries.db"

OUT_DIR = _HERE / "timeseries_exports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TS = datetime.now().strftime("%Y%m%d_%H%M%S")


# =============================================================================
# HELPERS
# =============================================================================

def _conn() -> sqlite3.Connection:
    if not _DB_PATH.exists():
        print(f"\nERROR: Database not found.")
        print(f"  Looked at: {_DB_PATH}")
        print(f"  Run scrapers first: python main.py run\n")
        sys.exit(1)
    return sqlite3.connect(str(_DB_PATH))


def _tables() -> list:
    conn = _conn()
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    conn.close()
    return tables


# =============================================================================
# 1. Raw price_history
# =============================================================================

def export_price_history_full() -> Path:
    conn = _conn()
    df   = pd.read_sql("SELECT * FROM price_history ORDER BY scraped_at", conn)
    conn.close()

    out = OUT_DIR / f"price_history_full_{TS}.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")

    print(f"[1/4] price_history_full    → {out.name}")
    print(f"      {len(df)} rows")
    print(f"      sources: {df['source_name'].value_counts().to_dict()}")
    return out


# =============================================================================
# 2. Monthly aggregates (target variable for your model)
# =============================================================================

def export_price_history_monthly() -> Path:
    conn = _conn()
    df   = pd.read_sql("SELECT * FROM price_history", conn)
    conn.close()

    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    df["month"]      = df["scraped_at"].dt.to_period("M").dt.to_timestamp()

    # filter noise
    valid = df[(df["price"] > 1000) & df["price"].notna()].copy()
    valid["price_per_m2"] = (
        valid["price"] / valid["surface"]
    ).replace([float("inf"), float("-inf")], None)

    # national (all)
    national = (
        valid.groupby("month")
        .agg(
            listing_count       = ("price",        "count"),
            median_price        = ("price",        "median"),
            mean_price          = ("price",        "mean"),
            p25_price           = ("price",        lambda x: x.quantile(0.25)),
            p75_price           = ("price",        lambda x: x.quantile(0.75)),
            median_price_per_m2 = ("price_per_m2", "median"),
        )
        .reset_index()
    )
    national["segment"] = "all"
    national["transaction_type"] = "all"
    national["property_type"]    = "all"
    national["region"]           = "all"

    # by transaction type
    by_txn = (
        valid.groupby(["month", "transaction_type"])
        .agg(
            listing_count       = ("price",        "count"),
            median_price        = ("price",        "median"),
            mean_price          = ("price",        "mean"),
            median_price_per_m2 = ("price_per_m2", "median"),
        )
        .reset_index()
    )
    by_txn["segment"]       = by_txn["transaction_type"]
    by_txn["property_type"] = "all"
    by_txn["region"]        = "all"

    # by property type
    by_prop = (
        valid.groupby(["month", "property_type"])
        .agg(
            listing_count       = ("price",        "count"),
            median_price        = ("price",        "median"),
            mean_price          = ("price",        "mean"),
            median_price_per_m2 = ("price_per_m2", "median"),
        )
        .reset_index()
    )
    by_prop["segment"]          = by_prop["property_type"]
    by_prop["transaction_type"] = "all"
    by_prop["region"]           = "all"

    # by region
    by_region = (
        valid[valid["region"].notna()]
        .groupby(["month", "region"])
        .agg(
            listing_count       = ("price",        "count"),
            median_price        = ("price",        "median"),
            mean_price          = ("price",        "mean"),
            median_price_per_m2 = ("price_per_m2", "median"),
        )
        .reset_index()
    )
    by_region["segment"]          = by_region["region"]
    by_region["transaction_type"] = "all"
    by_region["property_type"]    = "all"

    combined = pd.concat(
        [national, by_txn, by_prop, by_region], ignore_index=True
    )
    combined["month"] = combined["month"].dt.strftime("%Y-%m-%d")
    combined = combined.sort_values(["month", "segment"]).reset_index(drop=True)

    out = OUT_DIR / f"price_history_monthly_{TS}.csv"
    combined.to_csv(out, index=False, encoding="utf-8-sig")

    print(f"[2/4] price_history_monthly → {out.name}")
    print(f"      {len(combined)} rows | {combined['month'].nunique()} month(s)")
    return out


# =============================================================================
# 3. Macro indicators — long format (one row per indicator per date)
# =============================================================================

def export_macro_indicators() -> Path:
    out = OUT_DIR / f"macro_indicators_{TS}.csv"

    if "macro_indicators" not in _tables():
        print("[3/4] macro_indicators      → SKIPPED")
        print("      Run: python -m scrapers.macro_scrapers --source all --start 2005")
        pd.DataFrame(
            columns=["date", "indicator", "value", "source", "unit"]
        ).to_csv(out, index=False, encoding="utf-8-sig")
        return out

    conn = _conn()
    df   = pd.read_sql(
        "SELECT date, indicator, value, source, unit "
        "FROM macro_indicators ORDER BY date, indicator",
        conn,
    )
    conn.close()

    df.to_csv(out, index=False, encoding="utf-8-sig")

    print(f"[3/4] macro_indicators      → {out.name}")
    if not df.empty:
        summary = df.groupby(["source", "indicator"]).agg(
            rows      = ("value", "count"),
            from_date = ("date",  "min"),
            to_date   = ("date",  "max"),
        )
        print(summary.to_string())
    return out


# =============================================================================
# 4. Macro wide format (one row per month, one column per indicator)
#    This is the format you pass directly to Prophet / XGBoost / LSTM
# =============================================================================

def export_macro_wide() -> Path:
    out = OUT_DIR / f"macro_wide_{TS}.csv"

    if "macro_indicators" not in _tables():
        print("[4/4] macro_wide            → SKIPPED (no macro_indicators table)")
        return out

    conn = _conn()
    df   = pd.read_sql(
        "SELECT date, indicator, value FROM macro_indicators",
        conn,
    )
    conn.close()

    if df.empty:
        print("[4/4] macro_wide            → SKIPPED (0 rows)")
        return out

    wide = df.pivot_table(
        index="date", columns="indicator", values="value", aggfunc="mean"
    )
    wide.index      = pd.to_datetime(wide.index)
    wide            = wide.sort_index()
    wide.columns.name = None

    wide.to_csv(out, encoding="utf-8-sig")

    print(f"[4/4] macro_wide            → {out.name}")
    print(f"      {wide.shape[0]} months × {wide.shape[1]} indicators")
    print(f"      indicators: {list(wide.columns)}")
    print(f"      date range: {wide.index.min().date()} → {wide.index.max().date()}")
    return out


# =============================================================================
# SUMMARY
# =============================================================================

def print_summary(df: pd.DataFrame) -> None:
    print()
    print("=" * 55)
    print("  DATASET SUMMARY")
    print("=" * 55)
    print(f"  DB:             {_DB_PATH}")
    print(f"  Total listings: {len(df)}")

    if df["scraped_at"].notna().any():
        print(f"  Date range:     "
              f"{str(df['scraped_at'].min())[:10]} → "
              f"{str(df['scraped_at'].max())[:10]}")

    print(f"  Sources:        {df['source_name'].nunique()}")

    valid = df[df["price"] > 1000]["price"]
    if len(valid):
        print(f"  Valid prices:   {len(valid)}")
        print(f"  Median price:   {valid.median():,.0f} TND")
        print(f"  Price range:    {valid.min():,.0f} – {valid.max():,.0f} TND")

    print()
    print("  By source:")
    for src, cnt in df["source_name"].value_counts().items():
        print(f"    {src:<22} {cnt}")

    print()
    print("  By region:")
    for reg, cnt in df["region"].value_counts().head(8).items():
        print(f"    {str(reg):<22} {cnt}")

    if df["scraped_at"].dt.to_period("M").nunique() < 3:
        print()
        print("  ⚠  Less than 3 months of listing data.")
        print("  Run scrapers monthly to build time series.")
        print("  Target: 12+ months for SARIMAX, 24+ for LSTM.")
    print("=" * 55)


# =============================================================================
# MAIN
# =============================================================================

def main():
    print()
    print("=" * 55)
    print("  EstateMind — Time Series Export")
    print(f"  DB : {_DB_PATH}")
    print(f"  OUT: {OUT_DIR}")
    print("=" * 55)
    print()

    export_price_history_full()
    export_price_history_monthly()
    export_macro_indicators()
    export_macro_wide()

    conn    = _conn()
    df_full = pd.read_sql("SELECT * FROM price_history", conn)
    conn.close()
    df_full["scraped_at"] = pd.to_datetime(df_full["scraped_at"], errors="coerce")
    print_summary(df_full)

    print()
    print(f"  Files saved to: {OUT_DIR}")
    print()


if __name__ == "__main__":
    main()