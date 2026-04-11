"""
EstateMind — export_macro.py  (corrected)
==========================================
Exports time-series data from estatemind_timeseries.db into CSV files
ready for Prophet / SARIMAX / LSTM modeling.

Three exports produced:
  1. price_history_full.csv     — every raw row from price_history
  2. price_history_monthly.csv  — monthly aggregates (median, mean, count)
                                  broken down by transaction_type & property_type
  3. macro_indicators.csv       — BCT / INS / BVMT macro indicators
                                  (empty if macro scrapers haven't run yet)

WHERE TO PUT THIS FILE:
    EstateMind/serie_temporelle/export_macro.py   ← replace the existing one

HOW TO RUN (from EstateMind/ root):
    python serie_temporelle/export_macro.py
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# =============================================================================
# PATHS — resolves correctly regardless of where you run the script from
# =============================================================================

# This file lives at:  EstateMind/serie_temporelle/export_macro.py
# So parent = serie_temporelle/, parent.parent = EstateMind/
_HERE     = Path(__file__).resolve().parent   # serie_temporelle/
_ROOT     = _HERE.parent                      # EstateMind/

# The DB is at EstateMind/data/estatemind_timeseries.db
# (matches TIMESERIES_DB_PATH in config/settings.py)
_DB_PATH  = _ROOT / "data" / "estatemind_timeseries.db"

# Fallback: some runs put it directly under EstateMind/
if not _DB_PATH.exists():
    _DB_PATH = _ROOT / "estatemind_timeseries.db"

# Output folder: EstateMind/serie_temporelle/timeseries_exports/
OUT_DIR = _HERE / "timeseries_exports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TS = datetime.now().strftime("%Y%m%d_%H%M%S")

# =============================================================================
# HELPERS
# =============================================================================

def _conn() -> sqlite3.Connection:
    if not _DB_PATH.exists():
        print(f"\nERROR: Database not found at:\n  {_DB_PATH}")
        print("\nMake sure you have run the scrapers at least once:")
        print("  python main.py run\n")
        sys.exit(1)
    return sqlite3.connect(_DB_PATH)


def _tables() -> list:
    conn = _conn()
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    conn.close()
    return tables

# =============================================================================
# 1. Raw price_history — every listing ever scraped
# =============================================================================

def export_price_history_full() -> Path:
    conn = _conn()
    df = pd.read_sql("SELECT * FROM price_history ORDER BY scraped_at", conn)
    conn.close()

    out = OUT_DIR / f"price_history_full_{TS}.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")

    print(f"[1/3] price_history_full    → {out.name}")
    print(f"      {len(df)} rows")
    print(f"      sources : {df['source_name'].value_counts().to_dict()}")
    return out


# =============================================================================
# 2. Monthly aggregates — this is your actual time series target variable
# =============================================================================

def export_price_history_monthly() -> Path:
    conn = _conn()
    df = pd.read_sql("SELECT * FROM price_history", conn)
    conn.close()

    # parse dates
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    df["month"]      = df["scraped_at"].dt.to_period("M").dt.to_timestamp()

    # remove noise: prices below 1000 TND are likely bad data
    valid = df[(df["price"] > 1000) & df["price"].notna()].copy()

    # price per m2 (only where surface is known and > 0)
    valid["price_per_m2"] = (
        valid["price"] / valid["surface"]
    ).replace([float("inf"), float("-inf")], None)

    # ── national monthly (all types) ─────────────────────────────────────────
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
    national["segment"]          = "all"
    national["transaction_type"] = "all"
    national["property_type"]    = "all"
    national["region"]           = "all"

    # ── by transaction type ───────────────────────────────────────────────────
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
    by_txn["segment"]      = by_txn["transaction_type"]
    by_txn["property_type"] = "all"
    by_txn["region"]        = "all"

    # ── by property type ──────────────────────────────────────────────────────
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

    # ── by region ─────────────────────────────────────────────────────────────
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

    # ── combine all segments ──────────────────────────────────────────────────
    combined = pd.concat(
        [national, by_txn, by_prop, by_region],
        ignore_index=True,
    )
    combined["month"] = combined["month"].dt.strftime("%Y-%m-%d")
    combined = combined.sort_values(["month", "segment"]).reset_index(drop=True)

    out = OUT_DIR / f"price_history_monthly_{TS}.csv"
    combined.to_csv(out, index=False, encoding="utf-8-sig")

    print(f"[2/3] price_history_monthly → {out.name}")
    print(f"      {len(combined)} rows | {combined['month'].nunique()} month(s)")
    return out


# =============================================================================
# 3. Macro indicators — BCT + INS + BVMT economic time series
# =============================================================================

def export_macro_indicators() -> Path:
    out = OUT_DIR / f"macro_indicators_{TS}.csv"

    if "macro_indicators" not in _tables():
        print("[3/3] macro_indicators      → SKIPPED")
        print("      Table not found — run macro scrapers first:")
        print("      python -m scrapers.macro_scrapers --source all --start 2005")
        # write empty placeholder so downstream code doesn't break
        pd.DataFrame(
            columns=["date", "indicator", "value", "source", "unit"]
        ).to_csv(out, index=False, encoding="utf-8-sig")
        return out

    conn = _conn()
    df = pd.read_sql(
        """SELECT date, indicator, value, source, unit
           FROM macro_indicators
           ORDER BY date, indicator""",
        conn,
    )
    conn.close()

    df.to_csv(out, index=False, encoding="utf-8-sig")

    print(f"[3/3] macro_indicators      → {out.name}")
    if not df.empty:
        summary = df.groupby("indicator").agg(
            rows      = ("value", "count"),
            from_date = ("date",  "min"),
            to_date   = ("date",  "max"),
        )
        print(summary.to_string())
    else:
        print("      0 rows — run macro scrapers to populate")

    return out


# =============================================================================
# SUMMARY
# =============================================================================

def print_summary(df: pd.DataFrame) -> None:
    print()
    print("=" * 55)
    print("  DATASET SUMMARY")
    print("=" * 55)
    print(f"  DB path:           {_DB_PATH}")
    print(f"  Total listings:    {len(df)}")
    print(f"  Date range:        "
          f"{str(df['scraped_at'].min())[:10]} → "
          f"{str(df['scraped_at'].max())[:10]}")
    print(f"  Sources:           {df['source_name'].nunique()}")

    valid = df[df["price"] > 1000]["price"]
    if len(valid):
        print(f"  Valid prices:      {len(valid)}")
        print(f"  Median price:      {valid.median():,.0f} TND")
        print(f"  Price range:       {valid.min():,.0f} – {valid.max():,.0f} TND")

    print()
    print("  Listings by source:")
    for src, cnt in df["source_name"].value_counts().items():
        print(f"    {src:<22} {cnt}")

    print()
    print("  Listings by region:")
    for reg, cnt in df["region"].value_counts().head(8).items():
        print(f"    {str(reg):<22} {cnt}")

    months = df["scraped_at"].nunique()
    print()
    if df["scraped_at"].dt.to_period("M").nunique() < 3:
        print("  ⚠  Less than 3 months of data.")
        print("  Keep scrapers running to build the time series.")
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

    conn = _conn()
    df_full = pd.read_sql("SELECT * FROM price_history", conn)
    conn.close()
    df_full["scraped_at"] = pd.to_datetime(df_full["scraped_at"], errors="coerce")
    print_summary(df_full)

    print()
    print(f"  Files saved to: {OUT_DIR}")
    print()


if __name__ == "__main__":
    main()