# export_macro.py
"""
Export macro indicators from estatemind_timeseries.db to CSV and Excel.

Usage:
    python export_macro.py
"""

import sqlite3
import pandas as pd
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────
DB_PATH     = "data/estatemind_timeseries.db"
OUTPUT_DIR  = Path("data/exports")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Load ────────────────────────────────────────────────────────
conn = sqlite3.connect(DB_PATH)

all_data = pd.read_sql("""
    SELECT date, indicator, value, unit, source
    FROM macro_indicators
    ORDER BY date, indicator
""", conn)

conn.close()

# ── Wide format (one column per indicator) ──────────────────────
wide = all_data.pivot(index="date", columns="indicator", values="value")
wide.index    = pd.to_datetime(wide.index)
wide          = wide.sort_index()
wide.columns.name = None

# ── Long format (one row per indicator per date) ─────────────────
long = all_data.copy()
long["date"] = pd.to_datetime(long["date"])
long = long.sort_values(["indicator", "date"])

# ── Export ──────────────────────────────────────────────────────
wide.to_csv(OUTPUT_DIR / "macro_wide.csv")
print(f"Saved: {OUTPUT_DIR}/macro_wide.csv  — {wide.shape[0]} months x {wide.shape[1]} indicators")

long.to_csv(OUTPUT_DIR / "macro_long.csv", index=False)
print(f"Saved: {OUTPUT_DIR}/macro_long.csv  — {len(long)} rows")

wide.to_excel(OUTPUT_DIR / "macro_wide.xlsx")
print(f"Saved: {OUTPUT_DIR}/macro_wide.xlsx")

# ── Per-indicator CSV (one file per indicator) ───────────────────
per_indicator_dir = OUTPUT_DIR / "per_indicator"
per_indicator_dir.mkdir(exist_ok=True)

for indicator in long["indicator"].unique():
    df_ind = long[long["indicator"] == indicator][["date", "value"]].reset_index(drop=True)
    df_ind.to_csv(per_indicator_dir / f"{indicator}.csv", index=False)

print(f"Saved: {per_indicator_dir}/  — one CSV per indicator")

# ── Summary ─────────────────────────────────────────────────────
summary = all_data.groupby("indicator").agg(
    source   = ("source",    "first"),
    unit     = ("unit",      "first"),
    rows     = ("value",     "count"),
    from_    = ("date",      "min"),
    to_      = ("date",      "max"),
).reset_index()

print("\n=== EXPORT SUMMARY ===")
print(summary.to_string(index=False))