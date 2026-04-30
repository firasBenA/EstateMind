import pandas as pd
import numpy as np
from pathlib import Path

# ==============================
# PATHS
# ==============================
# ipim/
# ├── listings_vente_ipim.csv      ← annonces (scraping mars-avril 2026)
# ├── project_prices.py            ← ce script
# └── outputs/
#     ├── ipim_forecast_all.csv
#     └── listings_prix_projetes.csv  ← résultat

BASE_DIR   = Path(__file__).resolve().parent
output_dir = BASE_DIR / "outputs"
output_dir.mkdir(parents=True, exist_ok=True)

listings_path = BASE_DIR   / "listings_vente_ipim.csv"
forecast_path = output_dir / "ipim_forecast_all.csv"

print("📂 Chargement des fichiers...")

# ==============================
# 1. Chargement
# ==============================
listings = pd.read_csv(listings_path)
forecast = pd.read_csv(forecast_path, parse_dates=["date"])

print(f"   Listings : {len(listings)} biens")
print(f"   Forecast : {forecast['date'].nunique()} trimestres disponibles")

# ==============================
# 2. IPIM de référence = 2026-Q2
#
# Les annonces ont été scrapées en mars-avril 2026 → trimestre 2026-Q2
# C'est donc l'IPIM de CE trimestre qui représente le niveau de prix actuel.
#
# Formule corrigée :
#   prix_futur(t) = prix_actuel × (IPIM_forecast(t) / IPIM_2026Q2)
#
# Interprétation : si IPIM_2026Q2 = 196.53 et IPIM_2027Q1 = 204.36,
# alors les prix augmentent de (204.36/196.53 - 1) = +3.98% d'ici 2027-Q1
# ==============================

# Date de référence = trimestre actuel des annonces
REF_DATE = pd.Timestamp("2026-04-01")   # = 2026-Q2

TYPE_TO_ASSET = {
    "appartement": "ipim_appartement",
    "maison":      "ipim_maison",
    "terrain":     "ipim_terrain",
}

# Extraire l'IPIM de référence pour chaque actif depuis le forecast
IPIM_REF = {}
for ipim_type, asset in TYPE_TO_ASSET.items():
    row = forecast[(forecast["asset"] == asset) & (forecast["date"] == REF_DATE)]
    if row.empty:
        raise ValueError(f"IPIM 2026-Q2 introuvable pour {asset}")
    IPIM_REF[asset] = float(row["forecast"].iloc[0])

print(f"\n📍 IPIM de référence (2026-Q2) :")
for asset, val in IPIM_REF.items():
    print(f"   {asset} : {val:.4f}")

# ==============================
# 3. Trimestres FUTURS uniquement
#
# Les annonces existent en 2026-Q2 → le futur commence à 2026-Q3
# On exclut 2026-Q2 et tout ce qui est avant (2024, 2025, 2026-Q1)
# ==============================
FUTURE_START = pd.Timestamp("2026-07-01")   # = 2026-Q3

def quarter_label(date):
    q = (date.month - 1) // 3 + 1
    return f"{date.year}_Q{q}"

all_dates  = sorted(forecast["date"].unique())
future_dates  = [d for d in all_dates if pd.Timestamp(d) >= FUTURE_START]
future_labels = [quarter_label(pd.Timestamp(d)) for d in future_dates]

print(f"\n📅 Trimestres futurs : {future_labels[0]} → {future_labels[-1]}  ({len(future_labels)} trimestres)")

# ==============================
# 4. Filtrer les biens avec un prix
# ==============================
listings_valid = listings[listings["price"].notna()].copy()
print(f"🏠 Biens avec prix : {len(listings_valid)} / {len(listings)}\n")

# ==============================
# 5. Projection — une colonne par trimestre futur
#
# Pour chaque trimestre futur t :
#   price_YYYY_Qn     = prix_actuel × (IPIM_forecast(t) / IPIM_2026Q2)
#   price_YYYY_Qn_min = prix_actuel × (lower_ci(t)      / IPIM_2026Q2)
#   price_YYYY_Qn_max = prix_actuel × (upper_ci(t)      / IPIM_2026Q2)
#
# Exemple : appartement 350 000 TND, 2027-Q1
#   IPIM_2026Q2 = 196.53  |  IPIM_2027Q1 = 204.36
#   ratio = 204.36 / 196.53 = 1.0398
#   prix_2027Q1 = 350 000 × 1.0398 = 363 930 TND
# ==============================
print("🔮 Calcul des prix projetés...")

for date, label in zip(future_dates, future_labels):
    date_ts = pd.Timestamp(date)

    for ipim_type, asset in TYPE_TO_ASSET.items():
        mask = listings_valid["ipim_type"] == ipim_type
        ref  = IPIM_REF[asset]   # IPIM 2026-Q2 pour cet actif

        row = forecast[
            (forecast["asset"] == asset) &
            (forecast["date"]  == date_ts)
        ]
        if row.empty:
            continue

        fc    = float(row["forecast"].iloc[0])
        lower = float(row["lower_ci"].iloc[0])
        upper = float(row["upper_ci"].iloc[0])

        # Formule : prix_futur = prix_actuel × (IPIM_futur / IPIM_2026Q2)
        listings_valid.loc[mask, f"price_{label}"]     = (listings_valid.loc[mask, "price"] * (fc    / ref)).round(0)
        listings_valid.loc[mask, f"price_{label}_min"] = (listings_valid.loc[mask, "price"] * (lower / ref)).round(0)
        listings_valid.loc[mask, f"price_{label}_max"] = (listings_valid.loc[mask, "price"] * (upper / ref)).round(0)

n_proj = len([c for c in listings_valid.columns if c.startswith("price_20")])
print(f"   ✅ {n_proj} colonnes générées ({len(future_labels)} trimestres × 3 scénarios)")

# ==============================
# 6. Résumé prix médians
# ==============================
print("\n" + "="*72)
print("RÉSUMÉ — Prix médian projeté par type (TND)")
print(f"  Référence = 2026-Q2  |  Futur = 2026-Q3 → 2029-Q1")
print("="*72)

# Afficher 1 trimestre par année
summary = [l for l in future_labels if "Q1" in l] or future_labels[::4]
header  = f"  {'Type':12} | {'Actuel':>12}" + "".join(f" | {l.replace('_Q1',''):>10}" for l in summary)
print(header)
print("  " + "-"*72)

for t in ["appartement", "maison", "terrain"]:
    sub = listings_valid[listings_valid["ipim_type"] == t]
    row = f"  {t:12} | {sub['price'].median():>12,.0f}"
    for lbl in summary:
        col = f"price_{lbl}"
        row += f" | {sub[col].median():>10,.0f}" if col in sub.columns else f" | {'N/A':>10}"
    print(row)

# ==============================
# 7. Exemple détaillé — 1 bien
# ==============================
ex_rows = listings_valid[
    (listings_valid["ipim_type"] == "appartement") &
    (listings_valid["price"] == 350000)
]
if not ex_rows.empty:
    ex = ex_rows.iloc[0]
    ref_appart = IPIM_REF["ipim_appartement"]
    print(f"\n📌 Exemple — appartement 350 000 TND | ville: {ex.get('city','N/A')}")
    print(f"   IPIM référence 2026-Q2 = {ref_appart:.4f}")
    print(f"   {'Trimestre':10} | {'IPIM futur':>12} | {'Ratio':>7} | {'Central':>12} | {'Min':>12} | {'Max':>12}")
    print(f"   {'-'*72}")
    for lbl, date in zip(future_labels, future_dates):
        col = f"price_{lbl}"
        if col not in listings_valid.columns or pd.isna(ex.get(col)):
            continue
        row_fc = forecast[(forecast["asset"]=="ipim_appartement") & (forecast["date"]==pd.Timestamp(date))]
        if row_fc.empty: continue
        ipim_f = float(row_fc["forecast"].iloc[0])
        ratio  = ipim_f / ref_appart
        print(f"   {lbl:10} | {ipim_f:>12.4f} | {ratio:>7.4f} | {int(ex[col]):>12,} | {int(ex[f'{col}_min']):>12,} | {int(ex[f'{col}_max']):>12,}")

# ==============================
# 8. Sauvegarde
# ==============================
out_path = output_dir / "listings_prix_projetes.csv"
listings_valid.to_csv(out_path, index=False)

print(f"\n✅ Sauvegardé : {out_path}")
print(f"   {len(listings_valid)} biens × {len(listings_valid.columns)} colonnes totales")
print(f"   dont {n_proj} colonnes de prix projetés (2026-Q3 → 2029-Q1)")