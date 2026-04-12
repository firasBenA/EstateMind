"""
World Bank API — Données macroéconomiques Tunisie
API gratuite, pas de clé requise, historique complet depuis 1960
Docs : https://datahelpdesk.worldbank.org/knowledgebase/articles/889392
"""

import requests, pandas as pd
from pathlib import Path

# Indicateurs World Bank pour la Tunisie
# Format : "CODE_WB": "nom_colonne"
INDICATORS = {
    "FP.CPI.TOTL.ZG":     "inflation_pct",          # Inflation IPC (% annuel)
    "NY.GDP.MKTP.KD.ZG":  "pib_croissance_pct",      # Croissance PIB réel
    "NY.GDP.PCAP.KD":     "pib_par_habitant",         # PIB/habitant (USD constants)
    "SL.UEM.TOTL.ZS":     "taux_chomage",             # Chômage (% force de travail)
    "FR.INR.LEND":        "taux_credit_banques",      # Taux crédit bancaire
    "BN.CAB.XOKA.GD.ZS":  "balance_courante_pib",    # Balance courante / PIB
    "FI.RES.TOTL.MO":     "reserves_mois_import",     # Réserves (mois importations)
    "PA.NUS.FCRF":        "taux_change_tnd_usd",      # Taux de change TND/USD officiel
    "SP.URB.TOTL.IN.ZS":  "taux_urbanisation",        # % population urbaine
    "SP.POP.TOTL":        "population_totale",         # Population totale
}

WB_API = "https://api.worldbank.org/v2/country/TN/indicator"


def fetch_indicator(code: str, col_name: str, start_year=2010) -> pd.DataFrame:
    """Récupère un indicateur depuis l'API World Bank (JSON)."""
    url = f"{WB_API}/{code}"
    params = {
        "format":    "json",
        "per_page":  "100",
        "date":      f"{start_year}:2025",
        "mrv":       "50",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        if len(data) < 2 or not data[1]:
            return pd.DataFrame()

        records = []
        for item in data[1]:
            if item.get("value") is not None:
                records.append({
                    "year":    int(item["date"]),
                    col_name: float(item["value"]),
                })
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records).sort_values("year")
        print(f"   ✅ {col_name}: {len(df)} années ({df['year'].min()}–{df['year'].max()})")
        return df

    except Exception as e:
        print(f"   ⚠️  {col_name}: {e}")
        return pd.DataFrame()


def run_all(start_date="2015-01-01", output_dir="data") -> pd.DataFrame:
    """
    Télécharge tous les indicateurs WB Tunisie, fusionne et interpole mensuellement.
    Retourne un DataFrame avec une ligne par mois.
    """
    Path(output_dir).mkdir(exist_ok=True)
    start_year = int(start_date[:4])

    print("🌍 World Bank API — Indicateurs Tunisie...")
    dfs = []

    for code, col in INDICATORS.items():
        df = fetch_indicator(code, col, start_year=start_year)
        if not df.empty:
            dfs.append(df)

    if not dfs:
        print("❌ World Bank: aucune donnée")
        return pd.DataFrame()

    # Merger sur year
    merged = dfs[0]
    for df in dfs[1:]:
        merged = merged.merge(df, on="year", how="outer")

    merged = merged.sort_values("year").reset_index(drop=True)
    merged.to_csv(f"{output_dir}/worldbank_annual.csv", index=False)

    # Convertir en mensuel par interpolation
    # Créer une ligne par mois, puis interpoler les valeurs annuelles
    monthly_rows = []
    for _, row in merged.iterrows():
        year = int(row["year"])
        for month in range(1, 13):
            r = {"date": pd.Timestamp(year, month, 1)}
            for col in merged.columns:
                if col != "year":
                    r[col] = row[col]
            monthly_rows.append(r)

    df_monthly = pd.DataFrame(monthly_rows)
    df_monthly["date"] = pd.to_datetime(df_monthly["date"])
    df_monthly = df_monthly[df_monthly["date"] >= start_date].sort_values("date")

    # Interpolation linéaire pour lisser les transitions annuelles
    num_cols = [c for c in df_monthly.columns if c != "date"]
    df_monthly = df_monthly.set_index("date")
    df_monthly[num_cols] = df_monthly[num_cols].interpolate(method="time")
    df_monthly = df_monthly.reset_index()

    df_monthly.to_csv(f"{output_dir}/worldbank_monthly.csv", index=False)
    print(f"✅ World Bank: {len(df_monthly)} mois × {df_monthly.shape[1]} features")
    return df_monthly
