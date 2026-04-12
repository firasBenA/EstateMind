import io
import time
import zipfile
import requests
import pandas as pd
from pathlib import Path

HEADERS = {"User-Agent": "Mozilla/5.0"}
BASE = "https://www.bvmt.com.tn/sites/default/files/historiques"
ZIP_YEARS = list(range(2016, 2026))


def _get(url):
    print("📥 Téléchargement :", url)
    r = requests.get(url, headers=HEADERS, timeout=30)
    print("Status :", r.status_code)
    r.raise_for_status()
    return r.content


# 🔥 PARSING TXT ROBUSTE
def _parse_txt(data):
    try:
        text = data.decode("latin1")

        lines = text.split("\n")
        rows = []

        for line in lines:
            parts = line.split()
            if len(parts) < 2:
                continue

            try:
                date = pd.to_datetime(parts[0], errors="coerce")
                value = float(parts[-1].replace(",", "."))

                if pd.notna(date):
                    rows.append({"date": date, "tunindex": value})
            except:
                continue

        df = pd.DataFrame(rows)
        print("✅ TXT parsed :", df.shape)
        return df

    except Exception as e:
        print("❌ TXT error :", e)
        return pd.DataFrame()


# 🔥 PARSING CSV CORRIGÉ
def _parse_csv(data):
    try:
        df = pd.read_csv(io.BytesIO(data), sep=";", encoding="latin1")

        print("👉 Colonnes :", df.columns.tolist())

        df.columns = [c.strip().upper() for c in df.columns]

        # filtrer TUNINDEX
        df = df[df["LIB_INDICE"].str.contains("TUNINDEX", na=False)]

        df["date"] = pd.to_datetime(df["SEANCE"], errors="coerce")
        df["tunindex"] = pd.to_numeric(df["INDICE_JOUR"], errors="coerce")

        df = df.dropna(subset=["date"])

        print("✅ CSV parsed :", df.shape)

        return df[["date", "tunindex"]]

    except Exception as e:
        print("❌ CSV error :", e)
        return pd.DataFrame()


def _unzip_and_parse(zip_bytes):
    dfs = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        print("📦 ZIP contient :", zf.namelist())

        for name in zf.namelist():
            with zf.open(name) as f:
                data = f.read()

            name = name.lower()

            if name.endswith(".txt"):
                df = _parse_txt(data)

            elif name.endswith(".csv"):
                df = _parse_csv(data)

            else:
                continue

            if not df.empty:
                dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs).drop_duplicates("date").sort_values("date")


def run_all(start_date="2016-01-01", output_dir="data"):
    Path(output_dir).mkdir(exist_ok=True)

    print("📥 BVMT — récupération des données...")

    all_data = []

    for year in ZIP_YEARS:
        print(f"\n📅 Année {year}")

        url = f"{BASE}/indices/histo_indice_{year}.zip"
        data = _get(url)

        if data:
            df = _unzip_and_parse(data)

            if not df.empty:
                all_data.append(df)
                print(f"✅ {len(df)} lignes")
            else:
                print("⚠️ Aucune donnée")

        time.sleep(0.5)

    if not all_data:
        print("❌ BVMT: aucune donnée")
        return pd.DataFrame()

    df = pd.concat(all_data).drop_duplicates("date").sort_values("date")

    df = df.set_index("date").resample("MS").mean().reset_index()

    df.to_csv(f"{output_dir}/bvmt_monthly.csv", index=False)

    print(f"\n✅ BVMT OK: {len(df)} mois")
    return df
