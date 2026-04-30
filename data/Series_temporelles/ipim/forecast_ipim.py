import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import warnings
from pathlib import Path

from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_absolute_error, mean_squared_error

warnings.filterwarnings("ignore")

# ==============================
# PATHS
# ==============================
BASE_DIR   = Path(__file__).resolve().parent
csv_path   = BASE_DIR / "ipim_historical.csv"
output_dir = BASE_DIR / "outputs"
output_dir.mkdir(parents=True, exist_ok=True)

print(f"📂 Chargement fichier : {csv_path}")

# ==============================
# 1. Chargement
# ==============================
df = pd.read_csv(csv_path, parse_dates=["date"])
df.set_index("date", inplace=True)
df.columns = df.columns.str.strip().str.lower()
df = df.asfreq("QS")

print("Colonnes disponibles :", df.columns.tolist())
print(f"Période : {df.index[0].date()} → {df.index[-1].date()}")
print(f"Observations : {len(df)} trimestres\n")

# ==============================
# 2. Feature Engineering
# ==============================
df["vol_appart_lag1"]  = df["vol_appartement_pct"].shift(1)
df["vol_terrain_lag1"] = df["vol_terrain_pct"].shift(1)
df["vol_appart_ma4"]   = df["vol_appartement_pct"].rolling(4).mean()

# ==============================
# 3. Configuration par actif
# ==============================
TARGETS = {
    "ipim_appartement": {
        "order":          (2, 1, 2),
        "seasonal_order": (0, 1, 1, 4),
        "features":       ["vol_appart_lag1", "vol_appart_ma4"],
        "label":          "Appartements"
    },
    "ipim_maison": {
        "order":          (2, 1, 2),
        "seasonal_order": (0, 1, 1, 4),
        "features":       ["vol_appart_lag1", "vol_terrain_lag1"],
        "label":          "Maisons"
    },
    "ipim_terrain": {
        "order":          (2, 1, 2),
        "seasonal_order": (0, 1, 1, 4),
        "features":       ["vol_terrain_lag1", "vol_appart_ma4"],
        "label":          "Terrains"
    },
}

N_STEPS = 20
all_forecasts = {}

for target_col, config in TARGETS.items():

    print(f"\n{'='*55}")
    print(f"  Actif : {config['label']}")
    print(f"{'='*55}")

    # ==============================
    # 4. Nettoyage
    # ==============================
    features = [f for f in config["features"] if f in df.columns]
    data = pd.concat([df[target_col], df[features]], axis=1).dropna()
    target = data[target_col]
    exog   = data.drop(columns=[target_col])

    print(f"Variables exogènes : {features}")
    print(f"Observations après dropna : {len(data)}")

    # ==============================
    # 5. Split 80/20
    # ==============================
    train_size = int(len(data) * 0.8)
    y_train, y_test = target[:train_size], target[train_size:]
    X_train, X_test = exog[:train_size],   exog[train_size:]
    print(f"Train : {len(y_train)} | Test : {len(y_test)}")

    # ==============================
    # 6. Modèle d'évaluation (train seul)
    # ==============================
    model_eval = SARIMAX(
        y_train, exog=X_train,
        order=config["order"],
        seasonal_order=config["seasonal_order"],
        enforce_stationarity=False,
        enforce_invertibility=False
    )
    results_eval = model_eval.fit(maxiter=1000, disp=False)
    print(results_eval.summary())

    # ==============================
    # 7. Prédictions sur le test
    # ==============================
    predictions = results_eval.predict(
        start=len(y_train),
        end=len(y_train) + len(y_test) - 1,
        exog=X_test
    )

    # ==============================
    # 8. Évaluation
    # ==============================
    mae  = mean_absolute_error(y_test, predictions)
    rmse = np.sqrt(mean_squared_error(y_test, predictions))
    mape = np.mean(np.abs((y_test.values - predictions.values) / y_test.values)) * 100

    print(f"\n===== METRICS — {config['label']} =====")
    print(f"MAE  : {round(mae, 4)}")
    print(f"RMSE : {round(rmse, 4)}")
    print(f"MAPE : {round(mape, 2)} %")

    # ==============================
    # 9. Retrain sur TOUTE la série
    # ==============================
    model_full = SARIMAX(
        target, exog=exog,
        order=config["order"],
        seasonal_order=config["seasonal_order"],
        enforce_stationarity=False,
        enforce_invertibility=False
    )
    results_full = model_full.fit(maxiter=1000, disp=False)

    # ==============================
    # 10. Features futures = moyenne long terme
    # ==============================
    cutoff_normal = "2022"
    avg_lt = exog[:cutoff_normal].mean()

    future_dates = pd.date_range(
        start=target.index[-1], periods=N_STEPS + 1, freq="QS"
    )[1:]

    future_exog = pd.DataFrame(
        [avg_lt.values] * N_STEPS,
        columns=exog.columns,
        index=future_dates
    )
    for i in range(N_STEPS):
        future_exog.iloc[i] *= (1 + 0.002 * i)

    # ==============================
    # 11. Forecast futur
    # ==============================
    forecast        = results_full.get_forecast(steps=N_STEPS, exog=future_exog)
    forecast_values = forecast.predicted_mean
    forecast_ci     = forecast.conf_int()

    residuals  = y_test.values - predictions.values
    volatility = np.std(residuals)
    np.random.seed(42)
    noise = np.random.normal(0, volatility * 0.5, N_STEPS)

    forecast_values = forecast_values + noise
    forecast_values.index = future_dates
    forecast_ci.index     = future_dates

    # ==============================
    # 12. Visualisation
    # ==============================
    fig, ax = plt.subplots(figsize=(12, 6))

    ax.plot(target.index,  target,          label="Historique",     color="steelblue", linewidth=1.5)
    ax.plot(y_test.index,  predictions,     label="Test (prédit)",  linestyle="--", color="orange")
    ax.plot(future_dates,  forecast_values, label="Forecast 5 ans", linestyle="--", color="green")

    ax.fill_between(
        future_dates,
        forecast_ci.iloc[:, 0],
        forecast_ci.iloc[:, 1],
        alpha=0.2, color="green",
        label="Intervalle confiance 95%"
    )

    ax.axvline(x=target.index[-1], color="gray", linestyle=":", linewidth=1, label="Dernier observé")
    ax.legend()
    ax.set_title(f"Prévision IPIM — {config['label']} (SARIMAX)")
    ax.set_xlabel("Date")
    ax.set_ylabel("Indice (base 100 = 2015)")
    ax.grid(alpha=0.3)
    fig.tight_layout()

    # ==============================
    # 13. Sauvegarde image
    # dpi=80 : PNG léger (~150 KB) → évite l'erreur "No space left on device"
    # dpi=150 si tu veux plus de qualité et que le disque a de la place
    # ==============================
    img_path = output_dir / f"forecast_ipim_{target_col}.png"
    fig.savefig(img_path, dpi=80, bbox_inches="tight")
    plt.close(fig)
    print(f"📁 Image sauvegardée : {img_path}")

    # ==============================
    # 14. Sauvegarde CSV
    # ==============================
    forecast_df = pd.DataFrame({
        "date":     future_dates,
        "asset":    target_col,
        "forecast": forecast_values.values,
        "lower_ci": forecast_ci.iloc[:, 0].values,
        "upper_ci": forecast_ci.iloc[:, 1].values,
    })
    all_forecasts[target_col] = forecast_df

    csv_out = output_dir / f"forecast_{target_col}.csv"
    forecast_df.to_csv(csv_out, index=False)
    print(f"📁 CSV sauvegardé : {csv_out}")

# ==============================
# 15. CSV global
# ==============================
df_all = pd.concat(all_forecasts.values(), ignore_index=True)
df_all.to_csv(output_dir / "ipim_forecast_all.csv", index=False)
print(f"\n✅ CSV global : {output_dir / 'ipim_forecast_all.csv'}")

# ==============================
# 16. Résumé final
# ==============================
print("\n" + "="*55)
print("RÉSUMÉ — Forecast IPIM à 5 ans")
print("="*55)
for col, fdf in all_forecasts.items():
    last_hist = df[col].dropna().iloc[-1]
    last_fc   = fdf["forecast"].iloc[-1]
    variation = (last_fc / last_hist - 1) * 100
    print(f"  {col:22} | Actuel={last_hist:.1f} → 5ans={last_fc:.1f} ({variation:+.1f}%)")