import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_absolute_error, mean_squared_error

# ==============================
# PATHS
# ==============================
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent

csv_path = DATA_DIR / "exports" / "macro_features_final.csv"

output_dir = BASE_DIR / "outputs"
output_dir.mkdir(parents=True, exist_ok=True)

print(f"📂 Chargement fichier : {csv_path}")

# ==============================
# 1. Chargement
# ==============================
df = pd.read_csv(csv_path, parse_dates=["date"])
df.set_index("date", inplace=True)

df.columns = df.columns.str.strip().str.lower()
df = df.asfreq('MS')

print("Colonnes disponibles :", df.columns.tolist())

# ==============================
# 2. Feature Engineering
# ==============================
df["tunindex_ret_1m"] = df["tunindex"].pct_change()
df["tunindex_ma3"] = df["tunindex"].rolling(3).mean()

# ==============================
# 3. Variables
# ==============================
target = df["tunindex"]

features = [
    "tunindex_ret_1m",
    "tunindex_ma3",
    "inflation_pct",
    "pib_par_habitant",
    "taux_chomage",
    "taux_urbanisation"
]

features = [col for col in features if col in df.columns]
exog = df[features]

print("Variables utilisées :", features)

# ==============================
# 4. Nettoyage
# ==============================
data = pd.concat([target, exog], axis=1).dropna()

target = data["tunindex"]
exog = data.drop(columns=["tunindex"])

# ==============================
# 5. Split
# ==============================
train_size = int(len(data) * 0.8)

y_train, y_test = target[:train_size], target[train_size:]
X_train, X_test = exog[:train_size], exog[train_size:]

# ==============================
# 6. Modèle SARIMAX
# ==============================
model = SARIMAX(
    y_train,
    exog=X_train,
    order=(2,1,2),
    seasonal_order=(0,0,0,0),
    trend='t',
    enforce_stationarity=False,
    enforce_invertibility=False
)

results = model.fit(maxiter=1000, disp=False)

print(results.summary())

# ==============================
# 7. Prédictions
# ==============================
predictions = results.predict(
    start=len(y_train),
    end=len(y_train) + len(y_test) - 1,
    exog=X_test
)

# ==============================
# 8. Evaluation
# ==============================
mae = mean_absolute_error(y_test, predictions)
rmse = np.sqrt(mean_squared_error(y_test, predictions))

print("\n===== METRICS =====")
print("MAE  :", round(mae, 4))
print("RMSE :", round(rmse, 4))

# ==============================
# 9. FORECAST FUTUR
# ==============================
n_steps = 60
## a augmenter 

avg_exog = X_test.tail(12).mean()

future_exog = pd.DataFrame(
    [avg_exog.values] * n_steps,
    columns=X_test.columns
)

# évolution progressive
for i in range(n_steps):
    future_exog.iloc[i] *= (1 + 0.002 * i)

last_date = target.index[-1]
future_dates = pd.date_range(start=last_date, periods=n_steps+1, freq='MS')[1:]
future_exog.index = future_dates

forecast = results.get_forecast(steps=n_steps, exog=future_exog)
forecast_values = forecast.predicted_mean
forecast_ci = forecast.conf_int()

# ==============================
# VOLATILITÉ (réalisme)
# ==============================
residuals = y_test - predictions
volatility = np.std(residuals)

np.random.seed(42)
noise = np.random.normal(0, volatility * 0.5, n_steps)

forecast_values = forecast_values + noise

forecast_values.index = future_dates
forecast_ci.index = future_dates

# ==============================
# 10. VISUALISATION
# ==============================
plt.figure(figsize=(12,6))

plt.plot(target.index, target, label="Historique")
plt.plot(y_test.index, predictions, label="Test", linestyle="--")
plt.plot(forecast_values.index, forecast_values, label="Forecast 5 ans", linestyle="--")

plt.fill_between(
    forecast_values.index,
    forecast_ci.iloc[:, 0],
    forecast_ci.iloc[:, 1],
    alpha=0.2,
    label="Intervalle confiance"
)

plt.legend()
plt.title("Prévision Tunindex (SARIMAX)")
plt.grid()

# ==============================
# 11. SAUVEGARDE IMAGE
# ==============================
img_path = output_dir / "forecast_tunindex.png"
plt.savefig(img_path, dpi=300, bbox_inches='tight')

print(f"📁 Image sauvegardée : {img_path}")

# ==============================
# 12. SAUVEGARDE CSV
# ==============================
forecast_df = pd.DataFrame({
    "date": forecast_values.index,
    "forecast": forecast_values.values,
    "lower_ci": forecast_ci.iloc[:, 0].values,
    "upper_ci": forecast_ci.iloc[:, 1].values
})

csv_path_out = output_dir / "forecast_tunindex.csv"
forecast_df.to_csv(csv_path_out, index=False)

print(f"📁 CSV sauvegardé : {csv_path_out}")

plt.show()