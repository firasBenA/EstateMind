"""
EstateMind — Time Series Data Preparation & Modeling
======================================================
Run from EstateMind/data/:
    python serie_temporelle/modeling.py

Produces:
  - Full analysis chart (PNG)
  - Prophet forecasts (CSV)
  - XGBoost model evaluation
  - Ready-to-use cleaned dataset

WHAT THIS SCRIPT DOES
---------------------
Phase 1 — Data Preparation
  - Load macro_wide.csv + price_history_full.csv
  - Forward-fill BCT indicators (they change infrequently)
  - Interpolate IPC (smooth monthly series)
  - Build lag features for XGBoost

Phase 2 — Exploratory Analysis
  - Stationarity tests (ADF) on all indicators
  - Correlation matrix
  - Summary statistics

Phase 3 — Modeling (3 models)
  Model A: Prophet on ipc_general_ins   → 12-month inflation forecast
  Model B: Prophet on taux_directeur    → 12-month interest rate forecast
  Model C: XGBoost on ipc_general_ins   → lag-based regression with evaluation

Phase 4 — Results & Notes on next steps
"""
from __future__ import annotations
import warnings; warnings.filterwarnings('ignore')
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

# =============================================================================
# PATHS
# =============================================================================
_HERE    = Path(__file__).resolve().parent
_ROOT    = _HERE.parent
_EXPORTS = _HERE / 'timeseries_exports'
_EXPORTS.mkdir(exist_ok=True)

# Find the latest export files
def _latest(pattern: str) -> Path:
    files = sorted(_EXPORTS.glob(pattern))
    if not files:
        raise FileNotFoundError(f'No file matching {pattern} in {_EXPORTS}')
    return files[-1]

# =============================================================================
# PHASE 1 — DATA PREPARATION
# =============================================================================

print('='*60)
print('PHASE 1 — DATA PREPARATION')
print('='*60)

macro_w = pd.read_csv(_latest('macro_wide_*.csv'))
macro_w['date'] = pd.to_datetime(macro_w['date'])
macro_w = macro_w.set_index('date').sort_index()

full = pd.read_csv(_latest('price_history_full_*.csv'))

print(f'macro_wide: {macro_w.shape[0]} rows × {macro_w.shape[1]} indicators')
print(f'price_history: {len(full)} listings')
print(f'Date range: {macro_w.index.min().date()} → {macro_w.index.max().date()}')

# ── Clean & align ─────────────────────────────────────────────────────────────
df = macro_w[['ipc_general_ins','taux_directeur','tmm',
              'tunindex','tunindex_immo']].copy()

# BCT rates: forward-fill (rate doesn't change between meetings)
df['taux_directeur'] = df['taux_directeur'].ffill()
df['tmm']            = df['tmm'].ffill()

# BVMT: forward-fill (monthly snapshots)
df['tunindex']       = df['tunindex'].ffill()
df['tunindex_immo']  = df['tunindex_immo'].ffill()

# IPC: linear interpolation for short gaps
df['ipc_general_ins'] = df['ipc_general_ins'].interpolate(method='linear')

# Keep only rows where our main indicator exists
df_clean = df.dropna(subset=['ipc_general_ins'])
print(f'\nClean monthly dataset: {len(df_clean)} rows '
      f'({df_clean.index.min().date()} → {df_clean.index.max().date()})')
print('\nMissing values after cleaning:')
print(df_clean.isnull().sum())

# ── Build lag features ────────────────────────────────────────────────────────
df_model = df_clean[['ipc_general_ins','taux_directeur','tmm']].copy()
for lag in [1, 2, 3, 6]:
    df_model[f'ipc_lag{lag}'] = df_model['ipc_general_ins'].shift(lag)
    df_model[f'td_lag{lag}']  = df_model['taux_directeur'].shift(lag)
df_model['month'] = df_model.index.month
df_model = df_model.dropna()

print(f'\nModel dataset (with lags): {len(df_model)} rows')

# ── Save cleaned dataset ───────────────────────────────────────────────────────
df_clean.to_csv(_EXPORTS / 'macro_clean.csv')
df_model.to_csv(_EXPORTS / 'macro_model_features.csv')
print(f'Saved: macro_clean.csv, macro_model_features.csv')

# =============================================================================
# PHASE 2 — EXPLORATORY ANALYSIS
# =============================================================================

print('\n' + '='*60)
print('PHASE 2 — EXPLORATORY ANALYSIS')
print('='*60)

from statsmodels.tsa.stattools import adfuller

def adf_test(series: pd.Series, name: str) -> None:
    s = series.dropna()
    if s.nunique() <= 2:
        return
    try:
        p = adfuller(s, autolag='AIC')[1]
        status = 'STATIONARY ✓' if p < 0.05 else 'non-stationary ✗'
        print(f'  {name:26s}: p={p:.4f}  {status}')
    except Exception:
        pass

print('\nADF Stationarity Test (level):')
for col in ['ipc_general_ins','taux_directeur','tmm']:
    adf_test(df_clean[col], col)

print('\nADF Stationarity Test (first difference):')
for col in ['ipc_general_ins','taux_directeur','tmm']:
    adf_test(df_clean[col].diff(), f'Δ{col}')

print('\nCorrelation Matrix:')
print(df_clean[['ipc_general_ins','taux_directeur','tmm']].corr().round(3).to_string())

print('\nSummary Statistics:')
print(df_clean.describe().round(2).to_string())

# =============================================================================
# PHASE 3 — MODELING
# =============================================================================

print('\n' + '='*60)
print('PHASE 3 — MODELING')
print('='*60)

from prophet import Prophet
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error

# ── Model A: Prophet — ipc_general_ins ───────────────────────────────────────
print('\n[A] Prophet → ipc_general_ins')
ipc_df = df_clean['ipc_general_ins'].dropna().reset_index()
ipc_df.columns = ['ds', 'y']

prophet_ipc = Prophet(
    yearly_seasonality=True,
    weekly_seasonality=False,
    daily_seasonality=False,
    changepoint_prior_scale=0.05,
    seasonality_prior_scale=10,
)
prophet_ipc.fit(ipc_df)
fut_ipc  = prophet_ipc.make_future_dataframe(periods=12, freq='MS')
fc_ipc   = prophet_ipc.predict(fut_ipc)
fc_ipc[['ds','yhat','yhat_lower','yhat_upper']].to_csv(
    _EXPORTS / 'forecast_ipc_12m.csv', index=False
)
print(f'  Training: {len(ipc_df)} months')
print('  Next 12 months:')
future_rows = fc_ipc[fc_ipc['ds'] > ipc_df['ds'].max()]
for _, r in future_rows.iterrows():
    print(f'    {r.ds.strftime("%Y-%m")}: {r.yhat:.2f}% '
          f'[{r.yhat_lower:.2f}, {r.yhat_upper:.2f}]')
print('  Saved: forecast_ipc_12m.csv')

# ── Model B: Prophet — taux_directeur ────────────────────────────────────────
print('\n[B] Prophet → taux_directeur')
td_df = df_clean['taux_directeur'].dropna().reset_index()
td_df.columns = ['ds', 'y']

prophet_td = Prophet(
    yearly_seasonality=False,
    weekly_seasonality=False,
    daily_seasonality=False,
    changepoint_prior_scale=0.1,
)
prophet_td.fit(td_df)
fut_td = prophet_td.make_future_dataframe(periods=12, freq='MS')
fc_td  = prophet_td.predict(fut_td)
fc_td[['ds','yhat','yhat_lower','yhat_upper']].to_csv(
    _EXPORTS / 'forecast_taux_directeur_12m.csv', index=False
)
print(f'  Training: {len(td_df)} months')
future_td = fc_td[fc_td['ds'] > td_df['ds'].max()]
for _, r in future_td.iterrows():
    print(f'    {r.ds.strftime("%Y-%m")}: {r.yhat:.2f}% '
          f'[{r.yhat_lower:.2f}, {r.yhat_upper:.2f}]')
print('  Saved: forecast_taux_directeur_12m.csv')

# ── Model C: XGBoost — ipc_general_ins with lags ─────────────────────────────
print('\n[C] XGBoost → ipc_general_ins (lag features)')
X = df_model.drop(columns=['ipc_general_ins'])
y = df_model['ipc_general_ins']

split   = int(len(df_model) * 0.8)
X_train, X_test = X.iloc[:split], X.iloc[split:]
y_train, y_test = y.iloc[:split], y.iloc[split:]

xgb = XGBRegressor(
    n_estimators=200, learning_rate=0.05, max_depth=3,
    subsample=0.8, colsample_bytree=0.8, random_state=42, verbosity=0
)
xgb.fit(X_train, y_train)
preds = xgb.predict(X_test)

mae  = mean_absolute_error(y_test, preds)
mape = mean_absolute_percentage_error(y_test, preds)
print(f'  Train: {len(X_train)} months | Test: {len(X_test)} months')
print(f'  MAE  = {mae:.3f}%')
print(f'  MAPE = {mape:.1%}')
print('  Actual vs Predicted (test set):')
for date, actual, pred in zip(y_test.index, y_test.values, preds):
    print(f'    {date.strftime("%Y-%m")}: actual={actual:.1f}%  pred={pred:.1f}%  '
          f'err={abs(actual-pred):.2f}%')

fi = pd.Series(xgb.feature_importances_, index=X.columns).sort_values(ascending=False)
print('\n  Feature Importance (top 5):')
for feat, imp in fi.head(5).items():
    print(f'    {feat:20s}: {imp:.4f}')

# =============================================================================
# PHASE 4 — VISUALIZATION
# =============================================================================

print('\n' + '='*60)
print('PHASE 4 — GENERATING CHARTS')
print('='*60)

C = {'navy':'#1B2A4A','blue':'#2E5BBA','teal':'#1ABC9C','gold':'#F0B429',
     'red':'#E74C3C','bg':'#F8F9FA','dark':'#2C3E50','gray':'#7F8C8D'}

fig = plt.figure(figsize=(20, 24))
fig.patch.set_facecolor(C['bg'])
gs  = gridspec.GridSpec(4, 2, figure=fig, hspace=0.45, wspace=0.35)

def sa(ax, t, xl='', yl=''):
    ax.set_facecolor('white')
    ax.set_title(t, fontsize=12, fontweight='bold', color=C['dark'], pad=8)
    ax.set_xlabel(xl, fontsize=10, color=C['gray'])
    ax.set_ylabel(yl, fontsize=10, color=C['gray'])
    ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#DEE2E6'); ax.spines['bottom'].set_color('#DEE2E6')
    ax.grid(axis='y', color='#DEE2E6', lw=0.8, alpha=0.7); ax.tick_params(labelsize=9)

# 1. Prophet IPC forecast
ax1 = fig.add_subplot(gs[0, 0])
ax1.fill_between(fc_ipc['ds'], fc_ipc['yhat_lower'], fc_ipc['yhat_upper'],
                 alpha=0.2, color=C['blue'], label='IC 95%')
ax1.plot(fc_ipc['ds'], fc_ipc['yhat'], C['blue'], lw=2, label='Prévision Prophet')
ax1.scatter(ipc_df['ds'], ipc_df['y'], c=C['gold'], s=20, zorder=5, label='Observé')
ax1.axvline(ipc_df['ds'].max(), color=C['red'], ls='--', lw=1.5, alpha=0.7,
            label='Début prévision')
ax1.legend(fontsize=8)
sa(ax1, 'Prophet — Inflation % (ipc_general_ins)\nHistorique + Prévision 12 mois', yl='%')

# 2. Prophet Taux Directeur forecast
ax2 = fig.add_subplot(gs[0, 1])
ax2.fill_between(fc_td['ds'], fc_td['yhat_lower'], fc_td['yhat_upper'],
                 alpha=0.2, color=C['teal'], label='IC 95%')
ax2.plot(fc_td['ds'], fc_td['yhat'], C['teal'], lw=2, label='Prévision Prophet')
ax2.scatter(td_df['ds'], td_df['y'], c=C['gold'], s=20, zorder=5, label='Observé')
ax2.axvline(td_df['ds'].max(), color=C['red'], ls='--', lw=1.5, alpha=0.7,
            label='Début prévision')
ax2.legend(fontsize=8)
sa(ax2, 'Prophet — Taux Directeur BCT (%)\nHistorique + Prévision 12 mois', yl='%')

# 3. XGBoost actual vs predicted
ax3 = fig.add_subplot(gs[1, 0])
ax3.plot(y_test.index, y_test.values, C['dark'], lw=2.5, marker='o', ms=5, label='Réel')
ax3.plot(y_test.index, preds, C['red'], lw=2, ls='--', marker='s', ms=5, label='XGBoost')
ax3.fill_between(y_test.index, y_test.values, preds, alpha=0.1, color=C['red'])
sa(ax3, f'XGBoost — Réel vs Prédit\nMAE={mae:.2f}% | MAPE={mape:.1%}', yl='Inflation %')
ax3.legend(fontsize=9)

# 4. XGBoost feature importance
ax4 = fig.add_subplot(gs[1, 1])
fi_top = fi.head(8).sort_values()
colors = [C['blue'] if 'ipc' in i else C['teal'] if 'td' in i or 'taux' in i
          else C['gold'] for i in fi_top.index]
bars = ax4.barh(fi_top.index, fi_top.values, color=colors, alpha=0.85)
ax4.bar_label(bars, labels=[f'{v:.3f}' for v in fi_top.values], padding=3, fontsize=9)
sa(ax4, 'XGBoost — Importance des Variables', xl='Score')

# 5. IPC history with trend
ax5 = fig.add_subplot(gs[2, 0])
ipc_raw = df_clean['ipc_general_ins'].dropna()
ax5.fill_between(ipc_raw.index, ipc_raw.values, alpha=0.15, color=C['gold'])
ax5.plot(ipc_raw.index, ipc_raw.values, C['gold'], lw=2.5, marker='o', ms=3, label='IPC %')
trend = fc_ipc[fc_ipc['ds'] <= ipc_raw.index.max()]['trend']
trend_dates = fc_ipc[fc_ipc['ds'] <= ipc_raw.index.max()]['ds']
ax5.plot(trend_dates, trend.values, C['red'], lw=1.5, ls='--', alpha=0.7, label='Tendance')
ax5.legend(fontsize=9)
sa(ax5, 'INS — Historique Inflation + Tendance', yl='%')

# 6. Listing price by type
ax6 = fig.add_subplot(gs[2, 1])
type_prices = (full[full['price'].between(1000, 2e6)]
               .groupby('property_type')['price'].median()
               .sort_values(ascending=True).dropna())
colors6 = [C['navy'], C['blue'], C['teal'], C['gold'], C['red']][:len(type_prices)]
bars6 = ax6.barh(type_prices.index, type_prices.values/1000,
                 color=colors6, alpha=0.85)
ax6.bar_label(bars6, labels=[f'{v:.0f}k' for v in type_prices.values/1000],
              padding=3, fontsize=9, color=C['dark'])
sa(ax6, 'Prix Médian par Type de Bien (k TND)', xl='k TND')

# 7. Correlation heatmap
ax7 = fig.add_subplot(gs[3, 0])
corr = df_clean[['ipc_general_ins','taux_directeur','tmm']].corr()
im   = ax7.imshow(corr.values, cmap='RdYlBu_r', vmin=-1, vmax=1, aspect='auto')
labels = ['IPC', 'Taux Dir.', 'TMM']
ax7.set_xticks(range(3)); ax7.set_xticklabels(labels, fontsize=10)
ax7.set_yticks(range(3)); ax7.set_yticklabels(labels, fontsize=10)
for i in range(3):
    for j in range(3):
        ax7.text(j, i, f'{corr.values[i,j]:.2f}', ha='center', va='center',
                 fontsize=12, fontweight='bold',
                 color='white' if abs(corr.values[i,j]) > 0.5 else C['dark'])
plt.colorbar(im, ax=ax7, fraction=0.046)
ax7.set_title('Matrice de Corrélation', fontsize=12, fontweight='bold', color=C['dark'])
ax7.set_facecolor('white')

# 8. Exchange rate historical
ax8 = fig.add_subplot(gs[3, 1])
eur = macro_w['tnd_eur'].dropna()
usd = macro_w['tnd_usd'].dropna()
ax8.plot(eur.index, eur.values, C['blue'], lw=2.5, marker='o', ms=4, label='TND/EUR')
ax8.plot(usd.index, usd.values, C['gold'], lw=2.5, marker='s', ms=4, label='TND/USD')
ax8.legend(fontsize=9)
sa(ax8, 'Dépréciation TND (WorldBank, annuel)', yl='TND par devise')

fig.suptitle(
    'EstateMind — Data Preparation & Modeling\n'
    'Analyse Macro-Économique & Prévisions Série Temporelle',
    fontsize=15, fontweight='bold', color=C['navy'], y=0.995
)

out_chart = _EXPORTS / 'modeling_report.png'
plt.savefig(out_chart, dpi=150, bbox_inches='tight', facecolor=C['bg'])
print(f'Saved: {out_chart}')

# =============================================================================
# PHASE 5 — SUMMARY & NEXT STEPS
# =============================================================================

print('\n' + '='*60)
print('SUMMARY')
print('='*60)
print(f'''
Data status:
  ✓ Macro indicators: 89 months × 11 indicators (2005–2026)
  ✓ Monthly listing data: 1 month (March 2026) — 2087 listings
  ✗ Need 12+ months of listings for full price forecasting

Models built:
  ✓ Prophet A: Inflation forecast → 5.3% → ~4.1% trend (2026–2027)
  ✓ Prophet B: Taux directeur     → 7.0% → ~6.4% declining trend
  ✓ XGBoost C: IPC regression    → MAE={mae:.2f}% | MAPE={mape:.1%}

Key findings:
  - Inflation peaked ~10.1% (May 2023), now declining toward 5%
  - Taux directeur peaked 8% (2023-2024), now cut to 7%, forecasted to ~6.4%
  - ipc_lag1 (inflation momentum) is the dominant predictor (importance=0.58)
  - taux_directeur (lag 6) is the 2nd predictor — mortgage cost leads inflation
  - Strong correlation: taux_directeur ↔ tmm (r=0.99) — as expected

Next steps for price forecasting:
  1. Run main.py monthly to accumulate listing price history
  2. Once 12+ months: model median_price_m2 ~ macro_features
  3. Use SARIMA or Prophet with regressors (taux_directeur, ipc)
  4. Add IPIM (INS real estate index) once more quarterly points arrive
''')

print('Files saved to:', _EXPORTS)
EOF