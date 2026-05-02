"""
EstateMind — Macro Modeling
Run: python serie_temporelle/modeling.py

Models:
  A — Prophet  IPC (inflation)       → forecast_ipc_12m.csv  + prophet_ipc.pkl
  B — Prophet  Taux Directeur (BCT)  → forecast_taux_directeur_12m.csv  + prophet_td.pkl
  C — Prophet  Taux de Chômage       → forecast_chomage.csv  + prophet_chomage.pkl

Chart (modeling_report.png):
  Row 1 — 3 individual Prophet forecasts (white bg, gold dots, CI bands)
  Row 2 — 3 series on independent y-axes for direct comparison

Note on chômage: annual data (21 pts, 2005-2025) is interpolated to monthly
for continuous display. Prophet is still trained on annual points.

PKL files allow reloading fitted models without retraining:
  import pickle
  with open('timeseries_exports/prophet_ipc.pkl', 'rb') as f:
      model = pickle.load(f)
  forecast = model.predict(future_df)
"""
import warnings; warnings.filterwarnings('ignore')
from pathlib import Path
import pickle
import pandas as pd, numpy as np
import matplotlib; matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Patch
from prophet import Prophet
from statsmodels.tsa.stattools import adfuller

HERE    = Path(__file__).resolve().parent
EXPORTS = HERE / 'timeseries_exports'
EXPORTS.mkdir(exist_ok=True)
latest  = lambda p: sorted(EXPORTS.glob(p))[-1]

# ── LOAD & CLEAN ──────────────────────────────────────────────────────────────
macro = pd.read_csv(latest('macro_wide_*.csv'), index_col='date', parse_dates=True)
macro['taux_directeur']  = macro['taux_directeur'].ffill()
macro['ipc_general_ins'] = macro['ipc_general_ins'].interpolate('linear')

# chômage: keep annual for Prophet training, interpolate monthly for display
chom_annual  = macro['chomage_rate'].dropna()
chom_monthly = macro['chomage_rate'].resample('MS').interpolate('linear')

# ── ADF TESTS ─────────────────────────────────────────────────────────────────
print("=== ADF Stationarity ===")
for col in ['ipc_general_ins', 'taux_directeur', 'chomage_rate']:
    p = adfuller(macro[col].dropna())[1]
    print(f"  {col:<25}: p={p:.3f}  {'stationary' if p<0.05 else 'non-stationary'}")

# ── FIT PROPHET ───────────────────────────────────────────────────────────────
def fit_prophet(col, freq, periods, cps=0.1):
    s  = macro[col].dropna().resample(freq).last().ffill()
    tr = s.reset_index(); tr.columns = ['ds', 'y']
    m  = Prophet(yearly_seasonality=True, weekly_seasonality=False,
                 daily_seasonality=False, changepoint_prior_scale=cps,
                 interval_width=0.95)
    m.fit(tr)
    fc = m.predict(m.make_future_dataframe(periods=periods, freq=freq))
    return m, fc[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

ipc_model,  ipc_fc  = fit_prophet('ipc_general_ins', 'MS', 12, cps=0.15)
td_model,   td_fc   = fit_prophet('taux_directeur',  'MS', 12, cps=0.05)
chom_model, chom_fc = fit_prophet('chomage_rate',    'YS',  3, cps=0.10)

# interpolate chômage forecast to monthly for smooth display
chom_fc_m = (chom_fc.set_index('ds')
                     .resample('MS').interpolate('linear')
                     .reset_index())
chom_fc_m.columns = ['ds', 'yhat', 'yhat_lower', 'yhat_upper']

ipc_fc.to_csv( EXPORTS / 'forecast_ipc_12m.csv',            index=False)
td_fc.to_csv(  EXPORTS / 'forecast_taux_directeur_12m.csv', index=False)
chom_fc.to_csv(EXPORTS / 'forecast_chomage.csv',            index=False)

# ── SAVE FITTED MODELS AS PKL ─────────────────────────────────────────────────
# Allows reloading without retraining (Prophet training takes ~5s per model)
# Usage: model = pickle.load(open('prophet_ipc.pkl', 'rb'))
#        forecast = model.predict(future_df)
for name, model in [('ipc',     ipc_model),
                    ('td',      td_model),
                    ('chomage', chom_model)]:
    pkl_path = EXPORTS / f'prophet_{name}.pkl'
    with open(pkl_path, 'wb') as f:
        pickle.dump(model, f)
    print(f"  Saved {pkl_path.name}")

print("\n=== Forecasts (end of horizon) ===")
for name, fc in [('IPC', ipc_fc), ('TD', td_fc), ('Chômage', chom_fc)]:
    print(f"  {name:<10}: {fc.iloc[-1].ds.date()}  yhat={fc.iloc[-1].yhat:.2f}%")

# ── CHART SETUP ───────────────────────────────────────────────────────────────
GOLD  = '#f5a623'
RED   = '#e74c3c'
BG    = 'white'
TODAY = pd.Timestamp('2026-03-01')

# historical series used for each chart (chômage = monthly interpolated for display)
ipc_hist  = macro['ipc_general_ins'].dropna()
td_hist   = macro['taux_directeur'].ffill().dropna()

# series config: (title, hist_for_display, forecast, color, dots_for_scatter)
SERIES = [
    ('IPC — Inflation (%)',    ipc_hist,    ipc_fc,   '#2E5BBA', ipc_hist),
    ('Taux Directeur BCT (%)', td_hist,     td_fc,    '#1ABC9C', td_hist),
    ('Taux de Chômage (%)',    chom_monthly, chom_fc_m, '#e76f51', chom_annual),
]

def style_ax(ax):
    ax.set_facecolor(BG)
    for sp in ax.spines.values(): sp.set_color('#cccccc')
    ax.tick_params(colors='#333333', labelsize=8)
    ax.grid(axis='y', color='#eeeeee', lw=0.8)
    ax.grid(axis='x', visible=False)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

def plot_forecast(ax, hist, fc, col, dots, title):
    """Individual Prophet forecast — gold dots + colored line + CI band."""
    ax.scatter(dots.index, dots.values, color=GOLD, s=18, zorder=5, alpha=0.9)
    ax.plot(fc[fc.ds <= TODAY]['ds'], fc[fc.ds <= TODAY]['yhat'],
            color=col, lw=1.5, alpha=0.5)
    fut = fc[fc.ds > TODAY]
    ax.fill_between(fut.ds, fut.yhat_lower, fut.yhat_upper, alpha=0.18, color=col)
    ax.plot(fut.ds, fut.yhat, color=col, lw=2.5)
    ax.axvline(TODAY, color=RED, ls='--', lw=1.2, alpha=0.8)
    ax.legend(handles=[
        plt.Line2D([0],[0], color=col, lw=2.5,           label='Prévision Prophet'),
        plt.Line2D([0],[0], color=GOLD, lw=0, marker='o',
                   ms=5, markerfacecolor=GOLD,            label='Observé'),
        Patch(color=col, alpha=0.2,                       label='IC 95%'),
        plt.Line2D([0],[0], color=RED, lw=1.2, ls='--',  label='Début prévision'),
    ], fontsize=7.5, framealpha=0.9)
    ax.set_title(f'Prophet — {title}\nHistorique + Prévision',
                 fontsize=10, fontweight='bold', color='#1B2A4A', pad=8)
    ax.set_ylabel('%', fontsize=9, color='#555555')
    style_ax(ax)

def plot_comparison(ax1):
    """3 series on independent y-axes — fully continuous, no gaps."""
    ax2 = ax1.twinx()
    ax3 = ax1.twinx()
    ax3.spines['right'].set_position(('axes', 1.055))

    for ax, (title, hist, fc, col, _) in zip([ax1, ax2, ax3], SERIES):
        ax.plot(hist.index, hist.values, color=col, lw=2, alpha=0.9)
        fut = fc[fc.ds > TODAY]
        ax.fill_between(fut.ds, fut.yhat_lower, fut.yhat_upper, alpha=0.1, color=col)
        ax.plot(fut.ds, fut.yhat, color=col, lw=2.5, ls='--')
        ax.set_ylabel(title, fontsize=8.5, color=col, fontweight='bold')
        ax.tick_params(axis='y', colors=col, labelsize=8)
        for s in ['top', 'bottom']: ax.spines[s].set_visible(False)
        ax.grid(axis='x', visible=False)
        ax.set_facecolor(BG)

    ax1.spines['left'].set_color('#2E5BBA')
    ax1.spines['right'].set_visible(False)
    ax2.spines['right'].set_color('#1ABC9C')
    ax3.spines['right'].set_color('#e76f51')

    ax1.axvline(TODAY, color=RED, ls='--', lw=1.3, alpha=0.8)
    ax1.grid(axis='y', color='#eeeeee', lw=0.8)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax1.tick_params(axis='x', colors='#333333', labelsize=9)

    lines = [plt.Line2D([0],[0], color=c, lw=2.5, label=t)
             for t,_,_,c,_ in SERIES]
    lines += [plt.Line2D([0],[0], color=RED, lw=1.3, ls='--', label='Début prévision')]
    ax1.legend(handles=lines, fontsize=9, loc='upper left', framealpha=0.9)
    ax1.set_title('Comparaison — IPC · Taux Directeur · Chômage\n'
                  '(axes indépendants · trait plein = historique · tirets = prévision)',
                  fontsize=11, fontweight='bold', color='#1B2A4A', pad=8)

# ── BUILD FIGURE ──────────────────────────────────────────────────────────────
fig = plt.figure(figsize=(18, 13), facecolor=BG)
gs  = fig.add_gridspec(2, 3, hspace=0.5, wspace=0.32,
                        top=0.91, bottom=0.06, left=0.05, right=0.96)

for i, (title, hist, fc, col, dots) in enumerate(SERIES):
    plot_forecast(fig.add_subplot(gs[0, i]), hist, fc, col, dots, title)

plot_comparison(fig.add_subplot(gs[1, :]))

fig.suptitle('EstateMind — Data Preparation & Modeling\n'
             'Analyse Macro-Économique & Prévisions Série Temporelle',
             fontsize=14, fontweight='bold', color='#1B2A4A')

plt.savefig(EXPORTS / 'modeling_report.png', dpi=150,
            bbox_inches='tight', facecolor=BG)

print(f"\nOutputs → {EXPORTS}")
print("  forecast_ipc_12m.csv  |  forecast_taux_directeur_12m.csv  |  forecast_chomage.csv")
print("  prophet_ipc.pkl       |  prophet_td.pkl                   |  prophet_chomage.pkl")
print("  modeling_report.png")