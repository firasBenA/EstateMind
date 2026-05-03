"""
EstateMind — Property Price Forecasting
Run: python serie_temporelle/price_forecasting.py

4 steps (professor's validated method):
  1. Anchor   — median sale price per segment (March 2026 listings)
  2. IPIM     — 97 quarterly points 2000-2024, base 2015=100 (INS)
  3. Prophet  — one model per type, regressors: IPC + Taux Directeur
  4. TND      — price(Q) = ipim_forecast(Q) × (anchor / ipim_at_2024Q1)

PKL files saved per IPIM type:
  prophet_ipim_appartement.pkl  ·  prophet_ipim_maison.pkl  ·  prophet_ipim_terrain.pkl
  Usage: model = pickle.load(open('prophet_ipim_appartement.pkl', 'rb'))
"""
import warnings; warnings.filterwarnings('ignore')
from pathlib import Path
import pickle
import pandas as pd, numpy as np
from prophet import Prophet
import matplotlib; matplotlib.use('Agg')
import matplotlib.pyplot as plt

HERE    = Path(__file__).resolve().parent
EXPORTS = HERE / 'timeseries_exports'
latest  = lambda p: sorted(EXPORTS.glob(p))[-1]

# ── LOAD ──────────────────────────────────────────────────────────────────────
# Look for ipim_historical.csv in timeseries_exports/ OR the script's folder
_ipim_candidates = list(EXPORTS.glob('ipim_historical*.csv')) + \
                   list(HERE.glob('ipim_historical*.csv')) + \
                   list(HERE.parent.glob('ipim_historical*.csv'))
if not _ipim_candidates:
    raise FileNotFoundError(
        "ipim_historical.csv not found.\n"
        f"Copy it to: {EXPORTS}\n"
        "File to copy: ipim_historical__1_.csv (uploaded to EstateMind)"
    )
ipim  = pd.read_csv(_ipim_candidates[-1], on_bad_lines='skip', parse_dates=['date'])
macro = pd.read_csv(latest('macro_wide_*.csv'), index_col='date', parse_dates=True)
ipc_fc= pd.read_csv(EXPORTS/'forecast_ipc_12m.csv',   parse_dates=['ds'])
td_fc = pd.read_csv(EXPORTS/'forecast_taux_directeur_12m.csv', parse_dates=['ds'])
sales = pd.read_csv(latest('price_history_full_*.csv'))
sales = sales[(sales['transaction_type']=='Sale') & sales['price'].between(10_000, 5_000_000)]

# ── MACRO REGRESSORS — full timeline, zero NaN ────────────────────────────────
dates = pd.date_range('1999-01-01', '2028-06-01', freq='QS')
ipc_s = macro['ipc_general_ins'].reindex(dates)
td_s  = macro['taux_directeur'].reindex(dates)
for _, r in ipc_fc.iterrows():
    if r.ds in ipc_s.index: ipc_s[r.ds] = r.yhat
for _, r in td_fc.iterrows():
    if r.ds in td_s.index:  td_s[r.ds]  = r.yhat
ipc_s = ipc_s.interpolate('linear').ffill().bfill()
td_s  = td_s.ffill().bfill()
regs  = lambda d: {'ipc_general_ins': float(ipc_s.asof(d)), 'taux_directeur': float(td_s.asof(d))}

# ── SEGMENTS: label → (anchor TND, ipim column) ───────────────────────────────
SEG = {
    'Appt — TUNIS'   : (247_000, 'ipim_appartement'),
    'Appt — National': (360_000, 'ipim_appartement'),
    'Maison — Nat'   : (545_000, 'ipim_maison'),
    'Terrain — Nat'  : (324_000, 'ipim_terrain'),
}

# ── PROPHET — one model per IPIM type ─────────────────────────────────────────
# yearly_seasonality=True  : 97 pts → stable yearly patterns
# changepoint_prior_scale  : 0.1 → moderate flexibility (24-yr history)
# seasonality_prior_scale  : 5.0 → moderate seasonal amplitude
# interval_width           : 0.95 → 95% confidence interval
# regressors               : IPC (inflation) + TD (BCT rate → mortgages)
FQ, ANC = pd.date_range('2024-04-01', '2028-07-01', freq='QS'), pd.Timestamp('2026-01-01')
fc_ = {}

for col in ['ipim_appartement', 'ipim_maison', 'ipim_terrain']:
    tr = ipim[['date', col]].rename(columns={'date': 'ds', col: 'y'})
    tr = tr.join(pd.DataFrame([regs(d) for d in tr.ds], index=tr.index))
    fu = pd.DataFrame({'ds': FQ}).join(pd.DataFrame([regs(d) for d in FQ]))
    m  = Prophet(yearly_seasonality=True, weekly_seasonality=False, daily_seasonality=False,
                 changepoint_prior_scale=0.1, seasonality_prior_scale=5.0, interval_width=0.95)
    m.add_regressor('ipc_general_ins'); m.add_regressor('taux_directeur')
    m.fit(tr); fc_[col] = m.predict(fu)
    # save fitted model as pkl
    with open(EXPORTS / f'prophet_ipim_{col.replace("ipim_","")}.pkl', 'wb') as f:
        pickle.dump(m, f)
    print(f"  Saved prophet_ipim_{col.replace('ipim_','')}.pkl")

# ── CONVERT INDEX → TND ───────────────────────────────────────────────────────
# price_per_unit = anchor / ipim_at_2024Q1  (last real observation)
ppu  = lambda anchor, col: anchor / float(ipim.loc[ipim.date == '2024-01-01', col].values[0])
rows = []
for name, (anchor, col) in SEG.items():
    fwd = fc_[col][fc_[col].ds >= ANC].copy()
    b0  = fwd.iloc[0].yhat
    p   = ppu(anchor, col)
    for _, r in fwd.iterrows():
        rows.append({'seg': name, 'q': f"Q{(r.ds.month-1)//3+1} {r.ds.year}", 'date': r.ds,
                     'lo': round(r.yhat_lower * p), 'base': round(r.yhat * p),
                     'hi': round(r.yhat_upper * p), 'chg': round((r.yhat / b0 - 1) * 100, 1)})
df = pd.DataFrame(rows)
df.to_csv(EXPORTS / 'price_forecast.csv', index=False)

# ── PRINT ─────────────────────────────────────────────────────────────────────
for name in SEG:
    s = df[df.seg == name]
    print(f"\n{name}  (Q1 2026: {s.iloc[0]['base']:,.0f} TND)")
    print(f"  {'Quarter':<12} {'Lower':>12} {'Baseline':>12} {'Upper':>12} {'Δ%':>7}")
    for _, r in s.iterrows():
        print(f"  {r.q:<12} {r.lo:>12,.0f} {r['base']:>12,.0f} {r.hi:>12,.0f} {r.chg:>+6.1f}%")

# ── CHART ─────────────────────────────────────────────────────────────────────
C = dict(navy='#1B2A4A', blue='#2E5BBA', teal='#1ABC9C', gold='#F0B429',
         red='#E74C3C',  green='#27AE60', bg='#F8F9FA',  dark='#2C3E50', gray='#7F8C8D')

def sx(ax, t, yl=''):
    ax.set_facecolor('white'); ax.set_title(t, fontsize=11, fontweight='bold', color=C['dark'], pad=8)
    if yl: ax.set_ylabel(yl, fontsize=10, color=C['gray'])
    [ax.spines[s].set_visible(False) for s in ['top', 'right']]
    ax.grid(axis='y', alpha=0.3); ax.tick_params(labelsize=9)

fig, axes = plt.subplots(2, 2, figsize=(16, 12)); fig.patch.set_facecolor(C['bg'])
COLS = [C['navy'], C['teal'], C['gold'], C['blue']]

# Chart 1 — all segments
ax = axes[0, 0]
for (name, _), col in zip(SEG.items(), COLS):
    d = df[df.seg == name].sort_values('date')
    ax.fill_between(d.date, d.lo, d.hi, alpha=0.12, color=col)
    ax.plot(d.date, d['base'], color=col, lw=2, marker='o', ms=4, label=name)
    ax.annotate(f"{d.iloc[-1]['base']/1000:.0f}k", xy=(d.iloc[-1].date, d.iloc[-1]['base']),
                xytext=(5, 0), textcoords='offset points', fontsize=8, color=col, fontweight='bold')
ax.legend(fontsize=8); sx(ax, 'Tous Segments — Baseline + IC 95% (TND)')

# Chart 2 — IPIM history + forecast
ax = axes[0, 1]
ax.scatter(ipim.date, ipim.ipim_appartement, s=14, color=C['gold'], zorder=5, label='IPIM réel (INS)')
fwa = fc_['ipim_appartement'][fc_['ipim_appartement'].ds >= ANC]
ax.fill_between(fwa.ds, fwa.yhat_lower, fwa.yhat_upper, alpha=0.2, color=C['blue'])
ax.plot(fwa.ds, fwa.yhat, C['blue'], lw=2, ls='--', marker='o', ms=4, label='Prévision')
ax.axvline(ANC, color=C['red'], ls='--', lw=1.2, alpha=0.7, label='Q1 2026')
ax.legend(fontsize=8); sx(ax, 'IPIM Appartement — 97 pts + Prévision', 'Indice')

# Chart 3 — TUNIS apartment detail
ax = axes[1, 0]
ta = df[df.seg == 'Appt — TUNIS'].sort_values('date')
ax.fill_between(ta.date, ta.lo, ta.hi, alpha=0.2, color=C['blue'], label='IC 95%')
ax.plot(ta.date, ta['base'], C['blue'], lw=2.5, marker='o', ms=5, label='Baseline')
ax.plot(ta.date, ta.lo, C['red'], lw=1.2, ls='--', alpha=0.7)
ax.plot(ta.date, ta.hi, C['green'], lw=1.2, ls='--', alpha=0.7)
for _, r in ta.iterrows():
    ax.annotate(f"{r['base']/1000:.0f}k", xy=(r.date, r['base']), xytext=(0, 9),
                textcoords='offset points', ha='center', fontsize=8, color=C['blue'])
ax.legend(fontsize=8); sx(ax, 'Appartement TUNIS — Détail + IC 95% (TND)')

# Chart 4 — % change bar Q1 2028
ax = axes[1, 1]
q28  = df[df.q == 'Q1 2028']
vals = [float(q28[q28.seg == s]['chg'].values[0]) for s in SEG]
bars = ax.bar(range(4), vals, color=COLS, alpha=0.85, width=0.6)
ax.bar_label(bars, [f"{v:+.1f}%" for v in vals], padding=4, fontsize=11, fontweight='bold')
ax.set_xticks(range(4)); ax.set_xticklabels([s.replace(' — ', '\n') for s in SEG],
                                              rotation=10, ha='center', fontsize=9)
ax.axhline(0, color=C['gray'], lw=1); sx(ax, 'Variation % Q1 2026 → Q1 2028', '%')

fig.suptitle('EstateMind — Prévision Prix Immobiliers\n'
             'Prophet × IPIM (97 pts 2000–2024) + Régresseurs IPC & BCT',
             fontsize=13, fontweight='bold', color=C['navy'], y=1.01)
plt.tight_layout()
plt.savefig(EXPORTS / 'price_forecast_chart.png', dpi=150, bbox_inches='tight', facecolor=C['bg'])
print(f"\nCSV + Chart + PKL → {EXPORTS}")
print("  price_forecast.csv  ·  price_forecast_chart.png")
print("  prophet_ipim_appartement.pkl  ·  prophet_ipim_maison.pkl  ·  prophet_ipim_terrain.pkl")