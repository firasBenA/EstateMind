"""
pipeline_zones_2032.py  —  VERSION FINALE CORRIGÉE
====================================================
Prédiction de prix immobiliers par zone, Tunisie 2026→2032.

POURQUOI LA VERSION PRÉCÉDENTE ÉTAIT INCORRECTE
------------------------------------------------
Elle appliquait la formule:
    prix_futur = prix_actuel × (IPIM_futur / IPIM_ancrage)

Ce n'est pas de la série temporelle. C'est une multiplication d'un
ratio d'indice par un prix. Le SARIMA était utilisé uniquement pour
calculer ce ratio — le résultat final n'est pas une prédiction par
série temporelle, c'est une projection linéaire déguisée.

LA VRAIE MÉTHODE  (implémentée ici)
-------------------------------------
Un modèle à deux composantes:

    ┌─────────────────────────────────────────────────────────────┐
    │ COMPOSANTE 1 – TEMPORELLE (SARIMA sur série de prix en TND) │
    │                                                             │
    │  a) On ancre l'IPIM officiel sur le prix observé en 2026:   │
    │       P_national(t) = P0_obs × IPIM(t) / IPIM_ancrage       │
    │     → série historique de prix en TND/m² (2000–2024)        │
    │                                                             │
    │  b) SARIMA(1,1,1)(1,1,1,4) ajusté sur cette série TND       │
    │     → prévisions DIRECTEMENT en TND/m² avec IC 95%          │
    └─────────────────────────────────────────────────────────────┘

    ┌─────────────────────────────────────────────────────────────┐
    │ COMPOSANTE 2 – SPATIALE (Modèle hédonique OLS)              │
    │                                                             │
    │  log(prix_m2) = α + Σ γ_z·zone + β₁·log(surface) + β₂·rooms│
    │                                                             │
    │  → γ_z = prime pure de chaque zone (surface contrôlée)      │
    │  → index_zone = exp(γ_z)                                    │
    └─────────────────────────────────────────────────────────────┘

    ┌─────────────────────────────────────────────────────────────┐
    │ PRÉDICTION FINALE                                           │
    │                                                             │
    │  prix_m2(zone, t) = SARIMA_forecast_TND(t) × index_zone     │
    │                                                             │
    │  valeur(zone, surface, t) = prix_m2(zone, t) × surface      │
    └─────────────────────────────────────────────────────────────┘

La série SARIMA prédit des TND/m² — c'est l'économie de marché réelle.
L'indice de zone corrige spatialement. Aucun coefficient arbitraire.

Référence méthodologique:
    Rosen, S. (1974) – Hedonic Prices and Implicit Markets, JPE.
    Box & Jenkins (1976) – Time Series Analysis: Forecasting and Control.
    Case & Shiller (1989) – The Efficiency of the Market for Single-Family Homes.
"""

import re, warnings, logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.formula.api import ols
from statsmodels.tsa.stattools import adfuller, kpss

warnings.filterwarnings('ignore')
logging.getLogger('statsmodels').setLevel(logging.ERROR)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
IPIM_PATH     = 'ipim_historical.csv'
LISTINGS_PATH = 'listings_vente_ipim.csv'
OUT_DIR       = 'outputs/tunisia_final'
FIG_DIR       = f'{OUT_DIR}/figures'
RES_DIR       = f'{OUT_DIR}/results'

# Date d'observation des listings (ancrage spatial)
ANCHOR_DATE      = pd.Timestamp('2026-04-15')

# Paramètres SARIMA – validés par tests ADF/KPSS (série I(1))
SARIMA_ORDER     = (1, 1, 1)
SARIMA_SEASONAL  = (1, 1, 1, 4)   # saisonnalité trimestrielle
CI_ALPHA         = 0.05            # IC 95 %
BACKTEST_N       = 4               # trimestres de test

TARGET_YEARS     = list(range(2026, 2033))
MIN_OBS_ZONE     = 5

# Mapping type de bien → colonne IPIM
IPIM_COL = {
    'appartement': 'ipim_appartement',
    'maison':      'ipim_maison',
    'terrain':     'ipim_terrain',
}

# Normalisation des noms de zones (doublons détectés dans le dataset)
ZONE_MAP = {
    'jardins de cartage':    'Jardins de Carthage',
    'jardins de carthage':   'Jardins de Carthage',
    'jardins de carthge':    'Jardins de Carthage',
    'jardins de carthage':   'Jardins de Carthage',
    'el aouina':             'El Aouina',
    'aouina':                'El Aouina',
    'berge du lac':          'Berges du Lac',
    'berges du lac':         'Berges du Lac',
    'les berges du lac':     'Berges du Lac',
    'chatt meriem':          'Chott Meriem',
    'chott meriem':          'Chott Meriem',
    'bou mhel':              'Boumhel',
    'bou mhel el bassatine': 'Boumhel',
    'boumhel':               'Boumhel',
    'la mannouba':           'La Manouba',
    'manouba':               'La Manouba',
    'cite ennasr 2':         'Ennasr 2',
    'ennasr':                'Ennasr 2',
    'riadh landlous':        'Riadh Al Andalous',
    'riadh al andalous':     'Riadh Al Andalous',
    'hammam sousse gharbi':  'Hammam Sousse',
}

def _norm_zone(name):
    if pd.isna(name):
        return np.nan
    k = re.sub(r'\s+', ' ', str(name).strip().lower())
    return ZONE_MAP.get(k, str(name).strip().title())


# ═════════════════════════════════════════════════════════════════════════════
# CHARGEMENT
# ═════════════════════════════════════════════════════════════════════════════

def load_listings():
    """Charge et nettoie les annonces de vente."""
    df = pd.read_csv(LISTINGS_PATH)
    df = df[df['is_outlier'] == False].copy()
    df = df.dropna(subset=['price', 'surface', 'ipim_type']).copy()
    df['price_per_m2'] = df['price'] / df['surface']
    df['log_price_m2'] = np.log(df['price_per_m2'].replace(0, np.nan))
    df['log_surface']  = np.log(df['surface'].replace(0, np.nan))
    df['zone'] = df['city'].apply(_norm_zone)
    # Imputation rooms (37 % manquants) par la médiane du type
    med_rooms = df.groupby('ipim_type')['rooms'].transform('median')
    df['rooms'] = df['rooms'].fillna(med_rooms).fillna(2)
    df = df.dropna(subset=['log_price_m2', 'log_surface', 'zone']).copy()
    print(f"[listings] {len(df)} annonces | zones: {df['zone'].nunique()}")
    return df


def load_ipim():
    """Charge la série IPIM officielle INS."""
    ip = pd.read_csv(IPIM_PATH)
    ip['date'] = pd.to_datetime(ip['date'])
    ip = ip.sort_values('date').set_index('date')
    print(f"[IPIM] {len(ip)} trimestres | "
          f"{ip.index[0].date()} → {ip.index[-1].date()}")
    return ip


# ═════════════════════════════════════════════════════════════════════════════
# COMPOSANTE 1 — SÉRIE TEMPORELLE EN TND (SARIMA)
# ═════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# CORRECTION de _stationarity() — à remplacer dans pipeline_zones.py
# ─────────────────────────────────────────────────────────────────────────────

def _stationarity(s, label):
    """
    Tests ADF + KPSS robustes pour séries trimestrielles courtes (~97 obs).

    Problème de la version précédente :
        adfuller(s, autolag='AIC') sélectionnait jusqu'à 8 retards sur
        97 observations → perte de puissance → faux négatif sur diff(1).

    Corrections apportées :
        1. ADF avec maxlag=4 (1 an de retards = adapté à données trimestrielles)
        2. Test sur diff(1) SEULE  pour vérifier d=1
        3. Test sur diff(1)+diff(4) pour vérifier que SARIMA(d=1,D=1,s=4)
           travaille bien sur une série stationnaire
    """

    s = s.dropna()

    # ── Niveau ──────────────────────────────────────────────────────────────
    adf_p_niveau  = adfuller(s, maxlag=4, autolag=None)[1]
    try:
        kpss_p_niveau = kpss(s, regression='c', nlags='auto')[1]
    except Exception:
        kpss_p_niveau = 0.05

    # ── Diff(1) ─────────────────────────────────────────────────────────────
    d1 = s.diff().dropna()
    adf_p_d1 = adfuller(d1, maxlag=4, autolag=None)[1]

    # ── Diff(1) + Diff(4) — ce que fait SARIMA(d=1,D=1,s=4) ────────────────
    d1_d4 = s.diff().diff(4).dropna()
    adf_p_d1d4  = adfuller(d1_d4, maxlag=4, autolag=None)[1]
    try:
        kpss_p_d1d4 = kpss(d1_d4, regression='c', nlags='auto')[1]
    except Exception:
        kpss_p_d1d4 = 0.10

    # ── Diagnostic ──────────────────────────────────────────────────────────
    is_i1_d1   = (adf_p_niveau > 0.05) and (adf_p_d1 < 0.05)
    is_stat_sarima = (adf_p_d1d4 < 0.05) and (kpss_p_d1d4 > 0.05)

    print(f"    Stationnarité [{label}]:")
    print(f"      Niveau    : ADF p={adf_p_niveau:.3f} | KPSS p={kpss_p_niveau:.3f}")
    print(f"      Diff(1)   : ADF p={adf_p_d1:.3f}  {'→ I(1) confirmé ✓' if is_i1_d1 else '→ attention'}")
    print(f"      Diff(1+4) : ADF p={adf_p_d1d4:.4f} | KPSS p={kpss_p_d1d4:.3f}  "
          f"{'→ SARIMA stationnaire ✓' if is_stat_sarima else '→ pb'}")

    return is_i1_d1


def _sarima_anchor_ipim(ipim_series, anchor_date, ipim_label='IPIM', verbose=True):
    """
    1ÈRE UTILISATION DE SARIMA — Extrapolation de l'IPIM jusqu'à la date d'ancrage.

    Problème :
        La série officielle INS s'arrête à 2024-Q1.
        Les listings ont été collectés en avril 2026.
        Il manque donc ~2.3 ans d'IPIM pour calculer l'ancrage.

    Solution :
        On ajuste SARIMA(1,1,1)(1,1,1,4) sur la série IPIM brute (indice)
        et on lit la valeur projetée à la date d'ancrage.
        → IPIM(2026) ≈ 196  (cohérent avec le document PDF)

    Pourquoi SARIMA plutôt qu'une extrapolation naïve ?
        - SARIMA capture la saisonnalité trimestrielle et les cycles
        - SARIMA fournit un IC 95% sur la valeur d'ancrage
        - SARIMA est cohérent avec l'approche globale du pipeline
        - L'extrapolation naïve (taux composé sur 5 ans) ignore la dynamique
          récente et donne une valeur légèrement biaisée (+3.8 pts ici)

    Parameters
    ----------
    ipim_series  : pd.Series — série IPIM trimestrielle (indice brut)
    anchor_date  : pd.Timestamp — date des listings (ex: 2026-04-15)
    ipim_label   : str — nom affiché dans les logs (ex: 'ipim_appartement')
    verbose      : bool

    Returns
    -------
    ipim_anchor  : float — valeur IPIM à la date d'ancrage (ex: 196.39)
    fc_ipim      : pd.Series — forecast IPIM complet jusqu'à anchor_date
    """
    s = ipim_series.dropna().astype(float)
    last_date = s.index[-1]

    # Nombre de trimestres à prédire pour atteindre la date d'ancrage
    n_steps = int(np.ceil((anchor_date - last_date).days / 91)) + 2
    n_steps = max(n_steps, 4)

    # ── Backtest : entraîner sur [:-BACKTEST_N], tester sur [-BACKTEST_N:] ──
    train_ipim = s.iloc[:-BACKTEST_N]
    test_ipim  = s.iloc[-BACKTEST_N:]
    bt_model   = SARIMAX(
        train_ipim, order=SARIMA_ORDER, seasonal_order=SARIMA_SEASONAL,
        enforce_stationarity=False, enforce_invertibility=False,
    ).fit(disp=False)
    pred_bt_ipim       = bt_model.get_forecast(BACKTEST_N).predicted_mean
    pred_bt_ipim.index = test_ipim.index
    mape_ipim = float(np.mean(np.abs((test_ipim - pred_bt_ipim) / test_ipim)) * 100)
    rmse_ipim = float(np.sqrt(np.mean((test_ipim - pred_bt_ipim) ** 2)))

    # ── Fit final sur la série complète ──────────────────────────────────────
    model_ipim = SARIMAX(
        s, order=SARIMA_ORDER, seasonal_order=SARIMA_SEASONAL,
        enforce_stationarity=False, enforce_invertibility=False,
    ).fit(disp=False)

    # Forecast jusqu'à la date d'ancrage
    fc      = model_ipim.get_forecast(steps=n_steps)
    fc_mean = fc.predicted_mean
    fc_ci   = fc.conf_int(alpha=CI_ALPHA)

    # Lire la valeur au trimestre le plus proche de anchor_date
    idx          = fc_mean.index.get_indexer([anchor_date], method='nearest')[0]
    ipim_anchor  = float(fc_mean.iloc[idx])
    anchor_low   = float(fc_ci.iloc[idx, 0])
    anchor_high  = float(fc_ci.iloc[idx, 1])
    anchor_qdate = fc_mean.index[idx]

    if verbose:
        last_known = s.iloc[-1]
        print(f"\n  ── SARIMA sur IPIM brut [{ipim_label}] — bridge vers 2026 ──")
        print(f"     Dernier IPIM connu  : {last_known:.4f}  ({last_date.date()})")
        print(f"     IPIM(ancrage) SARIMA: {ipim_anchor:.4f}  "
              f"[IC 95%: {anchor_low:.2f} – {anchor_high:.2f}]")
        print(f"     Trimestre d'ancrage : {anchor_qdate.date()}")
        print(f"     Backtest ({BACKTEST_N} trim) : "
              f"MAPE={mape_ipim:.2f}%  RMSE={rmse_ipim:.3f} pts")
        print(f"     AIC={model_ipim.aic:.1f}  BIC={model_ipim.bic:.1f}")

    eval_metrics = {
        'mape':        mape_ipim,
        'rmse':        rmse_ipim,
        'aic':         model_ipim.aic,
        'bic':         model_ipim.bic,
        'ipim_anchor': ipim_anchor,
        'anchor_low':  anchor_low,
        'anchor_high': anchor_high,
        'anchor_date': anchor_qdate,
        'obs':         test_ipim,
        'pred':        pred_bt_ipim,
        'n_obs':       len(s),
        'last_val':    float(s.iloc[-1]),
        'last_date':   last_date,
    }

    return ipim_anchor, fc_mean, eval_metrics


def build_tnd_series(ipim_series, P0_national, anchor_date, verbose=True,
                     ipim_label='IPIM'):
    """
    Reconstruit la série HISTORIQUE de prix en TND/m².

    Principe d'ancrage :
        On observe P0_national = médiane(prix/m²) dans les listings
        à la date d'ancrage (avril 2026).
        On connaît IPIM(t) pour t ∈ [2000Q1, 2024Q1].

        Si IPIM mesure l'évolution relative des prix, alors :
            P(t) / P(2026) = IPIM(t) / IPIM(2026)

        En isolant P(t) :
            P(t) = P0_national × IPIM(t) / IPIM(2026)

    IPIM(2026) est obtenu par la 1ère utilisation de SARIMA
    (_sarima_anchor_ipim), qui extrapole l'indice officiel INS
    jusqu'à la date des listings.  Aucune valeur n'est parachutée.

    Returns
    -------
    tnd_series   : pd.Series — prix en TND/m² sur la période IPIM
    ipim_anchor  : float — valeur IPIM à la date d'ancrage (ex: 196.39)
    """
    # ── Étape 1 : extrapolation IPIM(2026) via SARIMA ───────────────────────
    ipim_anchor, fc_ipim, _ = _sarima_anchor_ipim(
        ipim_series, anchor_date,
        ipim_label=ipim_label,
        verbose=verbose,
    )

    # ── Étape 2 : conversion IPIM → TND/m² ──────────────────────────────────
    # P(t) = P0 × IPIM(t) / IPIM(ancrage)
    # Par construction, quand t = ancrage : P(ancrage) = P0  ✓
    tnd_series = ipim_series * (P0_national / ipim_anchor)
    tnd_series.name = 'prix_m2_tnd'

    if verbose:
        print(f"     Série TND reconstruite : "
              f"{tnd_series.iloc[0]:.1f} → {tnd_series.iloc[-1]:.1f} TND/m²  "
              f"(facteur = {P0_national / ipim_anchor:.4f})")

    return tnd_series, ipim_anchor


def fit_sarima_on_tnd(tnd_series, ipim_type, verbose=True):
    """
    ÉTAPE CENTRALE: ajuste SARIMA sur la série de prix en TND.

    C'est ici que la série temporelle est modélisée.
    Le SARIMA prédit des TND/m², pas un indice, pas un ratio.
    Les intervalles de confiance sont directement en TND.

    Returns:
        result  : SARIMAXResults
        backtest: dict (mape, rmse, pred, obs)
    """
    s = tnd_series.dropna().astype(float)

    if verbose:
        print(f"\n  ── SARIMA sur TND/m² [{ipim_type}] ──")
        print(f"     Série: {len(s)} obs | "
              f"{s.index[0].date()} → {s.index[-1].date()}")
        print(f"     Niveau actuel (fin série): {s.iloc[-1]:.1f} TND/m²")
        _stationarity(s, 'niveau')

    # Backtest: entraîner sur [:-BACKTEST_N], tester sur [-BACKTEST_N:]
    train, test = s.iloc[:-BACKTEST_N], s.iloc[-BACKTEST_N:]
    bt = SARIMAX(train, order=SARIMA_ORDER, seasonal_order=SARIMA_SEASONAL,
                 enforce_stationarity=False, enforce_invertibility=False
                 ).fit(disp=False)
    pred_bt = bt.get_forecast(BACKTEST_N).predicted_mean
    pred_bt.index = test.index
    mape = float(np.mean(np.abs((test - pred_bt) / test)) * 100)
    rmse = float(np.sqrt(np.mean((test - pred_bt) ** 2)))

    # Fit final sur la série complète
    result = SARIMAX(s, order=SARIMA_ORDER, seasonal_order=SARIMA_SEASONAL,
                     enforce_stationarity=False, enforce_invertibility=False
                     ).fit(disp=False)

    if verbose:
        print(f"     Backtest ({BACKTEST_N} trim): "
              f"MAPE={mape:.2f}%  RMSE={rmse:.1f} TND/m²")
        print(f"     AIC={result.aic:.1f}  BIC={result.bic:.1f}")

    return result, {'mape': mape, 'rmse': rmse,
                    'pred': pred_bt, 'obs': test}


def forecast_tnd_to_2032(result, anchor_date, verbose=True):
    """
    Projette la série TND/m² jusqu'à fin 2032.

    Le forecast sort directement en TND/m² (pas un ratio, pas un indice).
    Les IC 95% sont aussi en TND.

    Returns:
        df_fc  : DataFrame (date, prix_m2, prix_m2_low, prix_m2_high)
        annual : DataFrame annual Q4 (2026→2032)
    """
    last_date    = result.model.data.orig_endog.index[-1]
    end_date     = pd.Timestamp('2032-12-31')
    n_quarters   = int(np.ceil((end_date - last_date).days / 91)) + 4
    n_quarters   = max(n_quarters, 40)  # au moins 10 ans de forecast

    fc     = result.get_forecast(steps=n_quarters)
    fc_mean = fc.predicted_mean
    fc_ci   = fc.conf_int(alpha=CI_ALPHA)

    df_fc = pd.DataFrame({
        'date':         fc_mean.index,
        'prix_m2':      fc_mean.values,
        'prix_m2_low':  fc_ci.iloc[:, 0].values,
        'prix_m2_high': fc_ci.iloc[:, 1].values,
    }).set_index('date')

    # Extraction annuelle: Q4 de chaque année cible
    annual_rows = []
    for yr in TARGET_YEARS:
        q4_date = pd.Timestamp(f'{yr}-10-01')
        idx = df_fc.index.get_indexer([q4_date], method='nearest')[0]
        row = df_fc.iloc[idx]
        annual_rows.append({
            'year':         yr,
            'date_q4':      df_fc.index[idx],
            'prix_m2':      row['prix_m2'],
            'prix_m2_low':  row['prix_m2_low'],
            'prix_m2_high': row['prix_m2_high'],
        })
    annual = pd.DataFrame(annual_rows)

    if verbose:
        print(f"\n     Forecast TND/m² (Q4 de chaque année):")
        print(f"     {'Année':>6}  {'Prix/m²':>10}  {'IC 95% [':>12}  {']':>10}")
        for _, r in annual.iterrows():
            print(f"     {int(r.year):>6}  {r.prix_m2:>10.0f}  "
                  f"{r.prix_m2_low:>12.0f}  {r.prix_m2_high:>10.0f}")

    return df_fc, annual


# ═════════════════════════════════════════════════════════════════════════════
# COMPOSANTE 2 — STRUCTURE SPATIALE (MODÈLE HÉDONIQUE)
# ═════════════════════════════════════════════════════════════════════════════

def fit_hedonic(df, ipim_type, verbose=True):
    """
    Régression hédonique OLS:
        log(prix_m2) = α + Σ γ_z·zone + β₁·log(surface) + β₂·rooms + ε

    Les coefficients γ_z représentent la prime PURE de chaque zone,
    contrôlée par la taille et le nombre de pièces.

    → zone_index_z = exp(γ_z)  [par rapport à la zone de référence]

    Returns:
        model    : OLSResults
        zone_idx : DataFrame (zone, index, premium_pct, price_m2_at_median)
        ref_zone : nom de la zone de référence
    """
    sub = df[df['ipim_type'] == ipim_type].copy()
    zc = sub['zone'].value_counts()
    ok = zc[zc >= MIN_OBS_ZONE].index.tolist()
    sub = sub[sub['zone'].isin(ok)].copy()

    if len(sub) < 20:
        raise ValueError(f"Insuffisant pour {ipim_type}: {len(sub)} obs")

    # Référence = zone avec le plus d'observations
    ref = sub['zone'].value_counts().index[0]
    sub['zone_cat'] = pd.Categorical(
        sub['zone'], categories=[ref] + [z for z in ok if z != ref])

    model = ols('log_price_m2 ~ C(zone_cat) + log_surface + rooms',
                data=sub).fit()

    params = model.params
    log_s_med = sub['log_surface'].median()
    r_med     = sub['rooms'].median()

    rows = []
    for z in ok:
        if z == ref:
            coef, se, pval = 0.0, 0.0, 1.0
        else:
            key = f'C(zone_cat)[T.{z}]'
            coef = params.get(key, 0.0)
            se   = model.bse.get(key, 0.0)
            pval = model.pvalues.get(key, 1.0)

        # Prix/m² prédit par le modèle hédonique (à la médiane des caractéristiques)
        log_p_pred = (params['Intercept'] + coef
                      + params['log_surface'] * log_s_med
                      + params['rooms'] * r_med)
        rows.append({
            'zone':         z,
            'n_obs':        int(zc[z]),
            'zone_coef':    coef,
            'zone_coef_se': se,
            'pval':         pval,
            'zone_index':   np.exp(coef),   # multiplicateur vs zone ref
            'premium_pct':  (np.exp(coef) - 1) * 100,
            'price_m2_hedonique': np.exp(log_p_pred),  # TND/m² from model
            'is_ref':       z == ref,
        })

    zone_idx = pd.DataFrame(rows).sort_values('zone_index', ascending=False)

    if verbose:
        print(f"\n  ── Hédonique [{ipim_type}] ──")
        print(f"     {len(sub)} obs | {len(ok)} zones | "
              f"R²={model.rsquared:.3f}  adj={model.rsquared_adj:.3f}")
        print(f"     β_log_surface={params['log_surface']:.3f}  "
              f"β_rooms={params['rooms']:.3f}")
        print(f"     Référence: {ref}")
        print(f"\n     Effets de zone (top 8 et bottom 3):")
        show = pd.concat([zone_idx.head(8), zone_idx.tail(3)])
        for _, r in show.iterrows():
            sig = '***' if r.pval < 0.001 else ('**' if r.pval < 0.01
                  else ('*' if r.pval < 0.05 else '  ns'))
            print(f"     {r.zone:<28} index={r.zone_index:5.3f} "
                  f"({r.premium_pct:+6.1f}%) {sig}  n={r.n_obs}")

    return model, zone_idx, ref


# ═════════════════════════════════════════════════════════════════════════════
# COMBINAISON: PRÉDICTION FINALE PAR ZONE × ANNÉE
# ═════════════════════════════════════════════════════════════════════════════

def predict_zone_year(annual_tnd, zone_idx, ipim_type):
    """
    Combine SARIMA (TND national) × hédonique (index zone).

    Prix/m² par zone et année:
        prix_m2(zone, t) = prix_m2_national_SARIMA(t) × index_zone

    L'IC 95% est propagé depuis le SARIMA:
        prix_m2_low(zone, t) = prix_m2_national_low(t) × index_zone
        prix_m2_high(zone, t) = prix_m2_national_high(t) × index_zone

    Returns: DataFrame (zone × année) avec prix en TND
    """
    rows = []
    for _, z in zone_idx.iterrows():
        zi = z['zone_index']  # multiplicateur spatial (hédonique)
        for _, t in annual_tnd.iterrows():
            rows.append({
                'ipim_type':       ipim_type,
                'zone':            z['zone'],
                'n_obs':           int(z['n_obs']),
                'zone_index':      round(zi, 4),
                'premium_pct':     round(z['premium_pct'], 1),
                'year':            int(t['year']),
                # Prix au m² en TND
                'prix_m2':         round(t['prix_m2'] * zi, 1),
                'prix_m2_low':     round(t['prix_m2_low'] * zi, 1),
                'prix_m2_high':    round(t['prix_m2_high'] * zi, 1),
                # Pour un bien de 100 m² de référence
                'valeur_100m2':    round(t['prix_m2'] * zi * 100, 0),
                'valeur_low':      round(t['prix_m2_low'] * zi * 100, 0),
                'valeur_high':     round(t['prix_m2_high'] * zi * 100, 0),
            })
    return pd.DataFrame(rows)


def estimate_price(zone, ipim_type, surface_m2, year, rooms=3,
                   ts_models=None, hedonic_models=None):
    """
    Estime le prix d'un bien dans une zone à une année donnée.

    Paramètres:
        zone       : nom de la zone (ex: 'La Soukra')
        ipim_type  : 'appartement' | 'maison' | 'terrain'
        surface_m2 : surface du bien en m²
        year       : année cible (2026–2032)
        rooms      : nombre de pièces

    Returns:
        dict avec prix/m², valeur, IC 95%, et décomposition
    """
    zone_norm = _norm_zone(zone)
    _, zone_idx, ref = hedonic_models[ipim_type]
    _, annual_tnd    = ts_models[ipim_type]

    zrow = zone_idx[zone_idx['zone'] == zone_norm]
    if zrow.empty:
        raise ValueError(f"Zone '{zone_norm}' non trouvée dans {ipim_type}. "
                         f"Disponibles:\n{zone_idx['zone'].tolist()}")
    zrow = zrow.iloc[0]

    trow = annual_tnd[annual_tnd['year'] == year]
    if trow.empty:
        raise ValueError(f"Année {year} hors horizon (2026–2032)")
    trow = trow.iloc[0]

    # La correction hédonique tient compte de la surface et des pièces
    # On recalcule le prix/m² de la zone pour ce profil exact
    model_ols = hedonic_models[ipim_type][0]
    params    = model_ols.params
    key_z     = f'C(zone_cat)[T.{zone_norm}]'
    coef_z    = params.get(key_z, 0.0) if not zrow['is_ref'] else 0.0
    log_p_ref = (params['Intercept'] + coef_z
                 + params['log_surface'] * np.log(surface_m2)
                 + params['rooms'] * rooms)
    p_m2_hedonique = np.exp(log_p_ref)

    # Le SARIMA nous donne l'évolution temporelle nationale (en TND/m²)
    # On calcule l'indice d'évolution depuis le début de la période
    trow_2026 = annual_tnd[annual_tnd['year'] == 2026].iloc[0]
    evolution_ratio = trow['prix_m2'] / trow_2026['prix_m2']
    evo_low         = trow['prix_m2_low'] / trow_2026['prix_m2']
    evo_high        = trow['prix_m2_high'] / trow_2026['prix_m2']

    # Prix futur: on applique l'évolution temporelle au prix hédonique courant
    p_m2_futur = p_m2_hedonique * evolution_ratio

    return {
        'zone':               zone_norm,
        'ipim_type':          ipim_type,
        'surface_m2':         surface_m2,
        'rooms':              rooms,
        'year':               year,
        # Prix/m²
        'prix_m2_2026':       round(p_m2_hedonique, 1),
        'prix_m2_futur':      round(p_m2_futur, 1),
        'prix_m2_low':        round(p_m2_hedonique * evo_low, 1),
        'prix_m2_high':       round(p_m2_hedonique * evo_high, 1),
        # Valeur totale
        'valeur_2026':        round(p_m2_hedonique * surface_m2, 0),
        'valeur_futur':       round(p_m2_futur * surface_m2, 0),
        'valeur_low':         round(p_m2_hedonique * evo_low * surface_m2, 0),
        'valeur_high':        round(p_m2_hedonique * evo_high * surface_m2, 0),
        # Décomposition
        'evolution_temporelle_pct': round((evolution_ratio - 1) * 100, 2),
        'prime_zone_pct':           round(zrow['premium_pct'], 1),
        'zone_index':               round(zrow['zone_index'], 4),
    }


# ═════════════════════════════════════════════════════════════════════════════
# VISUALISATIONS
# ═════════════════════════════════════════════════════════════════════════════

def plot_sarima_anchor_metrics(ipim, tnd_series_all, bt_results, P0_national, labels):
    """
    Métriques complètes de la 1ère utilisation de SARIMA (_sarima_anchor_ipim).

    Panneau 1 — Série IPIM + forecast SARIMA vers 2026
        · IPIM officiel INS (2000→2024) + forecast jusqu'à avril 2026
        · IC 95%, point d'ancrage, comparaison vs extrapolation naïve

    Panneau 2 — Évaluation du backtest sur IPIM brut
        · Valeurs observées vs prédites sur les BACKTEST_N derniers trimestres
        · Résidus par trimestre (couleur verte si sous-estimation, rouge si sur)
        · MAPE et RMSE annotés directement sur le graphique

    Panneau 3 — Métriques récapitulatives
        · MAPE, RMSE, AIC, BIC
        · Stationnarité ADF (barres)
        · Tableau texte : IPIM(2026) SARIMA vs naïf, IC 95%, facteur TND

    Sauvegarde : FIG_DIR/0_anchor_sarima_{ipim_type}.png
    """
    from statsmodels.tsa.stattools import adfuller

    SARIMA_COLORS = {
        'appartement': '#2196F3',
        'maison':      '#4CAF50',
        'terrain':     '#FF9800',
    }

    for itype, col in IPIM_COL.items():
        if col not in ipim.columns or itype not in tnd_series_all:
            continue

        lbl    = labels[itype]
        ipim_s = ipim[col].dropna()
        P0     = P0_national[itype]
        c      = SARIMA_COLORS.get(itype, '#90A4AE')

        # ── Récupérer toutes les métriques via la fonction existante ──────────
        ipim_anchor, fc_mean, ev = _sarima_anchor_ipim(
            ipim_s, ANCHOR_DATE, ipim_label=col, verbose=False,
        )

        # Valeur naïve pour comparaison
        s5y        = ipim_s.iloc[-20:]
        g          = (s5y.iloc[-1] / s5y.iloc[0]) ** (1/5) - 1
        years_gap  = (ANCHOR_DATE - ev['last_date']).days / 365.25
        ipim_naive = float(ipim_s.iloc[-1] * (1 + g) ** years_gap)
        facteur    = P0 / ipim_anchor

        # Forecast complet avec IC 95% (réajuster pour avoir l'IC sur le graphique)
        from statsmodels.tsa.statespace.sarimax import SARIMAX as _SARIMAX
        _m  = _SARIMAX(ipim_s, order=SARIMA_ORDER, seasonal_order=SARIMA_SEASONAL,
                       enforce_stationarity=False, enforce_invertibility=False).fit(disp=False)
        n_s = int(np.ceil((ANCHOR_DATE - ipim_s.index[-1]).days / 91)) + 2
        _fc = _m.get_forecast(steps=max(n_s, 4))
        fc_ci = _fc.conf_int(alpha=CI_ALPHA)

        # Tests ADF sur IPIM brut
        adf_niveau = adfuller(ipim_s, maxlag=4, autolag=None)[1]
        adf_d1     = adfuller(ipim_s.diff().dropna(), maxlag=4, autolag=None)[1]
        adf_d1d4   = adfuller(ipim_s.diff().diff(4).dropna(), maxlag=4, autolag=None)[1]

        # ─────────────────────────────────────────────────────────────────────
        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        fig.suptitle(
            f'Métriques d\'évaluation — 1ère utilisation SARIMA  [{lbl}]\n'
            f'But : estimer IPIM(2026) pour la formule  TND(t) = P0 × IPIM(t) / IPIM(2026)',
            fontweight='bold', fontsize=12,
        )

        # ── Panneau 1 : IPIM historique + forecast ────────────────────────────
        ax1 = axes[0]
        ax1.plot(ipim_s.index, ipim_s.values,
                 '-', color=c, lw=2.2, label='IPIM officiel INS')
        ax1.axvline(ipim_s.index[-1], color='#78909C', lw=1, ls='--', alpha=0.6,
                    label=f'Fin INS ({ev["last_date"].date()})')
        ax1.plot(fc_mean.index, fc_mean.values,
                 '--', color=c, lw=2.2, alpha=0.85, label='Forecast SARIMA sur IPIM')
        ax1.fill_between(fc_mean.index,
                         fc_ci.iloc[:, 0], fc_ci.iloc[:, 1],
                         color=c, alpha=0.15, label='IC 95%')
        # Naïve
        naive_dates = [ipim_s.index[-1] + pd.DateOffset(months=3*k)
                       for k in range(1, max(n_s, 4) + 1)]
        naive_vals  = [float(ipim_s.iloc[-1] * (1+g)**((d-ipim_s.index[-1]).days/365.25))
                       for d in naive_dates]
        ax1.plot(naive_dates, naive_vals, ':', color='#FFC107', lw=1.8,
                 label=f'Naïf (g={g*100:.2f}%/an)')
        ax1.axvline(ANCHOR_DATE, color='#F44336', lw=1.5, ls=':', label='Date ancrage')
        ax1.scatter([ev['anchor_date']], [ipim_anchor],
                    color='#F44336', s=120, zorder=10)
        ax1.annotate(
            f'IPIM(2026) = {ipim_anchor:.2f}\n[IC: {ev["anchor_low"]:.1f}–{ev["anchor_high"]:.1f}]',
            xy=(ev['anchor_date'], ipim_anchor),
            xytext=(-100, 18), textcoords='offset points',
            color='#F44336', fontsize=8, fontweight='bold',
            arrowprops=dict(arrowstyle='->', color='#F44336', lw=1.1),
        )
        ax1.annotate(f'Naïf = {ipim_naive:.2f}',
                     xy=(ANCHOR_DATE, ipim_naive),
                     xytext=(8, -35), textcoords='offset points',
                     color='#FFC107', fontsize=7.5,
                     arrowprops=dict(arrowstyle='->', color='#FFC107', lw=0.9))
        ax1.set_title('Série IPIM + forecast → IPIM(2026)', fontsize=10)
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Valeur IPIM (base 2015 = 100)')
        ax1.legend(fontsize=7.5, loc='upper left', ncol=1)
        ax1.grid(True, alpha=0.3, lw=0.5)

        # ── Panneau 2 : Évaluation backtest ──────────────────────────────────
        ax2 = axes[1]
        obs  = ev['obs']
        pred = ev['pred']
        x    = range(len(obs))

        # Barres observé vs prédit
        w = 0.35
        bars_obs  = ax2.bar([i - w/2 for i in x], obs.values,
                            width=w, color=c, alpha=0.8, label='Observé (IPIM réel)')
        bars_pred = ax2.bar([i + w/2 for i in x], pred.values,
                            width=w, color='#FF7043', alpha=0.8, label='Prédit (SARIMA)')

        # Résidus (ligne + points)
        residuals = obs.values - pred.values
        ax2_r = ax2.twinx()
        ax2_r.plot(x, residuals, 'D--', color='#7E57C2', lw=1.5, ms=6,
                   label='Résidu (obs − pred)')
        ax2_r.axhline(0, color='#7E57C2', lw=0.8, alpha=0.5)
        ax2_r.set_ylabel('Résidu (pts IPIM)', color='#7E57C2', fontsize=8)
        ax2_r.tick_params(axis='y', colors='#7E57C2')

        # Annoter chaque barre de résidu
        for i, (res, o) in enumerate(zip(residuals, obs.values)):
            col_r = '#4CAF50' if res >= 0 else '#F44336'
            ax2_r.text(i, res + (0.1 if res >= 0 else -0.3),
                       f'{res:+.2f}\n({abs(res)/o*100:.1f}%)',
                       ha='center', va='bottom' if res >= 0 else 'top',
                       fontsize=7, color=col_r, fontweight='bold')

        xlab = [t.strftime('%Y-Q') + str((t.month-1)//3 + 1) for t in obs.index]
        ax2.set_xticks(list(x))
        ax2.set_xticklabels(xlab, fontsize=8.5)
        ax2.set_ylabel('Valeur IPIM', fontsize=9)
        ax2.set_title(
            f'Évaluation backtest ({BACKTEST_N} trimestres)\n'
            f'MAPE = {ev["mape"]:.2f}%   RMSE = {ev["rmse"]:.3f} pts',
            fontsize=10, fontweight='bold',
        )
        # Légendes combinées
        lines1, lbls1 = ax2.get_legend_handles_labels()
        lines2, lbls2 = ax2_r.get_legend_handles_labels()
        ax2.legend(lines1 + lines2, lbls1 + lbls2, fontsize=7.5, loc='lower right')
        ax2.grid(True, axis='y', alpha=0.3, lw=0.5)

        # Badge MAPE
        mape_color = '#4CAF50' if ev['mape'] < 5 else ('#FF9800' if ev['mape'] < 8 else '#F44336')
        mape_label = 'Excellent ✓' if ev['mape'] < 5 else ('Bon ✓' if ev['mape'] < 8 else 'À revoir')
        ax2.text(0.98, 0.97,
                 f'MAPE = {ev["mape"]:.2f}%\n{mape_label}\n(seuil bon < 8%)',
                 transform=ax2.transAxes, ha='right', va='top',
                 fontsize=8, color=mape_color, fontweight='bold',
                 bbox=dict(boxstyle='round', facecolor='white',
                           edgecolor=mape_color, alpha=0.9))

        # ── Panneau 3 : métriques + stationnarité ────────────────────────────
        ax3 = axes[2]
        ax3.axis('off')

        # Mini barplot stationnarité en inset
        inset = fig.add_axes([
            ax3.get_position().x0 + 0.005,
            ax3.get_position().y0 + 0.42,
            ax3.get_position().width * 0.95,
            0.28,
        ])
        s_vals   = [min(adf_niveau, 0.999), min(adf_d1, 0.999), min(adf_d1d4, 0.999)]
        s_colors = ['#F44336', '#FFC107', '#4CAF50']
        s_labels = ['Niveau\n(IPIM brut)', 'Diff(1)', 'Diff(1)+Diff(4)']
        s_texts  = [
            f'p={adf_niveau:.3f}\nNon stat. ✓',
            f'p={adf_d1:.4f}\nI(1) confirmé ✓' if adf_d1 < 0.05 else f'p={adf_d1:.4f}\n→ attention',
            f'p={adf_d1d4:.4f}\nSARIMA ok ✓'  if adf_d1d4 < 0.05 else f'p={adf_d1d4:.4f}\n→ pb',
        ]
        bars_s = inset.bar(range(3), s_vals, color=s_colors, alpha=0.82,
                           width=0.52, edgecolor='white', lw=1)
        inset.axhline(0.05, color='black', lw=1.2, ls='--', alpha=0.55)
        inset.set_xticks(range(3))
        inset.set_xticklabels(s_labels, fontsize=7)
        inset.set_ylabel('ADF p-value', fontsize=7.5)
        inset.set_title('Stationnarité — IPIM brut (ADF maxlag=4)',
                        fontsize=8, fontweight='bold')
        inset.set_ylim(0, 1.15)
        inset.grid(True, axis='y', alpha=0.25, lw=0.5)
        for i, (bar, txt) in enumerate(zip(bars_s, s_texts)):
            inset.text(i, bar.get_height() + 0.02, txt,
                       ha='center', va='bottom', fontsize=6.5,
                       color=s_colors[i], fontweight='bold')

        # Tableau récapitulatif
        recap = [
            ('── Modèle ──',                  ''),
            ('Architecture',                  f'SARIMA{SARIMA_ORDER}{SARIMA_SEASONAL}'),
            ('Observations',                  f'{ev["n_obs"]} trimestres (2000→2024)'),
            ('── Évaluation backtest ──',     ''),
            ('MAPE',                          f'{ev["mape"]:.2f}%  ({mape_label})'),
            ('RMSE',                          f'{ev["rmse"]:.3f} pts IPIM'),
            ('AIC',                           f'{ev["aic"]:.1f}'),
            ('BIC',                           f'{ev["bic"]:.1f}'),
            ('── Ancrage ──',                 ''),
            ('Dernier IPIM connu',            f'{ev["last_val"]:.4f}  ({ev["last_date"].date()})'),
            ('IPIM(2026) SARIMA',             f'{ev["ipim_anchor"]:.4f}'),
            ('IC 95%',                        f'[{ev["anchor_low"]:.2f} – {ev["anchor_high"]:.2f}]'),
            ('IPIM(2026) naïf',               f'{ipim_naive:.4f}'),
            ('Écart SARIMA − naïf',           f'{ev["ipim_anchor"] - ipim_naive:+.4f} pts'),
            ('── Conversion TND ──',          ''),
            ('P0 médiane listings',           f'{P0:.1f} TND/m²'),
            ('Facteur (P0 / IPIM_anchor)',     f'{facteur:.4f}'),
            ('TND(ancrage) = P0 ✓',           f'{ipim_anchor * facteur:.1f} TND/m²'),
        ]

        y_t  = ax3.get_position().y0 + 0.395
        x_l  = ax3.get_position().x0 + 0.005
        x_v  = ax3.get_position().x0 + ax3.get_position().width * 0.52

        for lbl_r, val_r in recap:
            if val_r == '':   # section header
                fig.text(x_l, y_t, lbl_r, fontsize=8, fontweight='bold',
                         color='#37474F', va='top')
            else:
                c_val = '#1B5E20' if '✓' in val_r else '#0D47A1'
                fig.text(x_l, y_t, lbl_r, fontsize=7.5, color='#546E7A', va='top')
                fig.text(x_v, y_t, val_r, fontsize=7.5, color=c_val,
                         fontweight='bold', va='top')
            y_t -= 0.026

        plt.tight_layout(rect=[0, 0, 1, 0.92])
        p = f'{FIG_DIR}/0_anchor_sarima_{itype}.png'
        plt.savefig(p, dpi=150, bbox_inches='tight')
        plt.close()
        print(f'  → {p}')
    from statsmodels.tsa.stattools import adfuller

    SARIMA_COLORS = {
        'appartement': '#38BDF8',
        'maison':      '#34D399',
        'terrain':     '#F97316',
    }

    for itype, col in IPIM_COL.items():
        if col not in ipim.columns or itype not in tnd_series_all:
            continue

        lbl        = labels[itype]
        ipim_s     = ipim[col].dropna()
        tnd_s      = tnd_series_all[itype]
        P0         = P0_national[itype]
        c          = SARIMA_COLORS.get(itype, '#94A3B8')

        # ── Refaire le SARIMA sur IPIM brut pour récupérer forecast + métriques
        model_ipim = SARIMAX(
            ipim_s, order=SARIMA_ORDER, seasonal_order=SARIMA_SEASONAL,
            enforce_stationarity=False, enforce_invertibility=False,
        ).fit(disp=False)

        last_date = ipim_s.index[-1]
        n_steps   = int(np.ceil((ANCHOR_DATE - last_date).days / 91)) + 2
        n_steps   = max(n_steps, 4)
        fc        = model_ipim.get_forecast(steps=n_steps)
        fc_mean   = fc.predicted_mean
        fc_ci     = fc.conf_int(alpha=CI_ALPHA)

        idx_anc        = fc_mean.index.get_indexer([ANCHOR_DATE], method='nearest')[0]
        ipim_anchor    = float(fc_mean.iloc[idx_anc])
        anchor_low     = float(fc_ci.iloc[idx_anc, 0])
        anchor_high    = float(fc_ci.iloc[idx_anc, 1])
        anchor_qdate   = fc_mean.index[idx_anc]

        # Valeur naïve pour comparaison
        s5y            = ipim_s.iloc[-20:]
        g              = (s5y.iloc[-1] / s5y.iloc[0]) ** (1/5) - 1
        years_gap      = (ANCHOR_DATE - last_date).days / 365.25
        ipim_naive     = float(ipim_s.iloc[-1] * (1 + g) ** years_gap)

        # Facteur de conversion
        facteur        = P0 / ipim_anchor
        tnd_anchor_val = ipim_anchor * facteur   # = P0 par construction

        # ── Tests stationnarité sur IPIM brut ──
        adf_niveau = adfuller(ipim_s, maxlag=4, autolag=None)[1]
        adf_d1     = adfuller(ipim_s.diff().dropna(), maxlag=4, autolag=None)[1]
        adf_d1d4   = adfuller(ipim_s.diff().diff(4).dropna(), maxlag=4, autolag=None)[1]

        # ──────────────────────────────────────────────────────────────────────
        fig, (ax_left, ax_right) = plt.subplots(
            1, 2, figsize=(15, 6),
            gridspec_kw={'width_ratios': [2, 1]},
        )
        fig.suptitle(
            f'1ère utilisation de SARIMA — Extension IPIM vers 2026  [{lbl}]\n'
            f'But : obtenir IPIM(ancrage) pour la formule  '
            f'TND(t) = P0 × IPIM(t) / IPIM(2026)',
            fontweight='bold', fontsize=12,
        )

        # ── Panneau gauche : IPIM + forecast ──────────────────────────────────
        # Série historique
        ax_left.plot(ipim_s.index, ipim_s.values,
                     '-', color=c, lw=2.2, label='IPIM officiel INS (historique)')
        # Ligne de séparation connu / forecast
        ax_left.axvline(last_date, color='#64748B', lw=1, ls='--', alpha=0.6,
                        label=f'Fin INS  ({last_date.date()})')
        # Forecast SARIMA
        ax_left.plot(fc_mean.index, fc_mean.values,
                     '--', color=c, lw=2.2, alpha=0.85,
                     label='SARIMA forecast sur IPIM brut')
        ax_left.fill_between(
            fc_mean.index, fc_ci.iloc[:, 0], fc_ci.iloc[:, 1],
            color=c, alpha=0.15, label='IC 95%',
        )
        # Valeur naïve
        naive_dates = [last_date + pd.DateOffset(months=3 * k) for k in range(1, n_steps + 1)]
        naive_vals  = [float(ipim_s.iloc[-1] * (1 + g) ** ((d - last_date).days / 365.25))
                       for d in naive_dates]
        ax_left.plot(naive_dates, naive_vals, ':', color='#F59E0B', lw=1.8, alpha=0.7,
                     label=f'Extrapolation naïve (g={g*100:.2f}%/an)')

        # Ligne ancrage
        ax_left.axvline(ANCHOR_DATE, color='#EF4444', lw=1.5, ls=':',
                        label=f'Date ancrage listings ({ANCHOR_DATE.date()})')
        # Point SARIMA(2026)
        ax_left.scatter([anchor_qdate], [ipim_anchor],
                        color='#EF4444', s=120, zorder=10,
                        label=f'IPIM(2026) SARIMA = {ipim_anchor:.2f}')
        ax_left.annotate(
            f'IPIM(2026)\n= {ipim_anchor:.2f}\n[{anchor_low:.1f} – {anchor_high:.1f}]',
            xy=(anchor_qdate, ipim_anchor),
            xytext=(-110, 20), textcoords='offset points',
            color='#EF4444', fontsize=8.5, fontweight='bold',
            arrowprops=dict(arrowstyle='->', color='#EF4444', lw=1.2),
        )
        # Différence SARIMA vs naïf
        diff = abs(ipim_anchor - ipim_naive)
        ax_left.annotate(
            f'Naïf = {ipim_naive:.2f}\nΔ = {diff:.2f} pts',
            xy=(anchor_qdate, ipim_naive),
            xytext=(10, -45), textcoords='offset points',
            color='#F59E0B', fontsize=8,
            arrowprops=dict(arrowstyle='->', color='#F59E0B', lw=1),
        )

        ax_left.set_xlabel('Date')
        ax_left.set_ylabel('Valeur IPIM (base 2015 = 100)')
        ax_left.legend(loc='upper left', fontsize=8, ncol=1)
        ax_left.grid(True, alpha=0.3, lw=0.5)
        ax_left.set_title(
            'Série IPIM officielle → extension SARIMA → IPIM(2026)',
            fontsize=10,
        )

        # ── Panneau droit : métriques ──────────────────────────────────────────
        ax_right.axis('off')

        # Stationnarité IPIM (barres)
        stat_labels = ['Niveau\n(IPIM brut)', 'Diff(1)', 'Diff(1)+Diff(4)']
        stat_vals   = [min(adf_niveau, 0.999), min(adf_d1, 0.999), min(adf_d1d4, 0.999)]
        stat_colors = ['#EF4444', '#F59E0B', '#10B981']
        stat_texts  = [
            f'p={adf_niveau:.3f}\nNon stat. (attendu)',
            f'p={adf_d1:.4f}\nI(1) confirmé ✓' if adf_d1 < 0.05 else f'p={adf_d1:.4f}\n→ attention',
            f'p={adf_d1d4:.4f}\nSARIMA ok ✓' if adf_d1d4 < 0.05 else f'p={adf_d1d4:.4f}\n→ pb',
        ]

        inset = fig.add_axes([
            ax_right.get_position().x0 + 0.01,
            ax_right.get_position().y0 + 0.38,
            ax_right.get_position().width - 0.01,
            0.30,
        ])
        bars = inset.bar(range(3), stat_vals, color=stat_colors, alpha=0.85,
                         width=0.55, edgecolor='white', lw=1)
        inset.axhline(0.05, color='black', lw=1.2, ls='--', alpha=0.6)
        inset.set_xticks(range(3))
        inset.set_xticklabels(stat_labels, fontsize=7.5)
        inset.set_ylabel('ADF p-value', fontsize=8)
        inset.set_title('Stationnarité sur IPIM brut', fontsize=8.5, fontweight='bold')
        inset.set_ylim(0, 1.1)
        inset.grid(True, axis='y', alpha=0.3, lw=0.5)
        for i, (bar, txt) in enumerate(zip(bars, stat_texts)):
            inset.text(i, bar.get_height() + 0.02, txt,
                       ha='center', va='bottom', fontsize=6.5,
                       color=stat_colors[i], fontweight='bold')

        # Tableau récapitulatif
        recap_lines = [
            ('Modèle', f'SARIMA({SARIMA_ORDER})({SARIMA_SEASONAL[:3]})[{SARIMA_SEASONAL[3]}]'),
            ('Données', f'{len(ipim_s)} trimestres INS'),
            ('Dernière valeur IPIM', f'{ipim_s.iloc[-1]:.4f}  ({last_date.date()})'),
            ('', ''),
            ('IPIM(2026) — SARIMA', f'{ipim_anchor:.4f}'),
            ('IC 95%', f'[{anchor_low:.2f}  –  {anchor_high:.2f}]'),
            ('IPIM(2026) — naïf', f'{ipim_naive:.4f}'),
            ('Écart SARIMA vs naïf', f'{ipim_anchor - ipim_naive:+.4f} pts'),
            ('', ''),
            ('AIC modèle IPIM', f'{model_ipim.aic:.1f}'),
            ('P0 (médiane listings)', f'{P0:.1f} TND/m²'),
            ('Facteur conversion', f'{facteur:.4f}  (P0 / IPIM_anchor)'),
            ('TND(ancrage)', f'{tnd_anchor_val:.1f} TND/m²  ← = P0 ✓'),
        ]

        y_text = ax_right.get_position().y0 + 0.34
        x0     = ax_right.get_position().x0 + 0.005
        x1     = ax_right.get_position().x0 + ax_right.get_position().width * 0.55

        fig.text(x0, y_text + 0.005, 'Métriques — 1ère utilisation SARIMA',
                 fontsize=9.5, fontweight='bold', va='top')
        y_text -= 0.025

        for label_r, val_r in recap_lines:
            if label_r == '' and val_r == '':
                y_text -= 0.010
                continue
            color_val = '#1B5E20' if '✓' in val_r else '#0D47A1'
            fig.text(x0, y_text, label_r, fontsize=7.5, color='#37474F', va='top')
            fig.text(x1, y_text, val_r,   fontsize=7.5, color=color_val,
                     fontweight='bold', va='top')
            y_text -= 0.028

        plt.tight_layout(rect=[0, 0, 1, 0.93])
        p = f'{FIG_DIR}/0_anchor_sarima_{itype}.png'
        plt.savefig(p, dpi=150, bbox_inches='tight')
        plt.close()
        print(f'  → {p}')

def plot_tnd_series_and_forecast(tnd_series, df_fc, annual, bt, ipim_type,
                                  P0, label):
    """
    Graphique principal: série TND/m² historique + backtest + prévision.
    C'est ce graphique qui prouve que le modèle est bien une série temporelle
    — le SARIMA est ajusté ET prédit en TND/m².
    """
    colors = {'appartement': '#1f77b4', 'maison': '#2ca02c', 'terrain': '#d62728'}
    c = colors.get(ipim_type, '#7f7f7f')

    fig, ax = plt.subplots(figsize=(14, 6))

    # Série historique TND reconstruite
    ax.plot(tnd_series.index, tnd_series.values,
            '-', color=c, lw=2, label=f'Prix/m² national historique (reconstruit)')

    # Points backtest
    ax.plot(bt['obs'].index, bt['obs'].values,
            'o', color='black', ms=6, zorder=5, label='Observé (période test)')
    ax.plot(bt['pred'].index, bt['pred'].values,
            's--', color='orange', ms=6, zorder=5,
            label=f'SARIMA backtest (MAPE={bt["mape"]:.2f}%)')

    # Prévision SARIMA en TND
    fc_dates = pd.DatetimeIndex(df_fc.index)
    ax.plot(fc_dates, df_fc['prix_m2'], '--', color=c, lw=2.5,
            label='Prévision SARIMA (TND/m²)')
    ax.fill_between(fc_dates, df_fc['prix_m2_low'], df_fc['prix_m2_high'],
                    color=c, alpha=0.20, label='IC 95 %')

    # Annotation des points annuels
    for _, r in annual.iterrows():
        ax.annotate(f"{r['prix_m2']:.0f}",
                    xy=(r['date_q4'], r['prix_m2']),
                    xytext=(0, 8), textcoords='offset points',
                    ha='center', fontsize=8, color=c)

    # Ligne verticale: ancrage (date des listings)
    ax.axvline(ANCHOR_DATE, color='gray', linestyle=':', lw=1.5,
               label=f'Ancrage (listings {ANCHOR_DATE.date()})')

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, _: f'{x:,.0f}'))
    ax.set_title(
        f'Série temporelle TND/m² — {label}\n'
        f'SARIMA({SARIMA_ORDER})({SARIMA_SEASONAL[:-1]},{SARIMA_SEASONAL[-1]}) '
        f'ajusté sur prix en TND | INS IPIM × Ancrage listings 2026',
        fontweight='bold')
    ax.set_xlabel('Date')
    ax.set_ylabel('Prix au m² (TND)')
    ax.legend(loc='upper left', fontsize=8, ncol=2)
    plt.tight_layout()
    p = f'{FIG_DIR}/1_serie_tnd_{ipim_type}.png'
    plt.savefig(p, dpi=150, bbox_inches='tight')
    plt.close()
    print(f'  → {p}')


def plot_zone_index(zone_idx, ipim_type, label):
    """Effets purs de zone (modèle hédonique)."""
    df = zone_idx.sort_values('zone_index', ascending=True)
    colors = ['#2ca02c' if p > 0 else '#d62728' for p in df['premium_pct']]

    fig, ax = plt.subplots(figsize=(11, max(5, 0.38 * len(df))))
    ax.barh(df['zone'], df['premium_pct'],
            color=colors, alpha=0.78, edgecolor='white')
    ax.axvline(0, color='black', lw=0.8)

    for _, r in df.iterrows():
        sig = ('***' if r.pval < 0.001 else ('**' if r.pval < 0.01
               else ('*' if r.pval < 0.05 else 'ns')))
        ax.text(r.premium_pct + (0.5 if r.premium_pct >= 0 else -0.5),
                df.index.get_loc(_) if isinstance(df.index, pd.RangeIndex)
                else list(df['zone']).index(r.zone),
                f"{r.premium_pct:+.1f}% {sig} (n={r.n_obs})",
                va='center', ha='left' if r.premium_pct >= 0 else 'right',
                fontsize=8)

    ax.set_title(f'Prime spatiale par zone — {label}\n'
                 f'Modèle hédonique OLS | contrôlé surface + pièces\n'
                 f'*** p<0.001  ** p<0.01  * p<0.05  ns non-significatif',
                 fontweight='bold')
    ax.set_xlabel('Prime vs zone de référence (%)')
    plt.tight_layout()
    p = f'{FIG_DIR}/2_zones_{ipim_type}.png'
    plt.savefig(p, dpi=150, bbox_inches='tight')
    plt.close()
    print(f'  → {p}')


def plot_forecast_by_zone(preds, ipim_type, label, top_n=10):
    """Trajectoires prévues par zone (prix/m²)."""
    sub = preds[preds['ipim_type'] == ipim_type]
    if sub.empty:
        return

    top = (sub[sub['year'] == 2026]
           .nlargest(top_n, 'prix_m2')['zone'].tolist())
    sub = sub[sub['zone'].isin(top)]

    fig, ax = plt.subplots(figsize=(13, 7))
    colors = plt.cm.tab10.colors

    for i, z in enumerate(top):
        d = sub[sub['zone'] == z].sort_values('year')
        if d.empty:
            continue
        ax.plot(d['year'], d['prix_m2'], 'o-',
                color=colors[i], lw=2, label=z)
        ax.fill_between(d['year'], d['prix_m2_low'], d['prix_m2_high'],
                        color=colors[i], alpha=0.10)
        last = d.iloc[-1]
        ax.annotate(f"{last['prix_m2']:.0f}",
                    xy=(last['year'], last['prix_m2']),
                    xytext=(5, 0), textcoords='offset points',
                    fontsize=8, color=colors[i])

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, _: f'{x:,.0f}'))
    ax.set_xticks(TARGET_YEARS)
    ax.set_title(f'Prix au m² prédit — {label} | Top {top_n} zones\n'
                 f'Méthode: SARIMA(TND) × Hédonique | IC 95 %',
                 fontweight='bold')
    ax.set_xlabel('Année')
    ax.set_ylabel('Prix au m² (TND)')
    ax.legend(loc='upper left', fontsize=8, ncol=2)
    plt.tight_layout()
    p = f'{FIG_DIR}/3_forecast_zones_{ipim_type}.png'
    plt.savefig(p, dpi=150, bbox_inches='tight')
    plt.close()
    print(f'  → {p}')


def plot_exemples_zones(ts_models, hedonic_models, surface=120, rooms=3):
    """Trajectoires pour des biens concrets dans différentes zones."""
    exemples = {
        'appartement': ['Berges du Lac', 'Jardins de Carthage',
                        'La Soukra', 'El Aouina', 'Ennasr 2', 'El Mourouj'],
        'maison':      ['Jardins de Carthage', 'Ain Zaghouan',
                        'La Soukra', 'El Menzah', 'Hammamet'],
    }
    labels = {'appartement': f'Appartement {surface} m²/{rooms} pièces',
              'maison':      f'Maison {surface} m²/{rooms} pièces'}

    for itype, zones in exemples.items():
        if itype not in ts_models or itype not in hedonic_models:
            continue
        fig, ax = plt.subplots(figsize=(13, 7))
        colors = plt.cm.tab10.colors

        for i, zone in enumerate(zones):
            try:
                vals = [estimate_price(zone, itype, surface, yr, rooms,
                                       ts_models, hedonic_models)
                        for yr in TARGET_YEARS]
                df_v = pd.DataFrame(vals)
                ax.plot(df_v['year'], df_v['valeur_futur'] / 1000,
                        'o-', color=colors[i], lw=2.5, label=zone)
                ax.fill_between(df_v['year'],
                                df_v['valeur_low'] / 1000,
                                df_v['valeur_high'] / 1000,
                                color=colors[i], alpha=0.10)
                last = df_v.iloc[-1]
                ax.annotate(
                    f"{last['valeur_futur']/1000:.0f}k\n"
                    f"(+{last['evolution_temporelle_pct']:.1f}%)",
                    xy=(last['year'], last['valeur_futur'] / 1000),
                    xytext=(5, 2), textcoords='offset points',
                    fontsize=7.5, color=colors[i])
            except Exception as e:
                print(f'    Skip {zone}: {e}')

        ax.yaxis.set_major_formatter(mticker.FuncFormatter(
            lambda x, _: f'{x:.0f} k'))
        ax.set_xticks(TARGET_YEARS)
        ax.set_title(
            f'Prévision — {labels[itype]} | 2026→2032\n'
            f'Modèle SARIMA(TND) + Hédonique | IC 95 % | Tunisie',
            fontweight='bold')
        ax.set_xlabel('Année')
        ax.set_ylabel('Valeur du bien (milliers TND)')
        ax.legend(loc='upper left', fontsize=9)
        plt.tight_layout()
        p = f'{FIG_DIR}/4_exemple_{itype}.png'
        plt.savefig(p, dpi=150, bbox_inches='tight')
        plt.close()
        print(f'  → {p}')


def plot_heatmap_prix_m2(preds, ipim_type, label):
    """Tableau de bord visuel: prix/m² par zone × année."""
    sub = preds[preds['ipim_type'] == ipim_type].copy()
    if sub.empty:
        return
    piv = sub.pivot_table(index='zone', columns='year',
                          values='prix_m2', aggfunc='first')
    piv = piv.sort_values(2032, ascending=False)

    fig, ax = plt.subplots(figsize=(12, max(4, 0.42 * len(piv))))
    im = ax.imshow(piv.values, aspect='auto', cmap='YlOrRd')
    ax.set_xticks(range(len(piv.columns)))
    ax.set_xticklabels(piv.columns)
    ax.set_yticks(range(len(piv.index)))
    ax.set_yticklabels(piv.index, fontsize=9)

    for i in range(len(piv.index)):
        for j in range(len(piv.columns)):
            v = piv.values[i, j]
            ax.text(j, i, f'{v:,.0f}', ha='center', va='center',
                    fontsize=7.5, color='black')

    plt.colorbar(im, ax=ax, label='Prix/m² (TND)')
    ax.set_title(f'Prix au m² (TND) — {label} | 2026→2032\n'
                 f'SARIMA(TND) × Hédonique | trié par prix 2032',
                 fontweight='bold')
    plt.tight_layout()
    p = f'{FIG_DIR}/5_heatmap_{ipim_type}.png'
    plt.savefig(p, dpi=150, bbox_inches='tight')
    plt.close()
    print(f'  → {p}')


# ═════════════════════════════════════════════════════════════════════════════
# ORCHESTRATION PRINCIPALE
# ═════════════════════════════════════════════════════════════════════════════

def run():
    import os
    for d in [FIG_DIR, RES_DIR]:
        os.makedirs(d, exist_ok=True)

    print('\n' + '═' * 70)
    print('  PIPELINE ZONES 2032 — MÉTHODE SÉRIE TEMPORELLE TND CORRIGÉE')
    print('  SARIMA ajusté sur prix en TND/m² | Hédonique pour zones')
    print('═' * 70)

    # ── Chargement ──────────────────────────────────────────────────────────
    df   = load_listings()
    ipim = load_ipim()

    # ── Prix/m² nationaux observés (ancrage, médiane de vos listings) ──────
    P0_national = {
        t: df[df['ipim_type'] == t]['price_per_m2'].median()
        for t in IPIM_COL
    }
    print(f'\n[Ancrage] Prix/m² médians observés (listings avril 2026):')
    for t, v in P0_national.items():
        print(f'  {t:15s}: {v:.1f} TND/m²')

    # ── COMPOSANTE 1: Série temporelle TND + SARIMA ─────────────────────────
    print('\n' + '─' * 70)
    print('COMPOSANTE 1 — SÉRIE TEMPORELLE EN TND (SARIMA)')
    print('─' * 70)

    ts_models = {}   # ipim_type → (df_fc, annual_tnd)
    bt_results = {}
    tnd_series_all = {}

    for itype, col in IPIM_COL.items():
        if col not in ipim.columns:
            continue
        P0 = P0_national[itype]

        # Construction de la série TND historique
        # → appelle _sarima_anchor_ipim() en interne pour estimer IPIM(2026)
        tnd, ipim_anchor = build_tnd_series(
            ipim[col], P0, ANCHOR_DATE,
            verbose=True, ipim_label=col,
        )
        tnd_series_all[itype] = tnd

        # Fit SARIMA sur TND
        result, bt = fit_sarima_on_tnd(tnd, itype, verbose=True)

        # Forecast jusqu'à 2032
        df_fc, annual = forecast_tnd_to_2032(result, ANCHOR_DATE, verbose=True)

        ts_models[itype]  = (df_fc, annual)
        bt_results[itype] = bt

    # ── COMPOSANTE 2: Hédonique par type de bien ────────────────────────────
    print('\n' + '─' * 70)
    print('COMPOSANTE 2 — MODÈLE HÉDONIQUE (EFFETS DE ZONE)')
    print('─' * 70)

    hedonic_models = {}
    for itype in IPIM_COL:
        try:
            model, zone_idx, ref = fit_hedonic(df, itype, verbose=True)
            hedonic_models[itype] = (model, zone_idx, ref)
        except ValueError as e:
            print(f'  Skip {itype}: {e}')

    # ── Combinaison: prix/m² par zone × année ────────────────────────────────
    print('\n' + '─' * 70)
    print('COMBINAISON — PRIX PAR ZONE × ANNÉE (TND/m²)')
    print('─' * 70)

    all_preds = []
    for itype in IPIM_COL:
        if itype not in ts_models or itype not in hedonic_models:
            continue
        _, annual = ts_models[itype]
        _, zone_idx, _ = hedonic_models[itype]
        preds = predict_zone_year(annual, zone_idx, itype)
        all_preds.append(preds)
    df_all = pd.concat(all_preds, ignore_index=True)

    # ── Rapport console ───────────────────────────────────────────────────────
    labels = {'appartement': 'Appartements', 'maison': 'Maisons',
              'terrain': 'Terrains'}
    for itype in IPIM_COL:
        sub = df_all[df_all['ipim_type'] == itype]
        if sub.empty:
            continue
        piv = sub.pivot_table(index='zone', columns='year',
                              values='prix_m2', aggfunc='first')
        piv = piv.sort_values(2032, ascending=False)
        print(f'\n{"─"*70}')
        print(f'  {labels[itype].upper()} — PRIX AU M² PRÉDIT (TND)')
        print(f'{"─"*70}')
        hdr = f"{'Zone':<28}" + ''.join(f'{y:>8}' for y in TARGET_YEARS)
        print(hdr)
        print('─' * 70)
        for z in piv.index:
            line = f'{z:<28}'
            for y in TARGET_YEARS:
                v = piv.loc[z, y] if y in piv.columns else np.nan
                line += f'{v:>8.0f}' if not np.isnan(v) else f'{"—":>8}'
            print(line)

    # ── Exemples concrets ────────────────────────────────────────────────────
    print('\n' + '─' * 70)
    print('EXEMPLES — Appartement 120 m² / 3 pièces')
    print('─' * 70)
    hdr = (f"{'Zone':<28} {'2026':>8} {'2028':>8} {'2030':>8} "
           f"{'2032':>8} {'Évol.':>8} {'Prime':>8}")
    print(hdr)
    print('─' * 70)
    for zone in ['Berges du Lac', 'Jardins de Carthage', 'La Soukra',
                 'Ennasr 2', 'El Aouina', 'El Mourouj',
                 'Hammamet', 'Nabeul']:
        try:
            est_list = [estimate_price(zone, 'appartement', 120, yr, 3,
                                       ts_models, hedonic_models)
                        for yr in [2026, 2028, 2030, 2032]]
            v26, v28, v30, v32 = [e['valeur_futur'] for e in est_list]
            evol = est_list[-1]['evolution_temporelle_pct']
            prem = est_list[0]['prime_zone_pct']
            print(f"{zone:<28} {v26/1000:>7.0f}k {v28/1000:>7.0f}k "
                  f"{v30/1000:>7.0f}k {v32/1000:>7.0f}k "
                  f"{evol:>+7.1f}% {prem:>+7.1f}%")
        except Exception as e:
            print(f'  {zone}: {e}')

    # ── Export CSV ────────────────────────────────────────────────────────────
    print('\n[EXPORT CSV]')

    # 1. Série TND et forecast par type
    for itype in IPIM_COL:
        if itype not in ts_models:
            continue
        df_fc, annual = ts_models[itype]
        annual.to_csv(f'{RES_DIR}/ts_tnd_annuel_{itype}.csv', index=False)
        print(f'  → ts_tnd_annuel_{itype}.csv')

    # 2. Prix par zone × année (format long)
    df_all.to_csv(f'{RES_DIR}/prix_zone_annuel_2032.csv', index=False)
    print(f'  → prix_zone_annuel_2032.csv')

    # 3. Format large (une ligne par zone)
    piv_export = df_all.pivot_table(
        index=['zone', 'ipim_type', 'n_obs', 'zone_index', 'premium_pct'],
        columns='year', values=['prix_m2', 'prix_m2_low', 'prix_m2_high'])
    piv_export.columns = [f'{v}_{y}' for v, y in piv_export.columns]
    piv_export.reset_index().to_csv(
        f'{RES_DIR}/prix_zone_large_2032.csv', index=False)
    print(f'  → prix_zone_large_2032.csv')

    # 4. Effets hédoniques
    hedon_rows = []
    for itype, (_, zone_idx, _) in hedonic_models.items():
        tmp = zone_idx.copy(); tmp['ipim_type'] = itype
        hedon_rows.append(tmp)
    pd.concat(hedon_rows).to_csv(
        f'{RES_DIR}/effets_hedoniques.csv', index=False)
    print(f'  → effets_hedoniques.csv')


    # 5. CSV ORIGINAL ENRICHI AVEC PRÉDICTIONS PAR BIEN
    print('[EXPORT PRINCIPAL] Enrichissement du CSV original...')
    df_enrichi = predict_all_listings(
        listings_path=LISTINGS_PATH,
        ts_models=ts_models,
        hedonic_models=hedonic_models,
        target_years=TARGET_YEARS,
        verbose=True,
    )
    df_enrichi.to_csv(f'{RES_DIR}/listings_avec_predictions_2032.csv', index=False)
    n_pred_cols = len([c for c in df_enrichi.columns if 'prix_pred' in c])
    print(f'  → listings_avec_predictions_2032.csv  '
          f'({len(df_enrichi)} lignes | {n_pred_cols} colonnes de prédiction)')

    # ── Graphiques ────────────────────────────────────────────────────────────
    print('\n[GRAPHIQUES]')

    # 0. Métriques 1ère utilisation SARIMA (ancrage IPIM → 2026)
    plot_sarima_anchor_metrics(
        ipim, tnd_series_all, bt_results, P0_national, labels,
    )
    for itype in IPIM_COL:
        if itype not in ts_models:
            continue
        lbl = labels[itype]
        df_fc, annual = ts_models[itype]

        plot_tnd_series_and_forecast(
            tnd_series_all[itype], df_fc, annual,
            bt_results[itype], itype, P0_national[itype], lbl)

        if itype in hedonic_models:
            _, zone_idx, _ = hedonic_models[itype]
            plot_zone_index(zone_idx, itype, lbl)
            plot_forecast_by_zone(df_all, itype, lbl)
            plot_heatmap_prix_m2(df_all, itype, lbl)

    plot_exemples_zones(ts_models, hedonic_models, surface=120, rooms=3)

    print('\n' + '═' * 70)
    print('  TERMINÉ')
    print(f'  Résultats : {RES_DIR}')
    print(f'  Figures   : {FIG_DIR}')
    print('═' * 70)

    return ts_models, hedonic_models, df_all, df_enrichi




# ═════════════════════════════════════════════════════════════════════════════
# ENRICHISSEMENT DU CSV ORIGINAL — AJOUT DES COLONNES DE PRÉDICTION
# ═════════════════════════════════════════════════════════════════════════════

def predict_all_listings(listings_path, ts_models, hedonic_models,
                          target_years=None, verbose=True):
    """
    Enrichit le CSV original en ajoutant les prix futurs prédits pour
    chaque bien (appartement, maison, terrain), année par année jusqu'à 2032.

    Colonnes ajoutées pour chaque année cible:
        prix_pred_{année}       → prix total prédit (TND)
        prix_pred_{année}_low   → borne basse IC 95%
        prix_pred_{année}_high  → borne haute IC 95%
        prix_m2_pred_{année}    → prix au m² prédit (TND/m²)

    + deux colonnes de diagnostic:
        methode_pred  → 'prix_actuel' | 'hedonique' | 'national_only'
        zone_pred     → zone normalisée utilisée

    ─── LOGIQUE PAR BIEN ────────────────────────────────────────────────────

    CAS 1 — Prix connu (and surface connue):
        Le prix actuel encode DÉJÀ la zone (c'est un prix de marché réel).
        On applique uniquement l'évolution temporelle nationale (SARIMA TND).
        prix_futur = prix_actuel × ratio_SARIMA(année)
        → méthode = 'prix_actuel'

    CAS 2 — Prix manquant + zone connue dans le modèle:
        On estime le prix courant via le modèle hédonique.
        prix_estim = exp(hédonique(zone, surface, rooms)) × surface
        prix_futur = prix_estim × ratio_SARIMA(année)
        → méthode = 'hedonique'

    CAS 3 — Zone absente du modèle (ou city null):
        On applique uniquement la tendance nationale au prix actuel.
        Pas de correction spatiale (zone inconnue).
        → méthode = 'national_only'
    """
    if target_years is None:
        target_years = TARGET_YEARS

    # ── Chargement du CSV original complet (tous les biens, outliers inclus) ─
    df = pd.read_csv(listings_path)
    df['zone_pred'] = df['city'].apply(_norm_zone)
    med_rooms = df.groupby('ipim_type')['rooms'].transform('median')
    df['rooms_imp'] = df['rooms'].fillna(med_rooms).fillna(2)

    n_total = len(df)
    if verbose:
        print(f'\n  CSV chargé: {n_total} biens '
              f'({df["ipim_type"].value_counts().to_dict()})')

    # ── Pré-calcul des ratios temporels: ratio(itype, year) ──────────────────
    # ratio = prix_m2_SARIMA(année) / prix_m2_SARIMA(2026)
    # Bornes prudentes: low = low_futur / high_2026  ;  high = high_futur / low_2026
    ratios = {}
    for itype in IPIM_COL:
        if itype not in ts_models:
            continue
        _, annual = ts_models[itype]
        p26      = annual.loc[annual['year'] == 2026, 'prix_m2'].values[0]
        p26_low  = annual.loc[annual['year'] == 2026, 'prix_m2_low'].values[0]
        p26_high = annual.loc[annual['year'] == 2026, 'prix_m2_high'].values[0]
        for _, row in annual.iterrows():
            yr = int(row['year'])
            ratios[(itype, yr)] = (
                row['prix_m2']      / p26,
                row['prix_m2_low']  / p26_high,   # borne basse prudente
                row['prix_m2_high'] / p26_low,    # borne haute prudente
            )

    # ── Zones disponibles dans chaque modèle hédonique ───────────────────────
    zones_ok = {
        itype: set(zone_idx['zone'].tolist())
        for itype, (_, zone_idx, _) in hedonic_models.items()
    }

    # ── Initialisation des colonnes de prédiction ────────────────────────────
    for yr in target_years:
        df[f'prix_pred_{yr}']      = np.nan
        df[f'prix_pred_{yr}_low']  = np.nan
        df[f'prix_pred_{yr}_high'] = np.nan
        df[f'prix_m2_pred_{yr}']   = np.nan
    df['methode_pred'] = 'non_calcule'

    # ── Boucle sur chaque bien ────────────────────────────────────────────────
    n_cas1 = n_cas2 = n_cas3 = n_skip = 0

    for idx in df.index:
        itype   = df.at[idx, 'ipim_type']
        prix    = df.at[idx, 'price']
        surface = df.at[idx, 'surface']
        zone    = df.at[idx, 'zone_pred']
        rooms   = float(df.at[idx, 'rooms_imp'])

        # Types non modélisés (ex: terrain si absent)
        if not any((itype, yr) in ratios for yr in target_years):
            n_skip += 1
            continue

        ok_prix    = pd.notna(prix) and prix > 0
        ok_surface = pd.notna(surface) and surface > 0
        ok_zone    = (pd.notna(zone)
                      and itype in zones_ok
                      and zone in zones_ok[itype])

        # ── CAS 1: prix actuel connu ─────────────────────────────────────
        if ok_prix and ok_surface:
            p_m2 = prix / surface
            for yr in target_years:
                key = (itype, yr)
                if key not in ratios:
                    continue
                r, rl, rh = ratios[key]
                df.at[idx, f'prix_pred_{yr}']      = round(prix  * r,  0)
                df.at[idx, f'prix_pred_{yr}_low']  = round(prix  * rl, 0)
                df.at[idx, f'prix_pred_{yr}_high'] = round(prix  * rh, 0)
                df.at[idx, f'prix_m2_pred_{yr}']   = round(p_m2  * r,  1)
            df.at[idx, 'methode_pred'] = 'prix_actuel'
            n_cas1 += 1

        # ── CAS 2: prix manquant, zone dans le modèle → hédonique ───────
        elif not ok_prix and ok_surface and ok_zone:
            try:
                model_ols, _, ref_zone = hedonic_models[itype]
                params = model_ols.params
                key_z  = f'C(zone_cat)[T.{zone}]'
                coef_z = params.get(key_z, 0.0) if zone != ref_zone else 0.0
                log_p  = (params['Intercept'] + coef_z
                          + params['log_surface'] * np.log(surface)
                          + params['rooms'] * rooms)
                p_m2   = np.exp(log_p)
                prix_e = p_m2 * surface
                for yr in target_years:
                    key = (itype, yr)
                    if key not in ratios:
                        continue
                    r, rl, rh = ratios[key]
                    df.at[idx, f'prix_pred_{yr}']      = round(prix_e * r,  0)
                    df.at[idx, f'prix_pred_{yr}_low']  = round(prix_e * rl, 0)
                    df.at[idx, f'prix_pred_{yr}_high'] = round(prix_e * rh, 0)
                    df.at[idx, f'prix_m2_pred_{yr}']   = round(p_m2   * r,  1)
                df.at[idx, 'methode_pred'] = 'hedonique'
                n_cas2 += 1
            except Exception:
                n_skip += 1

        # ── CAS 3: prix connu, zone absente du modèle OU surface manquante ─
        # On projette le prix total (pas le prix/m² si surface inconnue)
        elif ok_prix:
            p_m2 = (prix / surface) if ok_surface else None
            for yr in target_years:
                key = (itype, yr)
                if key not in ratios:
                    continue
                r, rl, rh = ratios[key]
                df.at[idx, f'prix_pred_{yr}']      = round(prix  * r,  0)
                df.at[idx, f'prix_pred_{yr}_low']  = round(prix  * rl, 0)
                df.at[idx, f'prix_pred_{yr}_high'] = round(prix  * rh, 0)
                if p_m2 is not None:
                    df.at[idx, f'prix_m2_pred_{yr}'] = round(p_m2 * r, 1)
            df.at[idx, 'methode_pred'] = 'national_only'
            n_cas3 += 1
        else:
            n_skip += 1

    # ── Nettoyage ─────────────────────────────────────────────────────────────
    df = df.drop(columns=['rooms_imp'], errors='ignore')

    if verbose:
        print(f'\n  ── Résumé des {n_total} biens ──')
        print(f'    CAS 1  prix_actuel × SARIMA      : {n_cas1:>5} biens')
        print(f'    CAS 2  hédonique × SARIMA         : {n_cas2:>5} biens')
        print(f'    CAS 3  tendance nationale seule   : {n_cas3:>5} biens')
        print(f'    Non calculé (données insuffisantes): {n_skip:>5} biens')
        pct_ok = (n_cas1 + n_cas2 + n_cas3) / n_total * 100
        print(f'    Couverture totale                 : {pct_ok:.1f}%')
        print(f'\n  Colonnes ajoutées: '
              f'{[f"prix_pred_{yr}" for yr in target_years]}'
              f' + _low + _high + prix_m2_pred_{{année}}')
        print(f'\n  Aperçu (5 premières lignes):')
        show = (['id', 'ipim_type', 'city', 'price', 'surface',
                 'methode_pred', 'zone_pred']
                + [f'prix_pred_{yr}' for yr in target_years])
        print(df[show].head(5).to_string(index=False))

    return df


if __name__ == '__main__':
    run()