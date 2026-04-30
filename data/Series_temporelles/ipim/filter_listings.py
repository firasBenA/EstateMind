import pandas as pd
from pathlib import Path

# ==============================
# PATHS
# ==============================
BASE_DIR   = Path(__file__).resolve().parent
csv_path   = BASE_DIR / "listings_rows.csv"
output_dir = BASE_DIR   
output_dir.mkdir(parents=True, exist_ok=True)

print(f"📂 Chargement fichier : {csv_path}")

# ==============================
# 1. Chargement
# ==============================
df = pd.read_csv(csv_path)

print(f"Shape initiale : {df.shape}")
print(f"transaction_type : {df['transaction_type'].value_counts().to_dict()}")
print(f"property_type    : {df['property_type'].value_counts().to_dict()}")

# ==============================
# 2. Normalisation de la casse
# Problème connu : 'Sale' et 'sale' coexistent dans le dataset
# Sans cette normalisation, on perd des annonces silencieusement
# ==============================
df['transaction_type'] = df['transaction_type'].str.lower().str.strip()
df['property_type']    = df['property_type'].str.lower().str.strip()

# ==============================
# 3. Filtrage
# On garde uniquement :
#   - transaction = vente (l'IPIM INS ne couvre que les ventes)
#   - type = apartment / villa / land (les 3 types couverts par l'IPIM)
# Exclus : Rent, Commercial, Other, Vacation
# ==============================
IPIM_TYPES = ['apartment', 'villa', 'land']

filtered = df[
    (df['transaction_type'] == 'sale') &
    (df['property_type'].isin(IPIM_TYPES))
].copy()

# ==============================
# 4. Mapping vers les labels IPIM officiels INS
# Nécessaire pour la jointure avec le forecast IPIM
#   apartment → appartement  (IPIM: actif bâti)
#   villa     → maison       (IPIM: actif bâti)
#   land      → terrain      (IPIM: actif foncier)
# ==============================
IPIM_LABEL = {
    'apartment': 'appartement',
    'villa':     'maison',
    'land':      'terrain'
}
filtered['ipim_type'] = filtered['property_type'].map(IPIM_LABEL)

# ==============================
# 5. Sélection des colonnes utiles
# On garde ce qui sera nécessaire pour la projection de prix
# ==============================
COLS = [
    'id', 'ipim_type', 'property_type', 'price', 'currency',
    'city', 'municipality', 'region', 'surface', 'rooms',
    'price_per_m2', 'latitude', 'longitude',
    'reliability_score', 'reliability_level', 'is_outlier',
    'scraped_at', 'last_updated'
]
cols_available = [c for c in COLS if c in filtered.columns]
filtered = filtered[cols_available]

# ==============================
# 6. Résumé
# ==============================
print("\n" + "="*55)
print("RÉSUMÉ — Dataset filtré (Ventes IPIM)")
print("="*55)
print(f"Avant filtrage : {len(df):>5} annonces")
print(f"Après filtrage : {len(filtered):>5} annonces")
print()
print("Répartition par type IPIM :")
for t, g in filtered.groupby('ipim_type'):
    n        = len(g)
    prix_med = g['price'].median()
    n_prix   = g['price'].notna().sum()
    print(f"  {t:12} | {n:4} annonces | prix médian = {prix_med:>12,.0f} TND | prix disponibles = {n_prix}")

print()
print(f"Prix manquants   : {filtered['price'].isna().sum()}")
print(f"Surface manquante: {filtered['surface'].isna().sum()}")

# ==============================
# 7. Sauvegarde
# ==============================
out_path = output_dir / "listings_vente_ipim.csv"
filtered.to_csv(out_path, index=False)
print(f"\n✅ Sauvegardé : {out_path}")
print(f"   {len(filtered)} lignes × {len(cols_available)} colonnes")