# backend/ml_service/predictor.py

import os
import json
import numpy as np
import pandas as pd
import joblib
from typing import Dict, Any

class PricePredictorV3:
    """
    v3 : Route automatically to the model by segment
    """

    # Define the mapping used during training
    SEGMENT_MAP = {
        'apartment': 'residential',
        'house': 'residential',
        'villa': 'residential',
        'studio': 'residential',
        'land': 'land_commercial',
        'commercial': 'land_commercial',
        'office': 'land_commercial'
    }
    
    MARGIN = {'HIGH': 0.10, 'GOOD': 0.16, 'LOW': 0.25, 'DROP': 0.38}

    def __init__(self, model_dir: str = 'ml_models'):
        self.dir = model_dir
        self.models  = {}
        self.weights = {}
        self.le  = {} # Label Encoders
        self.te  = {} # Target Encoders

        # Load Models
        for key in os.listdir(model_dir):
            if key.startswith('xgb_') and key.endswith('.pkl'):
                k = key[4:-4] # Extract 'sale_residential' from 'xgb_sale_residential.pkl'
                try:
                    self.models[k] = {
                        'xgb':   joblib.load(f'{model_dir}/xgb_{k}.pkl'),
                        'lgb':   joblib.load(f'{model_dir}/lgb_{k}.pkl'),
                        'ridge': joblib.load(f'{model_dir}/ridge_{k}.pkl'),
                    }
                    self.weights[k] = joblib.load(f'{model_dir}/weights_{k}.pkl')
                except Exception as e:
                    print(f"Warning: Could not load model {k}: {e}")

        # Load Encoders (if they exist in the folder)
        # Note: If you didn't save le/te files in Kaggle, you might need to 
        # hardcode simple mappings or re-save them. For now, we assume they exist 
        # or handle missing ones gracefully.
        for col in ['segment', 'region_clean', 'transaction_type']:
            p = f'{model_dir}/le_{col}.pkl'
            if os.path.exists(p): self.le[col] = joblib.load(p)
            
        for col in ['city_clean', 'property_type_clean']:
            p = f'{model_dir}/te_{col}.pkl'
            if os.path.exists(p): self.te[col] = joblib.load(p)

        # Load Market Stats
        stats_path = f'{model_dir}/market_stats.json'
        if os.path.exists(stats_path):
            with open(stats_path, 'r') as f:
                self.market = json.load(f)
        else:
            self.market = {'sale': {}, 'rent': {}}
            
        print(f'✅ PricePredictorV3 — {len(self.models)} modèles chargés')

    def _le(self, col, val):
        enc = self.le.get(col)
        if enc is None: return 0
        v = str(val).strip()
        return int(enc.transform([v])[0]) if v in enc.classes_ else 0

    def _te(self, col, val, fallback=12.0):
        enc = self.te.get(col)
        if enc is None: return fallback
        tmp = pd.DataFrame([{col: str(val)}])
        try: return float(enc.transform(tmp)[col].iloc[0])
        except: return fallback

    def _route(self, transaction_type, property_type):
        seg = self.SEGMENT_MAP.get(property_type.lower(), 'land_commercial')
        specific = f'{transaction_type}_{seg}'
        return specific if specific in self.models else transaction_type

    def predict(self,
                transaction_type: str,
                property_type: str,
                city: str,
                surface: float,
                rooms: int = 0,
                region: str = 'unknown',
                reliability_score: float = 80.0,
                reliability_level: str = 'HIGH',
                model_weight: float = 1.5,
                is_outlier: bool = False,
                suspected_duplicate: bool = False,
                images_count: int = 0,
                has_description: int = 1,
                desc_length: int = 200,
                has_coords: int = 0) -> dict:

        seg = self.SEGMENT_MAP.get(property_type.lower(), 'land_commercial')
        level_map = {'HIGH': 3, 'GOOD': 2, 'LOW': 1, 'DROP': 0}

        # Contexte marché
        city_clean = city if city in (self.market.get(transaction_type, {})) else 'other'
        
        # Fallback median if city not found
        median_ppm2 = self.market.get(transaction_type, {}).get(city_clean, 2000) 
        
        # Simple estimation for current_ppm2 if not provided
        current_ppm2 = median_ppm2 
        ppm2_ratio   = current_ppm2 / (median_ppm2 + 1) if median_ppm2 > 0 else 1.0

        est_price = median_ppm2 * surface

        x = pd.DataFrame([{
            'log_surface':             np.log1p(surface),
            'rooms_filled':            min(rooms, 15),
            'rooms_per_m2':            rooms / (surface + 1) if surface > 0 else 0,
            'images_count':            images_count,
            'has_coords':              int(has_coords),
            'n_features':              0, # Placeholder if not calculated
            'has_description':         has_description,
            'desc_length':             min(desc_length, 2000),
            'ppm2_vs_market':          ppm2_ratio,
            'is_luxury':               0,
            # Market features
            'city_seg_median_price':   np.log1p(est_price),
            'city_tx_median_price':    np.log1p(est_price),
            'type_tx_median_price':    np.log1p(est_price * 0.95),
            'seg_tx_median_price':     np.log1p(est_price * 0.98),
            # XAI
            'reliability_score':       reliability_score,
            'reliability_bucket':      level_map.get(reliability_level, 2),
            'model_weight':            model_weight,
            'suspected_dup_int':       int(suspected_duplicate),
            'is_outlier_int':          int(is_outlier),
            # Categorical
            'city_te':                 self._te('city_clean', city),
            'type_te':                 self._te('property_type_clean', property_type),
            'segment_enc':             self._le('segment', seg),
            'region_clean_enc':        self._le('region_clean', region),
            'transaction_type_enc':    self._le('transaction_type', transaction_type),
        }])

        model_key = self._route(transaction_type, property_type)
        
        # Fallback if model key not found
        if model_key not in self.models:
            # Try generic key
            if transaction_type in self.models:
                model_key = transaction_type
            else:
                raise ValueError(f"No model found for {model_key} or {transaction_type}")

        m = self.models[model_key]
        w = self.weights[model_key]
        mode = w['best_mode']

        xgb_p = float(m['xgb'].predict(x)[0])
        lgb_p = float(m['lgb'].predict(x)[0])

        if mode == 'stack':
            meta = np.array([[xgb_p, lgb_p, xgb_p - lgb_p]])
            final_log = float(m['ridge'].predict(meta)[0])
        else:
            final_log = w['w_xgb'] * xgb_p + w['w_lgb'] * lgb_p

        price = float(np.expm1(final_log))
        margin_pct = self.MARGIN.get(reliability_level, 0.16)
        margin = price * margin_pct

        return {
            'predicted_price':  round(price, 0),
            'price_low':        round(price - margin, 0),
            'price_high':       round(price + margin, 0),
            'price_per_m2':     round(price / surface, 1) if surface else None,
            'currency':         'TND',
            'transaction_type': transaction_type,
            'segment':          seg,
            'model_used':       model_key,
            'confidence':       reliability_level,
            'margin_pct':       round(margin_pct * 100, 0),
        }

# Singleton instance for efficiency
_predictor_instance = None

def get_predictor():
    global _predictor_instance
    if _predictor_instance is None:
        # Calculate path relative to this file
        current_dir = os.path.dirname(os.path.abspath(__file__))
        model_dir = os.path.join(current_dir) # Models are in the same folder as predictor.py
        
        _predictor_instance = PricePredictorV3(model_dir=model_dir)
    return _predictor_instance