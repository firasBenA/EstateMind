"""
services/prediction_service.py
Prediction service using 6 Prophet models.
"""

import joblib
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum
import logging
import warnings

warnings.filterwarnings("ignore", category=UserWarning)

logger = logging.getLogger(__name__)

# ── CORRECTION DU CHEMIN ──────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent.parent
MODELS_DIR = BASE_DIR / "data" / "serie_temporelle" / "timeseries_exports"

print(f"🔍 MODELS_DIR: {MODELS_DIR}")
print(f"📁 Exists: {MODELS_DIR.exists()}")


class PropertyType(str, Enum):
    APARTMENT  = "Apartment"
    VILLA      = "Villa"
    LAND       = "Land"
    COMMERCIAL = "Commercial"
    OTHER      = "Other"


@dataclass
class ScenarioInput:
    property_type: PropertyType = PropertyType.APARTMENT
    surface:       float        = 100.0
    city:          str          = "Tunis"
    region:        str          = "TUNIS"
    years:         int          = 10
    monthly_rent:  Optional[float] = None
    initial_price: Optional[float] = None


@dataclass
class PredictionResult:
    initial_price:      float
    yearly_predictions: List[Dict]
    total_roi:          float
    final_value:        float
    confidence_score:   float
    model_used:         str
    factors:            Dict


class PricePredictor:
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self.models_path = MODELS_DIR
        self.models: Dict = {}
        self._forecasts: Dict[str, pd.DataFrame] = {}
        self._load_all_models()

    def _load_model(self, key: str, filename: str) -> bool:
        path = self.models_path / filename
        if not path.exists():
            print(f"⚠️ Model not found: {path}")
            return False
        try:
            self.models[key] = joblib.load(path)
            print(f"✅ Loaded {key} from {filename}")
            return True
        except Exception as e:
            print(f"❌ Failed to load {filename}: {e}")
            return False

    def _load_all_models(self):
        """Load all Prophet models."""
        self._load_model("ipc",              "prophet_ipc.pkl")
        self._load_model("td",               "prophet_td.pkl")
        self._load_model("chomage",          "prophet_chomage.pkl")
        self._load_model("ipim_appartement", "prophet_ipim_appartement.pkl")
        self._load_model("ipim_maison",      "prophet_ipim_maison.pkl")
        self._load_model("ipim_terrain",     "prophet_ipim_terrain.pkl")

        loaded = list(self.models.keys())
        print(f"📦 Models loaded: {loaded}")

        # Pre-generate forecasts with regressors
        self._pregenerate_forecasts(horizon_months=240)

    def _pregenerate_forecasts(self, horizon_months: int = 240):
        """Run Prophet .predict() once for each model, handling regressors."""
        
        # First, generate IPC forecast (needed as regressor for IPIM models)
        ipc_forecast = None
        if "ipc" in self.models:
            try:
                future = self.models["ipc"].make_future_dataframe(periods=horizon_months, freq="MS")
                forecast = self.models["ipc"].predict(future)
                forecast = forecast[["ds", "yhat"]].copy()
                forecast.set_index("ds", inplace=True)
                ipc_forecast = forecast
                self._forecasts["ipc"] = forecast
                print(f"📈 Forecast generated for ipc ({len(forecast)} months)")
            except Exception as e:
                print(f"❌ Forecast failed for ipc: {e}")
        
        # For IPIM models, we need to provide the IPC regressor
        for key in ["ipim_appartement", "ipim_maison", "ipim_terrain"]:
            if key not in self.models:
                continue
                
            try:
                model = self.models[key]
                
                # Create future dataframe
                last_date = model.history['ds'].max()
                future_dates = pd.date_range(start=last_date + pd.DateOffset(months=1), periods=horizon_months, freq='MS')
                future = pd.DataFrame({'ds': future_dates})
                
                # Add required regressor: ipc_general_ins
                if ipc_forecast is not None:
                    # Align IPC forecast with future dates
                    future['ipc_general_ins'] = future['ds'].map(ipc_forecast['yhat']).fillna(ipc_forecast['yhat'].iloc[-1])
                else:
                    # Fallback: use default inflation values
                    future['ipc_general_ins'] = 6.0  # Default inflation %
                
                # Generate forecast
                forecast = model.predict(future)
                forecast = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
                forecast.set_index("ds", inplace=True)
                self._forecasts[key] = forecast
                print(f"📈 Forecast generated for {key} ({len(forecast)} months)")
                
            except Exception as e:
                print(f"❌ Forecast failed for {key}: {e}")
        
        # Generate forecasts for other models (TD, chomage)
        for key in ["td", "chomage"]:
            if key in self.models:
                try:
                    future = self.models[key].make_future_dataframe(periods=horizon_months, freq="MS")
                    forecast = self.models[key].predict(future)
                    forecast = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
                    forecast.set_index("ds", inplace=True)
                    self._forecasts[key] = forecast
                    print(f"📈 Forecast generated for {key} ({len(forecast)} months)")
                except Exception as e:
                    print(f"❌ Forecast failed for {key}: {e}")

    def _get_monthly_values(self, key: str, months: int) -> List[float]:
        """Return monthly forecast values for a model."""
        defaults = {
            "ipc":              [6.0] * months,
            "td":               [8.0] * months,
            "chomage":          [15.0] * months,
            "ipim_appartement": [3.5] * months,
            "ipim_maison":      [4.0] * months,
            "ipim_terrain":     [2.5] * months,
        }

        if key not in self._forecasts:
            return defaults.get(key, [3.0] * months)[:months]

        df = self._forecasts[key]
        values = df["yhat"].tolist()

        if len(values) < months:
            last = values[-1] if values else defaults.get(key, [3.0])[0]
            values.extend([last] * (months - len(values)))

        return values[:months]

    def _ipim_key_for_type(self, property_type: PropertyType) -> str:
        mapping = {
            PropertyType.APARTMENT:  "ipim_appartement",
            PropertyType.VILLA:      "ipim_maison",
            PropertyType.LAND:       "ipim_terrain",
            PropertyType.COMMERCIAL: "ipim_appartement",
            PropertyType.OTHER:      "ipim_appartement",
        }
        return mapping.get(property_type, "ipim_appartement")

    def get_base_price(self, scenario: ScenarioInput) -> float:
        """Calculate base price from type, surface, and location."""
        base_prices = {
            PropertyType.APARTMENT:  350_000,
            PropertyType.VILLA:      650_000,
            PropertyType.LAND:       450_000,
            PropertyType.COMMERCIAL: 550_000,
            PropertyType.OTHER:      300_000,
        }

        city_multipliers = {
            "Tunis": 1.25, "La Marsa": 1.35, "Ariana": 1.20,
            "Sousse": 1.15, "Sfax": 1.05, "Nabeul": 1.10,
            "Hammamet": 1.20, "Monastir": 1.08, "Ben Arous": 1.00,
            "Manouba": 0.95, "Boumhel": 0.90, "Akouda": 0.88,
            "Bizerte": 0.92, "Gabès": 0.80, "Gafsa": 0.75,
        }

        base = base_prices.get(scenario.property_type, 350_000)
        city_mult = city_multipliers.get(scenario.city, 1.0)

        if scenario.surface < 50:
            surface_factor = 1.15
        elif scenario.surface > 200:
            surface_factor = 0.85
        else:
            surface_factor = 1.0

        price = base * city_mult * surface_factor * (scenario.surface / 100)
        return round(price, -3)

    def get_future_inflation(self, years: int) -> List[float]:
        return self._get_monthly_values("ipc", years * 12)

    def predict(self, scenario: ScenarioInput) -> PredictionResult:
        base_price = scenario.initial_price or self.get_base_price(scenario)
        months = scenario.years * 12

        ipc_monthly = self._get_monthly_values("ipc", months)
        ipim_key = self._ipim_key_for_type(scenario.property_type)
        ipim_monthly = self._get_monthly_values(ipim_key, months)

        yearly_predictions = []
        current_price = base_price
        cumulative_rent = 0.0

        for year in range(1, scenario.years + 1):
            month_idx = (year - 1) * 12

            ipc_val = ipc_monthly[month_idx] if month_idx < len(ipc_monthly) else 6.0

            # IPIM growth calculation
            ipim_now = ipim_monthly[month_idx] if month_idx < len(ipim_monthly) else 100.0
            if month_idx >= 12 and month_idx - 12 < len(ipim_monthly):
                ipim_prev = ipim_monthly[month_idx - 12]
                ipim_growth = (ipim_now / ipim_prev - 1) if ipim_prev > 0 else 0.035
            else:
                ipim_growth = 0.035

            appreciation = ipim_growth
            ipc_premium = max(0, (ipc_val - 5.0) / 100) * 0.3
            appreciation += ipc_premium
            appreciation = max(-0.05, min(0.15, appreciation))

            new_price = current_price * (1 + appreciation)

            yearly_rent = 0.0
            if scenario.monthly_rent:
                rent_inflation = (1 + ipc_val / 100) ** (year - 1)
                yearly_rent = scenario.monthly_rent * 12 * rent_inflation
                cumulative_rent += yearly_rent

            total_value = new_price + cumulative_rent
            roi = ((total_value - base_price) / base_price) * 100

            yearly_predictions.append({
                "year": year,
                "price": round(new_price),
                "cumulative_rent": round(cumulative_rent),
                "total_value": round(total_value),
                "roi": round(roi, 1),
                "inflation": round(ipc_val, 1),
                "appreciation": round(appreciation * 100, 2),
            })

            current_price = new_price

        final = yearly_predictions[-1]

        confidence = 70.0
        if ipim_key in self.models:
            confidence += 15
        if scenario.monthly_rent:
            confidence += 5
        if "ipc" in self.models:
            confidence += 5

        used_models = [k for k in ["ipc", ipim_key] if k in self.models]
        model_label = f"Prophet ({', '.join(used_models)})" if used_models else "Statistical fallback"

        return PredictionResult(
            initial_price=base_price,
            yearly_predictions=yearly_predictions,
            total_roi=final["roi"],
            final_value=final["price"],
            confidence_score=min(100, confidence),
            model_used=model_label,
            factors={
                "base_price": base_price,
                "property_type": scenario.property_type.value,
                "ipim_model": ipim_key,
                "models_loaded": list(self.models.keys()),
            },
        )

    def compare_scenarios(self, scenarios: List[ScenarioInput]) -> List[PredictionResult]:
        return [self.predict(s) for s in scenarios]


predictor = PricePredictor()




def _get_training_data(self) -> pd.DataFrame:
        """
        Récupère les données d'entraînement utilisées par XGBoost.
        Construit le DataFrame avec les features et la target.
        """
        # Récupérer les données macro clean existantes
        try:
            import pandas as pd
            from pathlib import Path
            
            # Chemin vers macro_clean.csv (généré par modeling.py)
            macro_clean_path = self.models_path.parent / "macro_clean.csv"
            
            if macro_clean_path.exists():
                df = pd.read_csv(macro_clean_path, parse_dates=['date'])
                df.set_index('date', inplace=True)
                
                # Créer les lag features
                df['ipc_lag1'] = df['ipc_general_ins'].shift(1)
                df['ipc_lag2'] = df['ipc_general_ins'].shift(2)
                df['ipc_lag3'] = df['ipc_general_ins'].shift(3)
                df['ipc_lag6'] = df['ipc_general_ins'].shift(6)
                df['td_lag3'] = df['taux_directeur'].shift(3)
                df['td_lag6'] = df['taux_directeur'].shift(6)
                df['month'] = df.index.month
                
                # Supprimer les lignes avec NaN
                df = df.dropna()
                
                return df
            else:
                print("⚠️ macro_clean.csv not found, using fallback data")
                return self._create_fallback_training_data()
                
        except Exception as e:
            print(f"⚠️ Error loading training data: {e}")
            return self._create_fallback_training_data()
    
def _create_fallback_training_data(self) -> pd.DataFrame:
        """
        Crée des données d'entraînement de fallback basées sur les prévisions réelles
        """
        # Utiliser les prévisions réelles du modèle IPC
        inflation_forecast = self.get_future_inflation(years=3)
        
        dates = pd.date_range(start='2023-01-01', periods=len(inflation_forecast), freq='MS')
        
        df = pd.DataFrame({
            'ipc_general_ins': inflation_forecast,
            'taux_directeur': self._get_monthly_values("td", len(inflation_forecast)),
        }, index=dates)
        
        # Créer les lag features
        df['ipc_lag1'] = df['ipc_general_ins'].shift(1)
        df['ipc_lag2'] = df['ipc_general_ins'].shift(2)
        df['ipc_lag3'] = df['ipc_general_ins'].shift(3)
        df['ipc_lag6'] = df['ipc_general_ins'].shift(6)
        df['td_lag3'] = df['taux_directeur'].shift(3)
        df['td_lag6'] = df['taux_directeur'].shift(6)
        df['month'] = df.index.month
        
        return df.dropna()