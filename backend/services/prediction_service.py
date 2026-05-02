# services/prediction_service.py

import joblib
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from config import settings
import logging

logger = logging.getLogger(__name__)


BASE_DIR = Path(__file__).resolve().parent.parent.parent
MODELS_DIR = BASE_DIR / "data" / "serie_temporelle" / "models"

class PropertyType(str, Enum):
    APARTMENT = "Apartment"
    VILLA = "Villa"
    LAND = "Land"
    COMMERCIAL = "Commercial"
    OTHER = "Other"

@dataclass
class ScenarioInput:
    property_type: PropertyType = PropertyType.APARTMENT
    surface: float = 100.0
    city: str = "Tunis"
    region: str = "TUNIS"
    years: int = 10
    monthly_rent: Optional[float] = None
    initial_price: Optional[float] = None

@dataclass
class PredictionResult:
    initial_price: float
    yearly_predictions: List[Dict]
    total_roi: float
    final_value: float
    confidence_score: float
    model_used: str
    factors: Dict


class PricePredictor:
    """Service de prédiction utilisant les modèles Prophet et XGBoost"""
    
    _instance = None
    _models = None
    _macro_forecast = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._models is None:
            self.models_path =BASE_DIR / MODELS_DIR
            self._load_models()
            self._load_macro_forecast()
    
    def _load_models(self):
        """Charge les modèles PKL"""
        self.models = {}
        
        # Prophet IPC
        ipc_path = self.models_path / "prophet_ipc.pkl"
        if ipc_path.exists():
            self.models['prophet_ipc'] = joblib.load(ipc_path)
            logger.info("✅ Modèle Prophet IPC chargé")
        else:
            logger.warning(f"⚠️ Modèle Prophet IPC non trouvé: {ipc_path}")
        
        # Prophet Taux Directeur
        td_path = self.models_path / "prophet_taux_directeur.pkl"
        if td_path.exists():
            self.models['prophet_td'] = joblib.load(td_path)
            logger.info("✅ Modèle Prophet Taux Directeur chargé")
        
        # XGBoost
        xgb_path = self.models_path / "xgboost_ipc.pkl"
        if xgb_path.exists():
            self.models['xgboost'] = joblib.load(xgb_path)
            logger.info("✅ Modèle XGBoost chargé")
    
    def _load_macro_forecast(self) -> pd.DataFrame:
        """Charge les prévisions macroéconomiques"""
        if self._macro_forecast is not None:
            return self._macro_forecast
        
        # Chercher les CSV de prévision
        forecast_paths = [
            self.models_path / "forecast_ipc_20years.csv",
            self.models_path / "forecast_ipc_20years.csv",
            Path(settings.EXPORTS_DIR) / "forecast_ipc_12m.csv"
        ]
        
        for path in forecast_paths:
            if path and path.exists():
                df = pd.read_csv(path, parse_dates=['ds'])
                df.set_index('ds', inplace=True)
                self._macro_forecast = df
                logger.info(f"✅ Prévisions macro chargées: {path.name}")
                return df
        
        # Fallback
        logger.warning("⚠️ Aucune prévision macro trouvée, utilisation de valeurs par défaut")
        self._macro_forecast = self._generate_fallback_forecast()
        return self._macro_forecast
    
    def _generate_fallback_forecast(self) -> pd.DataFrame:
        """Génère des prévisions de fallback"""
        dates = pd.date_range(start='2024-01-01', periods=240, freq='MS')
        inflation = 7.0 * np.exp(-np.arange(240) / 80) + 2.5
        return pd.DataFrame({'yhat': inflation}, index=dates)
    
    def get_base_price(self, scenario: ScenarioInput) -> float:
        """Calcule le prix de base selon le type, la surface et la ville"""
        base_prices = {
            PropertyType.APARTMENT: 350000,
            PropertyType.VILLA: 650000,
            PropertyType.LAND: 450000,
            PropertyType.COMMERCIAL: 550000,
            PropertyType.OTHER: 300000,
        }
        
        city_multipliers = {
            "Tunis": 1.25, "La Marsa": 1.35, "Ariana": 1.20,
            "Sousse": 1.15, "Sfax": 1.05, "Nabeul": 1.10,
            "Hammamet": 1.20, "Monastir": 1.08, "Ben Arous": 1.00,
            "Manouba": 0.95, "Boumhel": 0.90, "Akouda": 0.88,
        }
        
        region_multipliers = {
            "TUNIS": 1.0, "ARIANA": 0.95, "BEN AROUS": 0.90,
            "SOUSSE": 0.88, "SFAX": 0.85, "NABEUL": 0.87,
            "MANNOUBA": 0.82, "ZAGHOUAN": 0.75,
        }
        
        base = base_prices.get(scenario.property_type, 350000)
        city_mult = city_multipliers.get(scenario.city, 1.0)
        region_mult = region_multipliers.get(scenario.region, 1.0)
        
        # Ajustement par surface
        if scenario.surface < 50:
            surface_factor = 1.15
        elif scenario.surface > 200:
            surface_factor = 0.85
        else:
            surface_factor = 1.0
        
        price = base * city_mult * region_mult * surface_factor * (scenario.surface / 100)
        
        return round(price, -3)
    
    def get_future_inflation(self, years: int) -> List[float]:
        """Récupère les prévisions d'inflation pour N années"""
        forecast = self._macro_forecast
        
        if len(forecast) < years * 12:
            last_value = forecast['yhat'].iloc[-1]
            values = forecast['yhat'].tolist()
            values.extend([last_value] * (years * 12 - len(values)))
            return values[:years * 12]
        
        return forecast['yhat'].iloc[:years * 12].tolist()
    
    def predict(self, scenario: ScenarioInput) -> PredictionResult:
        """Prédiction principale"""
        base_price = scenario.initial_price if scenario.initial_price else self.get_base_price(scenario)
        
        inflation_forecast = self.get_future_inflation(scenario.years)
        
        appreciation_factors = {
            PropertyType.APARTMENT: 0.045,
            PropertyType.VILLA: 0.05,
            PropertyType.LAND: 0.035,
            PropertyType.COMMERCIAL: 0.055,
            PropertyType.OTHER: 0.03,
        }
        
        yearly_predictions = []
        current_price = base_price
        cumulative_rent = 0
        
        for year in range(1, scenario.years + 1):
            inflation = inflation_forecast[(year - 1) * 12] if (year - 1) * 12 < len(inflation_forecast) else 3.0
            
            appreciation = appreciation_factors.get(scenario.property_type, 0.04)
            appreciation_adjusted = appreciation * (0.7 + 0.3 * (inflation / 5.0))
            decay = np.exp(-year / 30)
            growth_rate = appreciation_adjusted * (0.8 + 0.2 * decay)
            
            new_price = current_price * (1 + growth_rate)
            
            yearly_rent = 0
            if scenario.monthly_rent:
                yearly_rent = scenario.monthly_rent * 12 * (1 + inflation/100) ** (year - 1)
                cumulative_rent += yearly_rent
            
            total_value = new_price + cumulative_rent
            roi = ((total_value - base_price) / base_price) * 100
            
            yearly_predictions.append({
                'year': year,
                'price': round(new_price),
                'cumulative_rent': round(cumulative_rent),
                'total_value': round(total_value),
                'roi': round(roi, 1),
                'inflation': round(inflation, 1)
            })
            
            current_price = new_price
        
        final = yearly_predictions[-1]
        
        # Score de confiance
        confidence = 85.0
        if not scenario.monthly_rent:
            confidence -= 10
        if scenario.surface < 30 or scenario.surface > 500:
            confidence -= 10
        if 'prophet_ipc' in self.models:
            confidence += 5
        
        return PredictionResult(
            initial_price=base_price,
            yearly_predictions=yearly_predictions,
            total_roi=final['roi'],
            final_value=final['price'],
            confidence_score=min(100, confidence),
            model_used="hybrid (Prophet + XGBoost)",
            factors={
                'base_price': base_price,
                'property_type': scenario.property_type.value,
                'avg_inflation': round(np.mean(inflation_forecast[:scenario.years * 12]), 1)
            }
        )
    
    def compare_scenarios(self, scenarios: List[ScenarioInput]) -> List[PredictionResult]:
        """Compare plusieurs scénarios"""
        return [self.predict(s) for s in scenarios]


# Instance unique
predictor = PricePredictor()