# backend/dashboard/recommendation/model_loader.py
import joblib
import numpy as np
import pandas as pd
from pathlib import Path
from django.conf import settings
import logging
import warnings

logger = logging.getLogger(__name__)

# Suppress pickle warnings
warnings.filterwarnings('ignore', category=UserWarning)

class RecommendationModelLoader:
    """Load and manage all exported recommendation models"""
    
    _instance = None
    _models = {}
    _metadata = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_models()
        return cls._instance
    
    def _load_models(self):
        """Load all exported models from /models directory"""
        # Try multiple possible paths
        possible_paths = [
            Path(settings.BASE_DIR).parent / 'data' / 'models',
            Path(settings.BASE_DIR) / 'models',
            Path('/app/data/models'),  # Docker path
            Path('C:/Users/aloui oussema/Desktop/estateMindPi/data/models'),  # Absolute path
        ]
        
        models_path = None
        for path in possible_paths:
            if path.exists():
                models_path = path
                logger.info(f"Found models at: {path}")
                break
        
        if models_path is None:
            logger.warning(f"Models directory not found. Tried: {possible_paths}")
            return
        
        # Load metadata
        metadata_path = models_path / 'master_metadata.json'
        if metadata_path.exists():
            import json
            with open(metadata_path, 'r') as f:
                self._metadata = json.load(f)
            logger.info(f"Loaded model metadata. Best model: {self._metadata.get('best_model', 'unknown')}")
        
        # Load models with error handling
        model_files = {
            'xgboost': models_path / 'xgboost_model.pkl',
            'random_forest': models_path / 'random_forest_model.pkl',
            'neural_network': models_path / 'neural_network_model.pkl',
            'math_cf': models_path / 'math_based_cf.pkl'
        }
        
        for model_name, model_path in model_files.items():
            if model_path.exists():
                try:
                    # For math_cf, we need special handling or skip it
                    if model_name == 'math_cf':
                        logger.info(f"Skipping {model_name} - requires custom class, using ML models instead")
                        continue
                    
                    self._models[model_name] = joblib.load(model_path)
                    logger.info(f"Loaded {model_name} model")
                except Exception as e:
                    logger.error(f"Failed to load {model_name}: {e}")
        
        # Load feature columns and encoders
        try:
            feature_path = models_path / 'xgboost_feature_cols.pkl'
            if feature_path.exists():
                self.feature_cols = joblib.load(feature_path)
                logger.info(f"Loaded feature columns: {len(self.feature_cols)} features")
            else:
                self.feature_cols = None
                logger.warning("Feature columns file not found")
            
            encoders_path = models_path / 'xgboost_encoders.pkl'
            if encoders_path.exists():
                self.label_encoders = joblib.load(encoders_path)
                logger.info(f"Loaded label encoders: {list(self.label_encoders.keys())}")
            else:
                self.label_encoders = None
                logger.warning("Label encoders file not found")
        except Exception as e:
            logger.error(f"Failed to load artifacts: {e}")
            self.feature_cols = None
            self.label_encoders = None
        
        # If no ML models loaded, we can still provide fallback recommendations
        if not self._models:
            logger.warning("No ML models loaded. Using fallback recommendations.")
    
    def get_best_model(self):
        """Return the best performing model"""
        # Prefer XGBoost if available
        if 'xgboost' in self._models:
            return self._models['xgboost']
        if 'random_forest' in self._models:
            return self._models['random_forest']
        if 'neural_network' in self._models:
            return self._models['neural_network']
        return None
    
    def predict_interaction_probability(self, user_id, listing_data):
        """Predict probability user will interact with a listing"""
        model = self.get_best_model()
        if model is None:
            return 0.5  # Default neutral prediction
        
        try:
            features = self._create_feature_vector(user_id, listing_data)
            if features is None:
                return 0.5
            proba = model.predict_proba(features)[0, 1]
            return proba
        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            return 0.5
    
    def _create_feature_vector(self, user_id, listing_data):
        """Create feature vector matching training format"""
        if self.feature_cols is None:
            return None
        
        try:
            from services.models import UserBehaviorLog
            from django.db.models import Avg
            
            features = {}
            
            # Numerical features
            features['price_log'] = np.log1p(listing_data.get('price', 0) or 0)
            features['surface_log'] = np.log1p(listing_data.get('surface', 0) or 0)
            features['rooms'] = listing_data.get('rooms', 0) or 0
            price = listing_data.get('price', 0) or 0
            surface = listing_data.get('surface', 1) or 1
            features['price_per_m2'] = price / max(surface, 1)
            features['reliability_score'] = listing_data.get('reliability_score', 50) or 50
            
            # Get user's average price preference
            avg_price = UserBehaviorLog.objects.filter(
                user_id=user_id,
                behavior_type='view'
            ).aggregate(Avg('listing__price'))['listing__price__avg']
            features['user_avg_price'] = avg_price or 0
            features['user_preferred_type'] = 0  # Default
            
            # Categorical features
            city = listing_data.get('city', 'unknown') or 'unknown'
            property_type = listing_data.get('property_type', 'unknown') or 'unknown'
            agency = listing_data.get('source_name', 'unknown') or 'unknown'
            
            if self.label_encoders:
                features['city_encoded'] = self._encode_category('city', city)
                features['property_type_encoded'] = self._encode_category('property_type', property_type)
                features['agency_encoded'] = self._encode_category('agency', agency)
            else:
                features['city_encoded'] = 0
                features['property_type_encoded'] = 0
                features['agency_encoded'] = 0
            
            features['role_encoded'] = 0
            
            # Create DataFrame with correct column order
            X = pd.DataFrame([[features.get(col, 0) for col in self.feature_cols]], 
                            columns=self.feature_cols)
            
            return X
        except Exception as e:
            logger.error(f"Feature creation failed: {e}")
            return None
    
    def _encode_category(self, category, value):
        """Encode categorical value using saved encoder"""
        if self.label_encoders and category in self.label_encoders:
            encoder = self.label_encoders[category]
            try:
                if value in encoder.classes_:
                    return encoder.transform([value])[0]
                else:
                    return 0
            except Exception:
                return 0
        return 0


# Singleton instance
try:
    model_loader = RecommendationModelLoader()
    logger.info("RecommendationModelLoader initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize RecommendationModelLoader: {e}")
    model_loader = None