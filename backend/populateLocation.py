# backend/test_prediction.py

import os
import sys
import django
from pathlib import Path

# Setup Django
sys.path.append(str(Path(__file__).resolve().parent))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

# Import the debug predictor
from models.prediction_models.predictor import get_predictor

def test_predictions():
    """Test price predictions with various inputs"""
    
    predictor = get_predictor(debug=True)
    
    test_cases = [
        # Normal cases
        {
            'name': 'Small apartment in Tunis',
            'transaction_type': 'sale',
            'property_type': 'apartment',
            'city': 'Tunis',
            'surface': 80,
            'rooms': 2,
            'region': 'unknown',
        },
        {
            'name': 'Large villa in La Marsa',
            'transaction_type': 'sale',
            'property_type': 'villa',
            'city': 'La Marsa',
            'surface': 250,
            'rooms': 5,
            'region': 'unknown',
        },
        {
            'name': 'Rent apartment in Ariana',
            'transaction_type': 'rent',
            'property_type': 'apartment',
            'city': 'Ariana',
            'surface': 70,
            'rooms': 2,
            'region': 'unknown',
        },
        # Edge cases that might cause negative predictions
        {
            'name': 'Very small surface',
            'transaction_type': 'sale',
            'property_type': 'apartment',
            'city': 'Tunis',
            'surface': 5,
            'rooms': 1,
            'region': 'unknown',
        },
        {
            'name': 'Unknown city',
            'transaction_type': 'sale',
            'property_type': 'apartment',
            'city': 'NonExistentCity',
            'surface': 100,
            'rooms': 3,
            'region': 'unknown',
        },
        {
            'name': 'Land/commercial',
            'transaction_type': 'sale',
            'property_type': 'land',
            'city': 'Tunis',
            'surface': 500,
            'rooms': 0,
            'region': 'unknown',
        }
    ]
    
    print("\n" + "="*60)
    print("TESTING PRICE PREDICTIONS")
    print("="*60)
    
    for test in test_cases:
        print(f"\n📋 Test: {test['name']}")
        print(f"   Input: {test}")
        
        try:
            result = predictor.predict(**{k: v for k, v in test.items() if k != 'name'})
            
            if result.get('error'):
                print(f"   ❌ Error: {result['error']}")
            else:
                print(f"   ✅ Predicted Price: {result.get('predicted_price'):,} TND")
                print(f"   Price Range: {result.get('price_low'):,} - {result.get('price_high'):,} TND")
                print(f"   Price/m²: {result.get('price_per_m2')} TND")
                print(f"   Model Used: {result.get('model_used')}")
                print(f"   Confidence: {result.get('confidence')}")
        except Exception as e:
            print(f"   ❌ Exception: {e}")
    
    print("\n" + "="*60)

if __name__ == '__main__':
    test_predictions()