"""
Price prediction tool - calls existing ML predictor.
"""
from typing import Optional, Dict, Any
import logging
from unittest import result

logger = logging.getLogger(__name__)

# Map reliability levels to numeric confidence scores
RELIABILITY_TO_CONFIDENCE = {
    'HIGH': 0.95,
    'GOOD': 0.85,
    'MEDIUM': 0.70,
    'LOW': 0.50,
    'POOR': 0.30,
}


def predict_price(
    property_type: str,
    city: str,
    surface: float,
    rooms: int,
    region: str = "Tunis",
    transaction_type: str = "sale",
    reliability_score: float = 80.0,
    reliability_level: str = "GOOD",
    images_count: int = 0,
    has_description: bool = False,
    desc_length: int = 0,
    has_coords: bool = False,
) -> Dict[str, Any]:
    """Predict property price using ML model."""
    try:
        from models.prediction_models.predictor import get_predictor

        predictor = get_predictor()

        result = predictor.predict(
            transaction_type=transaction_type,
            property_type=property_type,
            city=city,
            surface=float(surface),
            rooms=int(rooms),
            region=region,
            reliability_score=float(reliability_score),
            reliability_level=reliability_level,
            images_count=int(images_count),
            has_description=int(has_description),
            desc_length=int(desc_length),
            has_coords=int(has_coords),
        )
        
        # 🔑 Extract and safely convert predicted_price
        predicted_price = result.get('predicted_price', 0)
        if isinstance(predicted_price, str):
            predicted_price = float(predicted_price.replace(',', '').replace(' ', ''))
        predicted_price = float(predicted_price)

        # 🔑 Extract and safely convert confidence (handle string reliability levels)
        confidence_raw = result.get('confidence', 0.8)
        
        if isinstance(confidence_raw, str):
            # Map reliability level string to numeric confidence
            confidence = RELIABILITY_TO_CONFIDENCE.get(
                confidence_raw.strip().upper(),
                0.80  # Default fallback
            )
        else:
            # Already numeric
            confidence = min(1.0, max(0.0, float(confidence_raw)))

        # Create confidence-based range
        margin = 0.15
        min_estimate = predicted_price * (1 - margin)
        max_estimate = predicted_price * (1 + margin)

        reasoning = (
            f"Predicted {transaction_type} price for {surface}m² "
            f"{property_type} with {rooms} rooms in {city}. "
            f"Confidence: {confidence*100:.0f}%"
        )

        return {
            'predicted_price': round(predicted_price, 2),
            'min_estimate': round(min_estimate, 2),
            'max_estimate': round(max_estimate, 2),
            'confidence': confidence,
            'reasoning': reasoning,
        }

    except ValueError as e:
        logger.error(f"Type conversion error: {str(e)}", exc_info=True)
        return {
            'error': f"Invalid prediction format: {str(e)}",
            'predicted_price': None,
            'confidence': 0.0,
        }
    except Exception as e:
        logger.error(f"Price prediction error: {str(e)}", exc_info=True)
        return {
            'error': f"Price prediction failed: {str(e)}",
            'predicted_price': None,
            'confidence': 0.0,
        }