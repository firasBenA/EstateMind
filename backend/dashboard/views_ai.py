
import os
import logging
import requests
import json
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods, require_POST
from django.views.decorators.csrf import csrf_exempt

# ✅ CORRECT IMPORT: From models.prediction_models import predictor
try:
    from models.prediction_models.predictor import get_predictor
except ImportError as e:
    # Helpful debug message if it still fails
    raise ImportError(f"Failed to import predictor: {e}. Ensure __init__.py files exist in models/ and models/prediction_models/")

logger = logging.getLogger(__name__)

AI_SERVICE_URL = os.environ.get("AI_SERVICE_URL", "http://localhost:8001")

# ✅ Initialize Predictor ONCE at startup
predictor = get_predictor()

@csrf_exempt
@require_http_methods(["POST"])
def generate_description(request):
    """
    POST /api/generate-description/
    Forwards request to FastAPI AI Service.
    """
    image_files = request.FILES.getlist("images")
    metadata = request.POST.get("metadata", "{}")

    if not image_files:
        return JsonResponse({"error": "At least 1 image is required"}, status=400)
    if len(image_files) > 3:
        return JsonResponse({"error": "Maximum 3 images allowed"}, status=400)

    try:
        files = [
            ("images", (f.name, f.read(), f.content_type))
            for f in image_files
        ]
        data = {"metadata": metadata}

        response = requests.post(
            f"{AI_SERVICE_URL}/generate-description",
            files=files,
            data=data,
            timeout=120,
        )
        response.raise_for_status()
        return JsonResponse(response.json(), status=response.status_code)

    except requests.exceptions.ConnectionError:
        logger.error("AI service unreachable at %s", AI_SERVICE_URL)
        return JsonResponse({"error": "AI service unavailable."}, status=503)
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        return JsonResponse({"error": "Internal server error"}, status=500)


@csrf_exempt
@require_POST
def predict_price(request):
    """
    POST /api/predict-price/
    Uses the local ML model to predict price.
    """
    try:
        data = json.loads(request.body)
        
        result = predictor.predict(
            transaction_type=data.get('transaction', 'sale'),
            property_type=data.get('type', 'apartment'),
            city=data.get('city', 'Tunis'),
            surface=float(data.get('surface', 0)),
            rooms=int(data.get('rooms', 0)),
            region=data.get('region', 'unknown'),
            reliability_score=float(data.get('reliability_score', 80)),
            reliability_level=data.get('reliability_level', 'GOOD'),
            images_count=int(data.get('images_count', 0)),
            has_description=int(data.get('has_description', 0)),
            desc_length=int(data.get('desc_length', 0)),
            has_coords=int(data.get('has_coords', 0)),
        )
        
        return JsonResponse(result)
        
    except Exception as e:
        logger.error(f"Price Prediction Error: {e}")
        return JsonResponse({"error": str(e)}, status=500)