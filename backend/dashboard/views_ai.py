"""
dashboard/views_ai.py

Django proxy endpoint: POST /api/generate-description/

Accepts multipart/form-data from the frontend, forwards to the
FastAPI AI microservice, and returns the result to the client.

This keeps the frontend talking to a single origin (Django :8000)
and avoids CORS complexity with the AI service (:8001).
"""

import os
import logging
import requests
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt

logger = logging.getLogger(__name__)

AI_SERVICE_URL = os.environ.get("AI_SERVICE_URL", "http://localhost:8001")
# predictor = PricePredictorV3(model_dir=OUTPUT_DIR) 

@csrf_exempt
@require_http_methods(["POST"])
def generate_description(request):
    """
    POST /api/generate-description/

    Accepts:
      - images (multipart, 1-3 files)
      - metadata (form field, JSON string)

    Returns the AI microservice response directly.
    """
    # ── Validate input ────────────────────────────────────────────────────
    image_files = request.FILES.getlist("images")
    metadata = request.POST.get("metadata", "{}")

    if not image_files:
        return JsonResponse({"error": "At least 1 image is required"}, status=400)
    if len(image_files) > 3:
        return JsonResponse({"error": "Maximum 3 images allowed"}, status=400)

    # ── Forward to AI microservice ────────────────────────────────────────
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
            timeout=120,  # Phi-3 can take up to 60s on CPU
        )
        response.raise_for_status()
        return JsonResponse(response.json(), status=response.status_code)

    except requests.exceptions.ConnectionError:
        logger.error("AI service unreachable at %s", AI_SERVICE_URL)
        return JsonResponse(
            {"error": "AI service unavailable. Please try again later."},
            status=503,
        )
    except requests.exceptions.Timeout:
        logger.error("AI service timed out")
        return JsonResponse(
            {"error": "Request timed out. The AI model is processing — please retry."},
            status=504,
        )
    except requests.exceptions.HTTPError as exc:
        logger.error("AI service returned %s: %s", exc.response.status_code, exc.response.text)
        return JsonResponse(
            {"error": exc.response.json().get("detail", "AI service error")},
            status=exc.response.status_code,
        )
    except Exception as exc:
        logger.exception("Unexpected error forwarding to AI service: %s", exc)
        return JsonResponse({"error": "Internal server error"}, status=500)



# @csrf_exempt
# @require_POST
# def predict_price(request):
#     data = json.loads(request.body)
#     result = predictor.predict(
#         transaction_type=data['transaction'],
#         property_type=data['type'],
#         city=data['city'],
#         surface=float(data['surface']),
#         rooms=int(data.get('rooms', 0)),
#         images_count=int(data.get('images_count', 0)),
#         has_description=int(data.get('has_description', 0)),
#         desc_length=int(data.get('desc_length', 0)),
#         has_coords=int(data.get('has_coords', 0)),
#     )
#     return JsonResponse(result)
