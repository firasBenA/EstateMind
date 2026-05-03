# backend/dashboard/views_ai.py

import os
import logging
import requests
import json
import asyncio
import httpx
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods, require_POST
from django.views.decorators.csrf import csrf_exempt

from config import settings
from .ollama_client import generate_with_ollama, generate_template_description, check_ollama_health
from .prediction_service import predictor, ScenarioInput, PropertyType
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from django.core.cache import cache
import logging
from opentelemetry import trace
from services.otel_ollama import trace_function, OllamaInstrumentor
logger = logging.getLogger(__name__)


tracer = trace.get_tracer(__name__)

AI_SERVICE_URL = os.environ.get("AI_SERVICE_URL", "http://localhost:8001")
PRIMARY_TIMEOUT = 15  # seconds - don't keep user waiting too long
ollama_url = getattr(settings, 'OLLAMA_URL', 'http://localhost:11434')
ollama_model = getattr(settings, 'OLLAMA_MODEL', 'gemma3:4b')

@csrf_exempt
@require_http_methods(["POST"])
def generate_description(request):
    """
    POST /api/generate-description/
    Primary: Forward to FastAPI AI Service (Qwen2-VL)
    Fallback 1: Use Ollama Gemma3:4b
    Fallback 2: Use template-based description
    """
    image_files = request.FILES.getlist("images")
    metadata = request.POST.get("metadata", "{}")

    if not image_files:
        return JsonResponse({"error": "At least 1 image is required"}, status=400)
    if len(image_files) > 3:
        return JsonResponse({"error": "Maximum 3 images allowed"}, status=400)

    # Parse metadata for fallback
    try:
        meta = json.loads(metadata)
    except:
        meta = {}

    # ──────────────────────────────────────────────────────────────────────────
    # PRIMARY: Try FastAPI service first (with timeout)
    # ──────────────────────────────────────────────────────────────────────────
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
            timeout=PRIMARY_TIMEOUT,
        )
        response.raise_for_status()
        
        logger.info("✅ Primary FastAPI service responded")
        return JsonResponse(response.json(), status=response.status_code)

    except requests.exceptions.Timeout:
        logger.warning(f"Primary service timeout after {PRIMARY_TIMEOUT}s")
    except requests.exceptions.ConnectionError:
        logger.warning(f"Primary service unreachable at {AI_SERVICE_URL}")
    except Exception as exc:
        logger.exception(f"Primary service error: {exc}")

    # ──────────────────────────────────────────────────────────────────────────
    # FALLBACK 1: Try Ollama (same as your RAG system)
    # ──────────────────────────────────────────────────────────────────────────
    logger.info("🔄 Trying Ollama fallback...")
    
    # Run async function in sync context
    loop = None
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    try:
        # Check if Ollama is available
        ollama_available = loop.run_until_complete(check_ollama_health())
        
        if ollama_available:
            description = loop.run_until_complete(generate_with_ollama(meta))
            
            if description:
                logger.info("✅ Ollama fallback successful")
                return JsonResponse({
                    "description": description,
                    "highlights": ["bien situé", "excellente opportunité", "à découvrir"],
                    "tone": "professional",
                    "generated_by": "ollama-fallback"
                })
            else:
                logger.warning("Ollama returned empty description")
        else:
            logger.warning("Ollama not available")
            
    except Exception as e:
        logger.error(f"Ollama fallback failed: {e}")

    # ──────────────────────────────────────────────────────────────────────────
    # FALLBACK 2: Ultimate template fallback
    # ──────────────────────────────────────────────────────────────────────────
    logger.warning("⚠️ Using template fallback")
    description = generate_template_description(meta)
    
    return JsonResponse({
        "description": description,
        "highlights": ["à visiter", "bon rapport qualité-prix", "contactez-nous"],
        "tone": "simple",
        "generated_by": "template-fallback"
    })


@csrf_exempt
@require_POST
def predict_price(request):
    """
    POST /api/predict-price/
    Uses the local ML model to predict price.
    """
    try:
        from models.prediction_models.predictor import get_predictor
        predictor = get_predictor()
        
        data = json.loads(request.body)
        
        # Log the incoming request
        logger.info(f"📊 Price prediction request: {data.get('city')} - {data.get('type')} - {data.get('surface')}m²")
        
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
    

@api_view(['POST'])
@permission_classes([AllowAny])  # Changez en IsAuthenticated pour la prod
def predict_property(request):
    """
    Endpoint de prédiction immobilière
    POST /api/ai/predict/
    """
    try:
        data = request.data
        
        scenario = ScenarioInput(
            property_type=PropertyType(data.get('property_type', 'Apartment')),
            surface=float(data.get('surface', 100)),
            city=data.get('city', 'Tunis'),
            region=data.get('region', 'TUNIS'),
            years=int(data.get('years', 10)),
            monthly_rent=data.get('monthly_rent'),
            initial_price=data.get('initial_price')
        )
        
        # Cache key
        cache_key = f"pred_{hash(str(sorted(data.items())))}"
        cached = cache.get(cache_key)
        
        if cached:
            return Response({
                'success': True,
                'data': cached,
                'cached': True
            })
        
        result = predictor.predict(scenario)
        
        response_data = {
            'initial_price': result.initial_price,
            'yearly_predictions': result.yearly_predictions,
            'total_roi': result.total_roi,
            'final_value': result.final_value,
            'confidence_score': result.confidence_score,
            'model_used': result.model_used,
            'factors': result.factors
        }
        
        cache.set(cache_key, response_data, 300)
        
        return Response({
            'success': True,
            'data': response_data,
            'cached': False
        })
        
    except Exception as e:
        logger.error(f"Prediction error: {str(e)}")
        return Response({
            'success': False,
            'error': str(e)
        }, status=400)


@api_view(['POST'])
@permission_classes([AllowAny])
def compare_scenarios(request):
    """
    Compare plusieurs scénarios
    POST /api/ai/compare/
    """
    try:
        scenarios_data = request.data.get('scenarios', [])
        scenarios = []
        
        for data in scenarios_data:
            scenario = ScenarioInput(
                property_type=PropertyType(data.get('property_type', 'Apartment')),
                surface=float(data.get('surface', 100)),
                city=data.get('city', 'Tunis'),
                region=data.get('region', 'TUNIS'),
                years=int(data.get('years', 10)),
                monthly_rent=data.get('monthly_rent')
            )
            scenarios.append(scenario)
        
        results = predictor.compare_scenarios(scenarios)
        
        return Response({
            'success': True,
            'comparison': [
                {
                    'initial_price': r.initial_price,
                    'yearly_predictions': r.yearly_predictions,
                    'total_roi': r.total_roi,
                    'final_value': r.final_value,
                    'confidence_score': r.confidence_score
                }
                for r in results
            ]
        })
        
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=400)


@api_view(['GET'])
@permission_classes([AllowAny])
def get_base_prices(request):
    """
    Récupère les prix de base par type et ville
    GET /api/ai/base-prices/
    """
    base_prices = {
        'Apartment': 350000,
        'Villa': 650000,
        'Land': 450000,
        'Commercial': 550000,
        'Other': 300000
    }
    
    city_multipliers = {
        "Tunis": 1.25, "La Marsa": 1.35, "Ariana": 1.20,
        "Sousse": 1.15, "Sfax": 1.05, "Nabeul": 1.10,
        "Hammamet": 1.20, "Monastir": 1.08, "Ben Arous": 1.00
    }
    
    return Response({
        'success': True,
        'base_prices': base_prices,
        'city_multipliers': city_multipliers
    })


@api_view(['GET'])
@permission_classes([AllowAny])
def get_macro_forecast(request):
    """
    Récupère les prévisions macroéconomiques
    GET /api/ai/macro-forecast/
    """
    inflation = predictor.get_future_inflation(10)
    yearly_inflation = [round(inflation[i*12], 1) for i in range(10)]
    
    return Response({
        'success': True,
        'inflation_forecast': yearly_inflation,
        'years': list(range(1, 11))
    })


@api_view(['GET'])
@permission_classes([AllowAny])
def get_model_status(request):
    """
    Vérifie le statut des modèles
    GET /api/ai/status/
    """
    models_status = {
        'prophet_ipc': hasattr(predictor.models, 'get', 'prophet_ipc') and 'prophet_ipc' in predictor.models,
        'prophet_td': hasattr(predictor.models, 'get', 'prophet_td') and 'prophet_td' in predictor.models,
        'xgboost': hasattr(predictor.models, 'get', 'xgboost') and 'xgboost' in predictor.models,
        'macro_forecast': predictor._macro_forecast is not None and len(predictor._macro_forecast) > 0
    }
    
    return Response({
        'success': True,
        'models': models_status,
        'models_path': str(predictor.models_path)
    })