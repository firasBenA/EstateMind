# backend/dashboard/views_validation.py

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from services.validation.title_validator import get_title_validator
from services.validation.delegation_matcher import get_delegation_matcher
from services.models import Governorate, Delegation
import json
import os 

from supabase import create_client, Client



_supabase_client: Client = None

def _get_supabase_client() -> Client:
    global _supabase_client
    if _supabase_client is None:
        supabase_url = os.environ.get("SUPABASE_URL", "https://amxnojlfczwffvtwutrb.supabase.co")
        supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        if not supabase_key:
            raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY not set in environment")
        _supabase_client = create_client(supabase_url, supabase_key)
    return _supabase_client

@csrf_exempt
@require_http_methods(["POST"])
def validate_title_api(request):
    """POST /api/validate-title/"""
    try:
        data = json.loads(request.body)
        title = data.get('title', '')
        
        validator = get_title_validator()
        valid, message, confidence = validator.validate(title)
        
        return JsonResponse({
            'valid': valid,
            'message': message,
            'confidence': confidence
        })
    except Exception as e:
        return JsonResponse({'valid': False, 'message': str(e), 'confidence': 0}, status=500)


@require_http_methods(["GET"])
def get_governorates(request):
    """GET /api/governorates/ - Returns all Tunisian governorates"""
    try:
        governorates = Governorate.objects.all().values(
            'id', 'name', 'name_ar', 'value', 'latitude', 'longitude'
        )
        governorates_list = list(governorates)
        
        print(f"📊 Returning {len(governorates_list)} governorates")  # Debug log
        
        return JsonResponse(governorates_list, safe=False)
    except Exception as e:
        print(f"❌ Error in get_governorates: {e}")
        return JsonResponse({"error": str(e)}, status=500)


@csrf_exempt
def get_delegations(request, governorate_id):
    """Get delegations for a specific governorate"""
    try:
        # Fetch from Supabase directly
        supabase = _get_supabase_client()
        result = supabase.table("delegations")\
            .select("*")\
            .eq("governorate_id", governorate_id)\
            .execute()
        
        delegations = []
        for d in result.data:
            delegations.append({
                "id": d["id"],
                "governorate_id": d["governorate_id"],
                "name": d["name"],
                "name_ar": d.get("name_ar", ""),
                "value": d["name"].lower().replace(" ", "_"),
                "postal_code": d.get("postal_code", ""),
                "latitude": d.get("latitude") if d.get("latitude") else None,
                "longitude": d.get("longitude") if d.get("longitude") else None
            })
        
        # Log for debugging
        print(f"📊 Returning {len(delegations)} delegations for governorate {governorate_id}")
        
        return JsonResponse(delegations, safe=False)
    except Exception as e:
        print(f"❌ Error loading delegations: {e}")
        return JsonResponse({"error": str(e)}, status=500)

# @require_http_methods(["GET"])
# def get_delegations(request, governorate_id):
#     """GET /api/governorates/<id>/delegations/"""
#     try:
#         delegations = Delegation.objects.filter(
#             governorate_id=governorate_id
#         ).values(
#             'id', 'governorate_id', 'name', 'name_ar', 'value', 
#             'postal_code', 'latitude', 'longitude'
#         )
#         delegations_list = list(delegations)
        
#         print(f"📊 Returning {len(delegations_list)} delegations for governorate {governorate_id}")
        
#         return JsonResponse(delegations_list, safe=False)
#     except Exception as e:
#         print(f"❌ Error in get_delegations: {e}")
#         return JsonResponse({"error": str(e)}, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def auto_correct_delegation(request):
    """POST /api/delegation/autocorrect/"""
    try:
        data = json.loads(request.body)
        delegation_name = data.get('delegation_name', '')
        governorate_id = data.get('governorate_id')
        
        matcher = get_delegation_matcher()
        result = matcher.auto_correct(delegation_name, governorate_id)
        
        return JsonResponse(result)
    except Exception as e:
        return JsonResponse({
            'original': delegation_name,
            'corrected': None,
            'matched': False,
            'confidence': 0,
            'delegation_id': None,
            'latitude': None,
            'longitude': None,
            'governorate': None,
            'message': str(e)
        }, status=500)