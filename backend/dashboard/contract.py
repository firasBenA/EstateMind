"""
backend/dashboard/report_views.py
===================================
Report generation API endpoints.

POST /api/reports/generate/
  Body: { "type": "market" | "investment", "params": {...} }
  Response: Server-Sent Events stream — each event is a JSON token

GET /api/reports/
  Returns saved reports for the current user

POST /api/reports/<id>/save/
  Saves a generated report
"""

import json
import sys
import os
from pathlib import Path
from datetime import datetime

from django.contrib.auth.decorators import login_required
from django.http import StreamingHttpResponse, JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.db import models as dj_models
from django.core.files.storage import default_storage

import markdown2
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle
from reportlab.lib.units import inch

from .models import UserProfile, SavedReport


# ── RAG engine path resolution ─────────────────────────────────────────────────

def _get_contract_engine():
    """
    Import the RAG engine for contract generation.
    """
    data_dir = Path(__file__).resolve().parent.parent.parent / "data/MODELS/RAG/Reports"
    rag_dir  = data_dir / "rag"

    if str(data_dir) not in sys.path:
        sys.path.insert(0, str(data_dir))
    if str(rag_dir) not in sys.path:
        sys.path.insert(0, str(rag_dir))

    from dotenv import load_dotenv
    env_file = data_dir / ".env"
    if env_file.exists():
        load_dotenv(env_file, override=False)

    from engine import generate_contract_stream
    return generate_contract_stream


# ── SSE helpers ────────────────────────────────────────────────────────────────

def _sse(data: dict) -> str:
    """Format a Server-Sent Event."""
    return f"data: {json.dumps(data)}\n\n"


def _stream_contract(contract_type: str, params: dict):
    """Generator: yields SSE-formatted strings as the LLM writes the contract."""
    try:
        generate = _get_contract_engine()
        for token in generate(contract_type, params):
            yield _sse({"token": token, "done": False})
        yield _sse({"token": "", "done": True})

    except ImportError as e:
        yield _sse({
            "error": f"Contract engine not found: {e}",
            "done": True,
        })
    except Exception as e:
        yield _sse({"error": str(e), "done": True})


def _get_db_connection():
    """Get database connection with proper SSL handling."""
    import psycopg2
    
    # Check if we're in development or production
    is_development = os.getenv("ENVIRONMENT", "development") == "development"
    db_host = os.getenv("SUPABASE_DB_HOST", "localhost")
    
    # Connection parameters
    conn_params = {
        "host": db_host,
        "port": int(os.getenv("SUPABASE_DB_PORT", "5432")),
        "dbname": os.getenv("SUPABASE_DB_NAME", "postgres"),
        "user": os.getenv("SUPABASE_DB_USER", "postgres"),
        "password": os.getenv("SUPABASE_DB_PASSWORD", ""),
    }
    
    # Only add SSL for production/non-localhost
    if not is_development and "localhost" not in db_host and "127.0.0.1" not in db_host:
        conn_params["sslmode"] = "require"
    else:
        conn_params["sslmode"] = "disable"
    
    return psycopg2.connect(**conn_params)


# ── Contract Views ─────────────────────────────────────────────────────────────

@csrf_exempt
@login_required
@require_http_methods(["POST"])
def generate_contract(request):
    """
    POST /api/contracts/generate/
    Streams the generated contract via Server-Sent Events.
    """
    try:
        body = json.loads(request.body.decode("utf-8"))
    except Exception:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    contract_type = body.get("contract_type", "")
    params = body.get("params", {})

    valid_contracts = ["promesse_de_vente", "compromis_de_vente", "contrat_de_location", "acte_de_vente"]
    if contract_type not in valid_contracts:
        return JsonResponse(
            {"error": f"contract_type must be one of: {', '.join(valid_contracts)}"},
            status=400
        )

    response = StreamingHttpResponse(
        _stream_contract(contract_type, params),
        content_type="text/event-stream",
    )
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    response["Access-Control-Allow-Origin"] = request.META.get("HTTP_ORIGIN", "*")
    return response


@login_required
@require_http_methods(["GET"])
def get_listing_for_contract(request, listing_id):
    """
    GET /api/contracts/listing/<listing_id>/
    Returns listing data needed for contract generation.
    """
    import psycopg2
    import psycopg2.extras
    
    try:
        conn = _get_db_connection()
        
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, title, description, price, surface, rooms,
                       city, municipality, zone, region, address,
                       property_type, transaction_type
                FROM listings
                WHERE id = %s AND (should_drop IS FALSE OR should_drop IS NULL)
            """, [listing_id])
            listing = cur.fetchone()
            
        conn.close()
        
        if not listing:
            return JsonResponse({"error": "Listing not found"}, status=404)
            
        return JsonResponse({"listing": listing})
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JsonResponse({"error": str(e)}, status=500)


@login_required
@require_http_methods(["GET"])
def list_contracts(request):
    """GET /api/contracts/ — returns user's saved contracts."""
    from .models import Contract
    
    try:
        # Get UserProfile for the current user
        user_profile = UserProfile.objects.get(user=request.user)
        contracts = Contract.objects.filter(user=user_profile)
    except UserProfile.DoesNotExist:
        # Return empty list if user profile doesn't exist
        return JsonResponse({"contracts": []})
    
    contracts_data = contracts.values(
        "id", "contract_type", "title", "params", "status", "created_at"
    )
    
    return JsonResponse({
        "contracts": [
            {
                **c,
                "created_at": c["created_at"].isoformat(),
                "buyer_name": c.get("params", {}).get("buyer_name", ""),
            }
            for c in contracts_data
        ]
    })


@csrf_exempt
@login_required
@require_http_methods(["POST"])
def save_contract(request):
    """POST /api/contracts/save/ — saves a generated contract."""
    from .models import Contract, UserProfile
    
    try:
        body = json.loads(request.body.decode("utf-8"))
    except Exception as e:
        return JsonResponse({"error": f"Invalid JSON: {str(e)}"}, status=400)

    contract_type = body.get("contract_type", "")
    title = body.get("title", f"{contract_type} Contract")
    params = body.get("params", {})
    content = body.get("content", "")

    if not content.strip():
        return JsonResponse({"error": "content is required"}, status=400)

    try:
        # Get or create UserProfile - ONLY use fields that exist
        user_profile, created = UserProfile.objects.get_or_create(
            user=request.user,
            # Remove 'full_name' and 'address' if they don't exist in your model
            # If you want to set defaults, only use fields that actually exist
        )
        
        # Create contract with UserProfile
        contract = Contract.objects.create(
            user=user_profile,
            contract_type=contract_type,
            title=title,
            params=params,
            content=content,
            status="draft",
        )
        
        return JsonResponse({
            "id": contract.id, 
            "created_at": contract.created_at.isoformat()
        }, status=201)
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JsonResponse({"error": f"Failed to save contract: {str(e)}"}, status=500)


@login_required
@require_http_methods(["GET"])
def get_contract(request, pk: int):
    """GET /api/contracts/<pk>/ — returns full contract content."""
    from .models import Contract, UserProfile
    
    try:
        # Get UserProfile first
        user_profile = UserProfile.objects.get(user=request.user)
        contract = Contract.objects.get(pk=pk, user=user_profile)
    except UserProfile.DoesNotExist:
        return JsonResponse({"error": "User profile not found. Please update your profile."}, status=404)
    except Contract.DoesNotExist:
        return JsonResponse({"error": "Contract not found"}, status=404)

    return JsonResponse({
        "id": contract.id,
        "contract_type": contract.contract_type,
        "title": contract.title,
        "params": contract.params,
        "content": contract.content,
        "status": contract.status,
        "created_at": contract.created_at.isoformat(),
    })


@login_required
@require_http_methods(["POST"])
def send_contract_for_signature(request, pk: int):
    """POST /api/contracts/<pk>/send/ — sends contract for signature."""
    from .models import Contract, UserProfile
    
    try:
        user_profile = UserProfile.objects.get(user=request.user)
        contract = Contract.objects.get(pk=pk, user=user_profile)
    except UserProfile.DoesNotExist:
        return JsonResponse({"error": "User profile not found"}, status=404)
    except Contract.DoesNotExist:
        return JsonResponse({"error": "Contract not found"}, status=404)
    
    try:
        body = json.loads(request.body.decode("utf-8"))
        email = body.get("email", "")
    except Exception:
        email = ""
    
    # Update status
    contract.status = "sent"
    contract.save()
    
    return JsonResponse({
        "message": f"Contract sent to {email} for signature",
        "status": "sent"
    })


@login_required
@require_http_methods(["GET"])
def export_contract_pdf(request, pk: int):
    """GET /api/contracts/<pk>/pdf/ — exports contract as PDF."""
    from .models import Contract, UserProfile
    
    try:
        user_profile = UserProfile.objects.get(user=request.user)
        contract = Contract.objects.get(pk=pk, user=user_profile)
    except UserProfile.DoesNotExist:
        return JsonResponse({"error": "User profile not found"}, status=404)
    except Contract.DoesNotExist:
        return JsonResponse({"error": "Contract not found"}, status=404)

    response = HttpResponse(content_type='application/pdf')
    filename = f"{contract.title.replace(' ', '_')}.pdf"
    response['Content-Disposition'] = f'attachment; filename="{filename}"'

    doc = SimpleDocTemplate(response, pagesize=A4, rightMargin=50, leftMargin=50, topMargin=50, bottomMargin=50)
    story = []
    styles = getSampleStyleSheet()

    # Custom styles for legal contracts
    contract_title_style = ParagraphStyle(
        'ContractTitle',
        parent=styles['Heading1'],
        fontSize=18,
        spaceAfter=12,
        alignment=1,  # Center alignment
    )
    
    article_style = ParagraphStyle(
        'Article',
        parent=styles['Heading2'],
        fontSize=14,
        spaceBefore=15,
        spaceAfter=8,
        textColor=colors.HexColor('#1B2A4A'),
    )
    
    body_style = ParagraphStyle(
        'Body',
        parent=styles['Normal'],
        fontSize=10,
        lineHeight=14,
        spaceAfter=6,
    )

    # Add title
    story.append(Paragraph(contract.title, contract_title_style))
    story.append(Spacer(1, 0.3*inch))

    # Parse content
    lines = contract.content.split('\n')
    for line in lines:
        line = line.strip()
        if not line:
            story.append(Spacer(1, 0.1*inch))
            continue
            
        if line.startswith('ARTICLE '):
            story.append(Paragraph(line, article_style))
        elif line.startswith('- ') or line.startswith('* '):
            story.append(Paragraph(f"• {line[2:]}", body_style))
        else:
            story.append(Paragraph(line, body_style))

    doc.build(story)
    return response