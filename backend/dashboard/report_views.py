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

def _get_engine():
    """
    Import the RAG engine from the data/ pipeline directory.
    Adjust DATA_DIR to wherever your data/ folder lives relative to backend/.
    """
    data_dir = Path(__file__).resolve().parent.parent.parent / "data/MODELS/RAG/Reports"
    rag_dir  = data_dir / "rag"

    if str(data_dir) not in sys.path:
        sys.path.insert(0, str(data_dir))
    if str(rag_dir) not in sys.path:
        sys.path.insert(0, str(rag_dir))

    # Also load the data .env (has PG creds + OLLAMA_BASE_URL)
    from dotenv import load_dotenv
    env_file = data_dir / ".env"
    if env_file.exists():
        load_dotenv(env_file, override=False)

    from engine import generate_report_stream
    return generate_report_stream


# ── SSE helpers ────────────────────────────────────────────────────────────────

def _sse(data: dict) -> str:
    """Format a Server-Sent Event."""
    return f"data: {json.dumps(data)}\n\n"


def _stream_report(report_type: str, params: dict):
    """Generator: yields SSE-formatted strings as the LLM writes."""
    try:
        generate = _get_engine()
        for token in generate(report_type, params):
            yield _sse({"token": token, "done": False})
        yield _sse({"token": "", "done": True})

    except ImportError as e:
        yield _sse({
            "error": (
                f"RAG engine not found: {e}. "
                "Make sure data/rag/engine.py exists and dependencies are installed."
            ),
            "done": True,
        })
    except Exception as e:
        yield _sse({"error": str(e), "done": True})


# ── Views ──────────────────────────────────────────────────────────────────────

@csrf_exempt
@login_required
@require_http_methods(["POST"])
def generate_report(request):
    """
    POST /api/reports/generate/
    Streams the LLM report via Server-Sent Events.
    """
    try:
        body = json.loads(request.body.decode("utf-8"))
    except Exception:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    report_type = body.get("type", "")
    params      = body.get("params", {})

    if report_type not in ("market", "investment"):
        return JsonResponse(
            {"error": "type must be 'market' or 'investment'"}, status=400
        )

    response = StreamingHttpResponse(
        _stream_report(report_type, params),
        content_type="text/event-stream",
    )
    response["Cache-Control"]        = "no-cache"
    response["X-Accel-Buffering"]    = "no"    # disable nginx buffering
    response["Access-Control-Allow-Origin"] = request.META.get("HTTP_ORIGIN", "*")
    return response


@login_required
@require_http_methods(["GET"])
def list_reports(request):
    """GET /api/reports/ — returns user's saved reports."""
    reports = SavedReport.objects.filter(user=request.user).values(
        "id", "report_type", "title", "params", "created_at"
    )
    return JsonResponse({
        "reports": [
            {
                **r,
                "created_at": r["created_at"].isoformat(),
            }
            for r in reports
        ]
    })


@csrf_exempt
@login_required
@require_http_methods(["POST"])
def save_report(request):
    """POST /api/reports/save/ — saves a generated report."""
    try:
        body = json.loads(request.body.decode("utf-8"))
    except Exception:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    report_type = body.get("type", "market")
    title       = body.get("title", f"{report_type.title()} Report")
    params      = body.get("params", {})
    content     = body.get("content", "")

    if not content.strip():
        return JsonResponse({"error": "content is required"}, status=400)

    report = SavedReport.objects.create(
        user        = request.user,
        report_type = report_type,
        title       = title,
        params      = params,
        content     = content,
    )
    return JsonResponse({"id": report.id, "created_at": report.created_at.isoformat()}, status=201)


@login_required
@require_http_methods(["GET"])
def get_report(request, pk: int):
    """GET /api/reports/<pk>/ — returns full report content."""
    try:
        report = SavedReport.objects.get(pk=pk, user=request.user)
    except SavedReport.DoesNotExist:
        return JsonResponse({"error": "Not found"}, status=404)

    return JsonResponse({
        "id":          report.id,
        "type":        report.report_type,
        "title":       report.title,
        "params":      report.params,
        "content":     report.content,
        "created_at":  report.created_at.isoformat(),
    })


@login_required
@require_http_methods(["GET"])
def export_report_pdf(request, pk: int):
    """GET /api/reports/<pk>/pdf/ — generates a professional PDF and downloads it."""
    try:
        report = SavedReport.objects.get(pk=pk, user=request.user)
    except SavedReport.DoesNotExist:
        return JsonResponse({"error": "Report not found"}, status=404)

    # 1. Prepare Response
    response = HttpResponse(content_type='application/pdf')
    filename = f"{report.title.replace(' ', '_')}.pdf"
    response['Content-Disposition'] = f'attachment; filename="{filename}"'

    # 2. Setup Document
    doc = SimpleDocTemplate(response, pagesize=A4, rightMargin=50, leftMargin=50, topMargin=50, bottomMargin=50)
    story = []
    styles = getSampleStyleSheet()

    # Custom styles
    title_style = ParagraphStyle(
        'MainTitle',
        parent=styles['Heading1'],
        fontSize=22,
        spaceAfter=12,
        textColor=colors.HexColor('#1B2A4A'),  # navy
    )
    subtitle_style = ParagraphStyle(
        'Subtitle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#7F8C8D'), # gray
        spaceAfter=20,
    )
    h2_style = ParagraphStyle(
        'H2',
        parent=styles['Heading2'],
        fontSize=16,
        spaceBefore=20,
        spaceAfter=10,
        textColor=colors.HexColor('#2E5BBA'), # blue
    )
    body_style = ParagraphStyle(
        'Body',
        parent=styles['Normal'],
        fontSize=11,
        lineHeight=14,
        spaceAfter=10,
    )

    # 3. Add Header (Logo + Meta)
    # Logo path: backend/dashboard/report_views.py -> frontend-client/src/assets/logo.png
    logo_path = Path(__file__).resolve().parent.parent.parent / "frontend-client/src/assets/logo.png"
    if logo_path.exists():
        img = Image(str(logo_path), width=1.5*inch, height=0.4*inch) # Adjust size as needed
        img.hAlign = 'LEFT'
        story.append(img)
        story.append(Spacer(1, 0.2*inch))

    story.append(Paragraph(report.title, title_style))
    date_str = report.created_at.strftime("%B %d, %Y")
    type_label = dict(SavedReport.REPORT_TYPES).get(report.report_type, "Report")
    story.append(Paragraph(f"{type_label} | Prepared on {date_str}", subtitle_style))
    story.append(Spacer(1, 0.1*inch))

    # 4. Parse & Add Content
    # Simple markdown-to-pdf logic (headers, lists, bold)
    # We use markdown2 to convert to basic HTML, then use reportlab's Paragraph tags
    
    html_content = markdown2.markdown(report.content)
    
    # Process the HTML tags that reportlab Paragraph supports
    # Paragraph supports: <b>, <i>, <u>, <font>, <br/>, etc.
    # markdown2 output needs a bit of conversion for best results in Paragraph
    
    lines = report.content.split('\n')
    for line in lines:
        line = line.strip()
        if not line:
            story.append(Spacer(1, 0.1*inch))
            continue
            
        if line.startswith('# '):
            story.append(Paragraph(line[2:], title_style))
        elif line.startswith('## '):
            story.append(Paragraph(line[3:], h2_style))
        elif line.startswith('### '):
            story.append(Paragraph(line[4:], styles['Heading3']))
        elif line.startswith('- ') or line.startswith('* '):
            story.append(Paragraph(f"• {line[2:]}", body_style))
        else:
            # Inline bold/italic (standard markdown2)
            # We'll just pass it through Paragraph which handles <b> and <i> if they exist
            # Let's use a simpler approach: convert ** to <b>
            clean_line = line.replace('**', '<b>').replace('**', '</b>') # This won't work perfectly for pairs
            # Better: use regex or markdown2's partial conversion
            html_line = markdown2.markdown(line).strip()
            if html_line.startswith('<p>') and html_line.endswith('</p>'):
                html_line = html_line[3:-4]
            story.append(Paragraph(html_line, body_style))

    # 5. Build PDF
    doc.build(story)
    return response