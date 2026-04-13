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
from django.http import StreamingHttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.db import models as dj_models

from .models import UserProfile


# ── Saved report model (add to models.py migration) ───────────────────────────
# We define it inline here; run makemigrations dashboard after adding this.

from django.contrib.auth.models import User

class SavedReport(dj_models.Model):
    REPORT_TYPES = [
        ("market",     "Market Overview"),
        ("investment", "Investment Analysis"),
        ("portfolio",  "Portfolio Performance"),
    ]
    user        = dj_models.ForeignKey(User, on_delete=dj_models.CASCADE, related_name="reports")
    report_type = dj_models.CharField(max_length=20, choices=REPORT_TYPES)
    title       = dj_models.CharField(max_length=200)
    params      = dj_models.JSONField(default=dict)
    content     = dj_models.TextField()               # full markdown text
    created_at  = dj_models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "saved_reports"
        ordering = ["-created_at"]


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