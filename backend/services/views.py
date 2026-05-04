"""
EstateMind Dashboard — Django views.

Step 1: Public listings API (GET /api/listings/, GET /api/listings/<id>/)
Step 2: Auth with UserProfile (register, login, logout, session)
        + existing EDA / metrics / quality endpoints → PostgreSQL via ORM
"""
from asyncio.windows_events import NULL
import json
import re
from datetime import date, timedelta
from collections import defaultdict
import traceback
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.models import User
from django.utils import timezone
from django.contrib.auth.decorators import login_required
from django.db import transaction, OperationalError, ProgrammingError
from django.core.paginator import Paginator
from django.db.models import Count, Avg, Min, Max, Q, F
from django.db.models.functions import TruncDate
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
import pusher
from django.conf import settings

import uuid
from django.utils import timezone

# Conditional import for sentence_transformers (handles Windows DLL error)
try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMER_AVAILABLE = True
    print("✅ sentence-transformers loaded successfully")
except (ImportError, OSError) as e:
    print(f"⚠️ sentence-transformers not available: {e}")
    SENTENCE_TRANSFORMER_AVAILABLE = False
    SentenceTransformer = None

from data.preprocessing.steps.scorer import compute_score
from .models import Listing
try:
    from models.prediction_models.predictor import get_predictor
except ModuleNotFoundError:
    def get_predictor(*args, **kwargs):
        return None

# Import UserProfile at the bottom to avoid circular imports
from .models import UserProfile, AgentMetrics

try:
    from data.preprocessing.steps.scorer import compute_score
except ImportError:
    compute_score = None

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _json_body(request) -> dict:
    try:
        return json.loads(request.body.decode("utf-8"))
    except Exception:
        return {}


def _listing_to_dict(l: "Listing") -> dict:
    def f(v):
        """Cast Decimal → float for JSON serialisation."""
        return float(v) if v is not None else None

    def _parse_list(val):
        """Column stored as JSON string or already a Python list."""
        if val is None:
            return []
        if isinstance(val, list):
            return val
        if isinstance(val, str):
            try:
                parsed = json.loads(val)
                return parsed if isinstance(parsed, list) else []
            except (json.JSONDecodeError, ValueError):
                return []
        return []

    def _normalise_images(raw: list) -> list:
        """
        Images are a plain list of URL strings (confirmed from DB).
        Convert to [{url, label}] so the frontend always gets objects.
        """
        out = []
        for item in raw:
            if isinstance(item, str) and item.startswith("http"):
                out.append({"url": item, "label": "photo"})
            elif isinstance(item, dict) and item.get("url"):
                out.append(item)
        return out

    images   = _normalise_images(_parse_list(l.images))
    features = _parse_list(l.features)

    return {
        "id":                  l.id,
        "source_name":         l.source_name,
        "title":               l.title or "",
        "description":         l.description,
        "url":                 l.url,
        "price":               f(l.price),
        "currency":            l.currency or "TND",
        "transaction_type":    l.transaction_type,
        "type":                l.property_type,        # frontend still calls it "type"
        "rooms":               l.rooms,
        "city":                l.city,
        "municipality":        l.municipality,
        "zone":                l.zone,
        "region":              l.region,
        "surface":             f(l.surface),
        "price_per_m2":        f(l.price_per_m2),
        "latitude":            f(l.latitude),
        "longitude":           f(l.longitude),
        "features":            features,
        "images":              images,
        "images_count":        l.images_count or len(images),
        "poi":                 l.poi or [],
        "fraud_score":         None,
        "fraud_reason":        None,
        "reliability_score":   f(l.reliability_score),
        "reliability_level":   l.reliability_level,
        "is_outlier":          bool(l.is_outlier),
        "outlier_flags":       l.outlier_flags or [],
        "suspected_duplicate": bool(l.suspected_duplicate),
        "change_type":         l.change_type,
        "has_price_history":   bool(l.has_price_history),
        "price_delta":         f(l.price_delta),
        "price_delta_pct":     f(l.price_delta_pct),
        "scraped_at":          l.scraped_at.isoformat()  if l.scraped_at  else None,
        "last_updated":        l.last_updated.isoformat() if l.last_updated else None,
        "created_at":          l.created_at.isoformat()  if l.created_at  else None,
        "nlp_enriched":        bool(l.nlp_enriched),
        "normalized":          bool(l.normalized),
        "should_drop":         bool(l.should_drop),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — Public listings endpoints
# ─────────────────────────────────────────────────────────────────────────────

def listings_list(request):
    """
    GET /api/listings/
    Params: page, page_size, q, city, region, transaction, type,
            min_price, max_price, min_surface, max_surface,
            min_rooms, max_rooms, fraud, sort
    """
    qs = Listing.objects.filter(
        Q(should_drop=False) | Q(should_drop__isnull=True)
    )

    q_text = request.GET.get("q", "").strip()
    if q_text:
        qs = qs.filter(Q(title__icontains=q_text) | Q(description__icontains=q_text))

    city = request.GET.get("city", "").strip()
    if city:
        qs = qs.filter(city__iexact=city)

    region = request.GET.get("region", "").strip()
    if region:
        qs = qs.filter(region__iexact=region)

    tx = request.GET.get("transaction", "").strip()
    if tx in ("sale", "rent"):
        qs = qs.filter(transaction_type=tx)

    prop_type = request.GET.get("type", "").strip()
    if prop_type:
        qs = qs.filter(property_type=prop_type)

    try:
        if request.GET.get("min_price"):
            qs = qs.filter(price__gte=float(request.GET["min_price"]))
        if request.GET.get("max_price"):
            qs = qs.filter(price__lte=float(request.GET["max_price"]))
        if request.GET.get("min_surface"):
            qs = qs.filter(surface__gte=float(request.GET["min_surface"]))
        if request.GET.get("max_surface"):
            qs = qs.filter(surface__lte=float(request.GET["max_surface"]))
        if request.GET.get("min_rooms"):
            qs = qs.filter(rooms__gte=int(request.GET["min_rooms"]))
        if request.GET.get("max_rooms"):
            qs = qs.filter(rooms__lte=int(request.GET["max_rooms"]))
    except (ValueError, TypeError):
        pass

    sort_map = {
        "recent":        "-scraped_at",
        "price_asc":     "price",
        "price_desc":    "-price",
        "price_m2_asc":  "price_per_m2",
        "price_m2_desc": "-price_per_m2",
    }
    qs = qs.order_by(sort_map.get(request.GET.get("sort", "recent"), "-scraped_at"))

    try:
        page_size = min(int(request.GET.get("page_size", 24)), 100)
        page_num  = max(int(request.GET.get("page", 1)), 1)
    except ValueError:
        page_size, page_num = 24, 1

    paginator = Paginator(qs, page_size)
    page_obj  = paginator.get_page(page_num)

    return JsonResponse({
        "count":   paginator.count,
        "pages":   paginator.num_pages,
        "page":    page_num,
        "results": [_listing_to_dict(l) for l in page_obj],
    })


def listing_detail(request, pk):
    """GET /api/listings/<pk>/"""
    try:
        listing = Listing.objects.get(pk=pk)
    except Listing.DoesNotExist:
        return JsonResponse({"error": "Not found"}, status=404)
    return JsonResponse(_listing_to_dict(listing))


def listings_meta(request):
    """Return metadata for listings: cities, price ranges, property types, etc."""
    try:
        qs = Listing.objects.filter(Q(should_drop=False) | Q(should_drop__isnull=True))
        
        cities = sorted(
            qs.exclude(city__isnull=True)
              .exclude(city__exact='')
              .values_list("city", flat=True)
              .distinct()
        )
        
        price_stats = qs.exclude(price__isnull=True).filter(price__gt=0).aggregate(
            min_price=Min("price"),
            max_price=Max("price"),
            avg_price=Avg("price"),
        )
        
        from django.db import connection
        with connection.cursor() as cur:
            cur.execute("""
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)
                FROM listings
                WHERE price IS NOT NULL AND price > 0 
                AND (should_drop = FALSE OR should_drop IS NULL)
            """)
            median_price = cur.fetchone()[0]
        
        property_types = sorted(
            qs.exclude(property_type__isnull=True)
              .exclude(property_type__exact='')
              .values_list("property_type", flat=True)
              .distinct()
        )
        
        transaction_types = sorted(
            qs.exclude(transaction_type__isnull=True)
              .exclude(transaction_type__exact='')
              .values_list("transaction_type", flat=True)
              .distinct()
        )
        
        regions = sorted(
            qs.exclude(region__isnull=True)
              .exclude(region__exact='')
              .values_list("region", flat=True)
              .distinct()
        )
        
        return JsonResponse({
            "cities": cities,
            "regions": regions,
            "property_types": property_types,
            "transaction_types": transaction_types,
            "total_listings": qs.count(),
            "cities_covered": len(cities),
            "avg_price_per_m2": round(float(
                qs.exclude(price_per_m2__isnull=True)
                  .filter(price_per_m2__gt=0)
                  .aggregate(avg_m2=Avg("price_per_m2"))["avg_m2"] or 0
            ), 2),
            "listings_this_week": qs.filter(scraped_at__gte=timezone.now() - timedelta(days=7)).count(),
            "price_range": {
                "min": float(price_stats["min_price"] or 0),
                "max": float(price_stats["max_price"] or 0),
                "avg": float(price_stats["avg_price"] or 0),
                "median": float(median_price or 0),
            }
        })
        
    except Exception as e:
        traceback.print_exc()
        return JsonResponse({"error": str(e)}, status=500)


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — Auth (register, login, logout, session)
# ─────────────────────────────────────────────────────────────────────────────

_MATRICULE_RE = re.compile(r"^\d{7}[A-Z]/[A-Z]/[A-Z]{3}/\d{3}$")
_PHONE_RE     = re.compile(r"^(\+216)?[0-9]{8}$")


def _validate_register(data: dict) -> list[str]:
    """Return a list of validation error messages (empty = valid)."""
    errors = []

    name = (data.get("name") or "").strip()
    if len(name) < 2:
        errors.append("Le nom doit contenir au moins 2 caractères.")

    email = (data.get("email") or "").strip()
    if not re.match(r"^[^@]+@[^@]+\.[^@]+$", email):
        errors.append("Adresse email invalide.")
    elif User.objects.filter(email__iexact=email).exists():
        errors.append("Cette adresse email est déjà utilisée.")

    password = data.get("password") or ""
    if len(password) < 8:
        errors.append("Le mot de passe doit contenir au moins 8 caractères.")
    if not re.search(r"[A-Z]", password):
        errors.append("Le mot de passe doit contenir au moins une majuscule.")
    if not re.search(r"[0-9]", password):
        errors.append("Le mot de passe doit contenir au moins un chiffre.")
    if not re.search(r"[^A-Za-z0-9]", password):
        errors.append("Le mot de passe doit contenir au moins un caractère spécial.")

    dob_str = data.get("date_of_birth") or ""
    if not dob_str:
        errors.append("La date de naissance est obligatoire.")
    else:
        try:
            dob = date.fromisoformat(dob_str)
            today = date.today()
            age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
            if age < 18:
                errors.append("Vous devez avoir au moins 18 ans pour vous inscrire.")
            if dob > today:
                errors.append("La date de naissance ne peut pas être dans le futur.")
        except ValueError:
            errors.append("Format de date invalide (attendu : YYYY-MM-DD).")

    role = data.get("role", "particular")
    if role not in ("particular", "agency"):
        errors.append("Rôle invalide.")

    if role == "agency":
        agency_name = (data.get("agency_name") or "").strip()
        if len(agency_name) < 2:
            errors.append("Le nom de l'agence est obligatoire.")

        mf = (data.get("matricule_fiscale") or "").strip().upper()
        if not mf:
            errors.append("Le matricule fiscal est obligatoire pour les agences.")
        elif not _MATRICULE_RE.match(mf):
            errors.append(
                "Matricule fiscal invalide. Format attendu : 1234567A/A/AAA/000"
            )

    return errors


@csrf_exempt
@require_http_methods(["POST"])
def api_register(request):
    """
    POST /api/register/
    Body (JSON):
      name, email, password, role, date_of_birth, phone?
      [agency only] agency_name, matricule_fiscale
    """
    data = _json_body(request)

    try:
        User.objects.exists()
    except (OperationalError, ProgrammingError):
        return JsonResponse(
            {"errors": ["La base de données n'est pas initialisée. Lancez : python manage.py migrate"]},
            status=500,
        )

    errors = _validate_register(data)
    if errors:
        return JsonResponse({"errors": errors}, status=400)

    role  = data.get("role", "particular")
    name  = data["name"].strip()
    email = data["email"].strip().lower()

    username_base = email.split("@")[0]
    username      = username_base
    suffix        = 1
    while User.objects.filter(username=username).exists():
        username = f"{username_base}{suffix}"
        suffix  += 1

    try:
        with transaction.atomic():
            user = User.objects.create_user(
                username   = username,
                email      = email,
                password   = data["password"],
                first_name = name.split(" ")[0],
                last_name  = " ".join(name.split(" ")[1:]),
            )
            dob = date.fromisoformat(data["date_of_birth"])
            UserProfile.objects.create(
                user              = user,
                role              = role,
                date_of_birth     = dob,
                phone             = (data.get("phone") or "").strip(),
                agency_name       = (data.get("agency_name") or "").strip() if role == "agency" else "",
                matricule_fiscale = (data.get("matricule_fiscale") or "").strip().upper() if role == "agency" else "",
            )
    except (OperationalError, ProgrammingError) as e:
        return JsonResponse(
            {"errors": ["Erreur base de données. Vérifiez que 'python manage.py migrate' a été exécuté.", str(e)]},
            status=500,
        )
    except Exception as e:
        return JsonResponse({"errors": [f"Erreur lors de la création du compte : {e}"]}, status=500)

    login(request, user)
    return JsonResponse(_session_payload(user), status=201)


@csrf_exempt
@require_http_methods(["POST"])
def api_login(request):
    """POST /api/login/  { email, password }"""
    data     = _json_body(request)
    email    = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if not email or not password:
        return JsonResponse({"error": "Email et mot de passe requis."}, status=400)

    try:
        user_obj = User.objects.get(email__iexact=email)
    except User.DoesNotExist:
        return JsonResponse({"error": "Identifiants invalides."}, status=400)

    user = authenticate(request, username=user_obj.username, password=password)
    if user is None:
        return JsonResponse({"error": "Identifiants invalides."}, status=400)
    if not user.is_active:
        return JsonResponse({"error": "Compte désactivé."}, status=403)

    login(request, user)
    return JsonResponse(_session_payload(user))


@csrf_exempt
@require_http_methods(["POST"])
def api_logout(request):
    logout(request)
    return JsonResponse({"detail": "ok"})


def api_session(request):
    """GET /api/session/ — returns current user or 401"""
    if not request.user.is_authenticated:
        return JsonResponse({"is_authenticated": False}, status=401)
    return JsonResponse(_session_payload(request.user))


def _session_payload(user: User) -> dict:
    try:
        profile = user.profile
        role    = profile.role
    except UserProfile.DoesNotExist:
        role = "admin" if user.is_superuser else "particular"

    return {
        "is_authenticated": True,
        "id":          user.id,
        "username":    user.username,
        "email":       user.email,
        "name":        user.get_full_name() or user.username,
        "role":        role,
        "is_superuser": user.is_superuser,
        "last_login":  user.last_login.isoformat() if user.last_login else None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard (admin-only)
# ─────────────────────────────────────────────────────────────────────────────

# @login_required
# def dashboard(request):
#     return render(request, "dashboard.html", {
#         "total_listings": 0, "total_by_source": [], "latest_runs": [],
#     })


@login_required
def metrics_api(request):
    try:
        total = Listing.objects.count()
        
        flagged_count = Listing.objects.filter(is_outlier=True).count()
        
        # Per source stats
        per_source = list(
            Listing.objects
            .values("source_name")
            .annotate(count=Count("id"))
            .order_by("-count")
        )
        
        # Get recent runs - handle if AgentMetrics table doesn't exist yet
        recent_runs = []
        latest_run = _empty_run()
        
        try:
            recent_runs_qs = (
                AgentMetrics.objects
                .filter(run_started_at__isnull=False)
                .order_by("-run_started_at")[:10]
            )
            
            recent_runs = [
                {
                    "source_name": r.source_name,
                    "strategy":    r.strategy,
                    "fetched":     r.fetched,
                    "inserted":    r.inserted,
                    "updated":     r.updated,
                    "unchanged":   r.unchanged,
                    "errors":      r.errors,
                    "started_at":  r.run_started_at.isoformat() if r.run_started_at else None,
                    "finished_at": r.run_finished_at.isoformat() if r.run_finished_at else None,
                }
                for r in recent_runs_qs
            ]
            
            if recent_runs:
                latest_run = recent_runs[0]
        except Exception as e:
            # Table doesn't exist yet or other DB error
            print(f"Note: AgentMetrics table not ready: {e}")
        
        return JsonResponse({
            "total_listings": total,
            "flagged_count":  flagged_count,
            "latest_run":     latest_run,
            "per_source":     per_source,
            "recent_runs":    recent_runs,
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JsonResponse({
            "total_listings": 0,
            "flagged_count": 0,
            "latest_run": _empty_run(),
            "per_source": [],
            "recent_runs": [],
            "error": str(e),
        })

@login_required
def data_quality_api(request):
    try:
        qs    = Listing.objects
        total = qs.count()
        if total == 0:
            return JsonResponse(_empty_quality())

        key_fields = ["price", "surface", "rooms", "city", "region",
                      "municipality", "latitude", "longitude",
                      "transaction_type", "property_type"]
        null_stats = []
        for field in key_fields:
            null_count = qs.filter(**{f"{field}__isnull": True}).count()
            null_stats.append({
                "field":        field,
                "null_count":   null_count,
                "filled_count": total - null_count,
                "null_pct":     round(null_count / total * 100, 1),
                "filled_pct":   round((total - null_count) / total * 100, 1),
            })
        null_stats.sort(key=lambda x: -x["null_pct"])

        avg_score = round(float(qs.aggregate(Avg("reliability_score"))["reliability_score__avg"] or 0), 1)
        
        dist_qs = qs.values("reliability_level").annotate(count=Count("id"))
        score_distribution = []
        for row in dist_qs:
            level = row["reliability_level"] or "UNKNOWN"
            count = row["count"]
            score_distribution.append({
                "level": level,
                "count": count,
                "pct": round(count / total * 100, 1)
            })

        source_quality = []
        sources = qs.values("source_name").annotate(total=Count("id")).order_by("-total")
        for src in sources:
            name = src["source_name"]
            stotal = src["total"]
            grades = qs.filter(source_name=name).values("reliability_level").annotate(count=Count("id"))
            grade_map = {row["reliability_level"]: row["count"] for row in grades}
            source_quality.append({
                "source": name,
                "total":  stotal,
                "high":   grade_map.get("HIGH", 0),
                "good":   grade_map.get("GOOD", 0),
                "low":    grade_map.get("LOW", 0),
                "drop":   grade_map.get("DROP", 0),
            })

        dup_count = qs.filter(suspected_duplicate=True).count()

        return JsonResponse({
            "total":               total,
            "null_field_stats":    null_stats,
            "duplicate_count":     dup_count,
            "duplicate_pct":       round(dup_count / total * 100, 1),
            "source_quality":      source_quality,
            "avg_reliability_score": avg_score,
            "score_distribution":  score_distribution,
            "nlp_enriched_count":  qs.filter(nlp_enriched=True).count(),
            "nlp_fields_filled":   [],
            "outlier_count":       qs.filter(is_outlier=True).count(),
            "outlier_pct":         round(qs.filter(is_outlier=True).count() / total * 100, 1),
            "outlier_flag_breakdown": [],
            "change_distribution": [],
        })
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def eda_metrics(request):
    try:
        qs    = Listing.objects
        total = qs.count()
        if total == 0:
            return JsonResponse(_empty_eda())

        region_stats = list(
            qs.exclude(region__isnull=True)
            .values("region").annotate(count=Count("id")).order_by("-count")
        )
        price_stats = list(
            qs.exclude(region__isnull=True).exclude(price__isnull=True).filter(price__gt=0)
            .values("region")
            .annotate(min_price=Min("price"), max_price=Max("price"), avg_price=Avg("price"))
            .order_by("region")
        )
        transaction_stats = list(
            qs.exclude(transaction_type__isnull=True)
            .values("transaction_type").annotate(count=Count("id")).order_by("-count")
        )
        property_type_stats = list(
            qs.exclude(property_type__isnull=True)
            .values("property_type")
            .annotate(type=F("property_type"), count=Count("id"))
            .values("type", "count")
            .order_by("-count")
        )
        top_areas = list(
            qs.exclude(city__isnull=True)
            .values("city").annotate(count=Count("id")).order_by("-count")[:10]
        )
        trend_stats = [
            {"date": str(row["date"]), "count": row["count"]}
            for row in (
                qs.exclude(scraped_at__isnull=True)
                .annotate(date=TruncDate("scraped_at"))
                .values("date").annotate(count=Count("id")).order_by("date")
            )
        ]
        price_m2_stats = [
            {"region": row["region"], "avg_m2": round(float(row["avg_m2"] or 0), 2)}
            for row in (
                qs.exclude(region__isnull=True)
                .exclude(price__isnull=True).exclude(surface__isnull=True)
                .filter(price__gt=0, surface__gt=0)
                .values("region")
                .annotate(avg_m2=Avg("price") / Avg("surface"))
                .order_by("-avg_m2")
            )
        ]

        feature_counts = defaultdict(int)
        for features_list in qs.exclude(features__isnull=True).values_list("features", flat=True)[:1000]:
            if isinstance(features_list, list):
                for f in features_list:
                    feature_counts[str(f).lower()] += 1
        
        top_features = [
            {"feature": k, "count": v} 
            for k, v in sorted(feature_counts.items(), key=lambda x: -x[1])[:15]
        ]

        return JsonResponse({
            "region_stats":        region_stats,
            "price_stats":         price_stats,
            "transaction_stats":   transaction_stats,
            "property_type_stats": property_type_stats,
            "top_areas":           top_areas,
            "trend_stats":         trend_stats,
            "price_m2_stats":      price_m2_stats,
            "top_features":        top_features,
        })
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/listings/ — Create a new user-submitted listing
# ─────────────────────────────────────────────────────────────────────────────

import math
import os
import requests
import logging
from supabase import create_client, Client
from threading import Lock
import time

logger = logging.getLogger(__name__)

_supabase_client: Client = None
_client_lock = Lock()

def __get_supabase_client() -> Client:
        global _supabase_client
    
        with _client_lock:
            if _supabase_client is None:
                supabase_url = os.environ.get("SUPABASE_URL", "https://amxnojlfczwffvtwutrb.supabase.co")
                supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
                
                if not supabase_key:
                    raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY not set in environment")
                
                _supabase_client = create_client(supabase_url, supabase_key)
                print("✅ Supabase client initialized")
            
            return _supabase_client


def _haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000 
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2.0) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * \
        math.sin(delta_lambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def _fetch_nearby_pois(lat: float, lon: float, radius_m: int = 1000) -> list:
    """Fetches nearby POIs from OpenStreetMap using Overpass API."""
    logger.info(f"🗺️ Fetching POIs: Lat={lat}, Lon={lon}, Radius={radius_m}m")
    
    if not lat or not lon:
        logger.warning("No coordinates provided for POI fetch.")
        return []

    query = f"""
    [out:json][timeout:15];
    (
      node["amenity"~"school|university|college|hospital|clinic|pharmacy|restaurant|cafe|bank|post_office|police"](around:{radius_m},{lat},{lon});
      way["amenity"~"school|university|college|hospital|clinic|pharmacy|restaurant|cafe|bank|post_office|police"](around:{radius_m},{lat},{lon});
      node["shop"~"supermarket|mall|convenience|bakery"](around:{radius_m},{lat},{lon});
      way["shop"~"supermarket|mall|convenience|bakery"](around:{radius_m},{lat},{lon});
      node["public_transport"="station"](around:{radius_m},{lat},{lon});
    );
    out center;
    """

    try:
        response = requests.post(
            "https://overpass-api.de/api/interpreter",
            data=query.encode('utf-8'),
            headers={'User-Agent': 'EstateMind/1.0'},
            timeout=15
        )
        
        if response.status_code != 200:
            logger.error(f"Overpass API Error: {response.status_code}")
            return []

        data = response.json()
        elements = data.get('elements', [])
        
        pois = []
        seen_names = set()

        for element in elements:
            tags = element.get('tags', {})
            name = tags.get('name')
            if not name:
                continue

            clean_name = name.strip()
            if clean_name.lower() in seen_names:
                continue
            
            el_lat = element.get('lat') or element.get('center', {}).get('lat')
            el_lon = element.get('lon') or element.get('center', {}).get('lon')
            
            if el_lat and el_lon:
                dist = _haversine_distance(lat, lon, el_lat, el_lon)
                if dist <= radius_m:
                    seen_names.add(clean_name.lower())
                    pois.append(clean_name)
                    if len(pois) >= 10:
                        break

        logger.info(f"Found {len(pois)} POIs")
        return pois

    except Exception as e:
        logger.error(f"Exception in POI fetch: {e}")
        return []


# Conditional image embedding functions (only if sentence_transformers is available)
_image_model = None

def _get_image_model():
    global _image_model
    if not SENTENCE_TRANSFORMER_AVAILABLE:
        logger.warning("sentence-transformers not available, image embeddings disabled")
        return None
    if _image_model is None:
        try:
            logger.info("Loading CLIP model for image embeddings...")
            _image_model = SentenceTransformer('clip-ViT-B-32')
        except Exception as e:
            logger.error(f"Failed to load CLIP model: {e}")
            return None
    return _image_model


def _generate_image_embedding(image_url: str):
    """Generates image embedding if sentence_transformers is available."""
    if not SENTENCE_TRANSFORMER_AVAILABLE:
        return None
    try:
        model = _get_image_model()
        if model is None:
            return None
        embedding = model.encode([image_url], convert_to_numpy=True)[0]
        return embedding.tolist()
    except Exception as e:
        logger.error(f"Failed to generate embedding for {image_url}: {e}")
        return None


def sanitize_listing_data(data: dict) -> dict:
    """Basic sanitization for listing data."""
    return {
        "title": data.get("title", "").strip(),
        "description": data.get("description", "").strip(),
        "price": data.get("price", 0),
        "surface": data.get("surface", 0),
        "rooms": data.get("rooms", 0),
        "transaction": data.get("transaction", "sale"),
        "type": data.get("type", "apartment"),
        "city": data.get("city", "").strip(),
        "custom_delegation": data.get("custom_delegation", ""),
        "delegation_id": data.get("delegation_id"),
        "governorate_id": data.get("governorate_id"),
        "governorate": data.get("governorate", ""),
        "latitude": data.get("latitude"),
        "longitude": data.get("longitude"),
        "images": data.get("images", []),
        "features": data.get("features", []),
        "municipality": data.get("municipality"),
    }


def get_title_validator():
    """Placeholder for title validator."""
    class SimpleTitleValidator:
        def validate(self, title):
            if len(title) < 5:
                return False, "Title too short", 0.0
            return True, "Valid", 1.0
    return SimpleTitleValidator()


def get_delegation_matcher():
    """Placeholder for delegation matcher."""
    class SimpleDelegationMatcher:
        def auto_correct(self, name, governorate_id):
            return {
                "matched": False,
                "original": name,
                "corrected": name,
                "confidence": 0.0,
                "delegation_id": None
            }
    return SimpleDelegationMatcher()


@csrf_exempt
@require_http_methods(["POST"])
@login_required
def create_listing(request):
    """
    POST /api/listings/create/
    Complete listing creation with all features.
    """
    logger.info(f"CREATE_LISTING called by user: {request.user.username}")
    
    try:
        data = json.loads(request.body.decode('utf-8'))
        sanitized = sanitize_listing_data(data)
        
        title = sanitized.get("title", "").strip()
        if not title or len(title) < 5:
            return JsonResponse({"error": "Title must be at least 5 characters"}, status=400)
        
        price = float(sanitized.get("price", 0))
        if price <= 0:
            return JsonResponse({"error": "Price must be greater than 0"}, status=400)
        
        surface = float(sanitized.get("surface", 0))
        if surface <= 0:
            return JsonResponse({"error": "Surface must be greater than 0"}, status=400)
        
        price_per_m2 = round(price / surface, 2) if price and surface and surface > 0 else None
        
        # Use city from the form (this is the delegation/city name)
        final_city = sanitized.get("city", "") or sanitized.get("custom_delegation", "")
        final_latitude = sanitized.get("latitude")
        final_longitude = sanitized.get("longitude")
        
        # Get municipality (governorate name)
        municipality = sanitized.get("municipality", "") or sanitized.get("governorate", "")
        
        # Get region (can be mapped from governorate)
        region = sanitized.get("region", "") or sanitized.get("governorate", "")
        
        # Get zone (North/South/Center/etc - could be derived from governorate)
        zone = sanitized.get("zone", "")
        
        # Extract POIs
        extracted_pois = []
        if final_latitude and final_longitude:
            try:
                extracted_pois = _fetch_nearby_pois(float(final_latitude), float(final_longitude), radius_m=1000)
            except (ValueError, TypeError) as e:
                logger.error(f"POI extraction error: {e}")
        
        image_urls = sanitized.get("images", [])
        
        # Calculate reliability score
        temp_meta = {
            "price": price,
            "surface": surface,
            "rooms": int(sanitized.get("rooms", 0)),
            "city": final_city,
            "governorate": municipality,
            "latitude": final_latitude,
            "longitude": final_longitude,
            "description": sanitized.get("description", ""),
            "image_count": len(image_urls),
            "features": sanitized.get("features", []),
            "municipality": municipality,
            "is_outlier": False,
        }
        
        flags = {
            "price_outlier": False,
            "suspected_duplicate": False,
            "nlp_enriched": False,
            "has_price_history": False,
            "price_changed": False,
            "cross_verified": False
        }
        
        score_result = compute_score(temp_meta, flags)
        logger.info(f"Reliability Score: {score_result['score']} ({score_result['level']})")
        
        listing_id = str(uuid.uuid4())
        text_embedding = [0.0] * 384
        
        listing_data = {
            "id": listing_id,
            "source_name": "user_submission",
            "title": title,
            "description": sanitized.get("description", "").strip(),
            "price": price,
            "currency": "TND",
            "transaction_type": sanitized.get("transaction", "sale"),
            "property_type": sanitized.get("type", "apartment"),
            "rooms": int(sanitized.get("rooms", 0)),
            "city": final_city,  # This is the delegation/city name
            "municipality": municipality,  # This is the governorate name
            "region": region,  # Region name
            "zone": zone,  # North/South/Center
            "surface": surface,
            "price_per_m2": price_per_m2,
            "latitude": float(final_latitude) if final_latitude else None,
            "longitude": float(final_longitude) if final_longitude else None,
            "poi": extracted_pois,
            "images": image_urls,
            "images_count": len(image_urls),
            "features": sanitized.get("features", []),
            "reliability_score": score_result["score"],
            "reliability_level": score_result["level"],
            "should_drop": score_result["should_drop"],
            "is_outlier": False,
            "normalized": True,
            "nlp_enriched": False,
            "text_embedding": text_embedding,
            "scraped_at": timezone.now().isoformat(),
            "last_updated": timezone.now().isoformat(),
            "created_at": timezone.now().isoformat(),
            "views_count": 0,  # Add this
            "likes_count": 0,  # Add this
        }
        
        # Save to Supabase
        logger.info("Saving Listing to Supabase...")
        supabase = __get_supabase_client()
        result = supabase.table("listings").insert(listing_data).execute()
        
        if hasattr(result, "error") and result.error:
            raise Exception(f"Supabase listing insert failed: {result.error}")
        
        logger.info(f"Listing Saved! ID: {listing_id}")
        
        # Generate image embeddings (if available)
        if image_urls and SENTENCE_TRANSFORMER_AVAILABLE:
            logger.info(f"Generating embeddings for {len(image_urls)} images...")
            embeddings_to_insert = []
            for index, img_url in enumerate(image_urls[:10]):
                embedding_vec = _generate_image_embedding(img_url)
                if embedding_vec:
                    embeddings_to_insert.append({
                        "id": f"{listing_id}_img_{index}",
                        "listing_id": listing_id,
                        "image_url": img_url,
                        "image_index": index,
                        "embedding": embedding_vec
                    })
            
            if embeddings_to_insert:
                emb_result = supabase.table("image_embeddings").insert(embeddings_to_insert).execute()
                if hasattr(emb_result, "error") and emb_result.error:
                    logger.warning("Failed to save some image embeddings")
                else:
                    logger.info(f"Saved {len(embeddings_to_insert)} image embeddings")
        
        # Get price prediction
        predicted_price_data = None
        try:
            predictor = get_predictor()
            prediction = predictor.predict(
                transaction_type=sanitized.get("transaction", "sale"),
                property_type=sanitized.get("type", "apartment"),
                city=final_city,
                surface=surface,
                rooms=int(sanitized.get("rooms", 0)),
                region=municipality,
                reliability_score=score_result["score"],
                reliability_level=score_result["level"],
                model_weight=1.0,
                is_outlier=False,
                suspected_duplicate=False,
                images_count=len(image_urls),
                has_description=1 if sanitized.get("description") else 0,
                desc_length=len(sanitized.get("description", "")),
                has_coords=1 if final_latitude and final_longitude else 0
            )
            predicted_price_data = prediction
            logger.info(f"Predicted Price: {prediction['predicted_price']} TND")
        except Exception as e:
            logger.error(f"Price Prediction Failed: {e}")
        
        response_data = {
            "success": True,
            "listing_id": listing_id,
            "pois_found": len(extracted_pois),
            "reliability_score": score_result["score"],
            "reliability_level": score_result["level"],
            "message": "Listing published successfully!"
        }
        
        if predicted_price_data:
            response_data["predicted_price"] = predicted_price_data.get('predicted_price')
            response_data["price_range"] = {
                "low": predicted_price_data.get('price_low'),
                "high": predicted_price_data.get('price_high'),
            }
        
        return JsonResponse(response_data, status=201)
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON Decode Error: {e}")
        return JsonResponse({"error": "Invalid JSON data"}, status=400)
    except Exception as e:
        logger.error(f"CRITICAL ERROR: {e}", exc_info=True)
        return JsonResponse({"error": str(e)}, status=500)
# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _empty_run():
    """Return empty run structure matching what frontend expects"""
    return {
        "source_name": None,
        "strategy": None,
        "fetched": None,
        "inserted": None,
        "updated": None,
        "unchanged": None,
        "errors": None,
        "started_at": None,
        "finished_at": None,
    }

def _empty_quality():
    """Return empty quality structure"""
    return {
        "total": 0,
        "null_field_stats": [],
        "duplicate_count": 0,
        "duplicate_pct": 0,
        "source_quality": [],
        "avg_reliability_score": 0,
        "score_distribution": [],
        "nlp_enriched_count": 0,
        "nlp_fields_filled": [],
        "outlier_count": 0,
        "outlier_pct": 0,
        "outlier_flag_breakdown": [],
        "change_distribution": [],
    }

def _empty_eda():
    """Return empty EDA structure"""
    return {
        "region_stats": [],
        "price_stats": [],
        "transaction_stats": [],
        "property_type_stats": [],
        "top_areas": [],
        "trend_stats": [],
        "price_m2_stats": [],
        "top_features": [],
    }

@login_required
def user_listings(request):
    """GET /api/user/listings/ - Get listings created by current user"""
    try:
        supabase = __get_supabase_client()
        
        # Use source_id as user_id (stored as string)
        source_id = str(request.user.id)
        
        result = supabase.table("listings")\
            .select("*")\
            .eq("source_id", source_id)\
            .order("created_at", desc=True)\
            .execute()
        
        listings = []
        for l in result.data:
            listings.append({
                "id": l.get("id"),
                "title": l.get("title", ""),
                "city": l.get("city", ""),
                "price": l.get("price"),
                "type": l.get("property_type"),
                "rooms": l.get("rooms"),
                "surface": l.get("surface"),
                "views": l.get("views_count", 0),
                "likes": l.get("likes_count", 0),
                "status": "active" if not l.get("should_drop") else "inactive",
                "image": l.get("images", [{}])[0] if l.get("images") else None,
                "created_at": l.get("created_at"),
                "scraped_at": l.get("scraped_at")
            })
        
        active_count = sum(1 for l in listings if l["status"] == "active")
        
        return JsonResponse({
            "listings": listings,
            "total": len(listings),
            "active_count": active_count
        })
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def user_likes(request):
    """GET /api/user/likes/ - Get listings liked by current user"""
    try:
        supabase = __get_supabase_client()
        
        user_id = str(request.user.id)
        
        # Get likes with listing details
        result = supabase.table("listing_likes")\
            .select("*, listings(*)")\
            .eq("user_id", user_id)\
            .order("created_at", desc=True)\
            .execute()
        
        liked_listings = []
        for item in result.data:
            listing = item.get("listings", {})
            liked_listings.append({
                "id": listing.get("id"),
                "title": listing.get("title", ""),
                "city": listing.get("city", ""),
                "price": listing.get("price"),
                "type": listing.get("property_type"),
                "rooms": listing.get("rooms"),
                "surface": listing.get("surface"),
                "image": listing.get("images", [{}])[0] if listing.get("images") else None,
                "liked_at": item.get("created_at")
            })
        
        return JsonResponse({
            "likes": liked_listings,
            "total": len(liked_listings)
        })
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def user_stats(request):
    try:
        supabase = __get_supabase_client()
        source_id = str(request.user.id)
        user_id   = request.user.id
        
        # ── Define time periods FIRST ─────────────────────────────────────
        from datetime import datetime, timedelta
        now = datetime.now()
        current_month = now.strftime("%Y-%m")
        last_month = (now.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
        week_ago = (now - timedelta(days=7)).isoformat()
        month_start = now.replace(day=1).isoformat()
        last_month_start = (now.replace(day=1) - timedelta(days=1)).replace(day=1).isoformat()
        last_month_end = now.replace(day=1).isoformat()

        # ── 1. Get user's listings ────────────────────────────────────────
        listings_result = supabase.table("listings")\
            .select("*")\
            .eq("source_id", source_id)\
            .execute()

        user_listings = listings_result.data or []
        listing_ids   = [l["id"] for l in user_listings]
        active_count  = sum(1 for l in user_listings if not l.get("should_drop", False))
        this_month_listings = sum(1 for l in user_listings if (l.get("created_at") or "").startswith(current_month))

        # Initialize default values
        total_likes = 0
        likes_this_week = 0
        total_views = 0
        views_this_month = 0
        views_last_month = 0
        views_change = "0% vs last month"

        # ── 2. Count likes RECEIVED on user's listings ────────────────────
        if listing_ids:
            try:
                # Total likes
                likes_result = supabase.table("listing_likes") \
                    .select("*", count="exact") \
                    .in_("user_id", source_id) \
                    .execute()
                total_likes = likes_result.count if hasattr(likes_result, 'count') else 0

                # Likes this week
                likes_this_week_result = supabase.table("listing_likes") \
                    .select("*", count="exact") \
                    .in_("user_id", source_id) \
                    .gte("created_at", week_ago) \
                    .execute()
                likes_this_week = likes_this_week_result.count if hasattr(likes_this_week_result, 'count') else 0

            except Exception as e:
                print(f"Error counting likes: {e}")
                total_likes = 0
                likes_this_week = 0

        # ── 3. Count views RECEIVED on user's listings ────────────────────
        if listing_ids:
            try:
                # Total views
                views_result = supabase.table("listing_views") \
                    .select("*", count="exact") \
                    .in_("user_id", source_id) \
                    .execute()
                total_views = views_result.count if hasattr(views_result, 'count') else 0

                # Views this month
                views_this_month_result = supabase.table("listing_views") \
                    .select("*", count="exact") \
                    .in_("user_id", source_id) \
                    .gte("created_at", month_start) \
                    .execute()
                views_this_month = views_this_month_result.count or 0

                # Views last month
                views_last_month_result = supabase.table("listing_views") \
                    .select("*", count="exact") \
                    .in_("user_id", source_id) \
                    .gte("created_at", last_month_start) \
                    .lt("created_at", last_month_end) \
                    .execute()
                views_last_month = views_last_month_result.count or 0

                # Calculate % change vs last month
                if views_last_month > 0:
                    pct = round((views_this_month - views_last_month) / views_last_month * 100)
                    views_change = f"+{pct}% vs last month" if pct >= 0 else f"{pct}% vs last month"
                elif views_this_month > 0:
                    views_change = "+100% vs last month"
                else:
                    views_change = "0% vs last month"
                
            except Exception as e:
                print(f"Error counting views: {e}")
                total_views = 0
                views_change = "0% vs last month"

        # ── 4. Unread messages ────────────────────────────────────────────
        unread_messages = 0
        total_messages = 0
        try:
            conv_result = supabase.table("conversations") \
                .select("id") \
                .or_(f"buyer_id.eq.{user_id},responsible_id.eq.{user_id}") \
                .execute()

            conv_ids = [c["id"] for c in (conv_result.data or [])]
            total_messages = len(conv_ids)

            for conv_id in conv_ids:
                unread = supabase.table("chat_messages") \
                    .select("id", count="exact") \
                    .eq("conversation_id", conv_id) \
                    .eq("receiver_id", user_id) \
                    .eq("is_read", False) \
                    .execute()
                unread_messages += unread.count or 0
        except Exception as e:
            print(f"Error counting messages: {e}")

        return JsonResponse({
            "active_listings": active_count,
            "active_change":   f"+{this_month_listings} this month" if this_month_listings > 0 else "0 this month",
            "total_views":     total_views,
            "views_change":    views_change,
            "total_likes":     total_likes,
            "likes_change":    f"{likes_this_week} this week",
            "messages":        total_messages,
            "unread_messages": unread_messages,
            "reports_generated": 0,
            "reports_last":    "Never",
            "roi_estimate":    8.2,
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return JsonResponse({
            "active_listings": 0, "active_change": "0 this month",
            "total_views": 0,    "views_change": "0% vs last month",
            "total_likes": 0,    "likes_change": "0 this week",
            "messages": 0,       "unread_messages": 0,
            "reports_generated": 0, "reports_last": "Never", "roi_estimate": 8.2,  # ← Fixed: keep 8.2
        })

@login_required
def user_activity(request):
    """GET /api/user/activity/ - Get user's recent activity"""
    try:
        supabase = __get_supabase_client()
        source_id = str(request.user.id)
        activities = []
        
        # Get recent views on user's listings
        try:
            views_result = supabase.table("listing_views")\
                .select("*, listings!inner(title)")\
                .eq("listings.source_id", source_id)\
                .order("created_at", desc=True)\
                .limit(5)\
                .execute()
            
            if views_result.data:
                for view in views_result.data:
                    listing = view.get("listings", {})
                    activities.append({
                        "text": f"Someone viewed your listing: {listing.get('title', 'Unknown')[:50]}",
                        "time": time_ago(view.get("created_at")),
                        "type": "view"
                    })
        except Exception as e:
            print(f"Error loading views: {e}")
        
        # Get recent likes on user's listings
        try:
            likes_result = supabase.table("listing_likes")\
                .select("*, listings!inner(title)")\
                .eq("listings.source_id", source_id)\
                .order("created_at", desc=True)\
                .limit(5)\
                .execute()
            
            if likes_result.data:
                for like in likes_result.data:
                    listing = like.get("listings", {})
                    activities.append({
                        "text": f"Someone liked your listing: {listing.get('title', 'Unknown')[:50]}",
                        "time": time_ago(like.get("created_at")),
                        "type": "like"
                    })
        except Exception as e:
            print(f"Error loading likes: {e}")
        
        # If no activities, return default
        if not activities:
            activities = [
                {"text": "Post your first listing to get started", "time": "now", "type": "info"},
                {"text": "Complete your profile for better visibility", "time": "now", "type": "info"}
            ]
        
        return JsonResponse({"activities": activities[:5]})
    except Exception as e:
        print(f"Error in user_activity: {e}")
        return JsonResponse({
            "activities": [
                {"text": "Welcome to EstateMind!", "time": "now", "type": "info"},
                {"text": "Start by posting your first listing", "time": "now", "type": "info"}
            ]
        })

@login_required
def toggle_like(request, listing_id):
    """POST /api/listings/<listing_id>/like/ - Toggle like on a listing"""
    try:
        supabase = __get_supabase_client()
        user_id = str(request.user.id)
        
        # First, check if the listing belongs to the current user
        listing_result = supabase.table("listings")\
            .select("source_id")\
            .eq("id", listing_id)\
            .execute()
        
        if listing_result.data:
            listing_owner_id = listing_result.data[0].get("source_id")
            if listing_owner_id == user_id:
                return JsonResponse({
                    "error": "You cannot like your own listing"
                }, status=400)
        
        # Check if already liked
        existing = supabase.table("listing_likes")\
            .select("*")\
            .eq("listing_id", listing_id)\
            .eq("user_id", user_id)\
            .execute()
        
        if existing.data:
            # Unlike: remove the like
            supabase.table("listing_likes")\
                .delete()\
                .eq("listing_id", listing_id)\
                .eq("user_id", user_id)\
                .execute()
            liked = False
        else:
            # Like: add the like
            supabase.table("listing_likes").insert({
                "listing_id": listing_id,
                "user_id": user_id,
                "created_at": timezone.now().isoformat()
            }).execute()
            liked = True
        
        # Get updated like count (only from other users)
        count_result = supabase.table("listing_likes")\
            .select("count", count="exact")\
            .eq("listing_id", listing_id)\
            .execute()
        
        like_count = count_result.count if hasattr(count_result, 'count') else 0
        
        # Update likes_count in listings table
        supabase.table("listings")\
            .update({"likes_count": like_count})\
            .eq("id", listing_id)\
            .execute()
        
        return JsonResponse({
            "liked": liked,
            "like_count": like_count
        })
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
    

@login_required

@login_required
def track_listing_view(request, listing_id):
    try:
        supabase = __get_supabase_client()
        user_id = request.user.id

        listing_result = supabase.table("listings") \
            .select("source_id, views_count") \
            .eq("id", listing_id) \
            .execute()

        if not listing_result.data:
            return JsonResponse({"error": "Listing not found"}, status=404)

        listing = listing_result.data[0]
        owner_id = listing.get("source_id")

        # Don't track owner's own views
        if owner_id and str(owner_id) == str(user_id):
            return JsonResponse({"message": "Owner views not tracked", "views_count": listing.get("views_count", 0)})

        # Try inserting into listing_views (may not exist — that's ok)
        try:
            supabase.table("listing_views").insert({
                "listing_id": listing_id,
                "user_id": user_id,
                "viewer_ip": request.META.get("REMOTE_ADDR", ""),
                "created_at": timezone.now().isoformat(),
            }).execute()
        except Exception as e:
            print(f"listing_views insert failed (table may not exist): {e}")

        # ✅ Always increment views_count directly on the listing
        new_count = (listing.get("views_count") or 0) + 1
        supabase.table("listings") \
            .update({"views_count": new_count}) \
            .eq("id", listing_id) \
            .execute()

        return JsonResponse({"success": True, "views_count": new_count})

    except Exception as e:
        print(f"Error tracking view: {e}")
        return JsonResponse({"error": str(e)}, status=500)
    
def time_ago(dt_str):
    """Convert datetime string to 'X hours ago' format"""
    if not dt_str:
        return "recently"
    from datetime import datetime
    try:
        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        now = datetime.now(dt.tzinfo)
        diff = now - dt
        if diff.days > 0:
            return f"{diff.days} day{'s' if diff.days > 1 else ''} ago"
        elif diff.seconds > 3600:
            hours = diff.seconds // 3600
            return f"{hours} hour{'s' if hours > 1 else ''} ago"
        elif diff.seconds > 60:
            minutes = diff.seconds // 60
            return f"{minutes} minute{'s' if minutes > 1 else ''} ago"
        else:
            return "just now"
    except:
        return "recently"
    
pusher_client = pusher.Pusher(
    app_id=settings.PUSHER_APP_ID,
    key=settings.PUSHER_KEY,
    secret=settings.PUSHER_SECRET,
    cluster=settings.PUSHER_CLUSTER,
    ssl=True
)


from django.contrib.auth.models import User

@login_required
@require_http_methods(["GET", "POST"])
def get_or_create_conversation(request, listing_id):
    """GET/POST /api/chat/conversation/<listing_id>/ - Get or create conversation"""
    try:
        supabase = __get_supabase_client()
        buyer_id = request.user.id
        
        # Get listing details
        listing_result = supabase.table("listings")\
            .select("source_name, source_id, title")\
            .eq("id", listing_id)\
            .execute()
        
        if not listing_result.data:
            return JsonResponse({"error": "Listing not found"}, status=404)
        
        listing = listing_result.data[0]
        source_name = listing.get("source_name")
        
        # Determine responsible user ID
        if source_name == "user_submission":
            # For user submissions, the responsible is the listing owner (source_id)
            responsible_id = listing.get("source_id")
            if not responsible_id:
                return JsonResponse({"error": "Listing owner not found"}, status=404)
            # Ensure it's an integer
            try:
                responsible_id = int(responsible_id)
            except (ValueError, TypeError):
                return JsonResponse({"error": "Invalid owner ID format"}, status=404)
        else:
            # For external sources, look up responsible user from source_responsibles table
            responsible_result = supabase.table("source_responsibles")\
                .select("user_id")\
                .eq("source_name", source_name)\
                .execute()
            
            if not responsible_result.data:
                return JsonResponse({
                    "error": f"No responsible user assigned for source: {source_name}"
                }, status=404)
            
            responsible_id = responsible_result.data[0]["user_id"]
        
        # Check if conversation already exists
        conv_result = supabase.table("conversations")\
            .select("*")\
            .eq("listing_id", listing_id)\
            .eq("buyer_id", buyer_id)\
            .execute()
        
        if conv_result.data:
            conversation = conv_result.data[0]
        else:
            # Create new conversation
            new_conv = {
                "listing_id": listing_id,
                "buyer_id": buyer_id,
                "responsible_id": responsible_id,
                "source_name": source_name,
                "is_active": True,
                "created_at": timezone.now().isoformat()
            }
            insert_result = supabase.table("conversations").insert(new_conv).execute()
            conversation = insert_result.data[0]
        
        # Get responsible user info
        responsible_name = f"User {responsible_id}"
        try:
            responsible_user = User.objects.get(id=responsible_id)
            responsible_name = responsible_user.get_full_name() or responsible_user.username
        except User.DoesNotExist:
            pass
        
        # If it's a user submission, show "Property Owner" as the name
        if source_name == "user_submission":
            responsible_name = "Property Owner"
        
        return JsonResponse({
            "conversation_id": conversation["id"],
            "responsible_id": responsible_id,
            "responsible_name": responsible_name,
            "listing_title": listing.get("title"),
            "source_name": source_name,
            "is_user_submission": source_name == "user_submission"
        })
        
    except Exception as e:
        print(f"Error in get_or_create_conversation: {e}")
        import traceback
        traceback.print_exc()
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def send_chat_message(request):
    """POST /api/chat/send/ - Send a real-time message"""
    try:
        data = json.loads(request.body.decode('utf-8'))
        conversation_id = data.get('conversation_id')
        message = data.get('message', '').strip()
        
        if not conversation_id or not message:
            return JsonResponse({"error": "Missing required fields"}, status=400)
        
        supabase = __get_supabase_client()
        sender_id = request.user.id
        
        # Get conversation details
        conv_result = supabase.table("conversations")\
            .select("*")\
            .eq("id", conversation_id)\
            .execute()
        
        if not conv_result.data:
            return JsonResponse({"error": "Conversation not found"}, status=404)
        
        conversation = conv_result.data[0]
        receiver_id = conversation["responsible_id"] if sender_id == conversation["buyer_id"] else conversation["buyer_id"]
        
        # Save message
        message_data = {
            "conversation_id": conversation_id,
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "message": message,
            "created_at": timezone.now().isoformat()
        }
        
        result = supabase.table("chat_messages").insert(message_data).execute()
        
        # Update conversation last message
        supabase.table("conversations")\
            .update({
                "last_message": message[:100],
                "last_message_at": timezone.now().isoformat()
            })\
            .eq("id", conversation_id)\
            .execute()
        
        # Trigger Pusher event - use the channel name that frontend is subscribed to
        channel_name = f"chat_{conversation_id}"
        
        # Get sender name for better display
        sender_name = f"User {sender_id}"
        try:
            sender_user = User.objects.get(id=sender_id)
            sender_name = sender_user.get_full_name() or sender_user.username
        except User.DoesNotExist:
            pass
        
        pusher_client.trigger(channel_name, 'new_message', {
            'id': result.data[0]['id'] if result.data else None,
            'message': message,
            'sender_id': sender_id,
            'sender_name': sender_name,
            'receiver_id': receiver_id,
            'created_at': message_data['created_at'],
            'is_read': False
        })
        
        return JsonResponse({
            "success": True,
            "message": message_data,
            "pusher_channel": channel_name
        })
        
    except Exception as e:
        print(f"Error in send_chat_message: {e}")
        import traceback
        traceback.print_exc()
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def get_conversations(request):
    """GET /api/chat/conversations/"""
    try:
        supabase = __get_supabase_client()
        user_id = request.user.id

        result = supabase.table("conversations") \
            .select("*, listings(id, title, images, source_name, source_id)") \
            .or_(f"buyer_id.eq.{user_id},responsible_id.eq.{user_id}") \
            .order("last_message_at", desc=True) \
            .execute()

        conversations = []
        for conv in result.data:
            listing = conv.get("listings") or {}
            is_buyer = conv["buyer_id"] == user_id
            other_party_id = conv["responsible_id"] if is_buyer else conv["buyer_id"]
            source_name = conv.get("source_name", "")

            # Resolve other party's display name
            if source_name == "user_submission" and is_buyer:
                other_party_name = "Property Owner"
            else:
                try:
                    other_user = User.objects.get(id=other_party_id)
                    other_party_name = other_user.get_full_name() or other_user.username
                except User.DoesNotExist:
                    other_party_name = f"User {other_party_id}"

            # Unread count
            try:
                unread_result = supabase.table("chat_messages") \
                    .select("id", count="exact") \
                    .eq("conversation_id", conv["id"]) \
                    .eq("receiver_id", user_id) \
                    .eq("is_read", False) \
                    .execute()
                unread_count = unread_result.count or 0
            except Exception:
                unread_count = 0

            # Safely get listing image
            images = listing.get("images") or []
            first_image = images[0] if images else None

            conversations.append({
                "id": conv["id"],
                "listing_id": conv["listing_id"],
                "listing_title": listing.get("title", "Unknown listing"),
                "listing_image": first_image,
                "other_party_name": other_party_name,
                "other_party_id": other_party_id,
                "is_buyer": is_buyer,
                "source_name": source_name,
                "last_message": conv.get("last_message", ""),
                "last_message_at": conv.get("last_message_at"),
                "unread_count": unread_count,
            })

        return JsonResponse({"conversations": conversations})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return JsonResponse({"error": str(e)}, status=500)

@login_required
def get_messages(request, conversation_id):
    """GET /api/chat/messages/<conversation_id>/ - Get all messages in a conversation"""
    try:
        supabase = __get_supabase_client()
        user_id = request.user.id
        
        # Verify user has access to this conversation
        conv_result = supabase.table("conversations")\
            .select("*")\
            .eq("id", conversation_id)\
            .execute()
        
        if not conv_result.data:
            return JsonResponse({"error": "Conversation not found"}, status=404)
        
        conversation = conv_result.data[0]
        if conversation["buyer_id"] != user_id and conversation["responsible_id"] != user_id:
            return JsonResponse({"error": "Unauthorized"}, status=403)
        
        # Get messages
        messages_result = supabase.table("chat_messages")\
            .select("*")\
            .eq("conversation_id", conversation_id)\
            .order("created_at", asc=True)\
            .execute()
        
        # Mark unread messages as read (only for current user as receiver)
        supabase.table("chat_messages")\
            .update({"is_read": True, "read_at": timezone.now().isoformat()})\
            .eq("conversation_id", conversation_id)\
            .eq("receiver_id", user_id)\
            .eq("is_read", False)\
            .execute()
        
        messages = []
        for msg in messages_result.data:
            messages.append({
                "id": msg["id"],
                "message": msg["message"],
                "sender_id": msg["sender_id"],
                "receiver_id": msg["receiver_id"],
                "is_read": msg["is_read"],
                "created_at": msg["created_at"],
                "is_mine": msg["sender_id"] == user_id
            })
        
        return JsonResponse({"messages": messages, "conversation": conversation})
        
    except Exception as e:
        print(f"Error in get_messages: {e}")
        import traceback
        traceback.print_exc()
        return JsonResponse({"error": str(e)}, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def pusher_auth(request):
    """Authenticate Pusher private channels"""
    try:
        import urllib.parse

        if 'application/x-www-form-urlencoded' in (request.content_type or ''):
            body = request.body.decode('utf-8')
            data = urllib.parse.parse_qs(body)
            socket_id    = data.get('socket_id',    [None])[0]
            channel_name = data.get('channel_name', [None])[0]
        else:
            data = json.loads(request.body.decode('utf-8'))
            socket_id    = data.get('socket_id')
            channel_name = data.get('channel_name')

        if not socket_id or not channel_name:
            return JsonResponse({"error": "Missing parameters"}, status=400)

        if not request.user.is_authenticated:
            return JsonResponse({"error": "Unauthorized"}, status=401)

        # ✅ For private channels, verify the user owns the channel
        if channel_name.startswith('private-user-'):
            channel_user_id = channel_name.replace('private-user-', '')
            if str(request.user.id) != channel_user_id:
                return JsonResponse({"error": "Forbidden"}, status=403)

        auth_response = pusher_client.authenticate(
            channel=channel_name,
            socket_id=socket_id,
        )
        return JsonResponse(auth_response)

    except Exception as e:
        import traceback
        traceback.print_exc()
        return JsonResponse({"error": str(e)}, status=500)
    
@login_required
def fraud_summary_api(request):
    """GET /api/fraud/summary/ — KPI cards for fraud detection dashboard."""
    try:
        from django.db import connection as db_conn
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*)                                                                  AS total,
                    SUM(CASE WHEN multimodal_score < 0.31 THEN 1 ELSE 0 END)                AS incoherent,
                    SUM(CASE WHEN multimodal_score >= 0.31 AND multimodal_score < 0.56
                             THEN 1 ELSE 0 END)                                             AS suspect,
                    SUM(CASE WHEN multimodal_score >= 0.56 THEN 1 ELSE 0 END)               AS coherent,
                    ROUND(AVG(multimodal_score)::numeric, 3)                                AS avg_score,
                    ROUND(AVG(ABS(price_deviation_pct))::numeric, 1)                        AS avg_price_deviation
                FROM fraud_detection_results
            """)
            row = cur.fetchone()
        total, incoherent, suspect, coherent, avg_score, avg_price_dev = row
        return JsonResponse({
            "total":              int(total or 0),
            "incoherent":         int(incoherent or 0),
            "suspect":            int(suspect or 0),
            "coherent":           int(coherent or 0),
            "avg_score":          float(avg_score or 0),
            "avg_price_deviation": float(avg_price_dev or 0),
        })
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def fraud_listings_api(request):
    """
    GET /api/fraud/listings/
    Params: page, page_size, risk (incoherent|suspect|coherent), region, flag
    Returns suspicious listings joined with listing details.
    """
    try:
        from django.db import connection as db_conn

        risk     = request.GET.get("risk", "").strip()
        region   = request.GET.get("region", "").strip()
        flag     = request.GET.get("flag", "").strip()
        try:
            page      = max(int(request.GET.get("page", 1)), 1)
            page_size = min(int(request.GET.get("page_size", 20)), 100)
        except ValueError:
            page, page_size = 1, 20
        offset = (page - 1) * page_size

        conditions, params = [], []
        if risk == "incoherent":
            conditions.append("f.multimodal_score < 0.31")
        elif risk == "suspect":
            conditions.append("f.multimodal_score >= 0.31 AND f.multimodal_score < 0.56")
        elif risk == "coherent":
            conditions.append("f.multimodal_score >= 0.56")
        if region:
            conditions.append("l.region ILIKE %s")
            params.append(f"%{region}%")
        if flag:
            conditions.append("f.mismatch_types::text ILIKE %s")
            params.append(f"%{flag}%")

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        base_join = """
            FROM fraud_detection_results f
            LEFT JOIN listings l
                   ON COALESCE(l.source_id, l.id::text) = f.property_id
        """

        with db_conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) {base_join} {where}", params)
            total = cur.fetchone()[0]

            cur.execute(f"""
                SELECT
                    f.property_id,
                    f.source_name,
                    f.multimodal_score,
                    f.image_text_similarity,
                    f.price_deviation_pct,
                    f.mismatch_types,
                    f.images_analyzed,
                    f.analyzed_at,
                    l.title,
                    l.price,
                    l.city,
                    l.region,
                    l.property_type,
                    l.url
                {base_join}
                {where}
                ORDER BY f.multimodal_score ASC
                LIMIT %s OFFSET %s
            """, params + [page_size, offset])
            rows = cur.fetchall()

        results = []
        for row in rows:
            (prop_id, src, score, sim, price_dev, mismatch,
             images, analyzed_at, title, price, city, reg, ptype, url) = row

            if isinstance(mismatch, str):
                try:
                    mismatch = json.loads(mismatch)
                except Exception:
                    mismatch = []
            elif mismatch is None:
                mismatch = []

            s = float(score or 0)
            risk_label = "incoherent" if s < 0.31 else ("suspect" if s < 0.56 else "coherent")

            results.append({
                "property_id":        prop_id,
                "source_name":        src or "",
                "multimodal_score":   round(s, 3),
                "risk_level":         risk_label,
                "price_deviation_pct": round(float(price_dev or 0), 1),
                "mismatch_types":     mismatch,
                "images_analyzed":    images or 0,
                "analyzed_at":        analyzed_at.isoformat() if analyzed_at else None,
                "title":              title or "",
                "price":              float(price) if price else None,
                "city":               city or "",
                "region":             reg or "",
                "property_type":      ptype or "",
                "url":                url or "",
            })

        return JsonResponse({
            "count":   int(total),
            "pages":   max(1, (int(total) + page_size - 1) // page_size),
            "page":    page,
            "results": results,
        })
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def fraud_flags_api(request):
    """GET /api/fraud/flags/ — distribution of mismatch flags for bar chart."""
    try:
        from django.db import connection as db_conn
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT mismatch_types
                FROM fraud_detection_results
                WHERE mismatch_types IS NOT NULL
            """)
            rows = cur.fetchall()

        flag_counts: dict = defaultdict(int)
        for (mismatch,) in rows:
            if isinstance(mismatch, list):
                flags = mismatch
            elif isinstance(mismatch, str):
                try:
                    flags = json.loads(mismatch)
                except Exception:
                    flags = []
            else:
                flags = []
            for f in flags:
                if f:
                    flag_counts[f] += 1

        sorted_flags = sorted(flag_counts.items(), key=lambda x: -x[1])[:12]
        return JsonResponse({
            "flags": [{"flag": f, "count": c} for f, c in sorted_flags]
        })
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)