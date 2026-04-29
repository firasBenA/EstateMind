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

import uuid
from django.utils import timezone
from sentence_transformers import SentenceTransformer

try:
    from data.preprocessing.steps.scorer import compute_score
except ImportError:
    compute_score = None

from .models import Listing # Make sure Listing is imported

from models.prediction_models.predictor import get_predictor



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
        # fraud_flag / fraud_score / fraud_reason don't exist in the table
        #"fraud_flag":          False,
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
        qs = qs.filter(property_type=prop_type)   # ← property_type, not type

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

    # fraud_flag column doesn't exist — skip filter silently
    # (no fraud data in this schema)

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
        # Use Django ORM directly (no need for get_db_connection)
        qs = Listing.objects.filter(Q(should_drop=False) | Q(should_drop__isnull=True))
        
        # Get unique cities
        cities = sorted(
            qs.exclude(city__isnull=True)
              .exclude(city__exact='')
              .values_list("city", flat=True)
              .distinct()
        )
        
        # Get price statistics using Django ORM
        price_stats = qs.exclude(price__isnull=True).filter(price__gt=0).aggregate(
            min_price=Min("price"),
            max_price=Max("price"),
            avg_price=Avg("price"),
        )
        
        # Get median price using raw SQL
        from django.db import connection
        with connection.cursor() as cur:
            cur.execute("""
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)
                FROM listings
                WHERE price IS NOT NULL AND price > 0 
                AND (should_drop = FALSE OR should_drop IS NULL)
            """)
            median_price = cur.fetchone()[0]
        
        # Get property types
        property_types = sorted(
            qs.exclude(property_type__isnull=True)
              .exclude(property_type__exact='')
              .values_list("property_type", flat=True)
              .distinct()
        )
        
        # Get transaction types
        transaction_types = sorted(
            qs.exclude(transaction_type__isnull=True)
              .exclude(transaction_type__exact='')
              .values_list("transaction_type", flat=True)
              .distinct()
        )
        
        # Get regions
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
        import traceback
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

    # Age ≥ 18
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

    # ── Check migrations have been run first ──────────────────────────────────
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

    # Build a unique username from email
    username_base = email.split("@")[0]
    username      = username_base
    suffix        = 1
    while User.objects.filter(username=username).exists():
        username = f"{username_base}{suffix}"
        suffix  += 1

    # ── Atomic: both User + UserProfile must succeed or both roll back ────────
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

    # Look up user by email
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

@login_required
def dashboard(request):
    return render(request, "dashboard.html", {
        "total_listings": 0, "total_by_source": [], "latest_runs": [],
    })


@login_required
def metrics_api(request):
    try:
        total = Listing.objects.count()

        per_source = list(
            Listing.objects
            .values("source_name")
            .annotate(count=Count("id"))
            .order_by("-count")
        )

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

        latest_run   = recent_runs[0] if recent_runs else _empty_run()
        flagged_count = Listing.objects.filter(is_outlier=True).count()

        return JsonResponse({
            "total_listings": total,
            "flagged_count":  flagged_count,
            "latest_run":     latest_run,
            "per_source":     per_source,
            "recent_runs":    recent_runs,
        })

    except Exception as e:
        return JsonResponse({
            "total_listings": 0, "latest_run": _empty_run(),
            "per_source": [], "recent_runs": [], "error": str(e),
        })


@login_required
def data_quality_api(request):
    try:
        qs    = Listing.objects
        total = qs.count()
        if total == 0:
            return JsonResponse(_empty_quality())

        key_fields = ["price", "surface", "rooms", "city", "region",
                      "municipalite", "latitude", "longitude",
                      "transaction_type", "type"]
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

        # Reliability Score Calculation
        avg_score = round(float(qs.aggregate(Avg("reliability_score"))["reliability_score__avg"] or 0), 1)
        
        # Score distribution grouping
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

        # Source quality breakdown
        source_quality = []
        sources = qs.values("source_name").annotate(total=Count("id")).order_by("-total")
        for src in sources:
            name = src["source_name"]
            stotal = src["total"]
            # Get grades for this source
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

        return JsonResponse({
            "total":               total,
            "null_field_stats":    null_stats,
            "duplicate_count":     dup_count,
            "duplicate_pct":       round(dup_count / total * 100, 1),
            "source_quality":      source_quality,
            "avg_reliability_score": avg_score,
            "score_distribution":  score_distribution,
            "nlp_enriched_count":  qs.filter(nlp_enriched=True).count(),
            "nlp_fields_filled":   [], # Placeholder for field-level NLP stats
            "outlier_count":       qs.filter(is_outlier=True).count(),
            "outlier_pct":         round(qs.filter(is_outlier=True).count() / total * 100, 1),
            "outlier_flag_breakdown": [], # Placeholder
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

        # Extract features (JSON field)
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

import json
import uuid
import logging
import traceback
import os
import requests
import math
from django.utils import timezone
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from supabase import create_client, Client

logger = logging.getLogger(__name__)

# Initialize Supabase client ONCE at module level (lazy load)
_supabase_client: Client = None
def _get_supabase_client() -> Client:
    global _supabase_client
    if _supabase_client is None:
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        
        # 🔍 DEBUG LOGGING
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"🔑 SUPABASE_URL loaded: {'✅' if supabase_url else '❌'}")
        logger.info(f"🔑 SUPABASE_SERVICE_ROLE_KEY loaded: {'✅' if supabase_key else '❌'}")
        if supabase_key:
            logger.info(f"🔑 Key preview: {supabase_key[:30]}...")  # Show first 30 chars
        
        if not supabase_key:
            raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY not set in environment. Check backend/.env")
        
        _supabase_client = create_client(supabase_url, supabase_key)
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
    """
    Fetches nearby POIs from OpenStreetMap using Overpass API.
    HEAVILY LOGGED for debugging.
    """
    logger.info(f"🗺️ STARTING POI FETCH: Lat={lat}, Lon={lon}, Radius={radius_m}m")
    
    if not lat or not lon:
        logger.warning("❌ No coordinates provided for POI fetch.")
        return []

    # Overpass QL Query
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
        logger.info("📡 Sending request to Overpass API...")
        response = requests.post(
            "https://overpass-api.de/api/interpreter",
            data=query.encode('utf-8'),
            headers={'User-Agent': 'EstateMind/1.0'},
            timeout=15
        )
        
        logger.info(f"📡 Overpass Response Status: {response.status_code}")
        
        if response.status_code != 200:
            logger.error(f"❌ Overpass API Error: {response.status_code} - {response.text[:200]}")
            return []

        data = response.json()
        elements = data.get('elements', [])
        logger.info(f"📦 Raw Elements Received: {len(elements)}")
        
        pois = []
        seen_names = set()

        for i, element in enumerate(elements):
            tags = element.get('tags', {})
            name = tags.get('name')
            
            # Debug first few elements
            if i < 3:
                logger.debug(f"   Element {i}: Tags={tags}, Name={name}")

            if not name:
                continue

            clean_name = name.strip()
            
            # Avoid duplicates
            if clean_name.lower() in seen_names:
                continue
            
            # Verify distance (Overpass 'around' can be loose for Ways)
            el_lat = element.get('lat') or element.get('center', {}).get('lat')
            el_lon = element.get('lon') or element.get('center', {}).get('lon')
            
            if el_lat and el_lon:
                dist = _haversine_distance(lat, lon, el_lat, el_lon)
                if dist <= radius_m:
                    seen_names.add(clean_name.lower())
                    pois.append(clean_name)
                    logger.info(f"   ✅ Added POI: '{clean_name}' (Dist: {int(dist)}m)")
                    
                    if len(pois) >= 10:
                        break

        logger.info(f"🏁 POI FETCH COMPLETE: Found {len(pois)} unique POIs.")
        return pois

    except requests.Timeout:
        logger.error("❌ Overpass API Timed Out")
        return []
    except Exception as e:
        logger.error(f"❌ Exception in POI fetch: {type(e).__name__}: {e}", exc_info=True)
        return []

from sentence_transformers import SentenceTransformer # For image embeddings
    
# Load Image Embedding Model (CLIP) once at startup
_image_model = None
def _get_image_model():
    global _image_model
    if _image_model is None:
        logger.info("Loading CLIP model for image embeddings...")
        # Using 'clip-ViT-B-32' which outputs 512-dim vectors, matching your schema
        _image_model = SentenceTransformer('clip-ViT-B-32')
    return _image_model

def _generate_image_embedding(image_url: str):
    """
    Downloads an image from URL and generates its 512-dim embedding using CLIP.
    """
    try:
        model = _get_image_model()
        
        # Download image content
        response = requests.get(image_url, timeout=10)
        response.raise_for_status()
        
        # Generate embedding
        # Note: SentenceTransformers can handle URLs directly or PIL images
        embedding = model.encode([image_url], convert_to_numpy=True)[0]
        
        return embedding.tolist()
    except Exception as e:
        logger.error(f"Failed to generate embedding for {image_url}: {e}")
        return None
# backend/dashboard/views.py

import json
import uuid
import logging
import traceback
import os
import requests
import math
from django.utils import timezone
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from supabase import create_client, Client
from sentence_transformers import SentenceTransformer # For Image Embeddings

logger = logging.getLogger(__name__)

# Initialize Supabase client
_supabase_client: Client = None

def _get_supabase_client() -> Client:
    global _supabase_client
    if _supabase_client is None:
        supabase_url = os.environ.get("SUPABASE_URL", "https://amxnojlfczwffvtwutrb.supabase.co")
        supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        if not supabase_key:
            raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY not set")
        _supabase_client = create_client(supabase_url, supabase_key)
    return _supabase_client

# Load Image Embedding Model (CLIP) once at module level for efficiency
_image_model = None
def _get_image_model():
    global _image_model
    if _image_model is None:
        logger.info("🧠 Loading CLIP model for image embeddings...")
        # 'clip-ViT-B-32' produces 512-dim vectors, matching your schema
        _image_model = SentenceTransformer('clip-ViT-B-32')
    return _image_model

def _generate_image_embedding(image_url: str):
    """
    Downloads an image from URL and generates its 512-dim embedding using CLIP.
    Returns list of floats or None if failed.
    """
    try:
        model = _get_image_model()
        
        # SentenceTransformers can handle URLs directly, but downloading ensures stability
        # Note: If images are private/signed, you might need to download bytes instead.
        # For public Supabase URLs, this works fine.
        embedding = model.encode([image_url], convert_to_numpy=True)[0]
        
        return embedding.tolist()
    except Exception as e:
        logger.error(f"Failed to generate embedding for {image_url}: {e}")
        return None

@require_http_methods(["POST"])
def create_listing(request):
    """
    POST /api/listings/create/
    Creates a user-submitted listing with:
    1. Automatic POI extraction
    2. Reliability Scoring
    3. Image Embeddings
    4. AI Price Prediction
    """
    logger.info(f"🔥 CREATE_LISTING CALLED")
    
    try:
        data = json.loads(request.body.decode('utf-8'))
        
        # --- Validation ---
        title = data.get("title", "").strip()
        city = data.get("city", "").strip()
        if not title or len(title) < 5:
            return JsonResponse({"error": "Title must be at least 5 characters"}, status=400)
        if not city:
            return JsonResponse({"error": "City is required"}, status=400)

        # --- Extract Coordinates ---
        latitude = data.get("latitude")
        longitude = data.get("longitude")
        
        logger.info(f"📍 Received Coords: Lat={latitude}, Lng={longitude}")
        
        # --- Automatic POI Extraction ---
        extracted_pois = []
        if latitude and longitude:
            try:
                lat_float = float(latitude)
                lon_float = float(longitude)
                
                if lat_float != 0.0 and lon_float != 0.0:
                    logger.info("🚀 Triggering POI Fetch...")
                    extracted_pois = _fetch_nearby_pois(lat_float, lon_float, radius_m=1000)
                    logger.info(f"📋 Found {len(extracted_pois)} POIs")
                    
            except (ValueError, TypeError) as e:
                logger.error(f"❌ Coord conversion error: {e}")

        # --- Prepare Listing Data for Scoring & Saving ---
        listing_id = str(uuid.uuid4())
        text_embedding = [0.0] * 384 # Placeholder
        
        price = float(data.get("price", 0))
        surface = float(data.get("surface", 0))
        price_per_m2 = round(price / surface, 2) if price and surface and surface > 0 else None
        image_urls = data.get("images", [])
        
        # Prepare flags for Reliability Scorer
        flags = {
            "price_outlier": False, 
            "suspected_duplicate": False,
            "nlp_enriched": False,
            "has_price_history": False,
            "price_changed": False,
            "cross_verified": False
        }
        
        # Create a temporary dict for scoring (needs to match scorer expectations)
        temp_meta = {
            "price": price,
            "surface": surface,
            "rooms": int(data.get("rooms", 0)),
            "city": city,
            "governorate": data.get("region"), # Map region to governorate if needed
            "latitude": latitude,
            "longitude": longitude,
            "description": data.get("description", ""),
            "image_count": len(image_urls),
            "features": data.get("features", []),
            "municipality": data.get("municipality"),
            "is_outlier": False,
        }

        # Calculate Reliability Score
        score_result = compute_score(temp_meta, flags)
        
        # Final Listing Data Dict
        listing_data = {
            "id": listing_id,
            "source_name": "user_submission",
            "title": title,
            "description": data.get("description", "").strip(),
            "price": price,
            "currency": "TND",
            "transaction_type": data.get("transaction", "sale"),
            "property_type": data.get("type", "apartment"),
            "rooms": int(data.get("rooms", 0)),
            "city": city,
            "surface": surface,
            "price_per_m2": price_per_m2,
            
            # Location
            "latitude": float(latitude) if latitude else None,
            "longitude": float(longitude) if longitude else None,
            
            # POIs & Images
            "poi": extracted_pois, 
            "images": image_urls,
            "images_count": len(image_urls),
            
            # Flags & Metadata
            "features": data.get("features", []),
            
            # ✅ Use Calculated Scores
            "reliability_score": score_result["score"],
            "reliability_level": score_result["level"],
            "should_drop": score_result["should_drop"],
            
            "is_outlier": False,
            "normalized": True,
            "nlp_enriched": False,
            "text_embedding": text_embedding,
            
            # Timestamps
            "scraped_at": timezone.now().isoformat(),
            "last_updated": timezone.now().isoformat(),
            "created_at": timezone.now().isoformat(),
        }
        
        logger.info(f"📊 Reliability Score: {score_result['score']} ({score_result['level']})")

        # --- 1. Insert Listing into Supabase ---
        logger.info("💾 Saving Listing to Supabase...")
        supabase = _get_supabase_client()
        result = supabase.table("listings").insert(listing_data).execute()
        
        if getattr(result, "error", None):
            raise Exception(f"Supabase listing insert failed: {result.error}")
            
        logger.info(f"✅ Listing Saved! ID: {listing_id}")

        # --- 2. Generate & Save Image Embeddings ---
        if image_urls:
            logger.info(f"️ Generating embeddings for {len(image_urls)} images...")
            embeddings_to_insert = []
            for index, img_url in enumerate(image_urls):
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
                if getattr(emb_result, "error", None):
                    logger.warning("⚠️ Failed to save some image embeddings")
                else:
                    logger.info(f"✅ Saved {len(embeddings_to_insert)} image embeddings")

        # --- 3. 🆕 AI Price Prediction ---
        predicted_price_data = None
        try:
            predictor = get_predictor()
            
            prediction = predictor.predict(
                transaction_type=data.get("transaction", "sale"),
                property_type=data.get("type", "apartment"),
                city=city,
                surface=surface,
                rooms=int(data.get("rooms", 0)),
                region=data.get("region", "unknown"),
                reliability_score=score_result["score"],
                reliability_level=score_result["level"],
                model_weight=1.0,
                is_outlier=False,
                suspected_duplicate=False,
                images_count=len(image_urls),
                has_description=1 if data.get("description") else 0,
                desc_length=len(data.get("description", "")),
                has_coords=1 if latitude and longitude else 0
            )
            
            predicted_price_data = prediction
            logger.info(f"💰 Predicted Price: {prediction['predicted_price']} TND")
            
            # Optional: Update the listing in Supabase with the predicted price
            # Uncomment if you added these columns to your DB
            # supabase.table("listings").update({
            #     "predicted_price": prediction['predicted_price'],
            #     "price_range_low": prediction['price_low'],
            #     "price_range_high": prediction['price_high']
            # }).eq("id", listing_id).execute()
            
        except Exception as e:
            logger.error(f"❌ Price Prediction Failed: {e}")
            # Don't fail the whole request if prediction fails

        return JsonResponse({
            "success": True,
            "listing_id": listing_id,
            "pois_found": len(extracted_pois),
            "reliability_score": score_result["score"],
            "reliability_level": score_result["level"],
            "predicted_price": predicted_price_data['predicted_price'] if predicted_price_data else None,
            "price_range": {
                "low": predicted_price_data['price_low'] if predicted_price_data else None,
                "high": predicted_price_data['price_high'] if predicted_price_data else None,
            },
            "message": "Listing published successfully!"
        }, status=201)
        
    except Exception as e:
        logger.error(f"❌ CRITICAL ERROR: {e}", exc_info=True)
        return JsonResponse({"error": str(e)}, status=500)
# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _empty_run():
    return {
        "source_name": None, "strategy": None, "fetched": None,
        "inserted": None, "updated": None, "unchanged": None,
        "errors": None, "started_at": None, "finished_at": None,
    }


def _empty_eda():
    return {
        "region_stats": [], "price_stats": [], "transaction_stats": [],
        "property_type_stats": [], "top_areas": [], "trend_stats": [],
        "price_m2_stats": [], "top_features": [],
    }


def _empty_quality():
    return {
        "total": 0, "avg_reliability_score": 0,
        "score_distribution": [], "null_field_stats": [],
        "nlp_enriched_count": 0, "nlp_fields_filled": [],
        "outlier_count": 0, "outlier_pct": 0,
        "outlier_flag_breakdown": [], "duplicate_count": 0,
        "duplicate_pct": 0, "change_distribution": [],
        "source_quality": [],
    }