"""
EstateMind Dashboard — Django views.

Step 1: Public listings API (GET /api/listings/, GET /api/listings/<id>/)
Step 2: Auth with UserProfile (register, login, logout, session)
        + existing EDA / metrics / quality endpoints → PostgreSQL via ORM
"""
import json
import re
from datetime import date, timedelta
from collections import defaultdict

from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.models import User
from django.contrib.auth.decorators import login_required
from django.db import transaction, OperationalError, ProgrammingError
from django.core.paginator import Paginator
from django.db.models import Count, Avg, Min, Max, Q
from django.db.models.functions import TruncDate
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from .models import Listing, AgentMetrics, UserProfile


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
        "fraud_flag":          False,
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
        flagged_count = Listing.objects.filter(fraud_flag=True).count()

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

        source_quality = [
            {"source": row["source_name"], "total": row["count"]}
            for row in qs.values("source_name").annotate(count=Count("id")).order_by("-count")
        ]
        dup_count = (
            qs.values("price", "city", "surface")
            .annotate(cnt=Count("id"))
            .filter(cnt__gt=1)
            .count()
        )

        return JsonResponse({
            "total":               total,
            "null_field_stats":    null_stats,
            "duplicate_count":     dup_count,
            "duplicate_pct":       round(dup_count / total * 100, 1),
            "source_quality":      source_quality,
            # Fields not yet in PG schema — kept for frontend compatibility
            "avg_reliability_score": 0,
            "score_distribution":  [],
            "nlp_enriched_count":  0,
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
            qs.exclude(type__isnull=True)
            .values("type").annotate(count=Count("id")).order_by("-count")
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
            {"region": row["region"], "avg_m2": round(row["avg_m2"] or 0, 2)}
            for row in (
                qs.exclude(region__isnull=True)
                .exclude(price__isnull=True).exclude(surface__isnull=True)
                .filter(price__gt=0, surface__gt=0)
                .values("region")
                .annotate(avg_m2=Avg("price") / Avg("surface"))
                .order_by("-avg_m2")
            )
        ]

        return JsonResponse({
            "region_stats":        region_stats,
            "price_stats":         price_stats,
            "transaction_stats":   transaction_stats,
            "property_type_stats": property_type_stats,
            "top_areas":           top_areas,
            "trend_stats":         trend_stats,
            "price_m2_stats":      price_m2_stats,
            "top_features":        [],
        })
    except Exception as e:
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