"""
EstateMind Dashboard — Django views.
Reads from Pinecone. Adds quality scores, null stats, data quality endpoints.
"""
import json
from collections import defaultdict
from django.contrib.auth import authenticate, login, logout
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import login_required


def _get_pinecone_index():
    import os
    from pinecone import Pinecone
    api_key = os.getenv("PINECONE_API_KEY")
    if not api_key:
        raise RuntimeError("PINECONE_API_KEY not set")
    pc = Pinecone(api_key=api_key)
    return pc.Index(os.getenv("PINECONE_INDEX_NAME", "property-listings"))


def _fetch_all_metadata(index, top_k: int = 10000):
    try:
        stats = index.describe_index_stats()
        total = stats.total_vector_count
        if total == 0:
            return [], 0
        zero_vec = [0.0] * stats.dimension
        result = index.query(
            vector=zero_vec,
            top_k=min(top_k, total),
            include_metadata=True,
            include_values=False,
        )
        return [m.metadata for m in result.matches if m.metadata], total
    except Exception:
        return [], 0


@login_required
def dashboard(request):
    return render(request, "dashboard.html", {
        "total_listings": 0, "total_by_source": [], "latest_runs": [],
    })


@login_required
def metrics_api(request):
    try:
        index = _get_pinecone_index()
        records, total = _fetch_all_metadata(index)
    except Exception as e:
        return JsonResponse({
            "total_listings": 0, "latest_run": _empty_run(),
            "per_source": [], "recent_runs": [], "error": str(e),
        })

    source_counts = defaultdict(int)
    latest_scraped = {}
    error_count = 0

    for rec in records:
        src = rec.get("source_name", "unknown")
        source_counts[src] += 1
        scraped_at = rec.get("scraped_at", "")
        if scraped_at:
            if src not in latest_scraped or scraped_at > latest_scraped[src]:
                latest_scraped[src] = scraped_at
        if rec.get("outlier_flags") and len(rec.get("outlier_flags", [])) > 0:
            error_count += 1

    per_source = sorted(
        [{"source_name": k, "count": v} for k, v in source_counts.items()],
        key=lambda x: -x["count"],
    )

    recent_runs = [
        {
            "source_name": src,
            "fetched": count, "inserted": count,
            "updated": 0, "unchanged": 0, "errors": 0,
            "started_at": latest_scraped.get(src),
        }
        for src, count in sorted(
            source_counts.items(),
            key=lambda x: latest_scraped.get(x[0], ""),
            reverse=True,
        )
    ][:10]

    latest_src = recent_runs[0] if recent_runs else None

    return JsonResponse({
        "total_listings": total,
        "flagged_count": error_count,
        "latest_run": {
            "source_name":  latest_src["source_name"] if latest_src else None,
            "strategy":     "BALANCED",
            "fetched":      latest_src["fetched"] if latest_src else None,
            "inserted":     latest_src["inserted"] if latest_src else None,
            "updated":      0, "unchanged": 0, "errors": error_count,
            "started_at":   latest_src["started_at"] if latest_src else None,
            "finished_at":  latest_src["started_at"] if latest_src else None,
        },
        "per_source":  per_source,
        "recent_runs": recent_runs,
    })


@login_required
def data_quality_api(request):
    """
    /api/quality/ — powers quality dashboard section.
    Returns reliability score distribution, null field stats, outlier breakdown.
    """
    try:
        index = _get_pinecone_index()
        records, total = _fetch_all_metadata(index)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)

    if not records:
        return JsonResponse(_empty_quality())

    # ── Score distribution ────────────────────────────────────────────────────
    score_dist = {"HIGH": 0, "GOOD": 0, "LOW": 0, "DROP": 0, "UNKNOWN": 0}
    score_values = []
    for rec in records:
        lvl = rec.get("reliability_level")
        score = rec.get("reliability_score")
        if score is not None:
            try:
                score_f = float(score)
            except (TypeError, ValueError):
                score_f = None
        else:
            score_f = None

        if not lvl:
            if score_f is None:
                lvl = "UNKNOWN"
            elif score_f < 25:
                lvl = "DROP"
            elif score_f < 60:
                lvl = "LOW"
            elif score_f < 85:
                lvl = "GOOD"
            else:
                lvl = "HIGH"
        score_dist[lvl] = score_dist.get(lvl, 0) + 1
        if score_f is not None:
            score_values.append(score_f)

    avg_score = round(sum(score_values) / len(score_values), 1) if score_values else 0

    # ── Null field analysis ───────────────────────────────────────────────────
    key_fields = ["price", "surface", "rooms", "city", "region",
                  "municipalite", "latitude", "longitude",
                  "transaction_type", "type", "description", "features"]

    null_stats = []
    for field in key_fields:
        null_count = sum(1 for r in records if not r.get(field))
        null_stats.append({
            "field": field,
            "null_count": null_count,
            "filled_count": total - null_count,
            "null_pct": round(null_count / total * 100, 1) if total else 0,
            "filled_pct": round((total - null_count) / total * 100, 1) if total else 0,
        })
    null_stats.sort(key=lambda x: -x["null_pct"])

    # ── NLP enrichment stats ──────────────────────────────────────────────────
    nlp_enriched = sum(1 for r in records if r.get("nlp_enriched"))
    nlp_fields_filled = defaultdict(int)
    for rec in records:
        for f in (rec.get("nlp_filled_fields") or []):
            nlp_fields_filled[f] += 1

    # ── Outlier breakdown ─────────────────────────────────────────────────────
    outlier_count = sum(1 for r in records if r.get("is_outlier"))
    flag_counts = defaultdict(int)
    for rec in records:
        for flag in (rec.get("outlier_flags") or []):
            flag_counts[flag] += 1

    # ── Duplicate stats ───────────────────────────────────────────────────────
    dup_count = sum(1 for r in records if r.get("suspected_duplicate"))

    # ── Change detection stats ────────────────────────────────────────────────
    change_dist = defaultdict(int)
    for rec in records:
        ct = rec.get("change_type") or "unknown"
        change_dist[ct] += 1

    # ── Source quality breakdown ──────────────────────────────────────────────
    source_quality = defaultdict(lambda: {"total": 0, "high": 0, "good": 0, "low": 0, "drop": 0})
    for rec in records:
        src = rec.get("source_name", "unknown")
        lvl = (rec.get("reliability_level") or "UNKNOWN").lower()
        source_quality[src]["total"] += 1
        if lvl in source_quality[src]:
            source_quality[src][lvl] += 1

    source_quality_list = [
        {"source": src, **stats}
        for src, stats in sorted(source_quality.items(), key=lambda x: -x[1]["total"])
    ]

    return JsonResponse({
        "total": total,
        "avg_reliability_score": avg_score,
        "score_distribution": [
            {"level": k, "count": v, "pct": round(v / total * 100, 1) if total else 0}
            for k, v in score_dist.items() if v > 0
        ],
        "null_field_stats": null_stats,
        "nlp_enriched_count": nlp_enriched,
        "nlp_fields_filled": [
            {"field": k, "count": v}
            for k, v in sorted(nlp_fields_filled.items(), key=lambda x: -x[1])
        ],
        "outlier_count": outlier_count,
        "outlier_pct": round(outlier_count / total * 100, 1) if total else 0,
        "outlier_flag_breakdown": [
            {"flag": k, "count": v}
            for k, v in sorted(flag_counts.items(), key=lambda x: -x[1])
        ],
        "duplicate_count": dup_count,
        "duplicate_pct": round(dup_count / total * 100, 1) if total else 0,
        "change_distribution": [
            {"change_type": k, "count": v}
            for k, v in sorted(change_dist.items(), key=lambda x: -x[1])
        ],
        "source_quality": source_quality_list,
    })


@login_required
def eda_metrics(request):
    try:
        index = _get_pinecone_index()
        records, total = _fetch_all_metadata(index)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)

    if not records:
        return JsonResponse(_empty_eda())

    region_counts    = defaultdict(int)
    region_prices    = defaultdict(list)
    transaction_counts = defaultdict(int)
    type_counts      = defaultdict(int)
    city_counts      = defaultdict(int)
    date_counts      = defaultdict(int)
    region_price_m2  = defaultdict(list)
    feature_counts   = defaultdict(int)

    for rec in records:
        region    = rec.get("region") or "Unknown"
        price     = rec.get("price")
        surface   = rec.get("surface")
        tx_type   = rec.get("transaction_type") or "Unknown"
        prop_type = rec.get("type") or "Unknown"
        city      = rec.get("city") or rec.get("municipalite") or "Unknown"
        scraped_at = rec.get("scraped_at", "")

        region_counts[region] += 1

        if price and price > 0:
            region_prices[region].append(price)

        transaction_counts[tx_type] += 1
        type_counts[prop_type] += 1

        if city and city != "Unknown":
            city_counts[city] += 1

        if scraped_at:
            try:
                date_counts[scraped_at[:10]] += 1
            except Exception:
                pass

        if price and price > 0 and surface and surface > 0:
            region_price_m2[region].append(price / surface)

        # Feature counting
        for feat in (rec.get("features") or []):
            if isinstance(feat, str):
                feature_counts[feat.lower()] += 1

    region_stats = sorted(
        [{"region": k, "count": v} for k, v in region_counts.items()],
        key=lambda x: -x["count"],
    )

    price_stats = sorted([
        {
            "region": r,
            "min_price": min(prices),
            "max_price": max(prices),
            "avg_price": sum(prices) / len(prices),
        }
        for r, prices in region_prices.items() if prices
    ], key=lambda x: x["region"])

    transaction_stats = sorted(
        [{"transaction_type": k, "count": v} for k, v in transaction_counts.items()],
        key=lambda x: -x["count"],
    )

    property_type_stats = sorted(
        [{"type": k, "count": v} for k, v in type_counts.items()],
        key=lambda x: -x["count"],
    )

    top_areas = sorted(
        [{"city": k, "count": v} for k, v in city_counts.items()],
        key=lambda x: -x["count"],
    )[:10]

    trend_stats = sorted(
        [{"date": k, "count": v} for k, v in date_counts.items()],
        key=lambda x: x["date"],
    )

    price_m2_stats = sorted([
        {"region": r, "avg_m2": sum(vals) / len(vals)}
        for r, vals in region_price_m2.items() if vals
    ], key=lambda x: -x["avg_m2"])

    top_features = sorted(
        [{"feature": k, "count": v} for k, v in feature_counts.items()],
        key=lambda x: -x["count"],
    )[:15]

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


# ── Auth ──────────────────────────────────────────────────────────────────────

@csrf_exempt
def api_login(request):
    if request.method != "POST":
        return JsonResponse({"detail": "Method not allowed"}, status=405)
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception:
        return JsonResponse({"detail": "Invalid JSON"}, status=400)
    username = payload.get("username")
    password = payload.get("password")
    if not username or not password:
        return JsonResponse({"detail": "Username and password required"}, status=400)
    user = authenticate(request, username=username, password=password)
    if user is None:
        return JsonResponse({"detail": "Invalid credentials"}, status=400)
    login(request, user)
    return JsonResponse({"detail": "ok"})


@csrf_exempt
def api_logout(request):
    if request.method != "POST":
        return JsonResponse({"detail": "Method not allowed"}, status=405)
    logout(request)
    return JsonResponse({"detail": "ok"})


@login_required
def api_session(request):
    user = request.user
    return JsonResponse({
        "is_authenticated": user.is_authenticated,
        "username": user.username,
        "is_superuser": user.is_superuser,
        "last_login": user.last_login.isoformat() if user.last_login else None,
    })


# ── Helpers ───────────────────────────────────────────────────────────────────

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
