"""
backend/dashboard/similarity_views.py
=======================================
Listing similarity API — two complementary strategies:

1. Vector similarity   — uses text_embedding (384-dim) already in listings table
                         cosine distance via pgvector  <=>
2. Feature similarity  — structured fallback when embeddings are sparse:
                         same city + type, closest price, overlapping features

GET /api/listings/<pk>/similar/?strategy=auto&limit=6
  strategy: "vector" | "feature" | "auto"  (auto = vector if available, else feature)
  limit: 1–12 (default 6)

Response:
  { "strategy_used": "vector", "results": [ ...listing dicts... ] }
"""

from __future__ import annotations

import json
import os
from typing import Any

import psycopg2
import psycopg2.extras
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods

# ── DB connection (Supabase) ──────────────────────────────────────────────────

def _pg():
    return psycopg2.connect(
        host     = os.getenv("PG_HOST",     "localhost"),
        port     = int(os.getenv("PG_PORT", "5432")),
        dbname   = os.getenv("PG_NAME",     "postgres"),
        user     = os.getenv("PG_USER",     "postgres"),
        password = os.getenv("PG_PASSWORD", ""),
        sslmode  = os.getenv("PG_SSLMODE",  "require"),
    )


# ── Listing serialiser (reuse shape from main views) ─────────────────────────

def _row_to_dict(row: dict) -> dict:
    def f(v):
        return float(v) if v is not None else None

    def parse_list(val):
        if val is None: return []
        if isinstance(val, list): return val
        if isinstance(val, str):
            try: return json.loads(val)
            except Exception: return []
        return []

    def normalise_images(raw: list) -> list:
        out = []
        for item in raw:
            if isinstance(item, str) and item.startswith("http"):
                out.append({"url": item, "label": "photo"})
            elif isinstance(item, dict) and item.get("url"):
                out.append(item)
        return out

    images   = normalise_images(parse_list(row.get("images")))
    features = parse_list(row.get("features"))

    return {
        "id":                  row.get("id"),
        "source_name":         row.get("source_name", ""),
        "title":               row.get("title") or "",
        "description":         row.get("description"),
        "url":                 row.get("url"),
        "price":               f(row.get("price")),
        "currency":            row.get("currency") or "TND",
        "transaction_type":    row.get("transaction_type"),
        "type":                row.get("property_type"),
        "rooms":               row.get("rooms"),
        "city":                row.get("city"),
        "municipality":        row.get("municipality"),
        "zone":                row.get("zone"),
        "region":              row.get("region"),
        "surface":             f(row.get("surface")),
        "price_per_m2":        f(row.get("price_per_m2")),
        "latitude":            f(row.get("latitude")),
        "longitude":           f(row.get("longitude")),
        "features":            features,
        "images":              images,
        "images_count":        row.get("images_count") or len(images),
        #"fraud_flag":          bool(row.get("fraud_flag", False)),
        "fraud_score":         None,
        "fraud_reason":        None,
        "reliability_score":   f(row.get("reliability_score")),
        "reliability_level":   row.get("reliability_level"),
        "is_outlier":          bool(row.get("is_outlier", False)),
        "outlier_flags":       parse_list(row.get("outlier_flags")),
        "suspected_duplicate": bool(row.get("suspected_duplicate", False)),
        "change_type":         row.get("change_type"),
        "has_price_history":   bool(row.get("has_price_history", False)),
        "price_delta":         f(row.get("price_delta")),
        "price_delta_pct":     f(row.get("price_delta_pct")),
        "scraped_at":          row.get("scraped_at").isoformat() if row.get("scraped_at") and hasattr(row.get("scraped_at"), "isoformat") else None,
        "last_updated":        row.get("last_updated").isoformat() if row.get("last_updated") and hasattr(row.get("last_updated"), "isoformat") else None,
        "nlp_enriched":        bool(row.get("nlp_enriched", False)),
        "normalized":          bool(row.get("normalized", False)),
        "should_drop":         bool(row.get("should_drop", False)),
        # similarity score injected by the search
        "similarity_score":    f(row.get("similarity_score")),
        "similarity_reason":   row.get("similarity_reason", ""),
    }


# ── Strategy 1: vector similarity ─────────────────────────────────────────────

def _vector_similar(conn, listing_id: str, limit: int) -> list[dict] | None:
    """
    Cosine similarity on text_embedding.
    Returns None if the source listing has no embedding yet.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Fetch source embedding
        cur.execute(
            "SELECT text_embedding FROM listings WHERE id = %s",
            [listing_id],
        )
        row = cur.fetchone()
        if not row or row["text_embedding"] is None:
            return None

        src_vec = row["text_embedding"]   # already a list from psycopg2

        # Find similar listings excluding source, dropped, and flagged
        # cur.execute(
        #     """
        #     SELECT *,
        #            1 - (text_embedding <=> %s::vector) AS similarity_score,
        #            'vector' AS similarity_reason
        #     FROM listings
        #     WHERE id != %s
        #       AND text_embedding IS NOT NULL
        #       AND (should_drop IS FALSE OR should_drop IS NULL)
        #       AND (fraud_flag  IS FALSE OR fraud_flag  IS NULL)
        #     ORDER BY text_embedding <=> %s::vector
        #     LIMIT %s
        #     """,
        #     [src_vec, listing_id, src_vec, limit],
        # )
        cur.execute(
            """
            SELECT *,
                   1 - (text_embedding <=> %s::vector) AS similarity_score,
                   'vector' AS similarity_reason
            FROM listings
            WHERE id != %s
              AND text_embedding IS NOT NULL
              AND (should_drop IS FALSE OR should_drop IS NULL)
              
            ORDER BY text_embedding <=> %s::vector
            LIMIT %s
            """,
            [src_vec, listing_id, src_vec, limit],
        )
        return [dict(r) for r in cur.fetchall()]


# ── Strategy 2: feature similarity ────────────────────────────────────────────

def _feature_similar(conn, listing_id: str, limit: int) -> list[dict]:
    """
    Structured fallback: same city + property_type, closest price, bonus for
    overlapping features. Always returns results (even if few).
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Fetch source listing
        cur.execute(
            """
            SELECT city, property_type, price, surface, rooms, transaction_type,
                   features, region
            FROM listings WHERE id = %s
            """,
            [listing_id],
        )
        src = cur.fetchone()
        if not src:
            return []

        city      = src["city"]
        prop_type = src["property_type"]
        price     = float(src["price"] or 0)
        tx_type   = src["transaction_type"]
        region    = src["region"]

        # Primary: same city + type + transaction, ordered by price proximity
        # cur.execute(
        #     """
        #     SELECT *,
        #            ABS(COALESCE(price, 0) - %s) AS price_diff,
        #            'feature' AS similarity_reason,
        #            NULL::float AS similarity_score
        #     FROM listings
        #     WHERE id != %s
        #       AND (should_drop IS FALSE OR should_drop IS NULL)
        #       AND (fraud_flag  IS FALSE OR fraud_flag  IS NULL)
        #       AND LOWER(city)          = LOWER(%s)
        #       AND property_type        = %s
        #       AND transaction_type     = %s
        #     ORDER BY price_diff ASC NULLS LAST
        #     LIMIT %s
        #     """,
        #     [price, listing_id, city or "", prop_type or "", tx_type or "", limit * 2],
        # )
        cur.execute(
            """
            SELECT *,
                   ABS(COALESCE(price, 0) - %s) AS price_diff,
                   'feature' AS similarity_reason,
                   NULL::float AS similarity_score
            FROM listings
            WHERE id != %s
              AND (should_drop IS FALSE OR should_drop IS NULL)
              
              AND LOWER(city)          = LOWER(%s)
              AND property_type        = %s
              AND transaction_type     = %s
            ORDER BY price_diff ASC NULLS LAST
            LIMIT %s
            """,
            [price, listing_id, city or "", prop_type or "", tx_type or "", limit * 2],
        )
        rows = [dict(r) for r in cur.fetchall()]

        # If not enough results, widen to same region + type
        if len(rows) < limit:
            existing_ids = {r["id"] for r in rows} | {listing_id}
            placeholders = ",".join(["%s"] * len(existing_ids))
            # cur.execute(
            #     f"""
            #     SELECT *,
            #            ABS(COALESCE(price, 0) - %s) AS price_diff,
            #            'feature_region' AS similarity_reason,
            #            NULL::float AS similarity_score
            #     FROM listings
            #     WHERE id NOT IN ({placeholders})
            #       AND (should_drop IS FALSE OR should_drop IS NULL)
            #       AND (fraud_flag  IS FALSE OR fraud_flag  IS NULL)
            #       AND LOWER(region)    = LOWER(%s)
            #       AND property_type    = %s
            #       AND transaction_type = %s
            #     ORDER BY price_diff ASC NULLS LAST
            #     LIMIT %s
            #     """,
            #     [price, *existing_ids, region or "", prop_type or "", tx_type or "",
            #      limit - len(rows)],
            # )
            cur.execute(
                f"""
                SELECT *,
                       ABS(COALESCE(price, 0) - %s) AS price_diff,
                       'feature_region' AS similarity_reason,
                       NULL::float AS similarity_score
                FROM listings
                WHERE id NOT IN ({placeholders})
                  AND (should_drop IS FALSE OR should_drop IS NULL)
                  
                  AND LOWER(region)    = LOWER(%s)
                  AND property_type    = %s
                  AND transaction_type = %s
                ORDER BY price_diff ASC NULLS LAST
                LIMIT %s
                """,
                [price, *existing_ids, region or "", prop_type or "", tx_type or "",
                 limit - len(rows)],
            )
            rows += [dict(r) for r in cur.fetchall()]

        # Score: invert normalised price distance → 0.0–1.0 similarity
        if price > 0:
            for r in rows:
                diff = float(r.get("price_diff") or 0)
                r["similarity_score"] = max(0.0, 1.0 - diff / price)

        return rows[:limit]


# ── Public endpoint ────────────────────────────────────────────────────────────

@require_http_methods(["GET"])
def similar_listings(request, pk):
    """GET /api/listings/<pk>/similar/"""
    strategy = request.GET.get("strategy", "auto")   # auto | vector | feature
    try:
        limit = min(int(request.GET.get("limit", 6)), 12)
    except ValueError:
        limit = 6

    listing_id = str(pk)

    try:
        conn = _pg()

        used = strategy
        results: list[dict] = []

        if strategy in ("vector", "auto"):
            vec_results = _vector_similar(conn, listing_id, limit)
            if vec_results is not None:
                results = vec_results
                used = "vector"
            elif strategy == "auto":
                results = _feature_similar(conn, listing_id, limit)
                used = "feature"

        if strategy == "feature":
            results = _feature_similar(conn, listing_id, limit)
            used = "feature"

        conn.close()

        return JsonResponse({
            "strategy_used": used,
            "count":         len(results),
            "results":       [_row_to_dict(r) for r in results],
        })

    except Exception as e:
        return JsonResponse({"error": str(e), "results": []}, status=500)