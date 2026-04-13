"""
data/rag/engine.py
==================
Report generation engine.

Called by Django via subprocess or directly imported.
Takes a report type + params, retrieves relevant chunks,
builds a structured prompt, streams gemma3:4b response.

Usage (from Django view):
    from rag.engine import generate_report_stream
    for token in generate_report_stream("market", {"city": "Tunis"}):
        yield token
"""

from __future__ import annotations

import os
import json
import textwrap
from typing import Generator, Any

import psycopg2
from dotenv import load_dotenv

load_dotenv()

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL    = os.getenv("OLLAMA_REPORT_MODEL", "gemma3:4b")
EMBED_MODEL     = "nomic-embed-text"
TOP_K           = 8        # chunks to retrieve per report
MAX_TOKENS      = 2048

PG_CONN = dict(
    host     = os.getenv("SUPABASE_DB_HOST", "aws-1-eu-central-1.pooler.supabase.com"),
    port     = int(os.getenv("SUPABASE_DB_PORT", "5432")),
    dbname   = os.getenv("SUPABASE_DB_NAME", "postgres"),
    user     = os.getenv("SUPABASE_DB_USER", "postgres.amxnojlfczwffvtwutrb"),
    password = os.getenv("SUPABASE_DB_PASSWORD", ""),
    sslmode  = os.getenv("SUPABASE_DB_SSLMODE", "require"),
)


# ── Retrieval ──────────────────────────────────────────────────────────────────

def _embed_query(query: str) -> list[float]:
    from langchain_ollama import OllamaEmbeddings
    embedder = OllamaEmbeddings(model=EMBED_MODEL, base_url=OLLAMA_BASE_URL)
    return embedder.embed_query(query)


def retrieve_context(query: str, top_k: int = TOP_K,
                     tags: list[str] | None = None) -> list[dict]:
    """Retrieve the most relevant document chunks for a given query."""
    vec = _embed_query(query)
    
    # Build the query without using tags for now (simpler)
    # The tags column exists but we'll skip filtering to avoid the error
    sql = """
        SELECT label, content, source,
               1 - (embedding <=> %s::vector) AS similarity
        FROM report_documents
        ORDER BY embedding <=> %s::vector
        LIMIT %s
    """

    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [vec, vec, top_k])
            rows = cur.fetchall()

    return [
        {"label": r[0], "content": r[1], "source": r[2], "similarity": float(r[3])}
        for r in rows
    ]
def _format_context(chunks: list[dict]) -> str:
    parts = []
    for i, c in enumerate(chunks, 1):
        parts.append(
            f"[Document {i} — {c['label']} (relevance: {c['similarity']:.2f})]\n"
            f"{c['content']}"
        )
    return "\n\n---\n\n".join(parts)


# ── DB stats helpers ───────────────────────────────────────────────────────────

def _get_listing_stats(filters: dict) -> dict:
    """Pull structured stats from listings table for a given filter set."""
    wheres = ["should_drop IS NOT TRUE", "price IS NOT NULL", "price > 0"]
    params: list[Any] = []

    if filters.get("city"):
        wheres.append("LOWER(city) = LOWER(%s)")
        params.append(filters["city"])
    if filters.get("region"):
        wheres.append("LOWER(region) = LOWER(%s)")
        params.append(filters["region"])
    if filters.get("transaction_type"):
        wheres.append("transaction_type = %s")
        params.append(filters["transaction_type"])
    if filters.get("property_type"):
        wheres.append("property_type = %s")
        params.append(filters["property_type"])

    where_sql = " AND ".join(wheres)

    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                    COUNT(*)::int,
                    AVG(price)::numeric(14,0),
                    MIN(price)::numeric(14,0),
                    MAX(price)::numeric(14,0),
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)::numeric(14,0),
                    AVG(price_per_m2)::numeric(8,0),
                    AVG(surface)::numeric(8,0),
                    AVG(rooms)::numeric(4,1)
                FROM listings
                WHERE {where_sql}
            """, params)
            row = cur.fetchone()

            cur.execute(f"""
                SELECT city, COUNT(*) as cnt,
                       AVG(price)::numeric(14,0) as avg_price
                FROM listings
                WHERE {where_sql} AND city IS NOT NULL
                GROUP BY city ORDER BY cnt DESC LIMIT 5
            """, params)
            top_cities = cur.fetchall()

            cur.execute(f"""
                SELECT property_type, COUNT(*) as cnt
                FROM listings
                WHERE {where_sql} AND property_type IS NOT NULL
                GROUP BY property_type ORDER BY cnt DESC
            """, params)
            by_type = cur.fetchall()

    return {
        "count":        int(row[0] or 0),
        "avg_price":    int(row[1] or 0),
        "min_price":    int(row[2] or 0),
        "max_price":    int(row[3] or 0),
        "median_price": int(row[4] or 0),
        "avg_price_m2": int(row[5] or 0),
        "avg_surface":  float(row[6] or 0),
        "avg_rooms":    float(row[7] or 0),
        "top_cities":   [{"city": r[0], "count": r[1], "avg_price": int(r[2] or 0)} for r in top_cities],
        "by_type":      [{"type": r[0], "count": r[1]} for r in by_type],
    }


# ── Prompt templates ───────────────────────────────────────────────────────────

def _market_prompt(params: dict, context: str, stats: dict) -> str:
    city    = params.get("city", "Tunisia")
    tx_type = params.get("transaction_type", "sale and rent")
    return textwrap.dedent(f"""
        You are a senior real estate analyst specialising in the Tunisian property market.
        Write a professional Market Overview Report in English.

        ## Report Parameters
        - Geographic focus: {city}
        - Transaction type: {tx_type}
        - Date: April 2026

        ## Live Market Data (from EstateMind database)
        - Active listings analysed: {stats['count']:,}
        - Average price: {stats['avg_price']:,} TND
        - Median price: {stats['median_price']:,} TND
        - Price range: {stats['min_price']:,} – {stats['max_price']:,} TND
        - Average price per m²: {stats['avg_price_m2']:,} TND/m²
        - Average surface: {stats['avg_surface']:.0f} m²
        - Property type breakdown: {json.dumps(stats['by_type'])}

        ## Knowledge Base Context
        {context}

        ## Instructions
        Write a structured report with these sections:
        1. **Executive Summary** (3–4 sentences)
        2. **Market Overview** — current state, volume, demand drivers
        3. **Price Analysis** — current prices, trends vs historical index, by zone
        4. **Supply & Demand Dynamics** — listing volume, days on market signals
        5. **Investment Outlook** — rental yields, capital appreciation potential
        6. **Key Risks** — economic, regulatory, currency
        7. **Conclusion & Recommendations**

        Use specific numbers from the data provided. Be analytical, not promotional.
        Write in clear professional English. Do not invent data not present in the context.
    """).strip()


def _investment_prompt(params: dict, context: str, stats: dict) -> str:
    city       = params.get("city", "")
    prop_type  = params.get("property_type", "residential")
    budget_min = params.get("budget_min", 0)
    budget_max = params.get("budget_max", 5_000_000)
    tx_type    = params.get("transaction_type", "sale")

    city_lines = ""
    if stats["top_cities"]:
        city_lines = "\n".join(
            f"  - {r['city']}: {r['count']} listings, avg {r['avg_price']:,} TND"
            for r in stats["top_cities"]
        )

    return textwrap.dedent(f"""
        You are a real estate investment advisor specialising in Tunisia.
        Write a professional Investment Analysis Report in English.

        ## Investment Parameters
        - Target city/region: {city or 'Tunisia (all regions)'}
        - Property type: {prop_type}
        - Transaction type: {tx_type}
        - Budget range: {budget_min:,} – {budget_max:,} TND
        - Analysis date: April 2026

        ## Market Data for This Segment
        - Matching listings: {stats['count']:,}
        - Average price: {stats['avg_price']:,} TND
        - Median price: {stats['median_price']:,} TND
        - Average price/m²: {stats['avg_price_m2']:,} TND/m²
        - Average surface: {stats['avg_surface']:.0f} m²
        - Average rooms: {stats['avg_rooms']:.1f}
        - Top markets in this segment:
        {city_lines or '  (no city filter applied)'}

        ## Knowledge Base Context
        {context}

        ## Instructions
        Write a structured investment analysis with these sections:
        1. **Investment Summary** — is this a good investment? (direct answer)
        2. **Market Position** — how this segment compares to the broader market
        3. **Financial Analysis**
           - Estimated rental yield (gross and net)
           - Capital appreciation potential (1yr, 5yr scenarios)
           - Break-even analysis
           - Transaction costs (registration, notary, taxes)
        4. **Comparable Market Analysis** — what {stats['avg_price']:,} TND buys elsewhere in Tunisia
        5. **Risk Assessment**
           - Market risks (liquidity, oversupply)
           - Economic risks (inflation, currency)
           - Regulatory risks
        6. **Strategic Recommendations** — buy / wait / avoid, with reasoning

        Use the specific data provided. Be direct and analytical.
        Give concrete numbers for yields (e.g. "estimated 5.2% gross yield").
        Do not hedge every statement — make a clear recommendation.
    """).strip()


# ── Ollama streaming ───────────────────────────────────────────────────────────

def _stream_ollama(prompt: str) -> Generator[str, None, None]:
    import httpx

    payload = {
        "model":   OLLAMA_MODEL,
        "messages": [
            {
                "role":    "system",
                "content": (
                    "You are EstateMind's AI analyst. You write professional, "
                    "data-driven real estate reports for the Tunisian market. "
                    "Always ground your analysis in the data provided. "
                    "Never invent statistics. Be concise and direct."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        "stream":  True,
        "options": {
            "temperature":    0.3,    # low temp for analytical reports
            "num_predict":    MAX_TOKENS,
            "num_ctx":        8192,   # use gemma3's large context
        },
    }

    with httpx.stream(
        "POST",
        f"{OLLAMA_BASE_URL}/api/chat",
        json=payload,
        timeout=300,
    ) as resp:
        resp.raise_for_status()
        for line in resp.iter_lines():
            if not line:
                continue
            try:
                data = json.loads(line)
                token = data.get("message", {}).get("content", "")
                if token:
                    yield token
                if data.get("done"):
                    break
            except json.JSONDecodeError:
                continue


# ── Public API ─────────────────────────────────────────────────────────────────

def generate_report_stream(
    report_type: str,
    params: dict,
) -> Generator[str, None, None]:
    """
    Main entry point. Yields tokens as the LLM generates them.

    report_type: "market" | "investment"
    params: dict of filter/config options
    """

    if report_type == "market":
        query = (
            f"real estate market overview Tunisia "
            f"{params.get('city', '')} "
            f"{params.get('transaction_type', '')} "
            "prices trends investment 2025 2026"
        )
        stat_filters = {
            "city":             params.get("city"),
            "transaction_type": params.get("transaction_type"),
        }
        # Remove the tags parameter temporarily
        chunks  = retrieve_context(query, TOP_K)  # ← Removed tags=["market", "prices"]
        context = _format_context(chunks)
        stats   = _get_listing_stats(stat_filters)
        prompt  = _market_prompt(params, context, stats)

    elif report_type == "investment":
        city      = params.get("city", "")
        prop_type = params.get("property_type", "")
        query = (
            f"investment analysis Tunisia {city} {prop_type} "
            "ROI rental yield capital appreciation risk 2025 2026"
        )
        stat_filters = {
            "city":             params.get("city"),
            "region":           params.get("region"),
            "transaction_type": params.get("transaction_type", "sale"),
            "property_type":    params.get("property_type"),
        }
        # Remove the tags parameter temporarily
        chunks  = retrieve_context(query, TOP_K)  # ← Removed tags=["investment", "market"]
        context = _format_context(chunks)
        stats   = _get_listing_stats(stat_filters)
        prompt  = _investment_prompt(params, context, stats)

    else:
        yield f"Unknown report type: {report_type}"
        return

    yield from _stream_ollama(prompt)