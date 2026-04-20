"""
data/rag/ingest.py
==================
Phase 1 — Build the RAG knowledge base for EstateMind reports.

What this does:
  1. Scrapes free Tunisia real estate sources (jarniascyril.com, ins.tn)
  2. Generates a live stats document from your own Supabase listings table
  3. Loads any local PDF/TXT files you drop in data/rag/documents/
  4. Chunks everything with LangChain's RecursiveCharacterTextSplitter
  5. Embeds with nomic-embed-text via Ollama (dedicated embedding model)
  6. Upserts into a `report_documents` table in Supabase using pgvector

Run once (or re-run to refresh):
  cd data
  pip install langchain langchain-community langchain-ollama psycopg2-binary
       beautifulsoup4 httpx pypdf loguru
  python rag/ingest.py

Requirements:
  - Ollama running locally with: ollama pull nomic-embed-text
  - .env with PG_HOST, PG_USER, PG_PASSWORD, PG_NAME, PG_PORT
"""

from __future__ import annotations

import os
import sys
import time
import hashlib
import textwrap
from pathlib import Path
from typing import Generator

import httpx
import psycopg2
from bs4 import BeautifulSoup
from loguru import logger
from dotenv import load_dotenv

# LangChain
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_ollama import OllamaEmbeddings

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────

OLLAMA_BASE_URL  = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
EMBED_MODEL      = "nomic-embed-text"          # 768-dim, free, runs locally
CHUNK_SIZE       = 600                          # chars per chunk
CHUNK_OVERLAP    = 80
DOCS_DIR         = Path(__file__).parent / "documents"   # drop PDFs here

# PG_CONN = dict(
#     host     = os.getenv("PG_HOST",     "localhost"),
#     port     = int(os.getenv("PG_PORT", "5432")),
#     dbname   = os.getenv("PG_NAME",     "postgres"),
#     user     = os.getenv("PG_USER",     "postgres"),
#     password = os.getenv("PG_PASSWORD", ""),
#     sslmode  = os.getenv("PG_SSLMODE",  "require"),
# )

PG_CONN = dict(
    host     = os.getenv("SUPABASE_DB_HOST", "aws-1-eu-central-1.pooler.supabase.com"),
    port     = int(os.getenv("SUPABASE_DB_PORT", "5432")),
    dbname   = os.getenv("SUPABASE_DB_NAME", "postgres"),
    user     = os.getenv("SUPABASE_DB_USER", "postgres.amxnojlfczwffvtwutrb"),
    password = os.getenv("SUPABASE_DB_PASSWORD", ""),
    sslmode  = os.getenv("SUPABASE_DB_SSLMODE", "require"),
)

# ── Sources to scrape ──────────────────────────────────────────────────────────

WEB_SOURCES = [
    {
        "url":   "https://www.jarniascyril.com/international-real-estate/"
                 "invest-in-real-estate-tunisia-opportunities-rules-returns/"
                 "tunisia-real-estate-market-trends/",
        "label": "Tunisia Real Estate Market Trends 2026 — JarniasCyril",
        "tags":  ["market", "trends", "investment", "2026"],
    },
    {
        "url":   "https://www.ins.tn/en/statistiques/45",
        "label": "INS Tunisia — Statistics by Topic",
        "tags":  ["statistics", "official", "ins"],
    },
    {
        "url":   "https://www.ins.tn/en/communique-de-presse",
        "label": "INS Tunisia — Press Releases (GDP, real estate index)",
        "tags":  ["official", "gdp", "price-index"],
    },
     {
        "url":   "https://maitre-haifaguedhami.me/actualites/property-law-foreigners-tunisia-2026",
        "label": "Property Law for Foreigners in Tunisia — Maitre Haifa Guedhami (April 2026)",
        "tags":  ["legal", "foreign-ownership", "2026","tunisian-law"],
    },
     {
        "url":   "https://www.jarniascyril.com/international-real-estate/invest-in-real-estate-tunisia-opportunities-rules-returns/lois-regulations-immobilieres-tunisie/",
        "label": "Real Estate Laws and Regulations in Tunisia — JarniasCyril (2026 update)",
        "tags":  ["legal", "regulations", "2026","tunisian-law"],
    },
     {
        "url":   "https://spinellimechri.com/en-US/notizie/acheter-en-tunisie-le-guide-que-tout-investisseur-etranger-reverait-davoir-avant-de-signer",
        "label": "Buying Property in Tunisia: Guide for Foreign Investors — Spinelli Mechri (2026)",
        "tags":  ["investment", "foreign-investors", "2026","legal","tunisian-law"],
    },
]

# Manually curated key facts extracted from sources that block scraping
# (TradingEconomics housing index data — visible without login)
MANUAL_FACTS = """
# Tunisia Real Estate & Economic Key Facts (curated April 2026)

## Housing Price Index (INS / Trading Economics)
- Tunisia Housing Index reached 147.60 points in Q4 2021 (latest published)
- All-time high: 148.80 points in Q2 2021
- Historical average (2000–2021): 77.82 points
- Projected Q2 2026 estimate: ~156 points (Trading Economics model)
- Projected 2027: ~162 points; 2028: ~170 points
- Source: National Institute of Statistics Tunisia (ins.tn)

## Macroeconomic Context (INS, Q1 2026)
- GDP growth Q4 2025: +2.7% (seasonally adjusted, annual rate)
- GDP growth Q3 2025: +2.4%
- GDP growth Q2 2025: +3.2%
- GDP growth Q1 2025: +1.6%
- Inflation rate: 5.0% (March 2026)
- Unemployment rate: 15.2% (Q4 2025)
- Population: 11,972,169 (2024 Census)
- Trade balance: -2,448.5 MD (March 2026)

## Real Estate Market Overview
- Tunisia real estate market driven by coastal tourism zones and urban expansion
- Grand Tunis (Tunis, Ariana, Ben Arous, Manouba) concentrates highest prices
- La Marsa, Gammarth, Les Berges du Lac: premium residential zones
- Sousse and Monastir: strong rental market from tourism
- Sfax: largest city in south, industrial + commercial real estate hub
- Interior regions: significantly lower prices, lower liquidity

## Typical Price Ranges (TND, 2025–2026)
- Apartments in Tunis (sale): 800,000–3,500,000 TND depending on zone
- Villas in La Marsa / Gammarth: 1,500,000–8,000,000 TND
- Apartments Sousse coastal: 400,000–1,200,000 TND
- Land (constructible) Grand Tunis: 400–2,500 TND/m²
- Rental studios Tunis centre: 800–1,500 TND/month
- Rental apartments S+2 Ariana: 1,200–2,200 TND/month
- Commercial premises Tunis: 3,000–10,000 TND/month

## Investment Considerations
- Foreign nationals can purchase property in Tunisia (subject to BCT authorization)
- Registration fees (droits d'enregistrement): ~3% of purchase price
- Notary fees: ~1–2% of purchase price
- Capital gains tax on real estate: 10% (individuals), 25% (companies)
- Rental yield estimates: 4–7% gross in urban areas
- Strong expat demand in coastal areas (European buyers)
- Currency risk: TND not freely convertible (repatriation requires BCT approval)

## Mortgage Market
- BCT key rate: ~8% (2025)
- Average mortgage rate: 10–13% for individuals
- Loan-to-value typically capped at 70%
- Tenure: up to 25 years for residential
- Social housing schemes (FOPROLOS): subsidized rates for low-income buyers

## Market Risks
- High inflation eroding purchasing power
- Currency depreciation risk (TND lost ~50% vs EUR over 10 years)
- Political and regulatory uncertainty
- Oversupply in some coastal markets (speculative construction)
- Liquidity risk: transaction times 3–12 months typical
"""


# ── DB setup ───────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS report_documents (
    id          BIGSERIAL PRIMARY KEY,
    doc_hash    TEXT UNIQUE NOT NULL,          -- SHA-256 of content — prevents duplicates
    source      TEXT NOT NULL,                  -- URL or filename
    label       TEXT NOT NULL,
    tags        TEXT[] DEFAULT '{}',
    content     TEXT NOT NULL,
    embedding   vector(768),                    -- nomic-embed-text dimension
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS report_documents_embedding_idx
    ON report_documents USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 50);
"""


def get_conn():
    return psycopg2.connect(**PG_CONN)


def ensure_schema():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()
    logger.info("report_documents table ready")


# ── Scraping ───────────────────────────────────────────────────────────────────

def scrape_url(url: str, retries: int = 3) -> str | None:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }
    for attempt in range(retries):
        try:
            resp = httpx.get(url, headers=headers, timeout=20, follow_redirects=True)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # Remove nav, header, footer, scripts, styles
            for tag in soup(["nav", "header", "footer", "script",
                              "style", "aside", "form", "noscript"]):
                tag.decompose()

            # Get article content or fall back to body
            article = (
                soup.find("article")
                or soup.find("main")
                or soup.find(class_=lambda c: c and "content" in c.lower() if c else False)
                or soup.body
            )
            if not article:
                return None

            # Clean text
            text = article.get_text(separator="\n", strip=True)
            # Collapse excessive blank lines
            lines = [l.strip() for l in text.splitlines() if l.strip()]
            text = "\n".join(lines)
            return text if len(text) > 200 else None

        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            time.sleep(2 ** attempt)
    return None


# ── Live DB stats document ─────────────────────────────────────────────────────

def generate_db_stats_doc() -> str:
    """Query listings table and produce a structured market stats document."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:

                cur.execute("SELECT COUNT(*) FROM listings WHERE should_drop IS NOT TRUE")
                total = cur.fetchone()[0]

                cur.execute("""
                    SELECT city, COUNT(*) as cnt,
                           AVG(price)::numeric(12,0) as avg_price,
                           AVG(price_per_m2)::numeric(8,0) as avg_m2,
                           MIN(price)::numeric(12,0) as min_price,
                           MAX(price)::numeric(12,0) as max_price
                    FROM listings
                    WHERE city IS NOT NULL AND price IS NOT NULL AND price > 0
                      AND should_drop IS NOT TRUE
                    GROUP BY city
                    ORDER BY cnt DESC
                    LIMIT 15
                """)
                city_rows = cur.fetchall()

                cur.execute("""
                    SELECT transaction_type, property_type, COUNT(*) as cnt,
                           AVG(price)::numeric(12,0) as avg_price
                    FROM listings
                    WHERE transaction_type IS NOT NULL AND property_type IS NOT NULL
                      AND price IS NOT NULL AND price > 0
                      AND should_drop IS NOT TRUE
                    GROUP BY transaction_type, property_type
                    ORDER BY cnt DESC
                """)
                type_rows = cur.fetchall()

                cur.execute("""
                    SELECT region, COUNT(*) as cnt,
                           AVG(price_per_m2)::numeric(8,0) as avg_m2
                    FROM listings
                    WHERE region IS NOT NULL AND price_per_m2 IS NOT NULL
                      AND price_per_m2 > 0 AND should_drop IS NOT TRUE
                    GROUP BY region
                    ORDER BY avg_m2 DESC NULLS LAST
                    LIMIT 10
                """)
                region_rows = cur.fetchall()

                cur.execute("""
                    SELECT COUNT(*) FROM listings
                    WHERE scraped_at >= NOW() - INTERVAL '7 days'
                      AND should_drop IS NOT TRUE
                """)
                week_count = cur.fetchone()[0]

                cur.execute("""
                    SELECT COUNT(*) FROM listings
                    WHERE scraped_at >= NOW() - INTERVAL '30 days'
                      AND should_drop IS NOT TRUE
                """)
                month_count = cur.fetchone()[0]

    except Exception as e:
        logger.error(f"Failed to generate DB stats: {e}")
        return ""

    lines = [
        "# EstateMind Live Market Data — Generated from Supabase Listings",
        f"Total active listings: {total:,}",
        f"New listings (last 7 days): {week_count:,}",
        f"New listings (last 30 days): {month_count:,}",
        "",
        "## Price Statistics by City (TND)",
        "city | listings | avg_price | avg_price_m2 | min_price | max_price",
    ]
    for row in city_rows:
        city, cnt, avg_p, avg_m2, mn, mx = row
        lines.append(
            f"{city} | {cnt} | {int(avg_p or 0):,} | {int(avg_m2 or 0):,} | "
            f"{int(mn or 0):,} | {int(mx or 0):,}"
        )

    lines += ["", "## Listings by Transaction Type and Property Type",
              "transaction | property_type | count | avg_price"]
    for row in type_rows:
        tx, pt, cnt, avg_p = row
        lines.append(f"{tx} | {pt} | {cnt} | {int(avg_p or 0):,} TND")

    lines += ["", "## Average Price per m² by Region (TND/m²)",
              "region | listings | avg_price_per_m2"]
    for row in region_rows:
        region, cnt, avg_m2 = row
        lines.append(f"{region} | {cnt} | {int(avg_m2 or 0):,}")

    return "\n".join(lines)


# ── PDF loading ────────────────────────────────────────────────────────────────

def load_local_docs() -> Generator[tuple[str, str, list[str]], None, None]:
    """Yield (content, label, tags) from PDF/TXT files in DOCS_DIR."""
    DOCS_DIR.mkdir(exist_ok=True)
    for path in DOCS_DIR.iterdir():
        if path.suffix.lower() == ".pdf":
            try:
                from pypdf import PdfReader
                reader = PdfReader(str(path))
                text = "\n".join(
                    page.extract_text() or "" for page in reader.pages
                )
                if len(text.strip()) > 100:
                    yield text, f"Local PDF: {path.name}", ["pdf", "local"]
                    logger.info(f"Loaded PDF: {path.name} ({len(text)} chars)")
            except Exception as e:
                logger.warning(f"Could not read {path.name}: {e}")

        elif path.suffix.lower() in (".txt", ".md"):
            text = path.read_text(encoding="utf-8", errors="ignore")
            if len(text.strip()) > 100:
                yield text, f"Local file: {path.name}", ["text", "local"]
                logger.info(f"Loaded text: {path.name} ({len(text)} chars)")


# ── Chunking ───────────────────────────────────────────────────────────────────

def chunk_text(text: str) -> list[str]:
    splitter = RecursiveCharacterTextSplitter(
        chunk_size    = CHUNK_SIZE,
        chunk_overlap = CHUNK_OVERLAP,
        separators    = ["\n\n", "\n", ". ", " ", ""],
    )
    return [c for c in splitter.split_text(text) if len(c.strip()) > 50]


# ── Embedding ──────────────────────────────────────────────────────────────────

def get_embedder() -> OllamaEmbeddings:
    return OllamaEmbeddings(
        model      = EMBED_MODEL,
        base_url   = OLLAMA_BASE_URL,
    )


# ── Upsert to DB ───────────────────────────────────────────────────────────────

def upsert_chunks(
    chunks:    list[str],
    embeddings: list[list[float]],
    source:    str,
    label:     str,
    tags:      list[str],
) -> tuple[int, int]:
    """Insert chunks, skip duplicates. Returns (inserted, skipped)."""
    inserted = skipped = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for chunk, vec in zip(chunks, embeddings):
                doc_hash = hashlib.sha256(chunk.encode()).hexdigest()
                cur.execute(
                    """
                    INSERT INTO report_documents
                        (doc_hash, source, label, tags, content, embedding)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (doc_hash) DO NOTHING
                    """,
                    (doc_hash, source, label, tags, chunk, vec),
                )
                if cur.rowcount:
                    inserted += 1
                else:
                    skipped += 1
        conn.commit()
    return inserted, skipped


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    logger.info("=== EstateMind RAG Ingestion Pipeline ===")

    # 1. Ensure schema
    ensure_schema()

    # 2. Init embedder
    logger.info(f"Loading embedder: {EMBED_MODEL} via Ollama")
    embedder = get_embedder()

    total_inserted = 0
    total_skipped  = 0

    def process(text: str, source: str, label: str, tags: list[str]):
        nonlocal total_inserted, total_skipped
        chunks = chunk_text(text)
        if not chunks:
            logger.warning(f"No usable chunks from: {label}")
            return
        logger.info(f"Embedding {len(chunks)} chunks from: {label}")
        vecs = embedder.embed_documents(chunks)
        ins, skp = upsert_chunks(chunks, vecs, source, label, tags)
        total_inserted += ins
        total_skipped  += skp
        logger.info(f"  → {ins} inserted, {skp} already existed")

    # 3. Manual curated facts (always refresh)
    logger.info("Processing curated manual facts...")
    process(MANUAL_FACTS, "manual:curated_facts",
            "Tunisia RE Curated Key Facts 2026", ["market", "official", "curated"])

    # 4. Live DB stats
    logger.info("Generating live stats from Supabase listings...")
    db_doc = generate_db_stats_doc()
    if db_doc:
        process(db_doc, "db:listings_stats",
                "EstateMind Live Market Stats (from listings table)",
                ["market", "live", "prices", "statistics"])
    else:
        logger.warning("Could not generate DB stats — check DB connection")

    # 5. Web scraping
    for src in WEB_SOURCES:
        logger.info(f"Scraping: {src['url']}")
        text = scrape_url(src["url"])
        if text:
            process(text, src["url"], src["label"], src["tags"])
        else:
            logger.warning(f"Could not scrape: {src['url']}")
        time.sleep(1.5)

    # 6. Local PDFs / TXT files
    for text, label, tags in load_local_docs():
        process(text, f"local:{label}", label, tags)

    # Summary
    logger.info("=" * 50)
    logger.info(f"Ingestion complete.")
    logger.info(f"  Total inserted : {total_inserted}")
    logger.info(f"  Already existed: {total_skipped}")

    # Quick test retrieval
    logger.info("Testing retrieval...")
    test_query = "average real estate price per m2 Tunis"
    test_vec = embedder.embed_query(test_query)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT label, content, 1 - (embedding <=> %s::vector) AS similarity
                FROM report_documents
                ORDER BY embedding <=> %s::vector
                LIMIT 3
            """, (test_vec, test_vec))
            rows = cur.fetchall()
    logger.info(f"Top 3 results for '{test_query}':")
    for label, content, sim in rows:
        logger.info(f"  [{sim:.3f}] {label[:60]}")
        logger.info(f"           {content[:120].replace(chr(10), ' ')}...")


if __name__ == "__main__":
    main()