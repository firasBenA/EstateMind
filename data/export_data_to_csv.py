# from __future__ import annotations

# import argparse
# import ast
# import csv
# import json
# import os
# import re
# import sqlite3
# import sys
# from datetime import datetime
# from pathlib import Path
# from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


# def _now_tag() -> str:
#     return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


# def _sanitize_string(text: str) -> str:
#     """
#     Remove characters that break row alignment in CSV files.

#     Even when a cell is correctly quoted, tools like Excel and LibreOffice
#     render each physical line as a new row when they encounter an embedded
#     newline, making the data appear shifted or corrupted.

#     Strategy:
#       - Replace \\r\\n and \\r with a single space (Windows / old Mac line endings)
#       - Replace \\n with a single space (Unix line endings inside description, etc.)
#       - Collapse runs of whitespace that result from the replacement to one space
#       - Strip leading / trailing whitespace
#     """
#     text = text.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
#     text = re.sub(r"[ \t]{2,}", " ", text)  # collapse runs of spaces/tabs
#     return text.strip()


# def _serialize_value(value: Any) -> Any:
#     """
#     Normalize a metadata value for safe CSV output.

#     Processing order:
#       1. None / missing      → empty string (explicit, avoids 'None' in output)
#       2. list / dict         → compact JSON string  (e.g. ["a","b"])
#       3. str with newlines   → newlines replaced by a single space
#       4. str that looks like a Python list/dict repr → converted to JSON string
#       5. Everything else     → returned as-is (DictWriter calls str() on numbers)

#     Why JSON for lists?
#       Python's str(list) produces ['a', 'b'] with single quotes — not valid JSON
#       and not readable by Excel / LibreOffice / pandas without extra parsing.

#     Why strip newlines from strings?
#       Even a correctly quoted CSV cell that contains \\n looks like a new row
#       when opened in Excel, LibreOffice, or most text editors. Replacing them
#       with spaces is the only way to guarantee one logical row = one visual line.
#     """
#     if value is None:
#         return ""

#     if isinstance(value, (list, dict)):
#         return json.dumps(value, ensure_ascii=False)

#     if isinstance(value, str):
#         # Sanitize newlines first (applies to all string fields)
#         value = _sanitize_string(value)

#         # Convert Python-repr list/dict strings to proper JSON
#         stripped = value.strip()
#         if (stripped.startswith("[") and stripped.endswith("]")) or (
#             stripped.startswith("{") and stripped.endswith("}")
#         ):
#             try:
#                 parsed = ast.literal_eval(stripped)
#                 if isinstance(parsed, (list, dict)):
#                     return json.dumps(parsed, ensure_ascii=False)
#             except (ValueError, SyntaxError):
#                 pass  # Not a valid Python literal – keep sanitized string

#     return value


# def _write_csv(path: Path, rows: Iterable[Dict[str, Any]], fieldnames: Sequence[str]) -> int:
#     """
#     Write rows to a CSV file.

#     Key fixes vs. the original:
#       1. All list/dict values are serialized to JSON strings before writing so
#          they don't produce Python-repr output like ['a', 'b'] which breaks
#          non-Python CSV readers (Excel, LibreOffice, etc.).
#       2. quoting=csv.QUOTE_ALL ensures every cell is quoted, which prevents
#          stray commas or newlines inside description/title fields from being
#          misinterpreted as column or row delimiters.
#       3. lineterminator="\n" (Unix line endings) avoids the \r\n that the csv
#          module emits by default on Windows, keeping the file consistent across
#          platforms.
#     """
#     path.parent.mkdir(parents=True, exist_ok=True)
#     count = 0
#     with path.open("w", newline="", encoding="utf-8") as f:
#         w = csv.DictWriter(
#             f,
#             fieldnames=list(fieldnames),
#             extrasaction="ignore",
#             quoting=csv.QUOTE_ALL,        # always quote every cell
#             lineterminator="\n",          # Unix line endings
#         )
#         w.writeheader()
#         for row in rows:
#             serialized = {k: _serialize_value(row.get(k)) for k in fieldnames}
#             w.writerow(serialized)
#             count += 1
#     return count


# def _export_pinecone_metadata(out_dir: Path, limit: int) -> Optional[Path]:
#     project_root = Path(__file__).resolve().parent
#     data_dir = project_root / "data"
#     if str(data_dir) not in sys.path:
#         sys.path.insert(0, str(data_dir))

#     try:
#         from database.vector_db import VectorDBHandler
#     except Exception:
#         return None

#     db = VectorDBHandler()
#     records = db.fetch_all_metadata(limit=limit if limit > 0 else 10_000_000)
#     if not records:
#         return None

#     # Collect all keys in insertion order (preserves column order across records)
#     all_keys: List[str] = []
#     seen: set = set()
#     for r in records:
#         for k in r.keys():
#             if k not in seen:
#                 seen.add(k)
#                 all_keys.append(k)

#     out_path = out_dir / f"pinecone_metadata_{_now_tag()}.csv"
#     _write_csv(out_path, records, all_keys)
#     return out_path


# def _sqlite_tables(conn: sqlite3.Connection) -> List[str]:
#     cur = conn.execute(
#         "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
#     )
#     return [r[0] for r in cur.fetchall()]


# def _sqlite_export_table(
#     conn: sqlite3.Connection, table: str, out_path: Path
# ) -> Tuple[int, List[str]]:
#     cur = conn.execute(f'SELECT * FROM "{table}"')
#     col_names = [d[0] for d in cur.description] if cur.description else []
#     rows = [dict(zip(col_names, row)) for row in cur.fetchall()] if col_names else []
#     count = _write_csv(out_path, rows, col_names)
#     return count, col_names


# def _find_db_files(root: Path) -> List[Path]:
#     dbs = sorted(root.glob("**/*.db"))
#     dedup: List[Path] = []
#     seen: set = set()
#     for p in dbs:
#         rp = str(p.resolve()).lower()
#         if rp in seen:
#             continue
#         seen.add(rp)
#         dedup.append(p)
#     return dedup


# def export_all(out_dir: Path, pinecone_limit: int) -> Dict[str, Any]:
#     out_dir.mkdir(parents=True, exist_ok=True)

#     result: Dict[str, Any] = {"out_dir": str(out_dir), "pinecone": {}, "sqlite": []}

#     pinecone_path = _export_pinecone_metadata(out_dir, limit=pinecone_limit)
#     result["pinecone"]["exported"] = bool(pinecone_path)
#     result["pinecone"]["csv"] = str(pinecone_path) if pinecone_path else None

#     project_root = Path(__file__).resolve().parent
#     db_files = _find_db_files(project_root)

#     for db_path in db_files:
#         db_info: Dict[str, Any] = {"db": str(db_path), "tables": []}
#         conn: Optional[sqlite3.Connection] = None
#         try:
#             conn = sqlite3.connect(str(db_path))
#             tables = _sqlite_tables(conn)
#             for table in tables:
#                 rel = db_path.resolve().relative_to(project_root.resolve())
#                 safe_db = rel.as_posix().replace("/", "__")
#                 if safe_db.lower().endswith(".db"):
#                     safe_db = safe_db[:-3]
#                 out_path = out_dir / f"{safe_db}__{table}.csv"
#                 row_count, cols = _sqlite_export_table(conn, table, out_path)
#                 db_info["tables"].append(
#                     {"table": table, "rows": row_count, "columns": cols, "csv": str(out_path)}
#                 )
#         finally:
#             if conn is not None:
#                 try:
#                     conn.close()
#                 except Exception:
#                     pass
#         result["sqlite"].append(db_info)

#     return result


# def main() -> None:
#     p = argparse.ArgumentParser()
#     p.add_argument("--out-dir", default="exports", type=str)
#     p.add_argument("--pinecone-limit", default=10000, type=int)
#     args = p.parse_args()

#     out_dir = Path(args.out_dir)
#     if not out_dir.is_absolute():
#         out_dir = Path(__file__).resolve().parent / out_dir

#     report = export_all(out_dir=out_dir, pinecone_limit=args.pinecone_limit)
#     print(report)


# if __name__ == "__main__":
#     main()


"""
pinecone_to_postgres.py
=======================
Exports ALL vectors (embeddings + metadata) from Pinecone
and inserts them into a local PostgreSQL database (pgAdmin)
with pgvector enabled.

What this script does:
  1. Connects to your Pinecone index
  2. Fetches every vector ID using list() pagination
  3. Fetches vectors in batches of 100 (embedding values + metadata)
  4. Connects to your local PostgreSQL (pgAdmin)
  5. Creates the required tables if they don't exist yet
  6. Inserts:
       - metadata  → listings table
       - embedding → image_embeddings OR text_embeddings table
         (detected automatically from vector dimension)

Requirements:
  pip install pinecone-client psycopg2-binary python-dotenv tqdm

Setup:
  Create a .env file next to this script with:

    PINECONE_API_KEY=your-pinecone-api-key
    PINECONE_INDEX_NAME=your-index-name

    # Local pgAdmin PostgreSQL connection
    POSTGRES_HOST=localhost
    POSTGRES_PORT=5432
    POSTGRES_DATABASE=estatemind        # whatever you named your DB in pgAdmin
    POSTGRES_USER=postgres
    POSTGRES_PASSWORD=your-pg-password

Usage:
  python pinecone_to_postgres.py
  python pinecone_to_postgres.py --batch-size 50 --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# ── third-party ──────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
except ImportError:
    sys.exit("Missing dependency: pip install python-dotenv")

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    sys.exit("Missing dependency: pip install psycopg2-binary")

try:
    from pinecone import Pinecone
except ImportError:
    sys.exit("Missing dependency: pip install pinecone-client")

try:
    from tqdm import tqdm
except ImportError:
    # tqdm is optional — fall back to plain print
    def tqdm(it, **kwargs):  # type: ignore
        return it


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()

PINECONE_API_KEY   = os.getenv("PINECONE_API_KEY", "")
PINECONE_INDEX     = os.getenv("PINECONE_INDEX_NAME", "")

POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "estatemind")
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")


# ─────────────────────────────────────────────────────────────────────────────
# POSTGRES HELPERS
# ─────────────────────────────────────────────────────────────────────────────

SETUP_SQL = """
-- Enable pgvector (safe to run multiple times)
CREATE EXTENSION IF NOT EXISTS vector;

-- ── listings ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS listings (
    id                  TEXT PRIMARY KEY,   -- Pinecone vector ID  e.g. "affare_1000"
    source_name         TEXT,
    url                 TEXT,
    title               TEXT,
    description         TEXT,
    price               NUMERIC,
    currency            TEXT,
    transaction_type    TEXT,
    type                TEXT,
    rooms               INTEGER,
    city                TEXT,
    municipality        TEXT,
    zone                TEXT,
    region              TEXT,
    surface             NUMERIC,
    features            JSONB,
    poi                 JSONB,
    images              JSONB,
    images_count        INTEGER,
    price_per_m2        NUMERIC,
    room_image_ratio    NUMERIC,
    fraud_score         NUMERIC,
    fraud_flag          BOOLEAN DEFAULT FALSE,
    fraud_reason        TEXT,
    fraud_model_used    TEXT,
    flagged_at          TIMESTAMPTZ,
    reliability_score   NUMERIC,
    reliability_level   TEXT,
    is_outlier          BOOLEAN,
    outlier_flags       JSONB,
    suspected_duplicate BOOLEAN,
    scraped_at          TIMESTAMPTZ,
    last_updated        TIMESTAMPTZ,
    change_type         TEXT,
    has_price_history   BOOLEAN,
    price_delta         NUMERIC,
    price_delta_pct     NUMERIC,
    should_drop         BOOLEAN,
    normalized          BOOLEAN,
    nlp_enriched        BOOLEAN,
    nlp_filled_fields   JSONB,
    latitude            NUMERIC,
    longitude           NUMERIC,
    model_weight        NUMERIC,
    property_id         TEXT,
    extra_metadata      JSONB    -- catches any field not explicitly mapped above
);

-- ── image_embeddings ─────────────────────────────────────────────────────────
-- One row per Pinecone vector that has ~512 dimensions (CLIP image embeddings)
CREATE TABLE IF NOT EXISTS image_embeddings (
    id          TEXT PRIMARY KEY,             -- Pinecone vector ID
    listing_id  TEXT REFERENCES listings(id) ON DELETE CASCADE,
    image_url   TEXT,
    image_label TEXT,
    embedding   vector(512)                   -- CLIP ViT-B/32 output
);

CREATE INDEX IF NOT EXISTS image_embeddings_hnsw_idx
    ON image_embeddings USING hnsw (embedding vector_cosine_ops);

-- ── text_embeddings ──────────────────────────────────────────────────────────
-- One row per Pinecone vector that has ~768 dimensions (nomic-embed-text)
CREATE TABLE IF NOT EXISTS text_embeddings (
    id          TEXT PRIMARY KEY,             -- Pinecone vector ID
    listing_id  TEXT REFERENCES listings(id) ON DELETE CASCADE,
    embedding   vector(768)                   -- nomic-embed-text output
);

CREATE INDEX IF NOT EXISTS text_embeddings_hnsw_idx
    ON text_embeddings USING hnsw (embedding vector_cosine_ops);
"""

# ── metadata field → postgres column type mapping ────────────────────────────
# Values are cast to these Python types before insert.
# Any field NOT listed here lands in extra_metadata (JSONB).

KNOWN_FIELDS: Dict[str, type] = {
    "source_name":         str,
    "url":                 str,
    "title":               str,
    "description":         str,
    "price":               float,
    "currency":            str,
    "transaction_type":    str,
    "type":                str,
    "rooms":               int,
    "city":                str,
    "municipality":        str,
    "zone":                str,
    "region":              str,
    "surface":             float,
    "features":            list,   # → JSONB
    "poi":                 list,   # → JSONB
    "images":              list,   # → JSONB
    "images_count":        int,
    "price_per_m2":        float,
    "room_image_ratio":    float,
    "fraud_score":         float,
    "fraud_flag":          bool,
    "fraud_reason":        str,
    "fraud_model_used":    str,
    "flagged_at":          str,
    "reliability_score":   float,
    "reliability_level":   str,
    "is_outlier":          bool,
    "outlier_flags":       list,   # → JSONB
    "suspected_duplicate": bool,
    "scraped_at":          str,
    "last_updated":        str,
    "change_type":         str,
    "has_price_history":   bool,
    "price_delta":         float,
    "price_delta_pct":     float,
    "should_drop":         bool,
    "normalized":          bool,
    "nlp_enriched":        bool,
    "nlp_filled_fields":   list,   # → JSONB
    "latitude":            float,
    "longitude":           float,
    "model_weight":        float,
    "property_id":         str,
}

JSONB_FIELDS = {k for k, v in KNOWN_FIELDS.items() if v is list}


def _cast(value: Any, typ: type) -> Any:
    """Best-effort type cast; returns None on failure."""
    if value is None:
        return None
    try:
        if typ is bool:
            if isinstance(value, bool):
                return value
            return str(value).lower() in ("true", "1", "yes")
        if typ in (list, dict):
            if isinstance(value, (list, dict)):
                return json.dumps(value)
            try:
                parsed = json.loads(str(value))
                return json.dumps(parsed)
            except Exception:
                return json.dumps([str(value)])
        return typ(value)
    except Exception:
        return None


def _parse_metadata(meta: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Split raw Pinecone metadata into:
      known   → columns that exist in the listings table
      extra   → everything else → goes into extra_metadata JSONB
    """
    known: Dict[str, Any] = {}
    extra: Dict[str, Any] = {}

    for key, value in meta.items():
        if key in KNOWN_FIELDS:
            known[key] = _cast(value, KNOWN_FIELDS[key])
        else:
            extra[key] = value

    return known, extra


def _connect_pg() -> psycopg2.extensions.connection:
    print(f"Connecting to PostgreSQL at {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE} …")
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DATABASE,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    conn.autocommit = False
    return conn


def _setup_schema(conn: psycopg2.extensions.connection) -> None:
    print("Creating tables and indexes if they don't exist …")
    with conn.cursor() as cur:
        cur.execute(SETUP_SQL)
    conn.commit()
    print("Schema ready.")


# ─────────────────────────────────────────────────────────────────────────────
# PINECONE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _connect_pinecone():
    print(f"Connecting to Pinecone index '{PINECONE_INDEX}' …")
    pc = Pinecone(api_key=PINECONE_API_KEY)
    index = pc.Index(PINECONE_INDEX)
    stats = index.describe_index_stats()
    total = stats.get("total_vector_count", "?")
    dim   = stats.get("dimension", "?")
    print(f"  Index stats: {total} vectors, {dim} dimensions")
    return index, int(dim) if dim != "?" else None


def _fetch_all_ids(index) -> List[str]:
    """
    Use Pinecone list() to paginate through all vector IDs.
    list() returns pages of ID strings.
    """
    print("Fetching all vector IDs …")
    all_ids: List[str] = []
    for page in index.list():          # each page is a list of ID strings
        if isinstance(page, list):
            all_ids.extend(page)
        else:
            # some SDK versions wrap in an object
            all_ids.extend(getattr(page, "ids", page))
    print(f"  Found {len(all_ids)} vector IDs.")
    return all_ids


def _fetch_batch(index, ids: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch a batch of vectors from Pinecone.
    Returns list of dicts: {id, values (list[float]), metadata (dict)}
    """
    result = index.fetch(ids=ids)
    vectors = result.get("vectors", result.vectors if hasattr(result, "vectors") else {})
    records = []
    for vec_id, vec_data in vectors.items():
        # handle both dict and object responses
        if isinstance(vec_data, dict):
            values   = vec_data.get("values", [])
            metadata = vec_data.get("metadata", {})
        else:
            values   = list(vec_data.values)   if hasattr(vec_data, "values")   else []
            metadata = dict(vec_data.metadata) if hasattr(vec_data, "metadata") else {}

        records.append({
            "id":       vec_id,
            "values":   values,
            "metadata": metadata,
        })
    return records


# ─────────────────────────────────────────────────────────────────────────────
# INSERT HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _upsert_listing(cur, vec_id: str, known: Dict[str, Any], extra: Dict[str, Any]) -> None:
    """Insert or update a row in the listings table."""

    cols = ["id"] + list(known.keys()) + ["extra_metadata"]
    vals = [vec_id] + list(known.values()) + [json.dumps(extra) if extra else None]

    placeholders = ", ".join(["%s"] * len(cols))
    col_names    = ", ".join(cols)

    # ON CONFLICT: update all non-id columns
    update_clause = ", ".join(
        f"{c} = EXCLUDED.{c}"
        for c in cols if c != "id"
    )

    sql = f"""
        INSERT INTO listings ({col_names})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET {update_clause}
    """
    cur.execute(sql, vals)


def _upsert_image_embedding(cur, vec_id: str, listing_id: str,
                             embedding: List[float], metadata: Dict[str, Any]) -> None:
    cur.execute("""
        INSERT INTO image_embeddings (id, listing_id, image_url, image_label, embedding)
        VALUES (%s, %s, %s, %s, %s::vector)
        ON CONFLICT (id) DO UPDATE SET
            listing_id  = EXCLUDED.listing_id,
            image_url   = EXCLUDED.image_url,
            image_label = EXCLUDED.image_label,
            embedding   = EXCLUDED.embedding
    """, (
        vec_id,
        listing_id,
        metadata.get("image_url"),
        metadata.get("image_label"),
        str(embedding),          # psycopg2 sends as text; pgvector casts automatically
    ))


def _upsert_text_embedding(cur, vec_id: str, listing_id: str,
                            embedding: List[float]) -> None:
    cur.execute("""
        INSERT INTO text_embeddings (id, listing_id, embedding)
        VALUES (%s, %s, %s::vector)
        ON CONFLICT (id) DO UPDATE SET
            listing_id = EXCLUDED.listing_id,
            embedding  = EXCLUDED.embedding
    """, (
        vec_id,
        listing_id,
        str(embedding),
    ))


# ─────────────────────────────────────────────────────────────────────────────
# MAIN MIGRATION
# ─────────────────────────────────────────────────────────────────────────────

def migrate(batch_size: int = 100, dry_run: bool = False) -> None:

    # ── validate env ──────────────────────────────────────────────────────────
    missing = [v for v in ("PINECONE_API_KEY", "PINECONE_INDEX_NAME",
                            "POSTGRES_PASSWORD") if not os.getenv(v)]
    if missing:
        sys.exit(
            f"Missing environment variables: {missing}\n"
            "Create a .env file next to this script — see the docstring at the top."
        )

    # ── connect ───────────────────────────────────────────────────────────────
    POSTGRES_conn = _connect_pg()
    _setup_schema(POSTGRES_conn)

    index, index_dim = _connect_pinecone()

    # ── detect dimension type ─────────────────────────────────────────────────
    # 512  → CLIP image embeddings
    # 768  → nomic-embed-text / sentence-transformers text embeddings
    # other → metadata-only (stored in listings, no vector table)
    DIM_IMAGE = 512
    DIM_TEXT  = 768

    # ── fetch all IDs ─────────────────────────────────────────────────────────
    all_ids = _fetch_all_ids(index)
    if not all_ids:
        print("No vectors found in index. Exiting.")
        POSTGRES_conn.close()
        return

    # ── process in batches ────────────────────────────────────────────────────
    total      = len(all_ids)
    inserted   = 0
    skipped    = 0
    errors     = 0

    print(f"\nMigrating {total} vectors in batches of {batch_size} …")
    if dry_run:
        print("DRY RUN — no data will be written to PostgreSQL.\n")

    for batch_start in tqdm(range(0, total, batch_size), unit="batch"):
        batch_ids = all_ids[batch_start : batch_start + batch_size]

        try:
            records = _fetch_batch(index, batch_ids)
        except Exception as exc:
            print(f"\n  Pinecone fetch error for batch starting at {batch_start}: {exc}")
            errors += len(batch_ids)
            continue

        if dry_run:
            for rec in records:
                dim = len(rec["values"])
                print(f"  [DRY RUN] id={rec['id']} dim={dim} "
                      f"meta_keys={list(rec['metadata'].keys())}")
            continue

        with POSTGRES_conn.cursor() as cur:
            for rec in records:
                vec_id    = rec["id"]
                embedding = rec["values"]      # list[float]
                metadata  = rec["metadata"]    # dict
                dim       = len(embedding)

                # Derive listing_id from metadata or from the vector ID itself
                # Adjust this if your IDs use a different format
                listing_id = (
                    metadata.get("listing_id")
                    or metadata.get("property_id")
                    or vec_id                   # fallback: use the vector ID directly
                )

                # ── 1. upsert into listings (always) ─────────────────────────
                known, extra = _parse_metadata(metadata)
                try:
                    _upsert_listing(cur, listing_id, known, extra)
                except Exception as exc:
                    print(f"\n  listings insert error for {vec_id}: {exc}")
                    POSTGRES_conn.rollback()
                    errors += 1
                    continue

                # ── 2. upsert embedding into correct table ────────────────────
                try:
                    if dim == DIM_IMAGE:
                        _upsert_image_embedding(
                            cur, vec_id, listing_id, embedding, metadata
                        )
                    elif dim == DIM_TEXT:
                        _upsert_text_embedding(
                            cur, vec_id, listing_id, embedding
                        )
                    else:
                        # Unknown dimension — store metadata only, skip embedding
                        pass
                except Exception as exc:
                    print(f"\n  embedding insert error for {vec_id} (dim={dim}): {exc}")
                    POSTGRES_conn.rollback()
                    errors += 1
                    continue

                inserted += 1

        POSTGRES_conn.commit()

    POSTGRES_conn.close()

    # ── summary ───────────────────────────────────────────────────────────────
    print(f"""
Migration complete
─────────────────
Total vectors : {total}
Inserted      : {inserted}
Errors        : {errors}
Dry run       : {dry_run}
""")

    if errors:
        print(f"  {errors} vectors had errors — check output above for details.")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate Pinecone vectors (embeddings + metadata) to PostgreSQL with pgvector."
    )
    parser.add_argument(
        "--batch-size", type=int, default=100,
        help="Number of vectors to fetch from Pinecone per request (default: 100)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch from Pinecone but do not write to PostgreSQL"
    )
    args = parser.parse_args()
    migrate(batch_size=args.batch_size, dry_run=args.dry_run)


if __name__ == "__main__":
    main()