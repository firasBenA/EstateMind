"""
EstateMind — Change Detector

Detects changes between scraping cycles for the same listing.
Writes every change event to SQLite for time series analysis (Obj 2 & 5).

Change types:
- new:         listing seen for the first time
- price_up:    price increased since last scrape
- price_down:  price decreased since last scrape
- relisted:    listing disappeared then reappeared
- unchanged:   no meaningful change detected
"""
from __future__ import annotations

import sqlite3
import os
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from loguru import logger


# ── SQLite setup ──────────────────────────────────────────────────────────────

DB_PATH = os.environ.get("TIMESERIES_DB_PATH", "data/estatemind_timeseries.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS price_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id      TEXT NOT NULL,
    source_name     TEXT NOT NULL,
    price           REAL,
    surface         REAL,
    rooms           INTEGER,
    city            TEXT,
    region          TEXT,
    zone            TEXT,
    transaction_type TEXT,
    property_type   TEXT,
    scraped_at      TEXT,
    change_type     TEXT,
    price_delta     REAL,
    price_delta_pct REAL,
    reliability_score INTEGER
);

CREATE INDEX IF NOT EXISTS idx_listing_id  ON price_history(listing_id);
CREATE INDEX IF NOT EXISTS idx_scraped_at  ON price_history(scraped_at);
CREATE INDEX IF NOT EXISTS idx_region      ON price_history(region);
CREATE INDEX IF NOT EXISTS idx_change_type ON price_history(change_type);
"""


def get_connection() -> sqlite3.Connection:
    """Get SQLite connection, creating DB and schema if needed."""
    os.makedirs(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


# ── Change detection ──────────────────────────────────────────────────────────

PRICE_CHANGE_THRESHOLD = 0.02  # 2% — ignore tiny fluctuations


def get_last_known(
    conn: sqlite3.Connection,
    listing_id: str,
) -> Optional[Dict[str, Any]]:
    """Get the most recent record for a listing from SQLite."""
    cur = conn.execute(
        """
        SELECT * FROM price_history
        WHERE listing_id = ?
        ORDER BY scraped_at DESC
        LIMIT 1
        """,
        (listing_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def detect_change(
    metadata: Dict[str, Any],
    last_known: Optional[Dict[str, Any]],
) -> Tuple[str, Optional[float], Optional[float]]:
    """
    Compare current metadata against last known state.

    Returns:
        (change_type, price_delta, price_delta_pct)
    """
    current_price = metadata.get("price")

    if last_known is None:
        return "new", None, None

    last_price = last_known.get("price")

    if current_price is None or last_price is None:
        return "unchanged", None, None

    current_price = float(current_price)
    last_price    = float(last_price)

    if last_price == 0:
        return "new", None, None

    delta     = current_price - last_price
    delta_pct = delta / last_price

    if abs(delta_pct) < PRICE_CHANGE_THRESHOLD:
        return "unchanged", round(delta, 2), round(delta_pct * 100, 2)

    change_type = "price_up" if delta > 0 else "price_down"
    return change_type, round(delta, 2), round(delta_pct * 100, 2)


def record_change(
    conn: sqlite3.Connection,
    metadata: Dict[str, Any],
    change_type: str,
    price_delta: Optional[float] = None,
    price_delta_pct: Optional[float] = None,
) -> None:
    """Write a change event to the price_history table."""
    scraped_at = metadata.get("scraped_at") or datetime.utcnow().isoformat()

    conn.execute(
        """
        INSERT INTO price_history (
            listing_id, source_name, price, surface, rooms,
            city, region, zone, transaction_type, property_type,
            scraped_at, change_type, price_delta, price_delta_pct,
            reliability_score
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            metadata.get("property_id"),
            metadata.get("source_name"),
            metadata.get("price"),
            metadata.get("surface"),
            metadata.get("rooms"),
            metadata.get("city"),
            metadata.get("region"),
            metadata.get("zone"),
            metadata.get("transaction_type"),
            metadata.get("type"),
            scraped_at,
            change_type,
            price_delta,
            price_delta_pct,
            metadata.get("reliability_score"),
        ),
    )


# ── Main processor ────────────────────────────────────────────────────────────

def process_changes(
    metadata: Dict[str, Any],
    conn: sqlite3.Connection,
) -> Dict[str, Any]:
    """
    Process change detection for a single listing.
    Writes to SQLite and adds change flags to metadata.

    Returns updated metadata with change_type, has_price_history flags.
    """
    listing_id = metadata.get("property_id")
    if not listing_id:
        return metadata

    last_known  = get_last_known(conn, listing_id)
    change_type, price_delta, price_delta_pct = detect_change(metadata, last_known)

    # Always record new and price changes
    # Skip writing 'unchanged' records to save space — only write changes
    if change_type != "unchanged":
        record_change(conn, metadata, change_type, price_delta, price_delta_pct)
        conn.commit()

        if change_type == "new":
            logger.debug(f"[ChangeDetector] NEW listing: {listing_id}")
        else:
            logger.info(
                f"[ChangeDetector] {change_type.upper()} {listing_id}: "
                f"price {last_known.get('price')} → {metadata.get('price')} "
                f"(Δ {price_delta_pct:+.1f}%)"
            )

    # Add change metadata to the record
    updated = dict(metadata)
    updated["change_type"]       = change_type
    updated["price_delta"]       = price_delta
    updated["price_delta_pct"]   = price_delta_pct
    updated["has_price_history"] = last_known is not None
    updated["price_changed"]     = change_type in ("price_up", "price_down")

    return updated


def batch_process_changes(
    records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Process change detection for a batch of records.
    Uses a single SQLite connection for the whole batch.
    """
    conn = get_connection()
    results = []
    stats = {"new": 0, "price_up": 0, "price_down": 0, "unchanged": 0}

    try:
        for record in records:
            metadata = record.get("metadata", record)
            updated  = process_changes(metadata, conn)
            results.append(updated)
            change   = updated.get("change_type", "unchanged")
            stats[change] = stats.get(change, 0) + 1
    finally:
        conn.close()

    logger.info(
        f"[ChangeDetector] {len(records)} records — "
        f"new={stats['new']} price_up={stats['price_up']} "
        f"price_down={stats['price_down']} unchanged={stats['unchanged']}"
    )
    return results


# ── Query helpers for modeling ────────────────────────────────────────────────

def get_price_history(
    listing_id: str,
    db_path: str = DB_PATH,
) -> List[Dict[str, Any]]:
    """Get full price history for a specific listing."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """
        SELECT * FROM price_history
        WHERE listing_id = ?
        ORDER BY scraped_at ASC
        """,
        (listing_id,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_regional_price_trends(
    region: str,
    property_type: str = "Apartment",
    transaction_type: str = "Sale",
    db_path: str = DB_PATH,
) -> List[Dict[str, Any]]:
    """
    Get aggregated price trends for a region over time.
    Used directly by the time series forecasting model (Obj 5).
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """
        SELECT
            DATE(scraped_at)    AS date,
            COUNT(*)            AS listing_count,
            AVG(price)          AS avg_price,
            MIN(price)          AS min_price,
            MAX(price)          AS max_price,
            AVG(price / NULLIF(surface, 0)) AS avg_price_m2
        FROM price_history
        WHERE
            region           = ?
            AND property_type = ?
            AND transaction_type = ?
            AND price IS NOT NULL
            AND price > 0
            AND change_type IN ('new', 'price_up', 'price_down')
        GROUP BY DATE(scraped_at)
        ORDER BY date ASC
        """,
        (region, property_type, transaction_type),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_change_summary(db_path: str = DB_PATH) -> Dict[str, Any]:
    """Summary stats for the dashboard."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    total = conn.execute("SELECT COUNT(*) as n FROM price_history").fetchone()["n"]
    by_type = conn.execute(
        "SELECT change_type, COUNT(*) as n FROM price_history GROUP BY change_type"
    ).fetchall()
    recent = conn.execute(
        """
        SELECT listing_id, source_name, price, price_delta_pct, scraped_at
        FROM price_history
        WHERE change_type IN ('price_up', 'price_down')
        ORDER BY scraped_at DESC LIMIT 10
        """
    ).fetchall()

    conn.close()
    return {
        "total_events": total,
        "by_change_type": {r["change_type"]: r["n"] for r in by_type},
        "recent_price_changes": [dict(r) for r in recent],
    }


if __name__ == "__main__":
    import tempfile, os
    # Use temp DB for testing
    test_db = tempfile.mktemp(suffix=".db")

    # Patch DB_PATH for test
    import preprocessing.steps.change_detector as cd
    cd.DB_PATH = test_db

    conn = get_connection()

    listing = {
        "property_id": "zitouna_immo_5518",
        "source_name": "zitouna_immo",
        "price": 4500,
        "surface": 120,
        "rooms": 3,
        "city": "La Marsa",
        "region": "Tunis",
        "zone": "north",
        "transaction_type": "Rent",
        "type": "Apartment",
        "scraped_at": "2026-03-20T00:00:00",
        "reliability_score": 85,
    }

    # First scrape — should be 'new'
    result1 = process_changes(listing, conn)
    print(f"First scrape: change_type={result1['change_type']}")

    # Same price — should be 'unchanged'
    result2 = process_changes(listing, conn)
    print(f"Same price: change_type={result2['change_type']}")

    # Price drop — should be 'price_down'
    listing_v2 = dict(listing)
    listing_v2["price"] = 4200
    listing_v2["scraped_at"] = "2026-03-21T00:00:00"
    result3 = process_changes(listing_v2, conn)
    print(f"Price drop: change_type={result3['change_type']}, "
          f"delta={result3['price_delta']}, pct={result3['price_delta_pct']}%")

    conn.close()

    # Check history
    history = get_price_history("zitouna_immo_5518", test_db)
    print(f"\nPrice history records: {len(history)}")
    for h in history:
        print(f"  {h['scraped_at']}: {h['price']} ({h['change_type']})")

    os.unlink(test_db)