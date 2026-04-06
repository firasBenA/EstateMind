"""
EstateMind — Outlier Detector

Flags statistical outliers in listing data at regional level.
Uses IQR (Interquartile Range) method — robust to skewed distributions
which are common in real estate data.

Flags (never deletes):
- price_outlier: price is statistically extreme for region + property type
- surface_outlier: surface is unrealistic
- price_m2_outlier: price per m² is extreme
- suspected_test_listing: price = 1 or round number like 999999

The scorer uses these flags to lower reliability score.
The modeling layer filters them out or downweights them.
"""
from __future__ import annotations

import statistics
from collections import defaultdict
from typing import Dict, Any, List, Optional, Tuple

from loguru import logger


# ── Absolute bounds (hard limits regardless of statistics) ───────────────────

ABSOLUTE_BOUNDS = {
    "price": {
        "Sale": {"min": 5_000, "max": 50_000_000},
        "Rent": {"min": 100,   "max": 50_000},
    },
    "surface": {"min": 10, "max": 8_000},
    "rooms":   {"min": 1,  "max": 20},
    "price_per_m2": {
        "Sale": {"min": 50,  "max": 25_000},
        "Rent": {"min": 1,   "max": 200},
    },
}

# Test/placeholder listing patterns
TEST_PRICES = {1, 0, 999999, 9999999, 11111, 22222, 33333, 12345}


# ── IQR outlier detection ─────────────────────────────────────────────────────

def _iqr_bounds(values: List[float], multiplier: float = 2.5) -> Tuple[float, float]:
    """
    Calculate IQR-based outlier bounds.
    multiplier=2.5 is less aggressive than 1.5 (standard) — real estate
    has naturally high variance so we don't want to flag legitimate luxury listings.
    """
    if len(values) < 4:
        return float('-inf'), float('inf')
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    q1 = sorted_vals[n // 4]
    q3 = sorted_vals[3 * n // 4]
    iqr = q3 - q1
    return q1 - multiplier * iqr, q3 + multiplier * iqr


def build_regional_stats(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Compute regional price statistics from a batch of records.
    Used to set context-aware outlier thresholds.

    Returns nested dict: stats[region][property_type][transaction_type]
    """
    # Group prices by region + property_type + transaction_type
    groups: Dict[str, List[float]] = defaultdict(list)
    groups_m2: Dict[str, List[float]] = defaultdict(list)

    for rec in records:
        price = rec.get("price")
        surface = rec.get("surface")
        region = (rec.get("region") or "unknown").lower()
        prop_type = (rec.get("type") or "other").lower()
        tx_type = (rec.get("transaction_type") or "Sale")

        if not price or float(price) <= 0:
            continue

        key = f"{region}|{prop_type}|{tx_type}"
        groups[key].append(float(price))

        if surface and float(surface) > 0:
            groups_m2[key].append(float(price) / float(surface))

    # Build stats
    stats = {}
    for key, prices in groups.items():
        if len(prices) < 3:
            continue
        low, high = _iqr_bounds(prices)
        stats[key] = {
            "price_low": low,
            "price_high": high,
            "price_median": statistics.median(prices),
            "price_count": len(prices),
        }
        if key in groups_m2 and len(groups_m2[key]) >= 3:
            m2_low, m2_high = _iqr_bounds(groups_m2[key])
            stats[key]["price_m2_low"] = m2_low
            stats[key]["price_m2_high"] = m2_high

    logger.info(f"[OutlierDetector] Built regional stats for {len(stats)} groups")
    return stats


# ── Single record flagging ────────────────────────────────────────────────────

def flag_outliers(
    metadata: Dict[str, Any],
    regional_stats: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """
    Flag outliers in a single listing record.
    Adds boolean flags and an outlier_reasons list to metadata.
    Never deletes or modifies the actual values.
    """
    updated = dict(metadata)
    flags = []
    reasons = []

    price     = metadata.get("price")
    surface   = metadata.get("surface")
    rooms     = metadata.get("rooms")
    region    = (metadata.get("region") or "unknown").lower()
    prop_type = (metadata.get("type") or "other").lower()
    tx_type   = metadata.get("transaction_type") or "Sale"

    # ── Absolute bound checks ─────────────────────────────────────────────────

    if price is not None:
        price = float(price)
        bounds = ABSOLUTE_BOUNDS["price"].get(tx_type, ABSOLUTE_BOUNDS["price"]["Sale"])

        if price < bounds["min"]:
            flags.append("price_too_low")
            reasons.append(f"price {price} below min {bounds['min']} for {tx_type}")

        elif price > bounds["max"]:
            flags.append("price_too_high")
            reasons.append(f"price {price} above max {bounds['max']} for {tx_type}")

        # Test/placeholder listing
        if int(price) in TEST_PRICES:
            flags.append("suspected_test_listing")
            reasons.append(f"price looks like test/placeholder: {price}")

    if surface is not None:
        surface = float(surface)
        if surface < ABSOLUTE_BOUNDS["surface"]["min"]:
            flags.append("surface_too_small")
            reasons.append(f"surface {surface}m² below minimum")
        elif surface > ABSOLUTE_BOUNDS["surface"]["max"]:
            flags.append("surface_too_large")
            reasons.append(f"surface {surface}m² above maximum")

    if rooms is not None:
        rooms = int(rooms)
        if rooms < ABSOLUTE_BOUNDS["rooms"]["min"] or rooms > ABSOLUTE_BOUNDS["rooms"]["max"]:
            flags.append("rooms_invalid")
            reasons.append(f"rooms={rooms} is outside valid range 1-20")

    # ── Price per m² check ────────────────────────────────────────────────────

    price_per_m2 = None
    if price and surface and float(surface) > 0:
        price_per_m2 = price / surface
        bounds_m2 = ABSOLUTE_BOUNDS["price_per_m2"].get(
            tx_type, ABSOLUTE_BOUNDS["price_per_m2"]["Sale"]
        )
        if price_per_m2 < bounds_m2["min"]:
            flags.append("price_m2_too_low")
            reasons.append(f"price/m²={price_per_m2:.0f} suspiciously low")
        elif price_per_m2 > bounds_m2["max"]:
            flags.append("price_m2_too_high")
            reasons.append(f"price/m²={price_per_m2:.0f} suspiciously high")

    # ── Regional statistical checks ───────────────────────────────────────────

    if regional_stats and price:
        key = f"{region}|{prop_type}|{tx_type}"
        group = regional_stats.get(key)

        if group and group.get("price_count", 0) >= 5:
            if price < group["price_low"]:
                flags.append("regional_price_outlier_low")
                reasons.append(
                    f"price {price} below regional IQR lower bound "
                    f"{group['price_low']:.0f} for {region}/{prop_type}"
                )
            elif price > group["price_high"]:
                flags.append("regional_price_outlier_high")
                reasons.append(
                    f"price {price} above regional IQR upper bound "
                    f"{group['price_high']:.0f} for {region}/{prop_type}"
                )

            if price_per_m2 and "price_m2_high" in group:
                if price_per_m2 > group["price_m2_high"]:
                    flags.append("regional_price_m2_outlier")
                    reasons.append(
                        f"price/m² {price_per_m2:.0f} above regional bound"
                    )

    # ── Write flags to metadata ───────────────────────────────────────────────

    updated["outlier_flags"] = flags
    updated["outlier_reasons"] = reasons
    updated["is_outlier"] = len(flags) > 0
    updated["outlier_count"] = len(flags)

    if flags:
        logger.debug(
            f"[OutlierDetector] {metadata.get('property_id', '?')} "
            f"flagged: {flags}"
        )

    return updated


def batch_flag_outliers(
    records: List[Dict[str, Any]],
    build_stats_from_batch: bool = True,
) -> List[Dict[str, Any]]:
    """
    Flag outliers in a batch of records.
    If build_stats_from_batch=True, uses the batch itself to build
    regional statistics (good when batch is large enough, 100+ records).
    """
    regional_stats = None
    if build_stats_from_batch and len(records) >= 50:
        regional_stats = build_regional_stats(records)

    results = []
    flagged_count = 0
    for record in records:
        metadata = record.get("metadata", record)
        updated = flag_outliers(metadata, regional_stats)
        results.append(updated)
        if updated.get("is_outlier"):
            flagged_count += 1

    logger.info(
        f"[OutlierDetector] {len(records)} records — "
        f"{flagged_count} flagged as outliers "
        f"({flagged_count/len(records)*100:.1f}%)"
    )
    return results


if __name__ == "__main__":
    test_records = [
        {
            "property_id": "normal_001",
            "price": 450000, "surface": 120, "rooms": 4,
            "region": "Tunis", "type": "Apartment",
            "transaction_type": "Sale",
        },
        {
            "property_id": "cheap_002",
            "price": 1000, "surface": 200, "rooms": 5,
            "region": "Tunis", "type": "Villa",
            "transaction_type": "Sale",
        },
        {
            "property_id": "test_003",
            "price": 999999, "surface": 90, "rooms": 3,
            "region": "Sousse", "type": "Apartment",
            "transaction_type": "Sale",
        },
        {
            "property_id": "huge_004",
            "price": 500000, "surface": 15000, "rooms": 8,
            "region": "Nabeul", "type": "Land",
            "transaction_type": "Sale",
        },
    ]

    # Build regional stats from batch
    stats = build_regional_stats(test_records)
    print(f"Regional groups found: {len(stats)}")

    print("\n=== Outlier Flags ===")
    results = batch_flag_outliers(test_records)
    for r in results:
        status = "🔴 OUTLIER" if r["is_outlier"] else "✅ OK"
        print(f"\n{r['property_id']} {status}")
        if r["outlier_reasons"]:
            for reason in r["outlier_reasons"]:
                print(f"  → {reason}")