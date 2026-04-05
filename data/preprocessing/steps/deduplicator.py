"""
EstateMind — Cross-Source Deduplicator

Detects the same physical property listed on multiple sites.
Example: same apartment on zitouna_immo AND mubawab AND affare.

Strategy:
1. For each listing, query Pinecone for similar vectors (cosine similarity > 0.95)
2. If similar listings exist from DIFFERENT sources with matching price+location
   → mark as cross-source duplicate
3. Keep the listing with highest reliability_score as the canonical one
4. Mark others with suspected_duplicate=True (never delete — keep for analysis)

Important: Same listing_id from same source = not a duplicate (handled by upsert).
This only detects CROSS-SOURCE duplicates.
"""
from __future__ import annotations

from typing import Dict, Any, List, Optional, Tuple
from loguru import logger


# ── Similarity thresholds ─────────────────────────────────────────────────────

COSINE_THRESHOLD     = 0.95   # vector similarity must exceed this
PRICE_TOLERANCE_PCT  = 0.10   # prices must be within 10% of each other
LOCATION_MATCH       = True   # must be in same city/region


# ── Duplicate detection ───────────────────────────────────────────────────────

def _prices_match(price1: Any, price2: Any) -> bool:
    """Check if two prices are within tolerance."""
    try:
        p1, p2 = float(price1), float(price2)
        if p1 == 0 or p2 == 0:
            return False
        diff_pct = abs(p1 - p2) / max(p1, p2)
        return diff_pct <= PRICE_TOLERANCE_PCT
    except (TypeError, ValueError):
        return False


def _locations_match(meta1: Dict, meta2: Dict) -> bool:
    """Check if two listings are in the same location."""
    # Region / Governorate must match if both present
    r1 = (meta1.get("governorate") or meta1.get("region") or "").lower().strip()
    r2 = (meta2.get("governorate") or meta2.get("region") or "").lower().strip()
    if r1 and r2 and r1 != r2:
        return False

    # City match (optional — city names vary across scrapers)
    c1 = (meta1.get("city") or "").lower().strip()
    c2 = (meta2.get("city") or "").lower().strip()
    if c1 and c2:
        # Allow partial match (e.g. "La Marsa" vs "Marsa")
        return c1 in c2 or c2 in c1 or c1 == c2

    return True  # location can't be compared — assume match


def _surface_matches(meta1: Dict, meta2: Dict) -> bool:
    """Check if surfaces are similar (within 15%)."""
    try:
        s1 = float(meta1.get("surface") or 0)
        s2 = float(meta2.get("surface") or 0)
        if s1 == 0 or s2 == 0:
            return True  # can't compare, assume match
        diff_pct = abs(s1 - s2) / max(s1, s2)
        return diff_pct <= 0.15
    except (TypeError, ValueError):
        return True


def is_duplicate_pair(
    meta1: Dict[str, Any],
    meta2: Dict[str, Any],
    similarity_score: float,
) -> bool:
    """
    Determine if two listings represent the same physical property.
    Requires high vector similarity + matching price + matching location.
    """
    # Must be from different sources
    if meta1.get("source_name") == meta2.get("source_name"):
        return False

    # Must have high semantic similarity
    if similarity_score < COSINE_THRESHOLD:
        return False

    # Price must be similar
    if not _prices_match(meta1.get("price"), meta2.get("price")):
        return False

    # Location must match
    if not _locations_match(meta1, meta2):
        return False

    # Surface should match if available
    if not _surface_matches(meta1, meta2):
        return False

    return True


def find_duplicates_in_batch(
    records: List[Dict[str, Any]],
    similarity_matrix: Optional[List[List[float]]] = None,
) -> List[Dict[str, Any]]:
    """
    Find duplicate pairs within a batch of records.
    Uses text-based heuristics when similarity matrix not available.

    Returns records with suspected_duplicate and canonical_id flags added.
    """
    n = len(records)
    duplicate_groups: Dict[int, int] = {}  # index → canonical index

    for i in range(n):
        for j in range(i + 1, n):
            meta_i = records[i].get("metadata", records[i])
            meta_j = records[j].get("metadata", records[j])

            # Skip same source
            if meta_i.get("source_name") == meta_j.get("source_name"):
                continue

            # Get similarity score if matrix provided
            sim_score = 1.0
            if similarity_matrix:
                sim_score = similarity_matrix[i][j]
            else:
                # Fallback: use text-based heuristic
                sim_score = _estimate_similarity(meta_i, meta_j)

            if is_duplicate_pair(meta_i, meta_j, sim_score):
                # Determine canonical (higher reliability score wins)
                score_i = meta_i.get("reliability_score", 0) or 0
                score_j = meta_j.get("reliability_score", 0) or 0
                canonical = i if score_i >= score_j else j
                duplicate = j if canonical == i else i

                # Map duplicate → canonical
                # Handle chains: if duplicate already maps somewhere, follow it
                root = duplicate_groups.get(canonical, canonical)
                duplicate_groups[duplicate] = root

                logger.info(
                    f"[Deduplicator] Duplicate found: "
                    f"{meta_i.get('property_id')} ({meta_i.get('source_name')}) ↔ "
                    f"{meta_j.get('property_id')} ({meta_j.get('source_name')}) "
                    f"sim={sim_score:.3f}"
                )

    # Apply flags
    results = []
    duplicate_count = 0
    for i, record in enumerate(records):
        metadata = record.get("metadata", record)
        updated = dict(metadata)

        if i in duplicate_groups:
            canonical_idx = duplicate_groups[i]
            canonical_meta = records[canonical_idx].get(
                "metadata", records[canonical_idx]
            )
            updated["suspected_duplicate"] = True
            updated["canonical_id"] = canonical_meta.get("property_id")
            updated["canonical_source"] = canonical_meta.get("source_name")
            duplicate_count += 1
        else:
            updated["suspected_duplicate"] = False

        results.append(updated)

    logger.info(
        f"[Deduplicator] {n} records — "
        f"{duplicate_count} suspected duplicates found"
    )
    return results


def _estimate_similarity(meta1: Dict, meta2: Dict) -> float:
    """
    Estimate similarity without Pinecone vectors.
    Used as fallback when similarity matrix is not available.
    Combines price, surface, location, and type signals.
    """
    score = 0.0
    signals = 0

    # Price similarity
    try:
        p1 = float(meta1.get("price") or 0)
        p2 = float(meta2.get("price") or 0)
        if p1 > 0 and p2 > 0:
            diff = abs(p1 - p2) / max(p1, p2)
            score += max(0, 1 - diff * 5)  # 20% diff = 0 score
            signals += 1
    except (TypeError, ValueError):
        pass

    # Surface similarity
    try:
        s1 = float(meta1.get("surface") or 0)
        s2 = float(meta2.get("surface") or 0)
        if s1 > 0 and s2 > 0:
            diff = abs(s1 - s2) / max(s1, s2)
            score += max(0, 1 - diff * 5)
            signals += 1
    except (TypeError, ValueError):
        pass

    # Region match
    r1 = (meta1.get("region") or "").lower()
    r2 = (meta2.get("region") or "").lower()
    if r1 and r2:
        score += 1.0 if r1 == r2 else 0.0
        signals += 1

    # Property type match
    t1 = (meta1.get("type") or "").lower()
    t2 = (meta2.get("type") or "").lower()
    if t1 and t2:
        score += 1.0 if t1 == t2 else 0.0
        signals += 1

    # Transaction type match
    tx1 = (meta1.get("transaction_type") or "").lower()
    tx2 = (meta2.get("transaction_type") or "").lower()
    if tx1 and tx2:
        score += 1.0 if tx1 == tx2 else 0.0
        signals += 1

    return score / signals if signals > 0 else 0.0


def dedup_report(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Summary of deduplication results."""
    total = len(records)
    duplicates = [r for r in records if r.get("suspected_duplicate")]
    canonical  = [r for r in records if not r.get("suspected_duplicate")]

    by_source: Dict[str, int] = {}
    for r in duplicates:
        src = r.get("source_name", "unknown")
        by_source[src] = by_source.get(src, 0) + 1

    return {
        "total": total,
        "unique_canonical": len(canonical),
        "suspected_duplicates": len(duplicates),
        "duplicate_rate_pct": round(len(duplicates) / total * 100, 1) if total else 0,
        "duplicates_by_source": by_source,
    }


if __name__ == "__main__":
    test_records = [
        {
            "property_id": "zitouna_001",
            "source_name": "zitouna_immo",
            "price": 450000, "surface": 120,
            "region": "Tunis", "city": "La Marsa",
            "type": "Apartment", "transaction_type": "Sale",
            "reliability_score": 80,
        },
        {
            "property_id": "mubawab_001",
            "source_name": "mubawab",
            "price": 455000, "surface": 118,  # slightly different
            "region": "Tunis", "city": "La Marsa",
            "type": "Apartment", "transaction_type": "Sale",
            "reliability_score": 65,
        },
        {
            "property_id": "affare_001",
            "source_name": "affare",
            "price": 200000, "surface": 85,   # different property
            "region": "Sousse", "city": "Hammam Sousse",
            "type": "Apartment", "transaction_type": "Sale",
            "reliability_score": 70,
        },
    ]

    results = find_duplicates_in_batch(test_records)
    print("\n=== Deduplication Results ===")
    for r in results:
        status = "🔴 DUPLICATE" if r.get("suspected_duplicate") else "✅ CANONICAL"
        print(f"\n{r['property_id']} ({r['source_name']}) — {status}")
        if r.get("suspected_duplicate"):
            print(f"  Canonical: {r.get('canonical_id')} ({r.get('canonical_source')})")

    report = dedup_report(results)
    print(f"\nSummary: {report}")