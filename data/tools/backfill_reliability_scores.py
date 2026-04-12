"""
EstateMind — Backfill Reliability Scores

This utility re-runs the scoring and preprocessing pipeline on ALL existing 
Pinecone vectors to eliminate UNKNOWN reliability scores.

Usage:
    python tools/backfill_reliability_scores.py                    # Process all vectors
    python tools/backfill_reliability_scores.py --source mubawab   # Process single source
    python tools/backfill_reliability_scores.py --limit 1000       # Process first 1000
    python tools/backfill_reliability_scores.py --dry-run          # Preview without updating
    python tools/backfill_reliability_scores.py --batch-size 50    # Custom batch size

This script:
1. Fetches all vectors from Pinecone (or filtered by source)
2. Runs the full preprocessing pipeline on them:
   - Normalization
   - NLP null filling
   - Deduplication detection
   - Outlier detection
   - Reliability scoring
3. Upserts the enriched metadata back to Pinecone

After running this, UNKNOWN scores should drop to near zero.
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from loguru import logger
from database.vector_db import VectorDBHandler, _clean_metadata
from preprocessing.steps.normalizer import batch_normalize
from preprocessing.steps.deduplicator import find_duplicates_in_batch
from preprocessing.steps.outlier_detector import batch_flag_outliers
from preprocessing.steps.scorer import compute_score, compute_model_weight


# ── Configuration ─────────────────────────────────────────────────────────────

DEFAULT_BATCH_SIZE = 100  # Pinecone upsert batch size
DEFAULT_LIMIT = 10_000    # Max vectors to process (0 = unlimited)


# ── Helper Functions ──────────────────────────────────────────────────────────

def _now_iso() -> str:
    """Return current UTC timestamp in ISO format."""
    from datetime import timezone
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def _get_source_from_id(vector_id: str) -> Optional[str]:
    """Extract source name from vector ID (format: source:property_id)."""
    if ":" in vector_id:
        return vector_id.split(":", 1)[0]
    return None


# ── Main Backfill Logic ───────────────────────────────────────────────────────

def backfill_scores(
    source: Optional[str] = None,
    limit: int = DEFAULT_LIMIT,
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Backfill reliability scores for all Pinecone vectors.
    
    Args:
        source: Optional source filter (e.g., "mubawab", "tecnocasa")
        limit: Max number of vectors to process (0 = unlimited)
        batch_size: Pinecone upsert batch size
        dry_run: If True, preview changes without updating Pinecone
    
    Returns:
        Report dict with processing statistics
    """
    logger.info("=" * 70)
    logger.info("EstateMind — Reliability Score Backfill")
    logger.info("=" * 70)
    
    start_time = time.time()
    from datetime import timezone
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    
    # Initialize vector DB
    try:
        db = VectorDBHandler()
        logger.info(f"✓ Connected to Pinecone index: {db.index._config.host}")
    except Exception as e:
        logger.error(f"✗ Failed to connect to Pinecone: {e}")
        return {"error": str(e), "status": "failed"}
    
    # Statistics
    stats = {
        "run_id": run_id,
        "started_at": _now_iso(),
        "source_filter": source,
        "limit": limit,
        "batch_size": batch_size,
        "dry_run": dry_run,
        "fetched": 0,
        "processed": 0,
        "updated": 0,
        "errors": 0,
        "skipped": 0,
        "score_distribution": {"HIGH": 0, "GOOD": 0, "LOW": 0, "DROP": 0, "UNKNOWN": 0},
        "before_unknown": 0,
        "after_unknown": 0,
    }
    
    # ── Step 1: Fetch all vectors ─────────────────────────────────────────────
    logger.info(f"Fetching vectors from Pinecone{f' (source={source})' if source else ''}...")
    
    all_records = []
    prefix = f"{source}:" if source else ""
    list_batch_size = 100
    fetch_batch_size = 50
    
    try:
        for ids_page in db.index.list(prefix=prefix, limit=list_batch_size):
            ids_list = list(ids_page) if not isinstance(ids_page, list) else ids_page
            if not ids_list:
                continue
            
            # Fetch in smaller batches to avoid timeouts
            for i in range(0, len(ids_list), fetch_batch_size):
                batch_ids = ids_list[i : i + fetch_batch_size]
                result = db.index.fetch(ids=batch_ids)
                vectors = getattr(result, "vectors", None) or {}
                
                for vector_id, vector_data in vectors.items():
                    if limit > 0 and len(all_records) >= limit:
                        break
                    
                    metadata = (getattr(vector_data, "metadata", None) or {}).copy()
                    metadata["_vector_id"] = vector_id
                    metadata["_vector_values"] = getattr(vector_data, "values", None)
                    
                    # Track UNKNOWN scores before processing
                    if not metadata.get("reliability_level"):
                        stats["before_unknown"] += 1
                    
                    all_records.append(metadata)
                    stats["fetched"] += 1
                
                if limit > 0 and len(all_records) >= limit:
                    break
            
            if limit > 0 and len(all_records) >= limit:
                break
        
        logger.info(f"✓ Fetched {stats['fetched']} vectors")
        
        if not all_records:
            logger.warning("No vectors found to process")
            stats["status"] = "no_data"
            return stats
    
    except Exception as e:
        logger.error(f"✗ Failed to fetch vectors: {e}")
        stats["error"] = str(e)
        stats["status"] = "fetch_failed"
        return stats
    
    # ── Step 2: Run preprocessing pipeline ────────────────────────────────────
    logger.info("Running preprocessing pipeline...")
    
    try:
        # 2.1 Normalize
        logger.info("  → Normalizing field formats...")
        records = batch_normalize(all_records)
        
        # 2.2 Skip NLP null filling (too slow for backfill, not needed for scoring)
        # The main pipeline will handle NLP enrichment for new data
        logger.info("  → Skipping NLP extraction (not required for scoring)...")
        
        # 2.3 Detect duplicates
        logger.info("  → Detecting cross-source duplicates...")
        records = find_duplicates_in_batch(records)
        
        # 2.4 Flag outliers
        logger.info("  → Flagging outliers...")
        records = batch_flag_outliers(records, build_stats_from_batch=True)
        
        # 2.5 Compute reliability scores
        logger.info("  → Computing reliability scores...")
        scored_records = []
        for rec in records:
            flags = {
                "price_outlier": rec.get("is_outlier", False),
                "suspected_duplicate": rec.get("suspected_duplicate", False),
                "nlp_enriched": rec.get("nlp_enriched", False),
                "has_price_history": rec.get("has_price_history", False),
                "price_changed": rec.get("price_changed", False),
            }
            score_result = compute_score(rec, flags)
            rec["reliability_score"] = score_result["score"]
            rec["reliability_level"] = score_result["level"]
            rec["should_drop"] = score_result["should_drop"]
            rec["model_weight"] = compute_model_weight(score_result["score"])
            rec["last_update"] = _now_iso()
            
            # Track score distribution
            level = rec.get("reliability_level", "UNKNOWN")
            stats["score_distribution"][level] = stats["score_distribution"].get(level, 0) + 1
            
            scored_records.append(rec)
        
        stats["processed"] = len(scored_records)
        stats["after_unknown"] = stats["score_distribution"]["UNKNOWN"]
        
        logger.info(f"✓ Processed {stats['processed']} records")
        logger.info(f"  Score distribution: {stats['score_distribution']}")
        logger.info(f"  UNKNOWN reduced: {stats['before_unknown']} → {stats['after_unknown']}")
    
    except Exception as e:
        logger.error(f"✗ Preprocessing failed: {e}")
        stats["error"] = str(e)
        stats["status"] = "preprocessing_failed"
        return stats
    
    # ── Step 3: Upsert back to Pinecone ───────────────────────────────────────
    if dry_run:
        logger.info("DRY RUN — Skipping Pinecone upsert")
        stats["status"] = "dry_run_success"
    else:
        logger.info("Upserting enriched metadata to Pinecone...")
        
        # Fields to keep in metadata
        keep_fields = [
            "property_id", "source_name", "url", "type", "title", "description",
            "price", "surface", "rooms", "region", "zone", "city", "municipalite",
            "latitude", "longitude", "images", "image_count", "features",
            "scraped_at", "last_update", "transaction_type", "currency", "poi",
            # Pipeline enrichments
            "normalized", "nlp_enriched", "nlp_filled_fields",
            "reliability_score", "reliability_level", "should_drop", "model_weight",
            "is_outlier", "outlier_flags", "suspected_duplicate", "canonical_id",
            "change_type", "price_delta", "price_delta_pct",
            "has_price_history", "price_per_m2",
        ]
        
        batch = []
        for rec in scored_records:
            vector_id = rec.get("_vector_id")
            vector_values = rec.get("_vector_values")
            
            if not vector_id or not vector_values:
                stats["skipped"] += 1
                continue
            
            # Clean metadata for Pinecone
            clean_meta = {
                k: v for k, v in rec.items()
                if k in keep_fields and v is not None
                and not k.startswith("_")
            }
            clean_meta = _clean_metadata(clean_meta)
            
            batch.append({
                "id": vector_id,
                "values": vector_values,
                "metadata": clean_meta,
            })
            
            # Upsert in batches
            if len(batch) >= batch_size:
                try:
                    db.index.upsert(vectors=batch)
                    stats["updated"] += len(batch)
                    logger.info(f"  ✓ Upserted batch of {len(batch)} vectors ({stats['updated']} total)")
                except Exception as e:
                    logger.error(f"  ✗ Batch upsert failed: {e}")
                    stats["errors"] += len(batch)
                batch = []
        
        # Upsert remaining batch
        if batch:
            try:
                db.index.upsert(vectors=batch)
                stats["updated"] += len(batch)
                logger.info(f"  ✓ Upserted final batch of {len(batch)} vectors ({stats['updated']} total)")
            except Exception as e:
                logger.error(f"  ✗ Final batch upsert failed: {e}")
                stats["errors"] += len(batch)
        
        logger.info(f"✓ Updated {stats['updated']} vectors in Pinecone")
        stats["status"] = "success"
    
    # ── Final Report ──────────────────────────────────────────────────────────
    elapsed = round(time.time() - start_time, 1)
    stats["elapsed_s"] = elapsed
    stats["finished_at"] = _now_iso()
    
    logger.info("=" * 70)
    logger.info("Backfill Complete")
    logger.info("=" * 70)
    logger.info(f"  Run ID:        {run_id}")
    logger.info(f"  Status:        {stats['status']}")
    logger.info(f"  Fetched:       {stats['fetched']}")
    logger.info(f"  Processed:     {stats['processed']}")
    logger.info(f"  Updated:       {stats['updated']}")
    logger.info(f"  Errors:        {stats['errors']}")
    logger.info(f"  Skipped:       {stats['skipped']}")
    logger.info(f"  Elapsed:       {elapsed}s")
    logger.info(f"  UNKNOWN:       {stats['before_unknown']} → {stats['after_unknown']}")
    logger.info(f"  Distribution:  {stats['score_distribution']}")
    logger.info("=" * 70)
    
    return stats


# ── CLI Entry Point ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Backfill reliability scores for all Pinecone vectors",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tools/backfill_reliability_scores.py
  python tools/backfill_reliability_scores.py --source mubawab
  python tools/backfill_reliability_scores.py --limit 1000 --dry-run
  python tools/backfill_reliability_scores.py --batch-size 50
        """
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help="Filter by source name (e.g., mubawab, tecnocasa)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Max vectors to process (0 = unlimited, default: {DEFAULT_LIMIT})"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Pinecone upsert batch size (default: {DEFAULT_BATCH_SIZE})"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without updating Pinecone"
    )
    
    args = parser.parse_args()
    
    # Run backfill
    report = backfill_scores(
        source=args.source,
        limit=args.limit,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )
    
    # Exit with appropriate code
    if report.get("status") in ("success", "dry_run_success"):
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
