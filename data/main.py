# """
# EstateMind — Main entry point.

# Usage:
#     python main.py run               # run once immediately
#     python main.py schedule          # run once then every 24h
#     python main.py run --no-vectors  # scrape only, skip Pinecone
#     python main.py run --site mubawab  # single site
#     python main.py status            # show agent status
# """
# from __future__ import annotations

# import argparse
# import sys
# import time
# import schedule
# from typing import Optional

# from dotenv import load_dotenv
# import os
# load_dotenv()

# from config.logging_config import log
# from scrapers.all_scrapers import build_all_scrapers
# from ai_agent.agent import IntelligentScrapingAgent


# def _build_vector_db(strategy: str = "huggingface"):
#     """
#     Initialize Pinecone. Returns None if API key missing (scraping still works).
#     """
#     try:
#         from database.vector_db import VectorDBHandler
#         db = VectorDBHandler(strategy=strategy)
#         log.info(f"Pinecone connected (strategy={strategy})")
#         return db
#     except Exception as e:
#         log.warning(f"Pinecone unavailable: {e} — running without vector storage")
#         return None


# def run_job(
#     store_vectors: bool = True,
#     site_filter: Optional[str] = None,
#     embedding_strategy: str = "huggingface",
#     ) -> dict:
#     """
#     Run one full scraping cycle.
#     Returns the agent summary dict.
#     """
#     scrapers = build_all_scrapers()
#     if site_filter:
#         # Flexible site filtering (case-insensitive, handles aliases)
#         site_map = {s.source_name.lower(): s for s in scrapers}
#         # Add common aliases
#         aliases = {
#             "zitouna": "zitouna_immo",
#             "tunisie-annonce": "tunisieannonce",
#             "tunisie_annonce": "tunisieannonce",
#             "ta": "tunisieannonce",
#             "c21": "century21",
#             "affar": "affare",
#         }
        
#         target = site_filter.lower()
#         if target in aliases:
#             target = aliases[target]
            
#         filtered = [s for s in scrapers if s.source_name.lower() == target]
        
#         if not filtered:
#             available = ", ".join([s.source_name for s in scrapers])
#             log.error(f"Unknown site '{site_filter}'. Available sites: {available}")
#             sys.exit(1)
#         scrapers = filtered

#     vector_db = None
#     if store_vectors:
#         vector_db = _build_vector_db(strategy=embedding_strategy)

#     dedup_disabled = os.getenv("DEDUP_DISABLE", "").lower() in ("1","true","yes","on")
#     agent = IntelligentScrapingAgent(
#         scrapers=scrapers,
#         vector_db=vector_db,
#         store_vectors=store_vectors,
#         deduplicate=not dedup_disabled,
#         pipeline=None,
#         enrich=True,
#         fetch_pois=True,
#     )

#     log.info("=" * 60)
#     log.info(f"Starting EstateMind scraping run ({len(scrapers)} scrapers)")
#     log.info("=" * 60)

#     summary = agent.run_once()

#     log.info("=" * 60)
#     log.info("Run complete:")
#     log.info(f"  Strategy: {summary.get('strategy')}")
#     log.info(f"  Fetched:  {summary.get('total_fetched')}")
#     log.info(f"  Stored:   {summary.get('total_stored')}")
#     log.info(f"  Dupes skipped: {summary.get('total_duplicates_skipped')}")
#     log.info(f"  Error rate: {summary.get('global_error_rate', 0):.1%}")
#     log.info(f"  Elapsed:  {summary.get('elapsed_s')}s")
#     log.info("Per source:")
#     for src, stats in summary.get("per_source", {}).items():
#         log.info(
#             f"  {src:20s} fetched={stats.get('fetched',0)} "
#             f"stored={stats.get('stored',0)} errors={stats.get('errors',0)} "
#             f"status={stats.get('status','?')}"
#         )
#     log.info("=" * 60)

#     if store_vectors and vector_db:
#         try:
#             log.info("=" * 60)
#             log.info("Starting preprocessing pipeline...")
#             from preprocessing.pipeline import PreprocessingPipeline
#             pipeline = PreprocessingPipeline(vector_db)
#             report = pipeline.run(export=True)
#             log.info(f"Preprocessing complete: {report.get('total_records', 0)} records")
#             if 'steps' in report and 'scorer' in report['steps']:
#                 score_dist = report['steps']['scorer'].get('score_distribution', {})
#                 log.info(f"  Score distribution: {score_dist}")
#                 unknown_count = score_dist.get('UNKNOWN', 0)
#                 if unknown_count > 0:
#                     log.warning(f"  ⚠️ {unknown_count} records still have UNKNOWN scores!")
#         except Exception as e:
#             log.error(f"Preprocessing pipeline failed: {e}", exc_info=True)
#     elif not store_vectors:
#         log.info("Preprocessing pipeline skipped: store_vectors=False")
#     elif not vector_db:
#         log.warning("Preprocessing pipeline skipped: vector_db is None")

#     return summary


# def start_scheduler(store_vectors: bool = True, embedding_strategy: str = "huggingface"):
#     log.info("Scheduler started — running every 24 hours")
#     run_job(store_vectors=store_vectors, embedding_strategy=embedding_strategy)
#     schedule.every(24).hours.do(
#         run_job,
#         store_vectors=store_vectors,
#         embedding_strategy=embedding_strategy,
#     )
#     while True:
#         schedule.run_pending()
#         time.sleep(60)


# def main():
#     parser = argparse.ArgumentParser(description="EstateMind Scraping Framework")
#     parser.add_argument("action", choices=["run", "schedule", "status"],
#                         help="run | schedule | status")
#     parser.add_argument("--no-vectors", action="store_true",
#                         help="Disable Pinecone storage")
#     parser.add_argument("--site", type=str, default=None,
#                         help="Scrape a single site by name")
#     parser.add_argument("--strategy", type=str, default="huggingface",
#                         choices=["huggingface", "openai"],
#                         help="Embedding model strategy")
#     args = parser.parse_args()

#     store = not args.no_vectors

#     if args.action == "run":
#         run_job(store_vectors=store, site_filter=args.site,
#                 embedding_strategy=args.strategy)

#     elif args.action == "schedule":
#         start_scheduler(store_vectors=store, embedding_strategy=args.strategy)

#     elif args.action == "status":
#         # Quick status check — build agent without running
#         scrapers = build_all_scrapers()
#         agent = IntelligentScrapingAgent(scrapers=scrapers, store_vectors=False)
#         import json
#         print(json.dumps(agent.status_report(), indent=2, default=str))


# if __name__ == "__main__":
#     try:
#         main()
#     except KeyboardInterrupt:
#         log.info("Stopping — goodbye")
#         sys.exit(0)
#     except Exception as e:
#         log.critical(f"Unhandled exception: {e}")
#         sys.exit(1)

# main.py
"""
EstateMind — Main entry point (pgvector version).

Usage:
    python main.py run               # run once immediately
    python main.py schedule          # run once then every 24h
    python main.py run --no-vectors  # scrape only, skip embeddings
    python main.py run --no-vectors  # scrape only, skip embeddings
    python main.py run --site mubawab  # single site
    python main.py status            # show agent status
"""
from __future__ import annotations

import argparse
import sys
import time
import schedule
from typing import Optional

from dotenv import load_dotenv
import os
import os
load_dotenv()

from config.logging_config import log
from scrapers.all_scrapers import build_all_scrapers
from scrapers.all_scrapers import build_all_scrapers
from ai_agent.agent import IntelligentScrapingAgent


def _build_vector_db(strategy: str = "huggingface"):
    """Initialize pgvector handler."""
    try:
        from database.vector_db import VectorDBHandler
        db = VectorDBHandler(strategy=strategy)
        log.info(f"pgvector connected (strategy={strategy})")
        
        # Show stats
        stats = db.get_stats()
        log.info(f"Database stats: {stats.get('total_listings', 0)} listings, "
                 f"{stats.get('with_text_embeddings', 0)} with embeddings")
        
        return db
    except Exception as e:
        log.warning(f"pgvector unavailable: {e} — running without vector storage")
        log.warning(f"pgvector unavailable: {e} — running without vector storage")
        return None

def auto_sync_to_supabase():
    """Automatically sync to Supabase after scraping."""
    if os.getenv("AUTO_SYNC_SUPABASE", "false").lower() != "true":
        return
    
    try:
        logger.info("Auto-syncing to Supabase...")
        from tools.sync_to_supabase import SupabaseFullSync
        
        syncer = SupabaseFullSync()
        stats = syncer.sync_all()
        
        logger.info(f"Supabase sync: {stats['listings']['success']} listings, "
                   f"{stats['image_embeddings']['success']} images")
        syncer.close()
    except Exception as e:
        logger.warning(f"Supabase auto-sync failed (non-critical): {e}")

def run_job(
    store_vectors: bool = True,
    site_filter: Optional[str] = None,
    embedding_strategy: str = "huggingface",
) -> dict:
    """Run one full scraping cycle."""
    
    scrapers = build_all_scrapers()
    if site_filter:
        aliases = {
            "zitouna": "zitouna_immo",
            "tunisie-annonce": "tunisieannonce",
            "tunisie_annonce": "tunisieannonce",
            "ta": "tunisieannonce",
            "c21": "century21",
            "affar": "affare",
        }
        target = site_filter.lower()
        if target in aliases:
            target = aliases[target]
        filtered = [s for s in scrapers if s.source_name.lower() == target]
        if not filtered:
            available = ", ".join([s.source_name for s in scrapers])
            log.error(f"Unknown site '{site_filter}'. Available sites: {available}")
            sys.exit(1)
        scrapers = filtered
        scrapers = filtered

    vector_db = None
    if store_vectors:
        vector_db = _build_vector_db(strategy=embedding_strategy)

    dedup_disabled = os.getenv("DEDUP_DISABLE", "").lower() in ("1", "true", "yes", "on")
    agent = IntelligentScrapingAgent(
        scrapers=scrapers,
        vector_db=vector_db,
        store_vectors=store_vectors,
        deduplicate=not dedup_disabled,
        pipeline=None,
        enrich=True,
        fetch_pois=True,
        deduplicate=not dedup_disabled,
        pipeline=None,
        enrich=True,
        fetch_pois=True,
    )

    log.info("=" * 60)
    log.info(f"Starting EstateMind scraping run ({len(scrapers)} scrapers)")
    log.info("=" * 60)

    summary = agent.run_once()

    log.info("=" * 60)
    log.info("Run complete:")
    log.info(f"  Strategy: {summary.get('strategy')}")
    log.info(f"  Fetched:  {summary.get('total_fetched')}")
    log.info(f"  Stored:   {summary.get('total_stored')}")
    log.info(f"  Dupes skipped: {summary.get('total_duplicates_skipped')}")
    log.info(f"  Error rate: {summary.get('global_error_rate', 0):.1%}")
    log.info(f"  Elapsed:  {summary.get('elapsed_s')}s")
    for src, stats in summary.get("per_source", {}).items():
        log.info(
            f"  {src:20s} fetched={stats.get('fetched',0)} "
            f"stored={stats.get('stored',0)} errors={stats.get('errors',0)}"
            f"stored={stats.get('stored',0)} errors={stats.get('errors',0)}"
        )
    log.info("=" * 60)

    # Run preprocessing pipeline
    if store_vectors and vector_db:
        try:
            log.info("Starting preprocessing pipeline...")
            from preprocessing.pipeline import PreprocessingPipeline
            pipeline = PreprocessingPipeline(vector_db, force_reprocess=True)
            pipeline = PreprocessingPipeline(vector_db, force_reprocess=True)
            report = pipeline.run(export=True)
            log.info(f"Preprocessing complete: {report.get('total_records', 0)} records processed")
            
            if 'steps' in report and 'scorer' in report['steps']:
                score_dist = report['steps']['scorer'].get('score_distribution', {})
                log.info(f"  Score distribution: {score_dist}")
        except Exception as e:
            log.error(f"Preprocessing pipeline failed: {e}", exc_info=True)
    elif not store_vectors:
        log.info("Preprocessing pipeline skipped: store_vectors=False")
    auto_sync_to_supabase()
    return summary


def start_scheduler(store_vectors: bool = True, embedding_strategy: str = "huggingface"):
    """Run scheduler for periodic scraping."""
    log.info("Scheduler started — running every 24 hours")
    run_job(store_vectors=store_vectors, embedding_strategy=embedding_strategy)
    schedule.every(24).hours.do(
        run_job, 
        store_vectors=store_vectors, 
        embedding_strategy=embedding_strategy
    )
    while True:
        schedule.run_pending()
        time.sleep(60)


def main():
    parser = argparse.ArgumentParser(description="EstateMind Scraping Framework")
    parser.add_argument("action", choices=["run", "schedule", "status", "stats"])
    parser.add_argument("--no-vectors", action="store_true", help="Disable vector storage")
    parser.add_argument("--site", type=str, default=None, help="Scrape single site")
    parser.add_argument("--strategy", type=str, default="huggingface", 
                       choices=["huggingface", "openai"])
    args = parser.parse_args()

    store = not args.no_vectors

    if args.action == "run":
        run_job(store_vectors=store, site_filter=args.site, embedding_strategy=args.strategy)
    
    elif args.action == "schedule":
        start_scheduler(store_vectors=store, embedding_strategy=args.strategy)
    
    elif args.action == "status":
        scrapers = build_all_scrapers()
        agent = IntelligentScrapingAgent(scrapers=scrapers, store_vectors=False)
        import json
        print(json.dumps(agent.status_report(), indent=2, default=str))
    
    elif args.action == "stats":
        db = _build_vector_db()
        if db:
            import json
            print(json.dumps(db.get_stats(), indent=2, default=str))
            db.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Stopping — goodbye")
        sys.exit(0)
    except Exception as e:
        log.critical(f"Unhandled exception: {e}")
        sys.exit(1)