"""
EstateMind — Main entry point.

Usage:
    python main.py run               # run once immediately
    python main.py schedule          # run once then every 24h
    python main.py run --no-vectors  # scrape only, skip Pinecone
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
load_dotenv()

from config.logging_config import log
from scrapers.all_scrapers import build_all_scrapers
from ai_agent.agent import IntelligentScrapingAgent


def _build_vector_db(strategy: str = "huggingface"):
    """
    Initialize Pinecone. Returns None if API key missing (scraping still works).
    """
    try:
        from database.vector_db import VectorDBHandler
        db = VectorDBHandler(strategy=strategy)
        log.info(f"Pinecone connected (strategy={strategy})")
        return db
    except Exception as e:
        log.warning(f"Pinecone unavailable: {e} — running without vector storage")
        return None


def run_job(
    store_vectors: bool = True,
    site_filter: Optional[str] = None,
    embedding_strategy: str = "huggingface",
) -> dict:
    """
    Run one full scraping cycle.
    Returns the agent summary dict.
    """
    scrapers = build_all_scrapers()
    if site_filter:
        scrapers = [s for s in scrapers if s.source_name == site_filter]
        if not scrapers:
            log.error(f"Unknown site '{site_filter}'")
            sys.exit(1)

    vector_db = None
    if store_vectors:
        vector_db = _build_vector_db(strategy=embedding_strategy)

    agent = IntelligentScrapingAgent(
        scrapers=scrapers,
        vector_db=vector_db,
        store_vectors=store_vectors,
        deduplicate=True,
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
    log.info("Per source:")
    for src, stats in summary.get("per_source", {}).items():
        log.info(
            f"  {src:20s} fetched={stats.get('fetched',0)} "
            f"stored={stats.get('stored',0)} errors={stats.get('errors',0)} "
            f"status={stats.get('status','?')}"
        )
    log.info("=" * 60)

    if store_vectors and vector_db:
        log.info("Starting preprocessing pipeline...")
        from preprocessing.pipeline import PreprocessingPipeline
        pipeline = PreprocessingPipeline(vector_db)
        report = pipeline.run(export=True)
        
        log.info(f"Preprocessing complete: {report['total_records']} records")
        log.info(f"Quality scores: {report['steps']['scorer']['score_distribution']}")

    return summary


def start_scheduler(store_vectors: bool = True, embedding_strategy: str = "huggingface"):
    log.info("Scheduler started — running every 24 hours")
    run_job(store_vectors=store_vectors, embedding_strategy=embedding_strategy)
    schedule.every(1).hours.do(
        run_job,
        
        store_vectors=store_vectors,
        embedding_strategy=embedding_strategy,
        
        
    )
    while True:
        schedule.run_pending()
        time.sleep(60)


def main():
    parser = argparse.ArgumentParser(description="EstateMind Scraping Framework")
    parser.add_argument("action", choices=["run", "schedule", "status"],
                        help="run | schedule | status")
    parser.add_argument("--no-vectors", action="store_true",
                        help="Disable Pinecone storage")
    parser.add_argument("--site", type=str, default=None,
                        help="Scrape a single site by name")
    parser.add_argument("--strategy", type=str, default="huggingface",
                        choices=["huggingface", "openai"],
                        help="Embedding model strategy")
    args = parser.parse_args()

    store = not args.no_vectors

    if args.action == "run":
        run_job(store_vectors=store, site_filter=args.site,
                embedding_strategy=args.strategy)

    elif args.action == "schedule":
        start_scheduler(store_vectors=store, embedding_strategy=args.strategy)

    elif args.action == "status":
        # Quick status check — build agent without running
        scrapers = build_all_scrapers()
        agent = IntelligentScrapingAgent(scrapers=scrapers, store_vectors=False)
        import json
        print(json.dumps(agent.status_report(), indent=2, default=str))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Stopping — goodbye")
        sys.exit(0)
    except Exception as e:
        log.critical(f"Unhandled exception: {e}")
        sys.exit(1)
