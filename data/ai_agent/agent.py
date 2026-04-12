"""
EstateMind Intelligent Scraping Agent

Dual-write: Pinecone (vectors) + PostgreSQL (dashboard).
Pipeline enrichment: features, geocoding, zone, optional POIs.
Logs to agent_metrics table after each scraper so dashboard shows live stats.
"""
from __future__ import annotations
import time, random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import List, Optional, Dict, Any
from loguru import logger
from core.models import PropertyListing
from database.vector_db import VectorDBHandler


class AgentState(Enum):
    IDLE=auto(); PLANNING=auto(); RUNNING=auto(); RATE_LIMITED=auto()
    BLOCKED=auto(); HEALING=auto(); SLEEPING=auto(); DONE=auto()

class ScrapingStrategy(Enum):
    AGGRESSIVE=auto(); BALANCED=auto(); CONSERVATIVE=auto(); MINIMAL=auto()

STRATEGY_PARAMS = {
    ScrapingStrategy.AGGRESSIVE:   {"delay_mult": 0.5,  "max_pages": 100},
    ScrapingStrategy.BALANCED:     {"delay_mult": 1.0,  "max_pages": 50},
    ScrapingStrategy.CONSERVATIVE: {"delay_mult": 2.5,  "max_pages": 20},
    ScrapingStrategy.MINIMAL:      {"delay_mult": 5.0,  "max_pages": 5},
}


@dataclass
class SourceState:
    source_name: str
    total_runs: int = 0
    total_fetched: int = 0
    total_errors: int = 0
    consecutive_failures: int = 0
    backoff_level: int = 0
    disabled_until: Optional[datetime] = None
    last_success_at: Optional[datetime] = None
    BASE_COOLDOWN_S = 600
    MAX_COOLDOWN_S = 7200

    @property
    def error_rate(self):
        total = self.total_fetched + self.total_errors
        return self.total_errors / total if total > 0 else 0.0

    def is_available(self):
        if not self.disabled_until: return True
        if datetime.utcnow() >= self.disabled_until:
            self.disabled_until = None
            logger.info(f"[{self.source_name}] Cooldown expired")
            return True
        return False

    def record_run(self, fetched, errors):
        self.total_runs += 1; self.total_fetched += fetched; self.total_errors += errors
        if errors > 0 or fetched == 0:
            self.consecutive_failures += 1
        else:
            self.consecutive_failures = 0; self.last_success_at = datetime.utcnow()
            if self.backoff_level > 0: self.backoff_level = max(0, self.backoff_level - 1)
        if self.consecutive_failures >= 3:
            self.backoff_level += 1
            cooldown = min(self.MAX_COOLDOWN_S, self.BASE_COOLDOWN_S * self.backoff_level)
            self.disabled_until = datetime.utcnow() + timedelta(seconds=cooldown)
            logger.warning(f"[{self.source_name}] Disabled for {cooldown}s (backoff {self.backoff_level})")

    def cooldown_remaining_s(self):
        if not self.disabled_until: return 0
        return max(0, int((self.disabled_until - datetime.utcnow()).total_seconds()))


@dataclass
class AgentMetrics:
    total_runs: int = 0; total_fetched: int = 0; total_stored: int = 0
    total_errors: int = 0; total_duplicates_skipped: int = 0
    last_run_started_at: Optional[datetime] = None
    last_run_finished_at: Optional[datetime] = None
    run_durations_s: List[float] = field(default_factory=list)

    @property
    def avg_run_duration_s(self):
        return sum(self.run_durations_s)/len(self.run_durations_s) if self.run_durations_s else 0.0

    @property
    def global_error_rate(self):
        total = self.total_fetched + self.total_errors
        return self.total_errors / total if total > 0 else 0.0


def _get_pg():
    try:
        from database.mongo_client import PostgresClient
        return PostgresClient()
    except Exception as e:
        logger.warning(f"[Agent] PostgreSQL unavailable — dashboard won't update: {e}")
        return None


class IntelligentScrapingAgent:
    """
    Scraping orchestrator with adaptive strategy, enrichment pipeline,
    and dual-write to Pinecone + PostgreSQL.
    """

    def __init__(self, scrapers, vector_db=None, store_vectors=True,
                 deduplicate=True, pipeline=None, enrich=True, fetch_pois=True):
        self.scrapers = scrapers
        self.vector_db = vector_db
        self.store_vectors = store_vectors and vector_db is not None
        self.deduplicate = deduplicate
        self.enrich = enrich
        self.fetch_pois = fetch_pois
        self.state = AgentState.IDLE
        self.strategy = ScrapingStrategy.BALANCED
        self.metrics = AgentMetrics()
        self.source_states = {s.source_name: SourceState(source_name=s.source_name) for s in scrapers}
        self._pg = _get_pg()

    def _choose_strategy(self):
        er = self.metrics.global_error_rate
        if er > 0.6: return ScrapingStrategy.MINIMAL
        if er > 0.4: return ScrapingStrategy.CONSERVATIVE
        if er < 0.05 and self.metrics.total_runs > 2: return ScrapingStrategy.AGGRESSIVE
        return ScrapingStrategy.BALANCED

    def _heal(self):
        logger.info("[Agent] HEALING — resetting failure counters")
        self.state = AgentState.HEALING
        for ss in self.source_states.values():
            if ss.consecutive_failures >= 3:
                ss.consecutive_failures = 0; ss.disabled_until = None
                ss.backoff_level = max(0, ss.backoff_level - 1)
        self.strategy = ScrapingStrategy.CONSERVATIVE
        time.sleep(300)

    def _handle_rate_limit(self, source_name, wait_s=90):
        logger.warning(f"[{source_name}] Rate-limited — waiting {wait_s}s")
        self.state = AgentState.RATE_LIMITED
        time.sleep(wait_s + random.uniform(0, 30))
        self.state = AgentState.RUNNING

    # def _enrich(self, listing: PropertyListing) -> PropertyListing:
    #     # 1. Features
    #     if not listing.features:
    #         try:
    #             from core.feature_extraction import enrich_listing_features
    #             listing = enrich_listing_features(listing)
    #         except Exception as e:
    #             logger.debug(f"features {listing.source_id}: {e}")
    #     # 2. Geocoding
    #     if not listing.location.latitude or not listing.location.longitude:
    #         try:
    #             from core.geolocation import geocode_location
    #             lat, lon, muni = geocode_location(
    #                 city=listing.location.city,
    #                 governorate=listing.location.governorate,
    #                 address=listing.location.address,
    #             )
    #             if lat and lon:
    #                 listing.location.latitude = lat; listing.location.longitude = lon
    #             if muni and not listing.location.municipalite:
    #                 listing.location.municipalite = muni
    #         except Exception as e:
    #             logger.debug(f"geocode {listing.source_id}: {e}")
    #     # 3. Zone
    #     if listing.location.governorate and not listing.location.zone:
    #         try:
    #             from core.base_scraper import infer_zone
    #             listing.location.zone = infer_zone(listing.location.governorate)
    #         except Exception as e:
    #             logger.debug(f"zone {listing.source_id}: {e}")
    #     # 4. POIs (optional, slow)
    #     if self.fetch_pois and not listing.pois and listing.location.latitude and listing.location.longitude:
    #         try:
    #             from core.geolocation import fetch_pois
    #             listing.pois = fetch_pois(listing.location.latitude, listing.location.longitude)
    #         except Exception as e:
    #             logger.debug(f"pois {listing.source_id}: {e}")
    #     return listing
    def _enrich(self, listing: PropertyListing) -> PropertyListing:
        """Enrich listing with features, geocoding, zone, and POIs"""
        # 1. Features extraction (from DOM or LLM-like fallback)
        if not listing.features:
            try:
                from core.feature_extraction import enrich_listing_features
                listing = enrich_listing_features(listing)
            except Exception as e:
                logger.debug(f"features extraction failed for {listing.source_id}: {e}")

        # 2. Geocoding - ONLY if coordinates are missing (don't overwrite!)
        if (not listing.location.latitude or not listing.location.longitude):
            try:
                from core.geolocation import geocode_location
                lat, lon, muni = geocode_location(
                    city=listing.location.city,
                    governorate=listing.location.governorate,
                    address=listing.location.address,
                )
                if lat and lon:
                    listing.location.latitude = lat
                    listing.location.longitude = lon
                if muni and not listing.location.municipalite:
                    listing.location.municipalite = muni
            except Exception as e:
                logger.debug(f"geocoding failed for {listing.source_id}: {e}")
        else:
            logger.debug(f"Skipping geocoding for {listing.source_id} - coordinates already present")

        # 3. Zone inference (based on governorate) - ONLY if zone missing
        if listing.location.governorate and not listing.location.zone:
            try:
                from core.base_scraper import infer_zone
                listing.location.zone = infer_zone(listing.location.governorate)
            except Exception as e:
                logger.debug(f"zone inference failed for {listing.source_id}: {e}")

        # 4. POIs - ONLY if coordinates exist and POIs are missing
        if (self.fetch_pois and not listing.pois and 
            listing.location.latitude and listing.location.longitude):
            try:
                from core.geolocation import fetch_pois
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(
                        fetch_pois, 
                        listing.location.latitude, 
                        listing.location.longitude
                    )
                    try:
                        listing.pois = future.result(timeout=10)
                    except concurrent.futures.TimeoutError:
                        logger.debug(f"POI fetch timeout for {listing.source_id}")
            except Exception as e:
                logger.debug(f"POI enrichment failed for {listing.source_id}: {e}")
        
        return listing

    def _store(self, listing: PropertyListing) -> bool:
        stored = False
        # Pinecone
        if self.store_vectors and self.vector_db is not None:
            if self.deduplicate and self.vector_db.check_duplicate(listing):
                self.metrics.total_duplicates_skipped += 1
            else:
                if self.vector_db.upsert_listing(listing):
                    stored = True
        # PostgreSQL — always independent of Pinecone
        if self._pg is not None:
            try:
                result = self._pg.upsert_listing(listing.model_dump())
                if result in ("inserted", "updated"):
                    stored = True
                    logger.debug(f"[PG] {result} {listing.source_id}")
            except Exception as e:
                logger.warning(f"[PG] {listing.source_id}: {e}")
        return stored

    def _maybe_store(self, listing: PropertyListing) -> bool:
        if self.enrich:
            listing = self._enrich(listing)
        return self._store(listing)

    def _log_metrics(self, source_name, run_start, run_end, fetched, stored, errors, ss):
        if self._pg is None: return
        try:
            self._pg.log_agent_metrics({
                "run_started_at":       run_start,
                "run_finished_at":      run_end,
                "source_name":          source_name,
                "strategy":             self.strategy.name,
                "fetched":              fetched,
                "inserted":             stored,
                "updated":              0,
                "unchanged":            max(0, fetched - stored - errors),
                "errors":               errors,
                "consecutive_failures": ss.consecutive_failures,
                "disabled_until":       ss.disabled_until,
            })
        except Exception as e:
            logger.warning(f"[Agent] metrics log {source_name}: {e}")

    def run_once(self) -> Dict[str, Any]:
        self.state = AgentState.PLANNING
        self.metrics.total_runs += 1
        run_start = datetime.utcnow()
        self.metrics.last_run_started_at = run_start
        self.strategy = self._choose_strategy()
        params = STRATEGY_PARAMS[self.strategy]
        logger.info(f"[Agent] Run #{self.metrics.total_runs} | strategy={self.strategy.name} | delay_mult={params['delay_mult']}x")

        active = [s for s in self.scrapers if self.source_states[s.source_name].is_available()]
        if not active:
            logger.warning("[Agent] All sources in cooldown — SLEEPING")
            self.state = AgentState.SLEEPING
            shortest = min(ss.cooldown_remaining_s() for ss in self.source_states.values())
            time.sleep(max(60, shortest))
            self.state = AgentState.IDLE
            return {"status": "slept"}

        self.state = AgentState.RUNNING
        all_stats: Dict[str, Dict] = {}

        for scraper in active:
            sname = scraper.source_name
            ss = self.source_states[sname]
            scraper_start = datetime.utcnow()
            logger.info(f"[Agent] Starting: {sname}")
            fetched = stored = errors = 0

            try:
                for listing in scraper.run():
                    fetched += 1; self.metrics.total_fetched += 1
                    if self._maybe_store(listing):
                        stored += 1; self.metrics.total_stored += 1
                    time.sleep(random.uniform(1.0, 3.0) * params["delay_mult"])

            except Exception as exc:
                errors += 1; self.metrics.total_errors += 1
                err_msg = str(exc).lower()
                if "captcha" in err_msg or "403" in err_msg:
                    logger.error(f"[{sname}] BLOCKED: {exc}")
                    ss.record_run(fetched=0, errors=1)
                    self._log_metrics(sname, scraper_start, datetime.utcnow(), fetched, stored, errors, ss)
                    all_stats[sname] = {"fetched": fetched, "stored": stored, "errors": errors, "status": "blocked"}
                    continue
                if "429" in err_msg or "rate" in err_msg:
                    logger.warning(f"[{sname}] RATE_LIMITED")
                    self._handle_rate_limit(sname)
                    try:
                        for listing in scraper.run():
                            fetched += 1
                            if self._maybe_store(listing): stored += 1
                    except Exception as e2:
                        errors += 1; logger.error(f"[{sname}] retry failed: {e2}")
                logger.error(f"[{sname}] Error: {exc}")

            scraper_end = datetime.utcnow()
            elapsed = (scraper_end - scraper_start).total_seconds()
            ss.record_run(fetched=fetched, errors=errors)
            self._log_metrics(sname, scraper_start, scraper_end, fetched, stored, errors, ss)
            all_stats[sname] = {
                "fetched": fetched, "stored": stored, "errors": errors,
                "elapsed_s": round(elapsed, 1), "error_rate": round(ss.error_rate, 3),
                "status": "ok" if errors == 0 else "partial",
            }
            logger.info(f"[{sname}] Done: fetched={fetched} stored={stored} errors={errors} ({elapsed:.0f}s)")

        run_elapsed = (datetime.utcnow() - run_start).total_seconds()
        self.metrics.run_durations_s.append(run_elapsed)
        self.metrics.last_run_finished_at = datetime.utcnow()
        if self.metrics.global_error_rate > 0.6 and self.metrics.total_runs > 1:
            self._heal()
        if self._pg:
            try: self._pg.close()
            except Exception: pass
            self._pg = None
        self.state = AgentState.DONE

        summary = {
            "status": "completed", "run": self.metrics.total_runs,
            "strategy": self.strategy.name,
            "total_fetched": self.metrics.total_fetched,
            "total_stored": self.metrics.total_stored,
            "total_duplicates_skipped": self.metrics.total_duplicates_skipped,
            "global_error_rate": round(self.metrics.global_error_rate, 3),
            "elapsed_s": round(run_elapsed, 1), "per_source": all_stats,
        }
        logger.info(f"[Agent] Complete | fetched={self.metrics.total_fetched} stored={self.metrics.total_stored} elapsed={run_elapsed:.0f}s")
        return summary

    def status_report(self) -> Dict[str, Any]:
        return {
            "state": self.state.name, "strategy": self.strategy.name,
            "metrics": {
                "total_runs": self.metrics.total_runs,
                "total_fetched": self.metrics.total_fetched,
                "total_stored": self.metrics.total_stored,
                "total_duplicates_skipped": self.metrics.total_duplicates_skipped,
                "global_error_rate": round(self.metrics.global_error_rate, 3),
                "avg_run_duration_s": round(self.metrics.avg_run_duration_s, 1),
            },
            "sources": {
                name: {
                    "available": ss.is_available(),
                    "consecutive_failures": ss.consecutive_failures,
                    "backoff_level": ss.backoff_level,
                    "error_rate": round(ss.error_rate, 3),
                    "cooldown_remaining_s": ss.cooldown_remaining_s(),
                    "total_fetched": ss.total_fetched,
                } for name, ss in self.source_states.items()
            },
        }
