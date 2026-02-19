from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import List, Optional, Dict, Any

from config.logging_config import log
from core.base_scraper import BaseScraper
from core.pipeline import ScrapingPipeline


class AgentState(Enum):
    IDLE = auto()
    RUNNING = auto()
    ERROR = auto()
    SLEEPING = auto()


class ScrapingStrategy(Enum):
    AGGRESSIVE = auto()
    BALANCED = auto()
    CONSERVATIVE = auto()
    MINIMAL = auto()


@dataclass
class AgentMetrics:
    total_runs: int = 0
    total_listings: int = 0
    total_errors: int = 0
    last_run_started_at: Optional[datetime] = None
    last_run_finished_at: Optional[datetime] = None
    last_error_message: Optional[str] = None
    per_source: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    @property
    def success_rate(self) -> float:
        if self.total_runs == 0:
            return 0.0
        if self.total_listings == 0:
            return 0.0
        if self.total_errors >= self.total_listings:
            return 0.0
        return (self.total_listings - self.total_errors) / self.total_listings

    @property
    def error_rate(self) -> float:
        if self.total_listings == 0:
            return 0.0
        return self.total_errors / self.total_listings


class IntelligentScrapingAgent:
    def __init__(self, scrapers: List[BaseScraper]):
        self.scrapers = scrapers
        self.state = AgentState.IDLE
        self.strategy = ScrapingStrategy.BALANCED
        self.metrics = AgentMetrics()

    def choose_strategy(self) -> ScrapingStrategy:
        error_rate = self.metrics.error_rate
        success_rate = self.metrics.success_rate
        if error_rate > 0.3:
            return ScrapingStrategy.CONSERVATIVE
        if success_rate > 0.9 and error_rate < 0.1:
            return ScrapingStrategy.AGGRESSIVE
        return ScrapingStrategy.BALANCED

    def run_once(self) -> None:
        if not self.scrapers:
            log.warning("Intelligent agent has no scrapers configured")
            return
        self.state = AgentState.RUNNING
        self.metrics.total_runs += 1
        self.metrics.last_run_started_at = datetime.utcnow()
        self.strategy = self.choose_strategy()
        log.info(f"[Agent] Starting run with strategy={self.strategy.name}")
        try:
            pipeline = ScrapingPipeline(self.scrapers)
            pipeline.run()
            self.state = AgentState.IDLE
        except Exception as exc:
            self.state = AgentState.ERROR
            self.metrics.total_errors += 1
            self.metrics.last_error_message = str(exc)
            log.error(f"[Agent] Run failed: {exc}")
        finally:
            self.metrics.last_run_finished_at = datetime.utcnow()
            log.info(
                f"[Agent] Run finished state={self.state.name} "
                f"success_rate={self.metrics.success_rate:.2%} "
                f"error_rate={self.metrics.error_rate:.2%}"
            )

