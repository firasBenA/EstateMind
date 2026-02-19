from __future__ import annotations
from typing import Iterable, Set
from scraping.models import Listing
from scraping.storage import CSVStore
from scraping.postgres_store import PostgresStore

class DualStore:
    """
    Postgres = source of truth
    CSV = backup/export
    """
    def __init__(self, pg: PostgresStore, csv: CSVStore, keep_seen_file: bool = True):
        self.pg = pg
        self.csv = csv
        self.keep_seen_file = keep_seen_file

    def load_seen(self) -> Set[str]:
        seen = self.pg.load_seen()
        if self.keep_seen_file:
            seen |= self.csv.load_seen()
        return seen

    def save_seen(self, seen: Set[str]) -> None:
        self.pg.save_seen(seen)
        if self.keep_seen_file:
            self.csv.save_seen(seen)

    def save(self, listings: Iterable[Listing]) -> None:
        listings = list(listings)
        if not listings:
            return
        self.pg.save(listings)
        self.csv.save(listings)  # uses append_all + upsert_daily
