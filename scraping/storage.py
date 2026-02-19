import csv
from pathlib import Path
from typing import Iterable, Set, Dict, Any, List
from datetime import date, datetime

from scraping.models import Listing

DATA_DIR = Path("data")
SEEN_FILE = DATA_DIR / "seen_urls.txt"
ALL_FILE = DATA_DIR / "listings_all.csv"


class CSVStore:
    def __init__(self):
        DATA_DIR.mkdir(exist_ok=True)
        
    
    def save(self, listings: Iterable[Listing]) -> None:
        """
        Compatibility method used by ScrapeAgent.
        Writes both master history + daily upsert.
        """
        self.append_all(listings)
        self.upsert_daily(listings)

    # -----------------------
    # Seen URLs
    # -----------------------
    def load_seen(self) -> Set[str]:
        if not SEEN_FILE.exists():
            return set()
        return set(SEEN_FILE.read_text(encoding="utf-8").splitlines())

    def save_seen(self, seen: Set[str]) -> None:
        SEEN_FILE.write_text("\n".join(sorted(seen)), encoding="utf-8")

    # -----------------------
    # Helpers
    # -----------------------
    def _fieldnames(self) -> List[str]:
        return list(Listing.model_fields.keys())

    def _read_csv(self, path: Path) -> List[Dict[str, Any]]:
        if not path.exists():
            return []
        with path.open("r", newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    def _write_csv(self, path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)

    def _append_csv(self, path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not path.exists()
        with path.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                w.writeheader()
            w.writerows(rows)

    def _ts(self, row: Dict[str, Any]) -> datetime:
        """
        Parse scraped_at for "keep latest" logic.
        Works with '2026-01-30 21:46:16.800636' and ISO forms.
        Missing/bad values -> very old timestamp.
        """
        s = (row.get("scraped_at") or "").strip()
        if not s:
            return datetime.min
        try:
            return datetime.fromisoformat(s)
        except Exception:
            return datetime.min

    # -----------------------
    # Master file: listings_all.csv
    # -----------------------
    def append_all(self, listings: Iterable[Listing]) -> Path:
        """
        Append-only master dataset (keeps history).
        """
        fieldnames = self._fieldnames()
        rows = [l.model_dump(mode="json") for l in listings]
        if rows:
            self._append_csv(ALL_FILE, rows, fieldnames)
        else:
            # ensure file exists with header
            if not ALL_FILE.exists():
                self._write_csv(ALL_FILE, [], fieldnames)
        return ALL_FILE

    # -----------------------
    # Daily file: listings_YYYY-MM-DD.csv
    # -----------------------
    def upsert_daily(self, listings: Iterable[Listing]) -> Path:
        """
        Daily file that accumulates across multiple runs in the same day.
        Upserts by url: keep the latest scraped_at per url.
        """
        today = date.today().isoformat()
        out = DATA_DIR / f"listings_{today}.csv"
        fieldnames = self._fieldnames()

        new_rows = [l.model_dump(mode="json") for l in listings]

        # If no new rows, keep existing file (or create header-only)
        if not new_rows:
            if not out.exists():
                self._write_csv(out, [], fieldnames)
            return out

        old_rows = self._read_csv(out)

        # Upsert by URL (keep latest scraped_at)
        by_url: Dict[str, Dict[str, Any]] = {}

        for r in old_rows:
            u = self._as_str(r.get("url")).strip()
            if not u:
                continue
            by_url[u] = r

        for r in new_rows:
            u = self._as_str(r.get("url")).strip()
            if not u:
                continue
            if u not in by_url:
                by_url[u] = r
            else:
                # keep the most recently scraped version
                if self._ts(r) >= self._ts(by_url[u]):
                    by_url[u] = r

        merged = list(by_url.values())

        # (optional) stable ordering by scraped_at then url
        merged.sort(key=lambda x: (self._ts(x), x.get("url", "")))

        self._write_csv(out, merged, fieldnames)
        return out

    def _as_str(self, v) -> str:
        if v is None:
            return ""
        return str(v)
