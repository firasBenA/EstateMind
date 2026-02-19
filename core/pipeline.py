from typing import List
from core.base_scraper import BaseScraper
from core.models import PropertyListing, POI
from database.mongo_client import PostgresClient
from database.file_storage import FileStorage
from config.logging_config import log


class ScrapingPipeline:
    def __init__(self, scrapers: List[BaseScraper]):
        self.scrapers = scrapers
        self.db = PostgresClient()
        self.storage = FileStorage()

    def run(self):
        log.info("Starting scraping pipeline...")
        for scraper in self.scrapers:
            try:
                self._run_scraper(scraper)
            except Exception as e:
                log.error(f"Scraper {scraper.source_name} failed: {e}")
        
        log.info("Pipeline completed.")
        self.db.close()

    def _run_scraper(self, scraper: BaseScraper):
        log.info(f"Running scraper: {scraper.source_name}")
        stats = {"fetched": 0, "inserted": 0, "updated": 0, "unchanged": 0, "error": 0}
        
        for listing in scraper.run():
            stats["fetched"] += 1
            if not self._validate_listing(listing):
                stats["error"] += 1
                continue

            raw_content = ""
            extension = "json"
            
            if hasattr(listing, 'raw_content') and listing.raw_content:
                 raw_content = listing.raw_content
                 if raw_content.strip().startswith("<"):
                     extension = "html"
                 else:
                     extension = "json"
            else:
                 # Fallback: Serialize the listing itself
                 raw_content = listing.model_dump_json()
                 extension = "json"

            saved_path = self.storage.save_raw_data(listing.source_name, listing.source_id, raw_content, extension)
            if saved_path:
                listing.raw_data_path = saved_path
            
            data_to_insert = listing.model_dump(exclude={'raw_content'})
            
            result_status = self.db.upsert_listing(data_to_insert)
            
            if result_status == "inserted":
                stats["inserted"] += 1
            elif result_status == "updated":
                stats["updated"] += 1
            elif result_status == "unchanged":
                stats["unchanged"] += 1
            else:
                stats["error"] += 1
        log.info(f"Finished {scraper.source_name}: Fetched={stats['fetched']} | Inserted={stats['inserted']} | Updated={stats['updated']} | Unchanged={stats['unchanged']} | Errors={stats['error']}")

    def _validate_listing(self, listing: PropertyListing) -> bool:
        try:
            location = listing.location
        except Exception:
            log.error("Listing missing location object")
            return False

        if not any(
            [
                location.city,
                location.governorate,
                location.address,
                location.latitude,
                location.longitude,
            ]
        ):
            log.error(
                f"Listing {listing.source_name}:{listing.source_id} has no usable location data"
            )
            return False

        pois = getattr(listing, "pois", [])
        valid_categories = {
            "school",
            "hospital",
            "shopping",
            "restaurant",
            "transport",
            "other",
        }
        category_map = {
            "school": "school",
            "ecole": "school",
            "college": "school",
            "university": "school",
            "hospital": "hospital",
            "clinic": "hospital",
            "pharmacy": "hospital",
            "shop": "shopping",
            "mall": "shopping",
            "supermarket": "shopping",
            "restaurant": "restaurant",
            "cafe": "restaurant",
            "transport": "transport",
            "bus": "transport",
            "metro": "transport",
            "train": "transport",
        }
        cleaned_pois = []
        for poi in pois:
            if isinstance(poi, POI):
                name = poi.name
                category = poi.category
            elif isinstance(poi, dict):
                name = poi.get("name")
                category = poi.get("category")
            else:
                continue
            if not name or not category:
                continue
            category_value = str(category).strip().lower()
            normalized_category = category_map.get(category_value, "other")
            if normalized_category not in valid_categories:
                normalized_category = "other"
            if isinstance(poi, POI):
                poi.category = normalized_category
                cleaned_pois.append(poi)
            else:
                cleaned_pois.append(
                    POI(
                        name=str(name).strip(),
                        category=normalized_category,
                        distance_m=poi.get("distance_m"),
                    )
                )

        if cleaned_pois:
            listing.pois = cleaned_pois

        return True
