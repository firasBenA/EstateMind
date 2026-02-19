from abc import ABC, abstractmethod
from typing import List, Dict, Any, Generator, Optional
import requests
import time
from fake_useragent import UserAgent
from config.settings import settings
from config.logging_config import log
from core.models import PropertyListing

class BaseScraper(ABC):
    def __init__(self, source_name: str, base_url: str):
        self.source_name = source_name
        self.base_url = base_url
        self.session = requests.Session()
        self.ua = UserAgent()
        self.session.headers.update({
            "User-Agent": settings.USER_AGENT
        })

    def _get_request(self, url: str, params: dict = None) -> Optional[requests.Response]:
        attempt = 1
        while attempt <= settings.REQUEST_RETRIES:
            time.sleep(settings.DELAY_MIN)
            try:
                headers = {"User-Agent": self.ua.random}
                response = self.session.get(url, params=params, headers=headers, timeout=settings.REQUEST_TIMEOUT)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt == settings.REQUEST_RETRIES:
                    log.error(f"[{self.source_name}] Request failed for {url}: {e}")
                    return None
                log.warning(f"[{self.source_name}] Request attempt {attempt} failed for {url}: {e}")
                attempt += 1

    @abstractmethod
    def fetch_listings(self) -> Generator[Dict[str, Any], None, None]:
        """
        Yields raw listing data (dict) or HTML content to be parsed.
        Should handle pagination internally.
        """
        pass

    @abstractmethod
    def parse_listing(self, raw_data: Any) -> Optional[PropertyListing]:
        """
        Parses raw data into a PropertyListing model.
        """
        pass
    
    def run(self) -> Generator[PropertyListing, None, None]:
        """
        Main execution method.
        """
        log.info(f"Starting scraper for {self.source_name}")
        for item in self.fetch_listings():
            try:
                # Check if item is already a PropertyListing object
                is_listing = isinstance(item, PropertyListing)
                if not is_listing and hasattr(item, '__class__') and item.__class__.__name__ == 'PropertyListing':
                    is_listing = True
                
                if is_listing:
                    yield item
                else:
                    parsed = self.parse_listing(item)
                    if parsed:
                        yield parsed
            except Exception as e:
                log.error(f"[{self.source_name}] Error processing listing: {e}")
