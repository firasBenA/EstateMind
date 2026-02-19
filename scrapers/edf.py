from typing import Generator, Optional
import re
from bs4 import BeautifulSoup
from core.base_scraper import BaseScraper
from core.models import PropertyListing, Location
from config.logging_config import log

class EdfScraper(BaseScraper):
    def __init__(self):
        super().__init__(source_name="edf", base_url="https://www.edf.tn")
        self.start_urls = [
            self.base_url,
            f"{self.base_url}/vente",
            f"{self.base_url}/location",
            f"{self.base_url}/fr"
        ]

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        seen = set()
        for url in self.start_urls:
            resp = self._get_request(url)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            links = soup.find_all("a", href=True)
            detail_urls = set()
            for a in links:
                href = a.get("href", "")
                if not href:
                    continue
                if not href.startswith("http"):
                    href = f"{self.base_url.rstrip('/')}/{href.lstrip('/')}"
                if self.base_url in href and any(token in href.lower() for token in ["annonce", "bien", "details", "property", "vente", "louer", "a-vendre", "a-louer"]):
                    detail_urls.add(href)
            for detail in detail_urls:
                if detail in seen:
                    continue
                seen.add(detail)
                r = self._get_request(detail)
                if not r:
                    continue
                listing_id = self._extract_id(detail)
                listing = self.parse_listing(r.text, detail, listing_id)
                if listing:
                    yield listing

    def _extract_id(self, url: str) -> str:
        m = re.search(r"/(\\d+)[-/]", url)
        if m:
            return m.group(1)
        return str(abs(hash(url)))

    def _infer_property_type(self, title: str, description: str) -> str:
        text = (title + " " + (description or "")).lower()
        if "terrain" in text:
            return "Land"
        if "bureau" in text or "commercial" in text:
            return "Office"
        if "villa" in text or "maison" in text or "duplex" in text:
            return "House"
        if "appartement" in text or "studio" in text or "s+" in text:
            return "Apartment"
        return "Other"

    def _infer_transaction_type(self, title: str, description: str) -> str:
        text = (title + " " + (description or "")).lower()
        if "louer" in text or "location" in text or "rent" in text:
            return "Rent"
        if "vendre" in text or "vente" in text or "sell" in text:
            return "Sale"
        return "Sale"

    def _extract_price(self, soup: BeautifulSoup) -> Optional[float]:
        texts = soup.find_all(text=re.compile(r"(DT|TND|\\d[\\d\\s.,]*\\s*DT)"))
        for t in texts:
            num = re.sub(r"[^\\d.]", "", t)
            try:
                if num:
                    return float(num)
            except:
                continue
        return None

    def parse_listing(self, html: str, url: str, listing_id: str) -> Optional[PropertyListing]:
        soup = BeautifulSoup(html, "html.parser")
        title_tag = soup.find(["h1","h2"])
        title = title_tag.get_text(strip=True) if title_tag else "No Title"
        desc_tag = soup.find("p")
        description = desc_tag.get_text(strip=True) if desc_tag else None
        if not description:
            paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
            paragraphs = [t for t in paragraphs if len(t) > 80]
            if paragraphs:
                description = max(paragraphs, key=len)
        if not description:
            log.warning(f"[{self.source_name}] No description extracted for {url}")
        price = self._extract_price(soup)
        transaction_type = self._infer_transaction_type(title, description or "")
        property_type = self._infer_property_type(title, description or "")
        loc = Location()
        imgs = []
        for img in soup.find_all("img", src=True):
            src = img.get("src")
            if src and src.startswith("http"):
                imgs.append(src)
        return PropertyListing(
            source_id=listing_id,
            source_name=self.source_name,
            url=url,
            title=title,
            description=description,
            price=price,
            currency="TND",
            property_type=property_type,
            transaction_type=transaction_type,
            location=loc,
            images=list(dict.fromkeys(imgs))
        )
