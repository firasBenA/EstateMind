
import requests
import re
from typing import Generator
from bs4 import BeautifulSoup
from core.base_scraper import BaseScraper
from core.models import PropertyListing
from config.logging_config import log

class Century21Scraper(BaseScraper):
    """
    Scraper for Century 21 Tunisia (century21.tn).
    """
    def __init__(self):
        super().__init__(source_name="century21", base_url="https://century21.tn")
        self.categories = [
            {"url": f"{self.base_url}/status/vente-immobilier-tunisie", "type": "Sale"},
            {"url": f"{self.base_url}/status/location-immobilier-tunisie", "type": "Rent"}
        ]

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        log.info(f"Starting {self.source_name} scraper...")
        
        for category in self.categories:
            cat_url = category["url"]
            trans_type = category["type"]
            page = 1
            max_pages = 50
            
            while page <= max_pages:
                # WordPress pagination style: .../page/2/
                url = f"{cat_url}/" if page == 1 else f"{cat_url}/page/{page}/"
                log.info(f"Scraping {trans_type} page {page}: {url}")
                
                response = self._get_request(url)
                if not response or response.status_code == 404:
                    log.info(f"Page {page} not found or error. Stopping {trans_type}.")
                    break
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Heuristic for listing links in C21
                # Usually look for 'detail' or 'ref'
                links = soup.find_all("a", href=True)
                unique_urls = set()
                
                for link in links:
                    href = link['href']
                    # Filter for likely property pages
                    # C21 uses /property/slug format now
                    if "/property/" in href:
                        # Avoid category links
                        if "status" in href or "page" in href:
                            continue
                        if href not in unique_urls and href.startswith("http"):
                            unique_urls.add(href)

                if not unique_urls:
                    log.warning(f"No listings found on page {page}. Stopping {trans_type}.")
                    break
                
                log.info(f"Found {len(unique_urls)} listings on page {page}")
                
                for link_url in unique_urls:
                    try:
                        listing_id = "unknown"
                        # Extract ID from URL if possible
                        # https://century21.tn/detail/ref-1234
                        match = re.search(r'ref-([a-zA-Z0-9]+)', link_url)
                        if match:
                            listing_id = match.group(1)
                        else:
                            listing_id = str(abs(hash(link_url)))

                        log.info(f"Fetching detail: {link_url}")
                        detail_resp = self._get_request(link_url)
                        
                        if detail_resp:
                            listing = self.parse_listing(detail_resp.text, link_url, listing_id, trans_type)
                            if listing:
                                yield listing
                        
                    except Exception as e:
                        log.error(f"Error processing listing {link_url}: {e}")
                
                page += 1

    def _infer_property_type(self, title: str, description: str) -> str:
        text = (title + " " + description).lower()
        if "terrain" in text:
            return "Land"
        if "bureau" in text or "commercial" in text:
            return "Office"
        if "villa" in text or "maison" in text:
            return "House"
        if "appartement" in text:
            return "Apartment"
        if "s+" in text:
            return "Apartment"
        return "Other"

    def parse_listing(self, html_content: str, url: str, listing_id: str, transaction_type: str) -> PropertyListing:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        title_tag = soup.find("h1")
        title = title_tag.get_text(strip=True) if title_tag else "No Title"
        
        price = 0.0
        price_text = ""
        # C21 specific price class often 'item-price' or similar
        price_tag = soup.find(class_="item-price") or soup.find(class_="price")
        if price_tag:
            price_text = price_tag.get_text(strip=True)
            clean = re.sub(r'[^\d]', '', price_text)
            if clean:
                try:
                    price = float(clean)
                except:
                    pass

        description = ""
        desc_tag = soup.find(class_="detail-description") or soup.find("div", id="description")
        if desc_tag:
            description = desc_tag.get_text(" ", strip=True)
        if not description:
            section = soup.find("div", id="property-description-wrap") or soup.find("div", class_="property-description-wrap")
            if section:
                paragraphs = [p.get_text(" ", strip=True) for p in section.find_all("p")]
                paragraphs = [t for t in paragraphs if len(t) > 40]
                if paragraphs:
                    description = " ".join(paragraphs)
        if not description:
            paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
            paragraphs = [t for t in paragraphs if len(t) > 80]
            if paragraphs:
                description = max(paragraphs, key=len)
        if not description:
            log.warning(f"[{self.source_name}] No description extracted for {url}")
        
        images = []
        for a in soup.find_all("a", class_="houzez-photoswipe-trigger"):
            img = a.find("img", src=True)
            if img:
                src = img.get("src")
                if src and src.startswith("http"):
                    images.append(src)
        if not images:
            for img in soup.find_all("img", src=True):
                src = img.get("src")
                if not src or not src.startswith("http"):
                    continue
                if any(token in src for token in ["Properties", "digitaloceanspaces", "/bien/", "/property/"]):
                    images.append(src)
        if not images:
            images = []
            
        location_text = ""
        addr_tag = soup.find(class_="detail-address")
        if addr_tag:
            location_text = addr_tag.get_text(strip=True)
            
        property_type = self._infer_property_type(title, description)

        return PropertyListing(
            source_name=self.source_name,
            source_id=listing_id,
            url=url,
            title=title,
            price=price,
            currency="TND",
            property_type=property_type,
            transaction_type=transaction_type,
            location={"city": "Tunis", "address": location_text},
            images=list(dict.fromkeys(images)),
            description=description,
            raw_content=html_content
        )
