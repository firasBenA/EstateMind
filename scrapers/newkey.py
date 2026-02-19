
import requests
import re
from typing import Generator
from bs4 import BeautifulSoup
from core.base_scraper import BaseScraper
from core.models import PropertyListing
from config.logging_config import log

class NewKeyScraper(BaseScraper):
    """
    Scraper for Newkey (newkey.com.tn).
    """
    def __init__(self):
        super().__init__(source_name="newkey", base_url="https://www.newkey.com.tn")
        self.categories = [
            {"url": f"{self.base_url}/acheter", "type": "Sale"},
            {"url": f"{self.base_url}/louer", "type": "Rent"}
        ]

    def fetch_listings(self) -> Generator[PropertyListing, None, None]:
        log.info(f"Starting {self.source_name} scraper...")
        
        for category in self.categories:
            cat_url = category["url"]
            trans_type = category["type"]
            page = 1
            max_pages = 50
            
            while page <= max_pages:
                url = f"{cat_url}?page={page}"
                log.info(f"Scraping {trans_type} page {page}: {url}")
                
                response = self._get_request(url)
                if not response:
                    break
                
                soup = BeautifulSoup(response.text, 'html.parser')
                links = soup.find_all("a", href=lambda h: h and "/bien/details/" in h)
                
                unique_urls = set()
                for link in links:
                    href = link.get('href')
                    if not href.startswith("http"):
                        href = f"{self.base_url.rstrip('/')}/{href.lstrip('/')}"
                    unique_urls.add(href)
                
                if not unique_urls:
                    log.warning(f"No listings found on page {page}. Stopping {trans_type}.")
                    break
                
                log.info(f"Found {len(unique_urls)} listings on page {page}")
                
                for link_url in unique_urls:
                    try:
                        listing_id = "unknown"
                        match = re.search(r'/details/(\d+)-', link_url)
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
        price_tags = soup.find_all(string=re.compile(r'DT'))
        for pt in price_tags:
            clean = re.sub(r'[^\d]', '', pt)
            if len(clean) > 3: 
                try:
                    price = float(clean)
                    break
                except:
                    continue

        full_text = soup.get_text(" ", strip=True)
        full_text = re.sub(r"\s+", " ", full_text)

        description = ""
        desc_tag = soup.find("div", class_="description")
        if desc_tag:
            description = desc_tag.get_text(" ", strip=True)
        if not description:
            m = re.search(r"Description\s+(.+?)(Pays:|Détail|Detail|Référence|Reference|Prix:)", full_text, re.IGNORECASE)
            if m:
                description = m.group(1).strip()
        if not description:
            paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
            paragraphs = [t for t in paragraphs if len(t) > 80]
            if paragraphs:
                description = max(paragraphs, key=len)
        if not description:
            log.warning(f"[{self.source_name}] No description extracted for {url}")

        surface = None
        rooms = None
        m_surface = re.search(r"Surface\s+habitable\s*:\s*(\d+)", full_text, re.IGNORECASE)
        if m_surface:
            try:
                surface = float(m_surface.group(1))
            except:
                surface = None
        m_rooms = re.search(r"Nb\.?chambres\s*:\s*(\d+)", full_text, re.IGNORECASE)
        if m_rooms:
            try:
                rooms = int(m_rooms.group(1))
            except:
                rooms = None

        images = []
        gallery = soup.find("div", class_="carousel-inner") or soup.find("div", class_="gallery") or soup
        if gallery:
            for img in gallery.find_all("img", src=True):
                src = img.get("src")
                if not src:
                    continue
                if src.startswith("//"):
                    src = "https:" + src
                elif src.startswith("/"):
                    src = f"{self.base_url.rstrip('/')}/{src.lstrip('/')}"
                if src.startswith("http"):
                    images.append(src)
        if not images:
            images = []

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
            location={"city": "Tunis", "governorate": "Tunis"},
            surface_area_m2=surface,
            rooms=rooms,
            images=list(dict.fromkeys(images)),
            description=description,
            raw_content=html_content
        )
