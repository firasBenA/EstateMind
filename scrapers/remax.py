
import time
import re
from typing import Iterator
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from core.base_scraper import BaseScraper
from core.models import PropertyListing
from config.logging_config import log

class RemaxScraper(BaseScraper):
    """
    Scraper for Remax Tunisia (remax.com.tn).
    Uses Selenium for dynamic content rendering.
    """
    def __init__(self):
        super().__init__("remax", "https://www.remax.com.tn")
        self.driver = None
        self.categories = [
            {"url": f"{self.base_url}/vente-immobilier-tunisie", "type": "Sale"},
            # Remax often puts rentals in a separate filter or path, but let's try a standard guess or skip if unknown.
            # Usually /location-immobilier-tunisie
             {"url": f"{self.base_url}/location-immobilier-tunisie", "type": "Rent"}
        ]

    def _setup_driver(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        options.add_argument("--log-level=3")
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)

    def run(self) -> Iterator[PropertyListing]:
        try:
            self._setup_driver()
            log.info(f"Starting {self.source_name} scraper with Selenium...")
            
            for cat in self.categories:
                yield from self.fetch_listings(cat["url"], cat["type"])
            
        except Exception as e:
            log.error(f"Error running Remax scraper: {e}")
        finally:
            if self.driver:
                self.driver.quit()

    def fetch_listings(self, url: str, transaction_type: str) -> Iterator[PropertyListing]:
        page = 1
        max_pages = 20
        
        while page <= max_pages:
            # Try query parameter pagination first
            page_url = f"{url}?page={page}"
            log.info(f"Scraping {transaction_type} page {page}: {page_url}")
            
            try:
                self.driver.get(page_url)
                
                # Wait for listings
                # Remax cards often have class 'gallery-item' or links to '/annonce/'
                WebDriverWait(self.driver, 20).until(
                    lambda d: len(d.find_elements(By.TAG_NAME, "a")) > 10
                )
                time.sleep(2)
                
                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                
                # Find listing links
                # Remax URL pattern: /annonce/ID-SLUG
                links = soup.find_all("a", href=True)
                unique_urls = set()
                
                for link in links:
                    href = link['href']
                    if "/annonce/" in href:
                        if not href.startswith("http"):
                            href = f"{self.base_url.rstrip('/')}/{href.lstrip('/')}"
                        unique_urls.add(href)
                
                if not unique_urls:
                    log.warning(f"No listings found on page {page}. Stopping {transaction_type}.")
                    break
                
                log.info(f"Found {len(unique_urls)} listings on page {page}")
                
                for link_url in unique_urls:
                    try:
                        # Process listing
                        # For now, we can just extract from the grid page if possible, 
                        # or visit the detail page. Visiting is safer for full data.
                        # But to save time/bandwidth, let's visit.
                        
                        listing_id = "unknown"
                        match = re.search(r'/annonce/(\d+)-', link_url)
                        if match:
                            listing_id = match.group(1)
                        else:
                            listing_id = str(abs(hash(link_url)))

                        # Visit detail page
                        # Optimization: We could parse data from the grid, but let's stick to visiting for quality.
                        self.driver.get(link_url)
                        # Wait for title or price
                        try:
                            WebDriverWait(self.driver, 10).until(
                                lambda d: d.find_element(By.TAG_NAME, "h1")
                            )
                        except:
                            pass # might just be slow, parse anyway
                        
                        detail_soup = BeautifulSoup(self.driver.page_source, "html.parser")
                        listing = self.parse_listing(detail_soup, link_url, listing_id, transaction_type)
                        if listing:
                            yield listing
                            
                    except Exception as e:
                        log.error(f"Error processing listing {link_url}: {e}")
                
                page += 1
                
            except Exception as e:
                log.error(f"Error fetching page {page}: {e}")
                break

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

    def parse_listing(self, soup: BeautifulSoup, url: str, listing_id: str, transaction_type: str) -> PropertyListing:
        title_tag = soup.find("h1")
        title = title_tag.get_text(strip=True) if title_tag else "No Title"
        
        price = 0.0
        # Remax price often in a specific class
        price_tag = soup.find(class_="price") or soup.find(text=re.compile(r'DT'))
        if price_tag:
            pt = price_tag.get_text(strip=True) if hasattr(price_tag, 'get_text') else str(price_tag)
            clean = re.sub(r'[^\d]', '', pt)
            if clean:
                try:
                    price = float(clean)
                except:
                    pass

        description = ""
        desc_tag = soup.find(class_="description") or soup.find(id="description")
        if desc_tag:
            description = desc_tag.get_text(strip=True)

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
            images=list(dict.fromkeys(images)),
            description=description,
            raw_content=str(soup)
        )
