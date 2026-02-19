
import time
import random
import re
from datetime import datetime
from typing import Iterator, Dict, Any, Optional
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from core.base_scraper import BaseScraper
from core.models import PropertyListing, Location
from config.logging_config import log

class AqariScraper(BaseScraper):
    def __init__(self):
        super().__init__("aqari", "https://www.aqari.tn")
        self.driver = None
        self.detail_driver = None

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
        self.detail_driver = webdriver.Chrome(service=service, options=options)

    def run(self) -> Iterator[PropertyListing]:
        try:
            self._setup_driver()
            log.info(f"Starting {self.source_name} scraper with Selenium...")
            
            # 1. Scrape Sale Listings
            yield from self.fetch_listings("Sale")
            
            # 2. Scrape Rent Listings
            yield from self.fetch_listings("Rent")
            
        except Exception as e:
            log.error(f"Error running Aqari scraper: {e}")
        finally:
            if self.driver:
                self.driver.quit()
            if self.detail_driver:
                self.detail_driver.quit()

    def fetch_listings(self, transaction_type: str) -> Iterator[PropertyListing]:
        # Valid endpoints based on inspection: /vente, /location
        category_url = "vente" if transaction_type == "Sale" else "location"
        url = f"{self.base_url}/{category_url}"
        
        log.info(f"Scraping {transaction_type} from: {url}")
        self.driver.get(url)
        
        # Wait for listings to load (hydration)
        # We wait for the specific 'bg-card' class or links with 'property' in href
        try:
            WebDriverWait(self.driver, 20).until(
                lambda d: len(d.find_elements(By.CSS_SELECTOR, "a[href*='/property/']")) > 0
            )
            time.sleep(2) # Extra buffer for images/text
        except Exception:
             log.warning("Timeout waiting for listings to load. Trying to parse anyway.")

        # Pagination Loop
        page = 1
        max_pages = 20 # Limit for now
        
        while page <= max_pages:
            log.info(f"Processing page {page}")
            
            # Parse current page source with BeautifulSoup
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            
            # Find all links that point to a property
            # The <a> tag wraps the entire card
            property_links = [a for a in soup.find_all("a", href=True) if "/property/" in a["href"]]
            
            if not property_links:
                log.warning(f"No listings found on page {page}. Stopping.")
                break
                
            count = 0
            # Use a set to avoid duplicates on the same page (if any)
            seen_urls = set()
            
            for item in property_links:
                try:
                    href = item["href"]
                    if href in seen_urls:
                        continue
                    seen_urls.add(href)
                    
                    listing = self.parse_listing(item, transaction_type)
                    if listing:
                        yield listing
                        count += 1
                except Exception as e:
                    log.error(f"[{self.source_name}] Error parsing listing card {href}: {e}")
            
            log.info(f"Found {count} listings on page {page}")
            
            # Handle Pagination
            # Look for "Next" button.
            # In modern SPAs, pagination might be buttons with numbers or "Suivant"
            # We need to find the pagination container first.
            try:
                # Scroll to bottom
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                
                # Find the "Next" button. 
                # Common patterns: aria-label="Next", text "Suivant", or > icon
                # Let's look for a button or link with text "Suivant" or an arrow
                next_buttons = self.driver.find_elements(By.XPATH, "//button[contains(text(), 'Suivant') or contains(text(), 'Next')] | //a[contains(text(), 'Suivant') or contains(text(), 'Next')]")
                
                # If not found by text, try by aria-label or specific classes if we knew them.
                # For now, let's rely on text or simple arrow chars
                if not next_buttons:
                     next_buttons = self.driver.find_elements(By.XPATH, "//button[contains(text(), '>')] | //a[contains(text(), '>')]")

                # If still not found, maybe check for the active page + 1
                if not next_buttons:
                    # Logic: Find the button for page + 1
                    next_page_num = page + 1
                    next_buttons = self.driver.find_elements(By.XPATH, f"//button[text()='{next_page_num}'] | //a[text()='{next_page_num}']")

                if next_buttons:
                    # Click the last one found (usually pagination is at bottom)
                    # Ensure it's clickable
                    btn = next_buttons[-1]
                    if btn.is_enabled():
                        btn.click()
                        time.sleep(random.uniform(3, 5)) # Wait for load
                        page += 1
                    else:
                        log.info("Next button disabled.")
                        break
                else:
                    log.info("No next page button found.")
                    break
            except Exception as e:
                log.error(f"Error navigating to next page: {e}")
                break

    def parse_listing(self, item: Any, transaction_type: str) -> Optional[PropertyListing]:
        try:
            # URL
            relative_url = item["href"]
            if not relative_url.startswith("http"):
                 url = f"{self.base_url.rstrip('/')}/{relative_url.lstrip('/')}"
            else:
                 url = relative_url
            
            # ID
            # Extract from URL: /property/{uuid}
            source_id = "unknown"
            match = re.search(r'/property/([a-zA-Z0-9-]+)', url)
            if match:
                source_id = match.group(1)
            else:
                 import hashlib
                 source_id = hashlib.md5(url.encode()).hexdigest()

            # The item IS the <a> tag which contains the card details
            
            # Title
            title_tag = item.select_one("h3")
            title = title_tag.get_text(strip=True) if title_tag else "No Title"

            # Price
            # Found in: p class="text-2xl font-bold text-primary"
            price = 0.0
            price_tag = item.select_one("p.text-2xl") # Less specific class to be safe
            if price_tag:
                price_text = price_tag.get_text(strip=True)
                # Remove non-numeric except dot/comma
                p_str = re.sub(r'[^\d.,]', '', price_text).replace(",", ".")
                try:
                    price = float(p_str)
                except:
                    pass
            
            # Location
            # Found near map-pin icon
            location_obj = Location()
            # Look for svg with map-pin class or path, then get sibling text
            # Using simple text search in the card might be safer/easier
            # But let's try the structure: div > svg + span
            # The structure seen: div.flex.items-center.gap-1.text-muted-foreground > span
            loc_span = item.select_one("svg.lucide-map-pin + span")
            if not loc_span:
                 # Try searching for text in text-muted-foreground
                 muted_divs = item.select("div.text-muted-foreground")
                 for div in muted_divs:
                     if len(div.get_text(strip=True)) > 3: # "tunis, tunis"
                         loc_span = div
                         break
            
            if loc_span:
                loc_text = loc_span.get_text(strip=True)
                parts = [p.strip() for p in loc_text.split(',')]
                if len(parts) >= 2:
                    location_obj.city = parts[0]
                    location_obj.governorate = parts[1]
                else:
                    location_obj.city = loc_text

            # Details: Surface and Rooms
            surface_area = None
            rooms = None
            
            # Surface: look for "m²"
            # Rooms: look for "ch" or "Chambres" or specific icon
            
            # Iterate all small text elements
            details_text = item.get_text(" ", strip=True)
            
            # Surface regex
            surf_match = re.search(r'(\d+)\s*m²', details_text, re.IGNORECASE)
            if surf_match:
                try:
                    surface_area = float(surf_match.group(1))
                except:
                    pass
            
            # Rooms regex
            room_match = re.search(r'(\d+)\s*ch', details_text, re.IGNORECASE)
            if room_match:
                try:
                    rooms = int(room_match.group(1))
                except:
                    pass
            
            # Image
            image_url = None
            img_tag = item.find("img")
            if img_tag and img_tag.get("src"):
                image_url = img_tag["src"]

            # Property Type Guess
            prop_type = "Other"
            title_lower = title.lower()
            if "appartement" in title_lower:
                prop_type = "Apartment"
            elif "villa" in title_lower or "maison" in title_lower:
                prop_type = "Villa"
            elif "terrain" in title_lower:
                prop_type = "Land"
            elif "bureau" in title_lower or "commerce" in title_lower:
                prop_type = "Commercial"

            detail_description = ""
            detail_surface = None
            detail_price = None
            detail_features = []

            if self.detail_driver:
                try:
                    self.detail_driver.get(url)
                    WebDriverWait(self.detail_driver, 20).until(
                        lambda d: len(d.page_source) > 5000
                    )
                    detail_html = self.detail_driver.page_source
                    detail_soup = BeautifulSoup(detail_html, "html.parser")
                    full_text = detail_soup.get_text(" ", strip=True)
                    full_text = re.sub(r"\s+", " ", full_text)

                    price_match = re.search(r"(\d[\d\s]{3,})\s*DT", full_text, re.IGNORECASE)
                    if price_match:
                        price_digits = re.sub(r"\s", "", price_match.group(1))
                        try:
                            detail_price = float(price_digits)
                        except:
                            detail_price = None

                    surf_match_detail = re.search(r"(\d+)\s*m²", full_text, re.IGNORECASE)
                    if surf_match_detail:
                        try:
                            detail_surface = float(surf_match_detail.group(1))
                        except:
                            detail_surface = None

                    paragraphs = [p.get_text(" ", strip=True) for p in detail_soup.find_all(["p", "div"])]
                    candidates = [p for p in paragraphs if len(p) > 80]
                    chosen_desc = None
                    for p_text in candidates:
                        lower = p_text.lower()
                        if any(k in lower for k in ["local commercial", "appartement", "villa", "maison", "terrain", "m²", "dt"]):
                            chosen_desc = p_text
                            break
                    if not chosen_desc and candidates:
                        chosen_desc = candidates[0]
                    if chosen_desc:
                        detail_description = chosen_desc

                    rent_match = re.search(r"(\d[\d\s]{3,})\s*DT\s*/\s*mois", full_text, re.IGNORECASE)
                    if rent_match:
                        rent_digits = re.sub(r"\s", "", rent_match.group(1))
                        detail_features.append(f"loyer_mensuel:{rent_digits} DT")

                    if re.search(r"R\+3", full_text, re.IGNORECASE):
                        detail_features.append("R+3 possible")

                    if not detail_description:
                        log.warning(f"[{self.source_name}] No description extracted from detail page {url}")
                except Exception as e:
                    log.error(f"[{self.source_name}] Error scraping detail page {url}: {e}")

            if detail_price is not None:
                price = detail_price
            if detail_surface is not None:
                surface_area = detail_surface
            description = detail_description or ""

            # Create listing object
            return PropertyListing(
                source_name=self.source_name,
                source_id=source_id,
                url=url,
                title=title,
                price=price,
                currency="TND",
                transaction_type=transaction_type,
                property_type=prop_type,
                location=location_obj,
                surface_area_m2=surface_area,
                rooms=rooms,
                images=[image_url] if image_url else [],
                description=description,
                features=detail_features,
                published_at=datetime.utcnow()
            )

        except Exception as e:
            log.error(f"[{self.source_name}] Error parsing item from card: {e}")
            return None
