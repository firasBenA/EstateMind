from typing import Generator, Dict, Any, Optional, List
from core.base_scraper import BaseScraper
from core.models import PropertyListing, Location
from config.logging_config import log
import time
import random
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

class TayaraScraper(BaseScraper):
    def __init__(self):
        super().__init__("tayara", "https://www.tayara.tn")

    def fetch_listings(self) -> Generator[Dict[str, Any], None, None]:
        log.info("Starting Tayara scraper with Selenium + BS4 Snapshot...")
        
        chrome_options = Options()
        chrome_options.add_argument("--headless") 
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")
        chrome_options.add_argument(f"user-agent={self.ua.random}")
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(60)
        
        try:
            # Iterate through first few pages
            for page in range(1, 3):
                url = f"{self.base_url}/ads/c/Immobilier/"
                if page > 1:
                    url += f"?page={page}"
                
                log.info(f"Navigating to {url}")
                try:
                    driver.get(url)
                except TimeoutException:
                    log.warning(f"Page load timed out for {url}. Attempting to scrape anyway...")
                    driver.execute_script("window.stop();")
                except Exception as e:
                    log.error(f"Failed to load page {url}: {e}")
                    continue
                
                try:
                    # Wait for articles to load
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.TAG_NAME, "article"))
                    )
                    # Give a little extra time for dynamic content to settle
                    time.sleep(2)
                except Exception:
                    log.warning(f"Timeout waiting for articles on page {page}")
                    # We continue anyway, maybe some loaded
                
                # --- SNAPSHOT STRATEGY ---
                # Capture the HTML once. This prevents StaleElementReferenceException completely.
                html_source = driver.page_source
                soup = BeautifulSoup(html_source, 'html.parser')
                
                articles = soup.find_all("article")
                count = len(articles)
                log.info(f"BS4 found {count} articles on page {page}")
                
                if count == 0:
                    log.warning("No articles found in DOM. Checking __NEXT_DATA__...")
                    # Future: Parse JSON here if needed
                
                for i, article in enumerate(articles):
                    try:
                        # 1. Raw HTML
                        raw_html = str(article)
                        
                        # 2. Link & ID
                        listing_url = None
                        link_elem = article.find("a")
                        if link_elem and link_elem.get("href"):
                            href = link_elem.get("href")
                            if href.startswith("/"):
                                listing_url = f"{self.base_url}{href}"
                            else:
                                listing_url = href

                        # 3. Image
                        images = []
                        img_elem = article.find("img")
                        if img_elem and img_elem.get("src"):
                            images.append(img_elem.get("src"))

                        # 4. Text Content Extraction
                        # Get all text, stripped
                        text_content = list(article.stripped_strings)
                        
                        price = 0.0
                        title = "Unknown"
                        location_text = ""
                        
                        found_price = False
                        
                        for text in text_content:
                            # Price usually contains 'DT' or looks like a number
                            if not found_price and ("DT" in text or any(char.isdigit() for char in text)):
                                clean_p = text.replace("DT", "").replace(" ", "").replace(",", ".").strip()
                                # Check if it's a valid number (allowing for dots)
                                if clean_p.replace(".", "").isdigit() and len(clean_p) < 15:
                                    try:
                                        price = float(clean_p)
                                        found_price = True
                                        continue # Don't use price as title
                                    except:
                                        pass
                            
                            # If it's not the price, and we don't have a title yet, it's likely the title
                            if title == "Unknown" and len(text) > 3 and not "DT" in text:
                                title = text
                                continue
                                
                            # If we have title and price, next might be location
                            if title != "Unknown" and location_text == "" and len(text) > 3:
                                location_text = text
                        
                        # Fallback ID
                        source_id = ""
                        if listing_url:
                            # /ads/12345-title -> 12345-title
                            parts = listing_url.split('/')
                            if parts:
                                source_id = parts[-1]
                        
                        if not source_id:
                            source_id = f"unknown_{hash(title)}_{random.randint(1000,9999)}"

                        yield {
                            "source_id": source_id,
                            "url": listing_url,
                            "title": title,
                            "price": price,
                            "location": location_text,
                            "images": images,
                            "raw_html": raw_html,
                            "scraped_at": datetime.now().isoformat()
                        }
                        
                    except Exception as e:
                        log.error(f"Error processing article index {i}: {e}")
                        continue

        except Exception as e:
            log.error(f"Selenium fatal error: {e}")
        finally:
            driver.quit()

    def parse_listing(self, raw_data: Dict[str, Any]) -> Optional[PropertyListing]:
        try:
            if not raw_data.get("url"):
                return None
                
            return PropertyListing(
                source_id=raw_data["source_id"],
                source_name=self.source_name,
                url=raw_data["url"],
                title=raw_data["title"],
                description=None, 
                price=raw_data["price"] if raw_data["price"] > 0 else None,
                currency="TND",
                property_type="Unknown",
                transaction_type="Sale",
                location=Location(
                    city=raw_data["location"],
                    governorate=raw_data["location"]
                ),
                images=raw_data["images"],
                raw_content=raw_data.get("raw_html"),
                scraped_at=datetime.fromisoformat(raw_data["scraped_at"])
            )
        except Exception as e:
            log.error(f"Failed to parse listing {raw_data.get('source_id')}: {e}")
            return None
