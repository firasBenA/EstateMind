
import requests
import time
import re
from typing import Generator, Dict, Any, Optional, List
from datetime import datetime
from bs4 import BeautifulSoup

from core.base_scraper import BaseScraper
from core.models import PropertyListing, Location
from config.logging_config import log

class ZitounaImmoScraper(BaseScraper):
    """
    Scraper for Zitouna Immobilier (zitounaimmo.com).
    Scrapes Sale and Rent listings.
    """
    
    def __init__(self):
        super().__init__(source_name="zitounaImmo", base_url="https://www.zitounaimmo.com")
        self.categories = [
            {"url": f"{self.base_url}/acheter", "type": "Sale"},
            {"url": f"{self.base_url}/louer", "type": "Rent"}
        ]

    def fetch_listings(self) -> Generator[Dict[str, Any], None, None]:
        """
        Iterates through categories and pages, fetching detail pages for each listing.
        Yields a dictionary containing raw HTML and metadata.
        """
        log.info(f"Starting {self.source_name} scraper...")
        
        for category in self.categories:
            cat_url = category["url"]
            trans_type = category["type"]
            page = 1
            has_next = True
            
            while has_next:
                url = f"{cat_url}?page={page}"
                log.info(f"Scraping {trans_type} page {page}: {url}")
                
                response = self._get_request(url)
                if not response:
                    break
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find listing cards
                # Based on analysis, listings are likely in 'div.col-md-4' or similar, 
                # but we can look for links to /bien/details/
                links = soup.find_all("a", href=lambda h: h and "/bien/details/" in h)
                
                # Deduplicate links on the page (often image and title link to same)
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
                        # Extract ID from URL for raw file naming
                        # URL format: .../details/ID-SLUG
                        # Example: .../details/6532-a-louer... -> ID 6532
                        listing_id = "unknown"
                        match = re.search(r'/details/(\d+)-', link_url)
                        if match:
                            listing_id = match.group(1)
                        else:
                            # Fallback hash
                            listing_id = str(abs(hash(link_url)))

                        # Fetch Detail Page
                        log.info(f"Fetching detail: {link_url}")
                        detail_resp = self._get_request(link_url)
                        
                        if detail_resp:
                            yield {
                                "source_id": listing_id,
                                "url": link_url,
                                "raw_html": detail_resp.text,
                                "transaction_type": trans_type,
                                "scraped_at": datetime.now().isoformat()
                            }
                        
                    except Exception as e:
                        log.error(f"Error processing listing {link_url}: {e}")
                
                # Pagination Check
                # Check for "next" button or if we found listings
                # If we found 0 listings, we broke above.
                # If we found listings, assume next page exists unless we check pagination controls.
                # Heuristic: If < 10 listings (assuming 10-12 per page), maybe last page.
                # Safest: Check for pagination 'active' or 'next'.
                pagination = soup.find("ul", class_="pagination")
                if not pagination or not pagination.find_all("li"):
                    has_next = False
                else:
                    # Check if current page is the last one
                    # Usually "Next" or simply increment.
                    # We'll just increment and let the empty check handle stop.
                    # But to be safe against infinite loops:
                    if page > 50: # Safety limit
                        has_next = False
                    else:
                        page += 1

    def parse_listing(self, raw_data: Dict[str, Any]) -> Optional[PropertyListing]:
        """
        Parses the detail page HTML into a PropertyListing object.
        """
        try:
            html = raw_data["raw_html"]
            soup = BeautifulSoup(html, 'html.parser')
            
            # 1. Title
            # Try multiple selectors
            title_elem = soup.find("div", class_="detail-title") or soup.find("h1") or soup.find("h2")
            if not title_elem:
                # Try finding text inside detail-header
                header = soup.find("div", class_="detail-header")
                if header:
                    title_elem = header.find("h2") or header.find("h3")
            
            title = title_elem.get_text(strip=True) if title_elem else "Unknown Title"
            
            # 2. Price
            # Selector: li.color-default.price or div.fp_price or just text search
            price = 0.0
            currency = "TND"
            
            price_text = ""
            price_elem = soup.find(class_="fp_price")
            if not price_elem:
                # Try finding in li with class price
                price_elem = soup.select_one("li.price")
            
            if not price_elem:
                # Fallback: regex search for price pattern in the whole text (risky but better than 0)
                # Look for "X DT" or "X TND"
                # But careful about "Ref" or other numbers.
                # Let's check specific list items first.
                price_items = soup.find_all(string=re.compile(r'\s*\d[\d\s]*\s*(DT|TND)'))
                for p in price_items:
                    # check if parent is likely a price container
                    if p.parent.name in ['div', 'span', 'strong', 'li']:
                        price_text = p
                        break

            if price_elem:
                price_text = price_elem.get_text(strip=True)
            
            if price_text:
                # Clean price: "3 500 DT" -> 3500
                clean_p = re.sub(r'[^\d]', '', price_text)
                if clean_p:
                    price = float(clean_p)
            
            # 3. Location
            # Often found in a specific div or breadcrumb or address field
            # Look for "Tunis, La Marsa" style text
            # From analysis: div.detail-header contains location?
            # Or map address.
            address = ""
            city = ""
            gov = ""
            district = ""

            address_li = None
            for li in soup.find_all("li"):
                span = li.find("span")
                label = span.get_text(strip=True) if span else ""
                if "adresse" in label.lower():
                    address_li = li
                    break
            if address_li:
                li_text = address_li.get_text(" ", strip=True)
                m_addr = re.search(r"Adresse\s*:?(.*)", li_text, re.IGNORECASE)
                if m_addr:
                    addr_str = m_addr.group(1).strip()
                    if addr_str:
                        address = addr_str
                        parts = [p.strip() for p in addr_str.split(",") if p.strip()]
                        if len(parts) == 1:
                            city = parts[0]
                        elif len(parts) == 2:
                            gov = parts[0]
                            city = parts[1]
                        elif len(parts) >= 3:
                            gov = parts[0]
                            city = parts[1]
                            district = ", ".join(parts[2:])

            # Try to find location in the text content or specific classes
            # Zitouna usually puts location in the header or under title
            # Let's try to extract from text or specific meta
            # Using a heuristic for now if specific class unknown
            # Ref6169a ... 3 500 DT\nLocation ... Tunis, La Marsa
            
            # 4. Characteristics (Surface, Rooms)
            surface_m2 = None
            rooms = None
            
            # Search for m²
            surf_elem = soup.find(string=re.compile(r"m²"))
            if surf_elem:
                s_text = surf_elem.strip() # "120 m²"
                s_match = re.search(r'(\d+)\s*m²', s_text)
                if s_match:
                    surface_m2 = float(s_match.group(1))
            
            # Search for Chambres
            room_elem = soup.find(string=re.compile(r"Chambre"))
            if room_elem:
                r_text = room_elem.strip() # "2 Chambres"
                r_match = re.search(r'(\d+)\s*Chambre', r_text, re.IGNORECASE)
                if r_match:
                    rooms = int(r_match.group(1))

            # 5. Description
            description = ""
            # Try specific headers
            desc_header = soup.find(lambda tag: tag.name in ["h3", "h4", "h5"] and "Description" in tag.get_text())
            if desc_header:
                # content might be in next sibling div or p
                # iterate next siblings
                for sib in desc_header.next_siblings:
                    if sib.name in ["div", "p"]:
                        description += sib.get_text(strip=True) + "\n"
                    if sib.name == "h3": # Stop at next header
                        break
            
            if not description:
                 # Fallback: generic content divs
                 desc_div = soup.select_one("div.listing_detail_description") or soup.find("div", class_="content")
                 if desc_div:
                     description = desc_div.get_text(strip=True)
            
            if not description:
                # Fallback: Find the container with most text (heuristic)
                # This is risky, let's try to find a div with class 'mb30' or 'col-lg-8' which usually holds content
                content_col = soup.find("div", class_="col-lg-8")
                if content_col:
                     # Remove potential junk like title/price if they are in there
                     description = content_col.get_text(strip=True)

            # 6. Images
            images = []
            # 1. Standard img tags
            img_tags = soup.find_all("img")
            for img in img_tags:
                src = img.get("src")
                if src and "upload" in src and not src.endswith(".svg"):
                    if not src.startswith("http"):
                        src = f"{self.base_url.rstrip('/')}/{src.lstrip('/')}"
                    if src not in images:
                        images.append(src)
            
            # 2. Links to images (Lightboxes)
            a_tags = soup.find_all("a", href=True)
            for a in a_tags:
                href = a.get("href")
                if href and "upload" in href and href.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')):
                    if not href.startswith("http"):
                        href = f"{self.base_url.rstrip('/')}/{href.lstrip('/')}"
                    if href not in images:
                        images.append(href)
            
            # 3. Background images (common in sliders)
            divs_with_bg = soup.find_all("div", style=lambda s: s and "background-image" in s)
            for div in divs_with_bg:
                style = div.get("style")
                match = re.search(r'url\([\'"]?([^\'"]+)[\'"]?\)', style)
                if match:
                    src = match.group(1)
                    if "upload" in src:
                        if not src.startswith("http"):
                            src = f"{self.base_url.rstrip('/')}/{src.lstrip('/')}"
                        if src not in images:
                            images.append(src)

            # 4. Fallback: Search in Script tags (JSON or arrays)
            if not images:
                script_text = ""
                for script in soup.find_all("script"):
                    if script.string:
                        script_text += script.string
                
                # Regex for image urls in scripts
                # Look for typical upload paths
                img_matches = re.findall(r'[\'"]([^\'"]*upload[^\'"]*\.(?:jpg|jpeg|png|webp))[\'"]', script_text)
                for src in img_matches:
                    # Unescape slashes
                    src = src.replace('\\/', '/')
                    if not src.startswith("http"):
                         src = f"{self.base_url.rstrip('/')}/{src.lstrip('/')}"
                    if src not in images:
                        images.append(src)

            # 5. Data attributes (lazy loading)
            for tag in soup.find_all(True):
                for attr in ["data-src", "data-background", "data-image", "data-lazy", "data-bg"]:
                    if tag.has_attr(attr):
                        val = tag[attr]
                        if val and "upload" in val and val.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')):
                             if not val.startswith("http"):
                                 val = f"{self.base_url.rstrip('/')}/{val.lstrip('/')}"
                             if val not in images:
                                 images.append(val)

            # 7. Property Type (guess from title or category)
            # Zitouna listings often have type in title "Appartement...", "Villa..."
            title_lower = title.lower()
            prop_type = "Other"
            if "appartement" in title_lower:
                prop_type = "Apartment"
            elif any(x in title_lower for x in ["villa", "maison", "demeure"]):
                prop_type = "Villa"
            elif "terrain" in title_lower:
                prop_type = "Land"
            elif any(x in title_lower for x in ["bureau", "commerce", "local"]):
                prop_type = "Commercial"

            # 8. Location parsing (Basic)
            # Attempt to find text like "Tunis, La Marsa"
            # We can look for common Tunisian cities in the whole text if needed
            # For now, default to empty or extract if we find a pattern
            
            loc_obj = Location(
                address=address,
                city=city,
                governorate=gov,
                district=district
            )

            return PropertyListing(
                source_id=raw_data["source_id"],
                source_name=self.source_name,
                url=raw_data["url"],
                title=title,
                price=price,
                currency=currency,
                property_type=prop_type,
                transaction_type=raw_data["transaction_type"],
                location=loc_obj,
                images=images,
                description=description,
                surface_area_m2=surface_m2, # Correct field name
                rooms=rooms,
                scraped_at=datetime.fromisoformat(raw_data["scraped_at"]),
                raw_content=html
            )

        except Exception as e:
            log.error(f"[{self.source_name}] Error parsing listing {raw_data.get('url')}: {e}")
            return None
