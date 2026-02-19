# scraping/adapters/mubawab.py
from __future__ import annotations

import asyncio
import json
import logging
import random
import re
from typing import Optional
from urllib.parse import urljoin, urlparse
from datetime import datetime

import httpx
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import warnings

from scraping.models import SearchParams, Listing
from scraping.utils import canonical_url  # ✅ normalize links (remove ? / #)

log = logging.getLogger(__name__)

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

class MubawabAdapter:
    BASE_URL = "https://www.mubawab.tn"
    source_name = "mubawab"

    # ✅ More tolerant:
    # - allow ar
    # - allow ? / # after the id
    # - match based on parsed PATH (not raw href)
    _DETAIL_RE = re.compile(r"^/(fr|en|ar)/(a|pa)/\d+(?:$|[/?#])", re.IGNORECASE)

    _PRICE_RE = re.compile(
        r"(\d{1,3}(?:[ \u00A0]\d{3})+|\d+)\s*(TND|DT|EUR|USD|€|\$)\b",
        re.IGNORECASE,
    )
    _AREA_RE = re.compile(r"(\d{1,3}(?:[ \u00A0]\d{3})+|\d+)\s*m²", re.IGNORECASE)

    def __init__(self, timeout_s: float = 30.0, delay_s: float = 1.5):
        # ✅ bump delay default a bit (0.6s triggers throttling more often)
        self.timeout_s = timeout_s
        self.delay_s = delay_s

        self._headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.7",
        }

        self.client = httpx.AsyncClient(
            headers=self._headers,
            timeout=httpx.Timeout(self.timeout_s),
            follow_redirects=True,
        )

    async def close(self):
        await self.client.aclose()

    async def polite_delay(self):
        # ✅ jitter avoids looking like a bot; also spreads load
        await asyncio.sleep(self.delay_s + random.uniform(0.2, 0.9))

    # -----------------------
    # Required by ScrapeAgent
    # -----------------------

    async def search_urls(self, params: SearchParams) -> list[str]:
        catalogue = self._catalogue_url(params)

        all_urls: list[str] = []
        seen: set[str] = set()

        empty_new_pages = 0

        for page in range(1, params.max_pages + 1):
            page_url = self._page_url(catalogue, page)
            html = await self.fetch(page_url)
            detail_urls = self._extract_detail_urls(html)

            # ✅ if the page has no listing links at all, that's a real end condition
            if not detail_urls:
                break

            new_count = 0
            for u in detail_urls:
                if u not in seen:
                    seen.add(u)
                    all_urls.append(u)
                    new_count += 1

            # ✅ allow 2 consecutive "no new" pages before stopping
            if new_count == 0:
                empty_new_pages += 1
                if empty_new_pages >= 2:
                    break
            else:
                empty_new_pages = 0

        return all_urls

    def extract_lat_lon_from_div(self, html):
        soup = BeautifulSoup(html, "html.parser")
        div = soup.find("div", id="mapOpen")
        if div:
            lat = div.get("lat")
            lon = div.get("lon")
            if lat and lon:
                return float(lat), float(lon)
        return None, None

    async def parse_listing(self, url: str, params: SearchParams) -> Listing:
        html = await self.fetch(url)
        soup = BeautifulSoup(html, "lxml")

        # ✅ Extract latitude and longitude here
        lat, lon = self.extract_lat_lon_from_div(html)

        image_urls = self._pick_images(soup)
        text = soup.get_text("\n", strip=True)

        # OpenGraph image
        og_img = soup.find("meta", attrs={"property": "og:image"})
        if og_img and og_img.get("content"):
            image_urls.append(og_img["content"].strip())

        # JSON-LD images
        for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw = (s.string or s.get_text() or "").strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            objs = data if isinstance(data, list) else [data]
            for obj in objs:
                if isinstance(obj, dict) and "image" in obj:
                    img = obj["image"]
                    if isinstance(img, str):
                        image_urls.append(img)
                    elif isinstance(img, list):
                        image_urls.extend([x for x in img if isinstance(x, str)])

        # De-dup keep order
        uniq = []
        for u in image_urls:
            if u and u not in uniq:
                uniq.append(u)
        image_urls = uniq

        # JSON-LD is the most reliable on Mubawab
        ld = self._pick_from_jsonld(soup)

        title = self._pick_title(soup, text) or ""
        price, currency = (
            (ld.get("price"), ld.get("currency"))
            if ld.get("price") is not None
            else self._pick_price(text)
        )

        city, governorate = (
            (ld.get("city"), ld.get("governorate"))
            if ld.get("city") is not None
            else self._pick_location(soup, text)
        )

        surface_m2 = ld.get("surface_m2") or self._pick_surface(text)
        rooms = ld.get("rooms") or self._pick_int(text, r"(\d+)\s*Chambres?")
        bathrooms = ld.get("bathrooms") or self._pick_int(text, r"(\d+)\s*Salles?\s+de\s+bains?")

        property_type = (
            ld.get("property_type")
            or self._pick_field_after_label(text, "Type de bien")
            or params.property_type
            or "Appartement"
        )

        listing_id = self._extract_listing_id(url)

        # ✅ Pass lat/lon to Listing
        listing = Listing(
            source="mubawab",
            source_listing_id=listing_id,
            transaction=params.transaction,
            url=url,
            image_urls=image_urls,
            title=title,
            price=price,
            currency="TND",
            governorate=governorate,
            city=city,
            zone=None,
            property_type=property_type,
            surface_m2=surface_m2,
            rooms=rooms,
            bathrooms=bathrooms,
            poi={},
            posted_at=None,
            scraped_at=datetime.utcnow(),
            lat=lat,    # added
            lon=lon     # added
        )
        print("LAT:", lat, "LON:", lon)
        return listing


    # -----------------------
    # HTTP
    # -----------------------

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1.0, min=1.0, max=20),
        retry=retry_if_exception_type((httpx.HTTPError, RuntimeError)),
    )
    async def fetch(self, url: str) -> str:
        # ✅ IMPORTANT: do NOT sleep here; ScrapeAgent already calls polite_delay()
        r = await self.client.get(url)

        if r.status_code in (403, 429):
            # respect Retry-After if present
            retry_after = r.headers.get("Retry-After")
            if retry_after:
                try:
                    await asyncio.sleep(float(retry_after))
                except Exception:
                    await asyncio.sleep(5.0)
            raise RuntimeError(f"Blocked/rate-limited ({r.status_code}) on {url}")

        r.raise_for_status()
        return r.text

    # -----------------------
    # Catalogue URL mapping
    # -----------------------

    def _catalogue_url(self, params: SearchParams) -> str:
        pt = (params.property_type or "apartment").lower()
        txn = params.transaction

        if pt in ("apartment", "appartement", "appartements"):
            return (
                "https://www.mubawab.tn/fr/sc/appartements-a-vendre"
                if txn == "sale"
                else "https://www.mubawab.tn/fr/sc/appartements-a-louer"
            )
        if pt in ("house", "maison", "maisons"):
            return (
                "https://www.mubawab.tn/fr/sc/maisons-a-vendre"
                if txn == "sale"
                else "https://www.mubawab.tn/fr/sc/maisons-a-louer"
            )
        if pt in ("villa", "villas"):
            return (
                "https://www.mubawab.tn/fr/sc/villas-et-maisons-de-luxe-a-vendre"
                if txn == "sale"
                else "https://www.mubawab.tn/fr/sc/villas-et-maisons-de-luxe-a-louer"
            )
        if pt in ("land", "terrain", "terrains"):
            return (
                "https://www.mubawab.tn/fr/sc/terrains-a-vendre"
                if txn == "sale"
                else "https://www.mubawab.tn/fr/sc/terrains-a-louer"
            )

        return (
            "https://www.mubawab.tn/fr/sc/appartements-a-vendre"
            if txn == "sale"
            else "https://www.mubawab.tn/fr/sc/appartements-a-louer"
        )

    def _page_url(self, base: str, page: int) -> str:
        return base if page <= 1 else f"{base}?page={page}"

    # -----------------------
    # List page: extract detail URLs
    # -----------------------

    def _extract_detail_urls(self, html: str) -> list[str]:
        soup = BeautifulSoup(html, "lxml")
        out: list[str] = []
        seen: set[str] = set()

        base_netloc = urlparse(self.BASE_URL).netloc

        for a in soup.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if not href:
                continue

            # ✅ always normalize to absolute + remove ? and #
            full = canonical_url(urljoin(self.BASE_URL, href))

            p = urlparse(full)

            # ✅ keep only mubawab.tn links
            if p.netloc and p.netloc != base_netloc:
                continue

            # ✅ match on PATH only
            if not self._DETAIL_RE.match(p.path):
                continue

            if full not in seen:
                seen.add(full)
                out.append(full)

        return out


    # -----------------------
    # JSON-LD helpers
    # -----------------------

    def _extract_json_ld(self, soup: BeautifulSoup) -> list[dict]:
        out: list[dict] = []
        for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw = (s.string or s.get_text() or "").strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue

            if isinstance(data, list):
                out.extend([x for x in data if isinstance(x, dict)])
            elif isinstance(data, dict):
                out.append(data)

        return out

    def _pick_from_jsonld(self, soup: BeautifulSoup) -> dict:
        data = self._extract_json_ld(soup)
        if not data:
            return {}

        node = None
        for obj in data:
            t = obj.get("@type")
            if t == "RealEstateListing" or (isinstance(t, list) and "RealEstateListing" in t):
                node = obj
                break
        if node is None:
            node = data[0]

        result: dict = {}

        offers = node.get("offers") or {}
        if isinstance(offers, dict):
            p = offers.get("price")
            c = offers.get("priceCurrency")
            if p is not None:
                try:
                    result["price"] = float(p)
                except Exception:
                    pass
            if isinstance(c, str) and c.strip():
                result["currency"] = c.strip().upper()

        item = node.get("itemOffered") or {}
        if isinstance(item, dict):
            addr = item.get("address") or {}
            if isinstance(addr, dict):
                city = addr.get("addressLocality")
                gov = addr.get("addressRegion")
                if isinstance(city, str) and city.strip():
                    result["city"] = city.strip()
                if isinstance(gov, str) and gov.strip():
                    result["governorate"] = gov.strip()

            for k, out_k in [
                ("numberOfBedrooms", "rooms"),
                ("numberOfBathroomsTotal", "bathrooms"),
            ]:
                v = item.get(k)
                if v is not None:
                    try:
                        result[out_k] = int(v)
                    except Exception:
                        pass

            pt = item.get("@type")
            if isinstance(pt, str) and pt.strip():
                result["property_type"] = pt.strip()

        return result

    # -----------------------
    # Detail page: field pickers
    # -----------------------

    def _pick_title(self, soup: BeautifulSoup, text: str) -> Optional[str]:
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            return h1.get_text(strip=True)

        og = soup.find("meta", attrs={"property": "og:title"})
        if og and og.get("content"):
            return og["content"]

        for line in text.splitlines():
            line = line.strip()
            if line and len(line) >= 6:
                return line
        return None

    def _pick_price(self, text: str) -> tuple[Optional[float], Optional[str]]:
        if not text or "Prix à consulter" in text:
            return None, None

        for ln in (x.strip() for x in text.splitlines()):
            if not ln:
                continue
            m = self._PRICE_RE.search(ln)
            if not m:
                continue
            num_raw, cur = m.group(1), m.group(2)
            cur = {"€": "EUR", "$": "USD"}.get(cur, cur).upper()
            digits = re.sub(r"[^\d]", "", num_raw)
            if digits:
                return float(int(digits)), cur

        flat = re.sub(r"\s+", " ", text)
        m = self._PRICE_RE.search(flat)
        if not m:
            return None, None
        digits = re.sub(r"[^\d]", "", m.group(1))
        cur = {"€": "EUR", "$": "USD"}.get(m.group(2), m.group(2)).upper()
        return (float(int(digits)) if digits else None, cur)

    def _pick_surface(self, text: str) -> Optional[float]:
        if not text:
            return None
        for ln in (x.strip() for x in text.splitlines()):
            m = self._AREA_RE.search(ln)
            if not m:
                continue
            digits = re.sub(r"[^\d]", "", m.group(1))
            if digits:
                return float(int(digits))
        return None

    # (the rest of your location / helpers / images are unchanged)
    # paste them from your existing file below this point ...

    def _pick_location(self, soup: BeautifulSoup, text: str) -> tuple[Optional[str], Optional[str]]:
        # --- keep your existing implementation here ---
        # (unchanged)
        og_loc = soup.find("meta", attrs={"property": "og:locality"})
        og_reg = soup.find("meta", attrs={"property": "og:region"})
        if og_loc and og_loc.get("content"):
            city = og_loc["content"].strip()
            gov = og_reg["content"].strip() if og_reg and og_reg.get("content") else None
            if city and self._looks_like_place(city) and (gov is None or self._looks_like_place(gov)):
                return city, gov

        candidates: list[str] = []
        for sel in [
            ".adBread a",
            "nav.breadcrumb a",
            ".breadcrumb a",
            ".breadcrumbs a",
            "[class*='breadcrumb'] a",
            "[class*='location']",
        ]:
            for el in soup.select(sel):
                t = el.get_text(" ", strip=True)
                t = self._clean_breadcrumb_token(t)
                if t and 2 <= len(t) <= 40 and self._looks_like_place(t):
                    candidates.append(t)

        uniq: list[str] = []
        for c in candidates:
            if c not in uniq:
                uniq.append(c)

        if len(uniq) >= 2:
            if uniq[-1].lower() == uniq[-2].lower():
                return uniq[-1], None
            return uniq[-1], uniq[-2]
        if len(uniq) == 1:
            return uniq[0], None

        return None, None

    def _clean_breadcrumb_token(self, s: str) -> str:
        if not s:
            return s
        s = s.strip()
        s = re.sub(r"^(Immobilier|Appartements|Maisons|Villas)\s+", "", s, flags=re.IGNORECASE).strip()
        if s.lower() in ("immobilier tunisie", "tunisie"):
            return ""
        return s

    def _looks_like_place(self, s: str) -> bool:
        if not s:
            return False
        low = s.lower().strip()
        blacklist = (
            "vos favoris", "j'accepte", "en cliquant", "ajouter",
            "votre message", "été envoyé", "machine", "laver", "coucher", "manger",
            "prix", "consulter", "durée", "mois", "crédit", "mensualité",
            "contacter", "message",
            "place de parking", "parking", "disposition",
            "à louer", "a louer", "à vendre", "a vendre",
        )
        if any(b in low for b in blacklist):
            return False
        if re.search(r"[\"<>/=]", s):
            return False
        if len(s) > 40:
            return False
        if len(s.split()) > 5:
            return False
        if not re.search(r"[A-Za-zÀ-ÿ]", s):
            return False
        return True

    def _pick_field_after_label(self, text: str, label: str) -> Optional[str]:
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        for i, ln in enumerate(lines):
            if ln == label and i + 1 < len(lines):
                val = lines[i + 1].strip()
                if val and len(val) <= 60 and val != label:
                    return val
        return None

    @staticmethod
    def _pick_int(text: str, pattern: str, default: Optional[int] = None) -> Optional[int]:
        if not text:
            return default
        cleaned = (
            text.replace("\u00a0", " ")
                .replace("\u202f", " ")
                .replace(",", " ")
        )
        m = re.search(pattern, cleaned, flags=re.IGNORECASE)
        if not m:
            return default
        try:
            return int(m.group(1))
        except (ValueError, TypeError):
            return default

    _ID_RE = re.compile(r"/(a|pa)/(\d+)", re.IGNORECASE)

    def _extract_listing_id(self, url: str) -> str:
        m = self._ID_RE.search(urlparse(url).path)
        return m.group(2) if m else url

    def _pick_images(self, soup: BeautifulSoup) -> list[str]:
        # --- keep your existing implementation here (unchanged) ---
        imgs: list[str] = []

        og = soup.find("meta", attrs={"property": "og:image"})
        if og and og.get("content"):
            imgs.append(og["content"].strip())

        for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw = (s.string or s.get_text() or "").strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue

            objs = data if isinstance(data, list) else [data]
            for obj in objs:
                if not isinstance(obj, dict):
                    continue
                image = obj.get("image")
                if isinstance(image, str):
                    imgs.append(image)
                elif isinstance(image, list):
                    imgs.extend([x for x in image if isinstance(x, str)])

                item = obj.get("itemOffered")
                if isinstance(item, dict):
                    image2 = item.get("image")
                    if isinstance(image2, str):
                        imgs.append(image2)
                    elif isinstance(image2, list):
                        imgs.extend([x for x in image2 if isinstance(x, str)])

        for im in soup.find_all("img"):
            for attr in ("src", "data-src", "data-lazy", "data-original", "data-img"):
                u = (im.get(attr) or "").strip()
                if u:
                    imgs.append(u)

            srcset = (im.get("srcset") or "").strip()
            if srcset:
                parts = [p.strip().split(" ")[0] for p in srcset.split(",") if p.strip()]
                imgs.extend([p for p in parts if p])

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if any(href.lower().endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp")):
                imgs.append(href)

        out: list[str] = []
        for u in imgs:
            if not u:
                continue
            u = u.strip()
            if u.startswith("//"):
                u = "https:" + u
            if not (u.startswith("http://") or u.startswith("https://")):
                continue
            if u not in out:
                out.append(u)

        out2 = [u for u in out if "/ad/" in u]
        return out2



    def get_osm_poi(lat, lon, poi_type="school", radius=1000):
        query = f"""
        [out:json][timeout:25];
        (
        node["amenity"="{poi_type}"](around:{radius},{lat},{lon});
        way["amenity"="{poi_type}"](around:{radius},{lat},{lon});
        relation["amenity"="{poi_type}"](around:{radius},{lat},{lon});
        );
        out center;
        """
        
        url = "https://overpass-api.de/api/interpreter"
        response = requests.post(url, data=query)
        data = response.json()
        
        pois = []
        for element in data['elements']:
            name = element.get('tags', {}).get('name', 'unknown')
            lat_ = element.get('lat') or element.get('center', {}).get('lat')
            lon_ = element.get('lon') or element.get('center', {}).get('lon')
            pois.append({"name": name, "lat": lat_, "lon": lon_})
        
        return pois
