# core/geolocation.py
from typing import Dict, Any, Tuple, List, Optional
import math
import time
import json
import re
import unicodedata
from functools import lru_cache
from pathlib import Path
import sqlite3
import os

import requests

from core.models import Location, POI
from config.logging_config import log

_POI_CACHE_DB = None

def _get_poi_cache_db():
    """Get SQLite connection for POI caching"""
    global _POI_CACHE_DB
    if _POI_CACHE_DB is None:
        cache_path = Path("data/poi_cache.db")
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        _POI_CACHE_DB = sqlite3.connect(str(cache_path))
        _POI_CACHE_DB.execute("""
            CREATE TABLE IF NOT EXISTS poi_cache (
                lat REAL,
                lon REAL,
                radius INTEGER,
                data TEXT,
                cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (lat, lon, radius)
            )
        """)
        _POI_CACHE_DB.commit()
    return _POI_CACHE_DB


# ─── Location data from data.ts ──────────────────────────────────────────────

@lru_cache(maxsize=1)
def _load_tunisia_municipalities() -> List[Dict[str, Any]]:
    """Load all governorates and delegations from data.ts"""
    try:
        # geolocation.py is in core/ so data.ts is at ../data/data.ts
        base_dir = Path(__file__).resolve().parent.parent
        ts_path = base_dir / "data" / "data.ts"
        
        if not ts_path.exists():
            log.error(f"data.ts not found at {ts_path}")
            return []
            
        raw = ts_path.read_text(encoding="utf-8")
        
    except Exception as e:
        log.error(f"Failed to read Tunisia municipalities file: {e}")
        return []
    
    # Parse the data.ts file
    m = re.search(r"export\s+const\s+data\s*=\s*(\[.*\]);?\s*$", raw, re.S)
    if not m:
        log.error("Could not locate data array in data.ts")
        return []
    
    payload = m.group(1)
    # Fix JSON format
    for key in ("Name", "NameAr", "Value", "Delegations", "PostalCode", "Latitude", "Longitude"):
        payload = re.sub(rf"{key}\s*:", f'"{key}":', payload)
    payload = re.sub(r",(\s*[}\]])", r"\1", payload)
    
    try:
        data = json.loads(payload)
        log.info(f"Loaded {len(data)} governorates from data.ts")
        return data
    except Exception as e:
        log.error(f"Failed to parse Tunisia municipalities JSON: {e}")
        return []

@lru_cache(maxsize=1)
def _get_governorate_mapping() -> Dict[str, str]:
    """Create mapping from delegation names to governorates"""
    mapping = {}
    data = _load_tunisia_municipalities()
    
    for gov in data:
        gov_name = gov.get("Name", "")
        gov_value = gov.get("Value", "")
        
        # Add governorate names
        mapping[gov_name.lower()] = gov_name
        if gov_value:
            mapping[gov_value.lower()] = gov_name
        
        # Add all delegations
        for del_gov in gov.get("Delegations", []):
            del_name = del_gov.get("Name", "")
            del_value = del_gov.get("Value", "")
            
            if del_name:
                mapping[del_name.lower()] = gov_name
            if del_value:
                mapping[del_value.lower()] = gov_name
            
            # Also add the city name (without parentheses)
            if '(' in del_name:
                city = del_name.split('(')[0].strip()
                if city:
                    mapping[city.lower()] = gov_name
    
    log.info(f"Created location mapping with {len(mapping)} entries")
    return mapping


@lru_cache(maxsize=1)
def _get_delegation_coords() -> Dict[str, Tuple[float, float]]:
    """Get coordinates for each delegation from data.ts"""
    coords = {}
    data = _load_tunisia_municipalities()
    
    for gov in data:
        for del_gov in gov.get("Delegations", []):
            del_name = del_gov.get("Name", "")
            del_value = del_gov.get("Value", "")
            lat = del_gov.get("Latitude")
            lon = del_gov.get("Longitude")
            
            if lat and lon:
                if del_name:
                    coords[del_name.lower()] = (lat, lon)
                if del_value:
                    coords[del_value.lower()] = (lat, lon)
    
    log.info(f"Loaded coordinates for {len(coords)} delegations")
    return coords


def infer_governorate(text: str) -> Optional[str]:
    """Improved governorate inference using data.ts"""
    if not text:
        return None
    
    text_lower = text.lower().strip()
    mapping = _get_governorate_mapping()
    
    # Direct match
    if text_lower in mapping:
        return mapping[text_lower]
    
    # Partial match
    for key, gov in mapping.items():
        if key in text_lower or text_lower in key:
            return gov
    
    # Fallback to simple mapping for common cases
    simple_mapping = {
        "tunis": "Tunis",
        "ariana": "Ariana",
        "ben arous": "Ben Arous",
        "manouba": "Manouba",
        "nabeul": "Nabeul",
        "zaghouan": "Zaghouan",
        "bizerte": "Bizerte",
        "béja": "Béja",
        "jendouba": "Jendouba",
        "le kef": "Le Kef",
        "siliana": "Siliana",
        "sousse": "Sousse",
        "monastir": "Monastir",
        "mahdia": "Mahdia",
        "sfax": "Sfax",
        "kairouan": "Kairouan",
        "kasserine": "Kasserine",
        "sidi bouzid": "Sidi Bouzid",
        "gabès": "Gabès",
        "médenine": "Médenine",
        "tataouine": "Tataouine",
        "gafsa": "Gafsa",
        "tozeur": "Tozeur",
        "kébili": "Kébili",
    }
    
    for key, gov in simple_mapping.items():
        if key in text_lower:
            return gov
    
    return None


def get_delegation_coords(name: str) -> Optional[Tuple[float, float]]:
    """Get coordinates for a delegation from data.ts"""
    if not name:
        return None
    
    coords = _get_delegation_coords()
    name_lower = name.lower().strip()
    
    # Direct match
    if name_lower in coords:
        return coords[name_lower]
    
    # Try without parentheses content
    if '(' in name:
        clean_name = name.split('(')[0].strip().lower()
        if clean_name in coords:
            return coords[clean_name]
    
    return None


def build_location_from_subtitle(subtitle: str) -> Location:
    parts = [p.strip() for p in subtitle.split(",") if p.strip()]
    city = None
    district = None

    if len(parts) >= 2:
        district = parts[-2]
        city = parts[-1]
    elif len(parts) == 1:
        city = parts[0]

    return Location(city=city or "", district=district)


def infer_region_and_zone(location: Dict[str, Any]) -> Tuple[str, str]:
    gov_raw = location.get("governorate") or ""
    city_raw = location.get("city") or ""
    governorate = str(gov_raw).strip().lower()
    city = str(city_raw).strip().lower()

    if not governorate and city:
        governorate = infer_governorate(city).lower() if infer_governorate(city) else ""

    region_name = governorate.title() if governorate else None

    north_govs = {
        "tunis", "ariana", "ben arous", "manouba", "nabeul",
        "bizerte", "béja", "jendouba", "le kef", "zaghouan", "siliana"
    }
    east_govs = {
        "sousse", "monastir", "mahdia", "sfax"
    }
    west_govs = {
        "kairouan", "kasserine", "sidi bouzid"
    }
    south_govs = {
        "gabès", "médenine", "tataouine", "gafsa", "tozeur", "kébili"
    }

    zone = None
    if governorate in north_govs:
        zone = "north"
    elif governorate in east_govs:
        zone = "east"
    elif governorate in west_govs:
        zone = "west"
    elif governorate in south_govs:
        zone = "south"

    return region_name, zone


def normalize_poi_dict(raw_poi: Dict[str, Any]) -> List[POI]:
    pois: List[POI] = []
    for cat, entries in raw_poi.items():
        for name in entries:
            if not name:
                continue
            pois.append(POI(name=name, category=cat, distance_m=None))
    return pois


def _normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return text.strip()


def _flatten_tunisia_delegations() -> List[Dict[str, Any]]:
    """Flatten delegations from data.ts"""
    data = _load_tunisia_municipalities()
    flat: List[Dict[str, Any]] = []
    
    for gov in data:
        gov_name = gov.get("Name") or ""
        gov_value = gov.get("Value") or ""
        gov_norm = _normalize_text(gov_value or gov_name)
        
        for d in gov.get("Delegations") or []:
            muni_name = d.get("Name") or ""
            muni_value = d.get("Value") or ""
            flat.append(
                {
                    "governorate_name": gov_name,
                    "governorate_value": gov_value,
                    "governorate_norm": gov_norm,
                    "muni_name": muni_name,
                    "muni_value": muni_value,
                    "muni_norm": _normalize_text(muni_value or muni_name),
                    "postal_code": str(d.get("PostalCode") or ""),
                    "lat": d.get("Latitude"),
                    "lon": d.get("Longitude"),
                }
            )
    return flat


def _match_local_delegation(city: Optional[str], governorate: Optional[str], address: Optional[str]) -> Optional[Dict[str, Any]]:
    """Match location against data.ts delegations"""
    all_text = " ".join(
        part for part in [city or "", governorate or "", address or ""] if part
    )
    norm_all = _normalize_text(all_text)
    norm_city = _normalize_text(city)
    norm_gov = _normalize_text(governorate)
    
    if not norm_all and not norm_city:
        return None
    
    candidates = _flatten_tunisia_delegations()
    best = None
    best_score = 0
    
    for rec in candidates:
        score = 0
        muni_norm = rec["muni_norm"]
        gov_norm = rec["governorate_norm"]
        
        if muni_norm and muni_norm in norm_all:
            score += 4
        if muni_norm and muni_norm == norm_city:
            score += 3
        if gov_norm and gov_norm in norm_all:
            score += 2
        if gov_norm and gov_norm == norm_gov:
            score += 1
            
        if score > best_score:
            best_score = score
            best = rec
            if best_score >= 5:
                break
    
    return best


def _build_address_query(city: Optional[str], governorate: Optional[str], address: Optional[str]) -> Optional[str]:
    parts: List[str] = []
    if address:
        parts.append(str(address))
    if city:
        parts.append(str(city))
    if governorate:
        parts.append(str(governorate))
    parts.append("Tunisia")
    joined = ", ".join([p for p in parts if p])
    if not joined:
        return None
    return joined


# Rate limiting for Nominatim
_last_geocode_time = 0

# core/geolocation.py (MODIFIED - only add this function, keep everything else)

# core/geolocation.py (MODIFIED - only add this function, keep everything else)

def geocode_location(city: Optional[str], governorate: Optional[str], address: Optional[str]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Geocode location using data.ts first, then Nominatim.
    IMPORTANT: Returns coordinates only if they exist.
    """
    global _last_geocode_time
    
    # Step 1: Try local delegation from data.ts (fast and reliable)
    if city and "lac 3" in city.lower() or (address and "lac 3" in address.lower()):
        lat, lon = 36.8488, 10.2733
        municipality = "Lac 3"
        log.info(f"Geocoded using Lac 3 hardcoded coordinates: ({lat}, {lon})")
        return lat, lon, municipality

    local_match = _match_local_delegation(city, governorate, address)
    if local_match and local_match.get("lat") is not None and local_match.get("lon") is not None:
        lat = local_match["lat"]
        lon = local_match["lon"]
        muni_raw = local_match["muni_value"] or local_match["muni_name"]
        municipality = str(muni_raw).title() if muni_raw else None
        log.info(f"Geocoded using data.ts: {city} → ({lat}, {lon}) via {municipality}")
        return lat, lon, municipality
    
    # Step 2: Try to get coordinates from delegation name directly
    if city:
        coords = get_delegation_coords(city)
        if coords:
            lat, lon = coords
            log.info(f"Geocoded using delegation coords: {city} → ({lat}, {lon})")
            return lat, lon, city
    
    # Step 3: Fallback to Nominatim (online geocoding)
    query = _build_address_query(city, governorate, address)
    if not query:
        return None, None, None
    
    log.info(f"Geocoding via Nominatim: {query}")
    
    # Rate limit Nominatim (1 request per second max)
    now = time.time()
    if now - _last_geocode_time < 1.0:
        time.sleep(1.0 - (now - _last_geocode_time))
    _last_geocode_time = time.time()
    
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q": query,
                "format": "json",
                "addressdetails": 1,
                "limit": 1,
            },
            headers={"User-Agent": "EstateMind/1.0"},
            timeout=10,
        )
    except Exception as e:
        log.error(f"Geocoding error for query={query!r}: {e}")
        return None, None, None
    
    if resp.status_code != 200:
        log.error(f"Geocoding HTTP {resp.status_code} for query={query!r}")
        return None, None, None
    
    try:
        data = resp.json()
    except Exception as e:
        log.error(f"Geocoding parse error for query={query!r}: {e}")
        return None, None, None
    
    if not data:
        return None, None, None
    
    first = data[0]
    try:
        lat = float(first.get("lat"))
        lon = float(first.get("lon"))
    except Exception:
        return None, None, None
    
    address_data = first.get("address") or {}
    muni_candidates = [
        "municipality",
        "city_district",
        "suburb",
        "town",
        "village",
        "neighbourhood",
    ]
    municipality = None
    for key in muni_candidates:
        value = address_data.get(key)
        if value:
            municipality = value
            break
    
    return lat, lon, municipality
def _haversine_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def fetch_pois(lat: float, lon: float, radius_m: int = 750) -> List[POI]:
    """Fetch POIs with thread-safe caching"""
    if lat is None or lon is None:
        return []
    
    # Round to 4 decimal places for cache key
    lat_key = round(lat, 4)
    lon_key = round(lon, 4)
    
    # Check cache first (using local connection to be thread-safe)
    cache_path = Path("data/poi_cache.db")
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with sqlite3.connect(str(cache_path), timeout=20) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS poi_cache (
                    lat REAL,
                    lon REAL,
                    radius INTEGER,
                    data TEXT,
                    cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (lat, lon, radius)
                )
            """)
            cursor = conn.execute(
                "SELECT data FROM poi_cache WHERE lat = ? AND lon = ? AND radius = ?",
                (lat_key, lon_key, radius_m)
            )
            row = cursor.fetchone()
            if row:
                data = json.loads(row[0])
                return [POI(**p) for p in data]
    except Exception as e:
        log.warning(f"POI cache read error: {e}")
    
    # Build Overpass query (using nwr for node/way/relation and center for geometry)
    query = f"""
    [out:json][timeout:25];
    (
      nwr["amenity"~"school|university|college"](around:{radius_m},{lat},{lon});
      nwr["amenity"~"hospital|clinic|pharmacy|doctors"](around:{radius_m},{lat},{lon});
      nwr["amenity"~"restaurant|cafe|fast_food|bar"](around:{radius_m},{lat},{lon});
      nwr["shop"~"supermarket|mall|convenience|bakery"](around:{radius_m},{lat},{lon});
      nwr["public_transport"~"stop|station|platform"](around:{radius_m},{lat},{lon});
      nwr["amenity"="bus_station"](around:{radius_m},{lat},{lon});
      nwr["railway"="station"](around:{radius_m},{lat},{lon});
      nwr["leisure"~"park|garden|playground"](around:{radius_m},{lat},{lon});
    );
    out center;
    """
    
    try:
        resp = requests.post(
            "https://overpass-api.de/api/interpreter",
            data=query.encode("utf-8"),
            headers={"User-Agent": "EstateMind/1.0"},
            timeout=15,
        )
        
        if resp.status_code == 429:
            log.warning("Overpass API rate limited")
            return []
        if resp.status_code != 200:
            log.warning(f"POI HTTP {resp.status_code} at ({lat},{lon})")
            return []
            
        data = resp.json()
        elements = data.get("elements") or []
        
        pois = []
        category_map = {
            "school": "school", "university": "school", "college": "school",
            "hospital": "hospital", "clinic": "hospital", "pharmacy": "hospital", "doctors": "hospital",
            "restaurant": "restaurant", "cafe": "restaurant", "fast_food": "restaurant", "bar": "restaurant",
            "supermarket": "shopping", "mall": "shopping", "convenience": "shopping", "bakery": "shopping",
            "bus_station": "transport", "station": "transport", "stop": "transport", "platform": "transport",
            "park": "leisure", "garden": "leisure", "playground": "leisure",
        }
        
        for el in elements:
            tags = el.get("tags") or {}
            name = tags.get("name") or tags.get("official_name")
            if not name:
                continue
            
            amenity = tags.get("amenity") or ""
            shop = tags.get("shop") or ""
            railway = tags.get("railway") or ""
            leisure = tags.get("leisure") or ""
            
            cat = "other"
            if amenity in category_map:
                cat = category_map[amenity]
            elif shop in category_map:
                cat = category_map[shop]
            elif leisure in category_map:
                cat = category_map[leisure]
            elif railway == "station":
                cat = "transport"
            
            # Calculate approximate distance if possible
            dist = None
            try:
                el_lat = el.get("lat") or el.get("center", {}).get("lat")
                el_lon = el.get("lon") or el.get("center", {}).get("lon")
                if el_lat and el_lon:
                    # Simple Haversine or Euclidean for short distances
                    from math import radians, cos, sin, asin, sqrt
                    dlon = radians(el_lon - lon)
                    dlat = radians(el_lat - lat)
                    a = sin(dlat/2)**2 + cos(radians(lat)) * cos(radians(el_lat)) * sin(dlon/2)**2
                    dist = round(2 * asin(sqrt(a)) * 6371 * 1000, 1) # in meters
            except: pass

            pois.append(POI(name=name[:100], category=cat, distance_m=dist))
        
        # Sort by distance and limit to 10 POIs per listing
        pois = sorted([p for p in pois if p.distance_m is not None], key=lambda x: x.distance_m or 9999)[:10]
        if not pois and elements:
            # Fallback if distance calculation failed but we have elements
            for el in elements[:10]:
                tags = el.get("tags") or {}
                name = tags.get("name") or tags.get("official_name")
                if name:
                    pois.append(POI(name=name[:100], category="other", distance_m=None))
        
        # Cache results
        try:
            with sqlite3.connect(str(cache_path), timeout=20) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO poi_cache (lat, lon, radius, data) VALUES (?, ?, ?, ?)",
                    (lat_key, lon_key, radius_m, json.dumps([p.model_dump() for p in pois]))
                )
                conn.commit()
        except Exception as e:
            log.warning(f"POI cache write error: {e}")
        
        return pois
        
    except requests.Timeout:
        log.warning(f"POI timeout for ({lat},{lon})")
        return []
    except Exception as e:
        log.warning(f"POI fetch error: {e}")
        return []