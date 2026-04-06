"""
EstateMind — Data Normalizer

Standardizes all field formats so the modeling layer gets clean,
consistent data regardless of which scraper produced it.

Handles:
- Price: various TND formats → float
- Surface: string formats → float m²
- Transaction type: vente/louer/sale/rent → "Sale"/"Rent"
- Property type: consistent casing and mapping
- Governorate: normalized to official 24 governorate names
- City: cleaned and title-cased
- Rooms: various formats → int
"""
from __future__ import annotations

import re
from typing import Dict, Any, Optional
from loguru import logger


# ── Governorate normalization map ─────────────────────────────────────────────

TRANSACTION_NORM = {
    "vente": "Sale", "vendre": "Sale", "sale": "Sale",
    "achat": "Sale", "acheter": "Sale", "à vendre": "Sale",
    "location": "Rent", "louer": "Rent", "rent": "Rent",
    "à louer": "Rent", "rental": "Rent",
}

PROPERTY_TYPE_NORM = {
    "appartement": "Apartment", "apartment": "Apartment",
    "appart": "Apartment", "flat": "Apartment",
    "studio": "Apartment",
    "villa": "Villa", "maison": "Villa", "house": "Villa",
    "duplex": "Villa", "triplex": "Villa", "rdc": "Villa",
    "terrain": "Land", "land": "Land", "lot": "Land",
    "bureau": "Commercial", "office": "Commercial",
    "commerce": "Commercial", "local": "Commercial",
    "commercial": "Commercial",
    "other": "Other",
}


# ── Individual normalizers ────────────────────────────────────────────────────

def normalize_price(value: Any) -> Optional[float]:
    """Normalize price to float TND."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        return v if v > 0 else None
    s = str(value).strip()
    # Remove currency labels
    s = re.sub(r'(?:TND|DT|dinars?|€|\$)', '', s, flags=re.IGNORECASE)
    # Remove spaces used as thousands separators
    s = re.sub(r'\s+', '', s)
    # Handle comma as decimal separator
    if s.count(',') == 1 and len(s.split(',')[1]) <= 2:
        s = s.replace(',', '.')
    else:
        s = s.replace(',', '')
    # Handle multiple dots (1.200.000 → 1200000)
    if s.count('.') > 1:
        s = s.replace('.', '')
    try:
        v = float(s)
        return v if 500 <= v <= 50_000_000 else None
    except (ValueError, TypeError):
        return None


def normalize_surface(value: Any) -> Optional[float]:
    """Normalize surface to float m²."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        return v if 10 <= v <= 10_000 else None
    s = str(value).strip()
    s = re.sub(r'(?:m²|m2|m\b|mètres?|metres?)', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\s+', '', s).replace(',', '.')
    try:
        v = float(s)
        return v if 10 <= v <= 10_000 else None
    except (ValueError, TypeError):
        return None


def normalize_rooms(value: Any) -> Optional[int]:
    """Normalize rooms to int."""
    if value is None:
        return None
    if isinstance(value, int):
        return value if 1 <= value <= 20 else None
    try:
        v = int(float(str(value).strip()))
        return v if 1 <= v <= 20 else None
    except (ValueError, TypeError):
        return None


def normalize_transaction_type(value: Any) -> str:
    """Normalize transaction type to 'Sale' or 'Rent'."""
    if not value:
        return "Sale"
    v = str(value).strip().lower()
    for key, normalized in TRANSACTION_NORM.items():
        if key in v:
            return normalized
    # Direct match
    if v in ("sale", "vente"):
        return "Sale"
    if v in ("rent", "location", "loyer"):
        return "Rent"
    return "Sale"  # default


def normalize_property_type(value: Any) -> str:
    """Normalize property type to standard categories."""
    if not value:
        return "Other"
    v = str(value).strip().lower()
    for key, normalized in PROPERTY_TYPE_NORM.items():
        if key in v:
            return normalized
    return value.title() if value else "Other"


def normalize_governorate(value: Any) -> Optional[str]:
    """Normalize governorate using core.geolocation"""
    if not value:
        return None
    
    from core.geolocation import infer_governorate
    v = str(value).strip()
    return infer_governorate(v)


def normalize_city(value: Any) -> Optional[str]:
    """Clean and title-case city name."""
    if not value:
        return None
    s = str(value).strip()
    # Remove common suffixes that get mixed in
    s = re.sub(r'\s*[,|]\s*.*$', '', s)  # remove everything after comma/pipe
    s = re.sub(r'\s+', ' ', s).strip()
    return s.title() if len(s) >= 2 else None


def normalize_zone(governorate: Optional[str]) -> Optional[str]:
    """Infer zone (north/east/west/south) from governorate."""
    if not governorate:
        return None
    
    try:
        from core.geolocation import infer_region_and_zone
        _, zone = infer_region_and_zone({"governorate": governorate})
        return zone
    except:
        return None


# ── Master normalizer ─────────────────────────────────────────────────────────

def normalize(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize all fields in a single metadata record.
    Returns updated dict with normalized values and a 'normalized' flag.
    """
    updated = dict(metadata)
    changes = []

    # Price
    raw_price = metadata.get("price")
    norm_price = normalize_price(raw_price)
    if norm_price != raw_price:
        updated["price"] = norm_price
        changes.append("price")

    # Surface
    raw_surface = metadata.get("surface")
    norm_surface = normalize_surface(raw_surface)
    if norm_surface != raw_surface:
        updated["surface"] = norm_surface
        changes.append("surface")

    # Rooms
    raw_rooms = metadata.get("rooms")
    norm_rooms = normalize_rooms(raw_rooms)
    if norm_rooms != raw_rooms:
        updated["rooms"] = norm_rooms
        changes.append("rooms")

    # Transaction type
    raw_tx = metadata.get("transaction_type")
    norm_tx = normalize_transaction_type(raw_tx)
    if norm_tx != raw_tx:
        updated["transaction_type"] = norm_tx
        changes.append("transaction_type")

    # Property type
    raw_type = metadata.get("type")
    norm_type = normalize_property_type(raw_type)
    if norm_type != raw_type:
        updated["type"] = norm_type
        changes.append("type")

    # Governorate / region
    raw_gov = metadata.get("region") or metadata.get("governorate")
    norm_gov = normalize_governorate(raw_gov)
    if norm_gov and norm_gov != raw_gov:
        updated["governorate"] = norm_gov
        updated["region"] = norm_gov
        changes.append("governorate")

    # City
    raw_city = metadata.get("city")
    norm_city = normalize_city(raw_city)
    if norm_city != raw_city:
        updated["city"] = norm_city
        changes.append("city")

    # Zone — infer if missing
    if not metadata.get("zone") and updated.get("governorate"):
        zone = normalize_zone(updated["governorate"])
        if zone:
            updated["zone"] = zone
            changes.append("zone")

    # Price per m² — computed field useful for modeling and outlier detection
    price = updated.get("price")
    surface = updated.get("surface")
    if price and surface and float(surface) > 0:
        updated["price_per_m2"] = round(float(price) / float(surface), 2)
    
    # Ensure image_count is present
    if "images" in updated and isinstance(updated["images"], list):
        updated["image_count"] = len(updated["images"])
    elif "image_count" not in updated:
        updated["image_count"] = 0

    if changes:
        updated["normalized"] = True
        logger.debug(
            f"[Normalizer] {metadata.get('property_id', '?')} — "
            f"normalized: {changes}"
        )

    return updated


def batch_normalize(records: list) -> list:
    """Normalize a list of metadata records."""
    results = []
    changed = 0
    for record in records:
        # Pipeline passes records as dicts directly
        updated = normalize(record)
        results.append(updated)
        if updated.get("normalized"):
            changed += 1
    logger.info(
        f"[Normalizer] Processed {len(records)} records — "
        f"normalized {changed} ({changed/len(records)*100:.1f}% had changes)"
    )
    return results
