# """
# EstateMind — Data Normalizer

# Standardizes all field formats so the modeling layer gets clean,
# consistent data regardless of which scraper produced it.

# Handles:
# - Price: various TND formats → float
# - Surface: string formats → float m²
# - Transaction type: vente/louer/sale/rent → "Sale"/"Rent"
# - Property type: consistent casing and mapping
# - Governorate: normalized to official 24 governorate names
# - City: cleaned and title-cased
# - Rooms: various formats → int
# """
# from __future__ import annotations

# import re
# from typing import Dict, Any, Optional
# from preprocessing.nlp.data_utils import get_location_data
# from loguru import logger


# # ── Governorate normalization map ─────────────────────────────────────────────



# TRANSACTION_NORM = {
#     "vente": "Sale", "vendre": "Sale", "sale": "Sale",
#     "achat": "Sale", "acheter": "Sale", "à vendre": "Sale",
#     "location": "Rent", "louer": "Rent", "rent": "Rent",
#     "à louer": "Rent", "rental": "Rent",
# }

# PROPERTY_TYPE_NORM = {
#     "appartement": "Apartment", "apartment": "Apartment",
#     "appart": "Apartment", "flat": "Apartment",
#     "studio": "Apartment",
#     "villa": "Villa", "maison": "Villa", "house": "Villa",
#     "duplex": "Villa", "triplex": "Villa", "rdc": "Villa",
#     "terrain": "Land", "land": "Land", "lot": "Land",
#     "bureau": "Commercial", "office": "Commercial",
#     "commerce": "Commercial", "local": "Commercial",
#     "commercial": "Commercial",
#     "other": "Other",
# }


# # ── Individual normalizers ────────────────────────────────────────────────────

# def normalize_price(value: Any) -> Optional[float]:
#     """Normalize price to float TND."""
#     if value is None:
#         return None
#     if isinstance(value, (int, float)):
#         v = float(value)
#         return v if v > 0 else None
#     s = str(value).strip()
#     # Remove currency labels
#     s = re.sub(r'(?:TND|DT|dinars?|€|\$)', '', s, flags=re.IGNORECASE)
#     # Remove spaces used as thousands separators
#     s = re.sub(r'\s+', '', s)
#     # Handle comma as decimal separator
#     if s.count(',') == 1 and len(s.split(',')[1]) <= 2:
#         s = s.replace(',', '.')
#     else:
#         s = s.replace(',', '')
#     # Handle multiple dots (1.200.000 → 1200000)
#     if s.count('.') > 1:
#         s = s.replace('.', '')
#     try:
#         v = float(s)
#         return v if 500 <= v <= 50_000_000 else None
#     except (ValueError, TypeError):
#         return None


# def normalize_surface(value: Any) -> Optional[float]:
#     """Normalize surface to float m²."""
#     if value is None:
#         return None
#     if isinstance(value, (int, float)):
#         v = float(value)
#         return v if 10 <= v <= 10_000 else None
#     s = str(value).strip()
#     s = re.sub(r'(?:m²|m2|m\b|mètres?|metres?)', '', s, flags=re.IGNORECASE)
#     s = re.sub(r'\s+', '', s).replace(',', '.')
#     try:
#         v = float(s)
#         return v if 10 <= v <= 10_000 else None
#     except (ValueError, TypeError):
#         return None


# def normalize_rooms(value: Any) -> Optional[int]:
#     """Normalize rooms to int."""
#     if value is None:
#         return None
#     if isinstance(value, int):
#         return value if 1 <= value <= 20 else None
#     try:
#         v = int(float(str(value).strip()))
#         return v if 1 <= v <= 20 else None
#     except (ValueError, TypeError):
#         return None


# def normalize_transaction_type(value: Any) -> str:
#     """Normalize transaction type to 'Sale' or 'Rent'."""
#     if not value:
#         return "Sale"
#     v = str(value).strip().lower()
#     for key, normalized in TRANSACTION_NORM.items():
#         if key in v:
#             return normalized
#     # Direct match
#     if v in ("sale", "vente"):
#         return "Sale"
#     if v in ("rent", "location", "loyer"):
#         return "Rent"
#     return "Sale"  # default


# def normalize_property_type(value: Any) -> str:
#     """Normalize property type to standard categories."""
#     if not value:
#         return "Other"
#     v = str(value).strip().lower()
#     for key, normalized in PROPERTY_TYPE_NORM.items():
#         if key in v:
#             return normalized
#     return value.title() if value else "Other"


# def normalize_governorate(value: Any) -> Optional[str]:
#     """Normalize governorate using data.ts"""
#     if not value:
#         return None
    
#     location_data = get_location_data()
#     v = str(value).strip().lower()
    
#     # Direct lookup in data.ts
#     for gov in location_data.governorates:
#         if gov.lower() == v:
#             return gov
    
#     # Check if it's a delegation
#     gov = location_data.get_governorate_for_delegation(v)
#     if gov:
#         return gov
    
#     # Return title-cased if no match
#     return value.strip().title()


# def normalize_city(value: Any) -> Optional[str]:
#     """Clean and title-case city name."""
#     if not value:
#         return None
#     s = str(value).strip()
#     # Remove common suffixes that get mixed in
#     s = re.sub(r'\s*[,|]\s*.*$', '', s)  # remove everything after comma/pipe
#     s = re.sub(r'\s+', ' ', s).strip()
#     return s.title() if len(s) >= 2 else None


# def normalize_zone(governorate: Optional[str]) -> Optional[str]:
#     """Infer zone (north/east/west/south) from governorate."""
#     if not governorate:
#         return None
#     g = governorate.lower()
#     north = {"tunis", "ariana", "ben arous", "manouba", "nabeul",
#              "bizerte", "béja", "jendouba", "le kef", "siliana", "zaghouan"}
#     east  = {"sousse", "monastir", "mahdia", "sfax"}
#     west  = {"kairouan", "kasserine", "sidi bouzid"}
#     south = {"gabès", "médenine", "tataouine", "gafsa", "tozeur", "kébili"}
#     if g in north: return "north"
#     if g in east:  return "east"
#     if g in west:  return "west"
#     if g in south: return "south"
#     return None


# # ── Master normalizer ─────────────────────────────────────────────────────────

# def normalize(metadata: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Normalize all fields in a single metadata record.
#     Returns updated dict with normalized values and a 'normalized' flag.
#     """
#     updated = dict(metadata)
#     changes = []

#     # Price
#     raw_price = metadata.get("price")
#     norm_price = normalize_price(raw_price)
#     if norm_price != raw_price:
#         updated["price"] = norm_price
#         changes.append("price")

#     # Surface
#     raw_surface = metadata.get("surface")
#     norm_surface = normalize_surface(raw_surface)
#     if norm_surface != raw_surface:
#         updated["surface"] = norm_surface
#         changes.append("surface")

#     # Rooms
#     raw_rooms = metadata.get("rooms")
#     norm_rooms = normalize_rooms(raw_rooms)
#     if norm_rooms != raw_rooms:
#         updated["rooms"] = norm_rooms
#         changes.append("rooms")

#     # Transaction type
#     raw_tx = metadata.get("transaction_type")
#     norm_tx = normalize_transaction_type(raw_tx)
#     if norm_tx != raw_tx:
#         updated["transaction_type"] = norm_tx
#         changes.append("transaction_type")

#     # Property type
#     raw_type = metadata.get("type")
#     norm_type = normalize_property_type(raw_type)
#     if norm_type != raw_type:
#         updated["type"] = norm_type
#         changes.append("type")

#     # Governorate / region
#     raw_gov = metadata.get("region")
#     norm_gov = normalize_governorate(raw_gov)
#     if norm_gov != raw_gov:
#         updated["region"] = norm_gov
#         changes.append("region")

#     # City
#     raw_city = metadata.get("city")
#     norm_city = normalize_city(raw_city)
#     if norm_city != raw_city:
#         updated["city"] = norm_city
#         changes.append("city")

#     # Zone — infer if missing
#     if not metadata.get("zone") and updated.get("region"):
#         zone = normalize_zone(updated["region"])
#         if zone:
#             updated["zone"] = zone
#             changes.append("zone")

#     # Price per m² — computed field useful for modeling and outlier detection
#     price = updated.get("price")
#     surface = updated.get("surface")
#     if price and surface and float(surface) > 0:
#         updated["price_per_m2"] = round(float(price) / float(surface), 2)

#     if changes:
#         updated["normalized"] = True
#         logger.debug(
#             f"[Normalizer] {metadata.get('property_id', '?')} — "
#             f"normalized: {changes}"
#         )

#     return updated


# def batch_normalize(records: list) -> list:
#     """Normalize a list of metadata records."""
#     results = []
#     changed = 0
#     for record in records:
#         metadata = record.get("metadata", record)
#         updated = normalize(metadata)
#         results.append(updated)
#         if updated.get("normalized"):
#             changed += 1
#     logger.info(
#         f"[Normalizer] Processed {len(records)} records — "
#         f"normalized {changed} ({changed/len(records)*100:.1f}% had changes)"
#     )
#     return results


# if __name__ == "__main__":
#     test_cases = [
#         {
#             "property_id": "test_001",
#             "price": "850 000 TND",
#             "surface": "145 m²",
#             "rooms": "4",
#             "transaction_type": "vente",
#             "type": "appartement",
#             "region": "tunis",
#             "city": "la marsa, tunis",
#         },
#         {
#             "property_id": "test_002",
#             "price": "1,200,000",
#             "surface": "280",
#             "rooms": 4,
#             "transaction_type": "Sale",
#             "type": "Villa",
#             "region": "nabeul",
#             "city": "Hammamet | bord de mer",
#         },
#         {
#             "property_id": "test_003",
#             "price": None,
#             "surface": "0",
#             "rooms": 25,   # invalid — too many
#             "transaction_type": "louer",
#             "type": "bureau",
#             "region": "sfax",
#             "city": None,
#         },
#     ]

#     for case in test_cases:
#         result = normalize(case)
#         print(f"\n{case['property_id']}:")
#         print(f"  price: {case['price']} → {result['price']}")
#         print(f"  surface: {case['surface']} → {result['surface']}")
#         print(f"  rooms: {case['rooms']} → {result['rooms']}")
#         print(f"  transaction_type: {case['transaction_type']} → {result['transaction_type']}")
#         print(f"  type: {case['type']} → {result['type']}")
#         print(f"  region: {case['region']} → {result['region']}")
#         print(f"  city: {case['city']} → {result['city']}")
#         print(f"  zone: {result.get('zone')}")
#         print(f"  price_per_m2: {result.get('price_per_m2')}")


"""
EstateMind — Data Normalizer

Applies normalization rules to Pinecone data.
Rules are generated once by LLM, then applied to all records.
"""

from __future__ import annotations

import re
import json
from typing import Dict, Any, Optional
from loguru import logger

from rule_generator import get_rules_generator


class RuleBasedNormalizer:
    """Apply normalization rules to data"""
    
    def __init__(self):
        self.rules = self._load_rules()
    
    def _load_rules(self) -> Dict:
        """Load normalization rules"""
        try:
            with open("data/normalization_rules.json", 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning("Rules file not found, generating...")
            generator = get_rules_generator()
            # You need to provide sample data here
            return generator._get_fallback_rules()
    
    def normalize_price(self, value: Any) -> Optional[float]:
        """Apply price normalization rules"""
        if value is None:
            return None
        
        rules = self.rules.get("price", {})
        
        # Convert to string for processing
        s = str(value).strip()
        
        # Remove currency indicators
        for remove in rules.get("remove", []):
            s = s.replace(remove, "")
        
        # Handle thousands separators
        for sep in rules.get("thousands_sep", []):
            s = s.replace(sep, "")
        
        # Handle decimal separator
        if rules.get("decimal_sep") == ".":
            s = s.replace(",", ".")
        
        # Remove all non-numeric except . and -
        s = re.sub(r'[^\d.-]', '', s)
        
        try:
            v = float(s)
            range_rule = rules.get("range", {})
            if range_rule.get("min", 0) <= v <= range_rule.get("max", float('inf')):
                return v
        except:
            pass
        
        return None
    
    def normalize_surface(self, value: Any) -> Optional[float]:
        """Apply surface normalization rules"""
        if value is None:
            return None
        
        rules = self.rules.get("surface", {})
        s = str(value).strip()
        
        # Remove units
        for remove in rules.get("remove", []):
            s = s.replace(remove, "")
        
        # Clean up
        s = re.sub(r'[^\d.]', '', s)
        
        try:
            v = float(s)
            range_rule = rules.get("range", {})
            if range_rule.get("min", 0) <= v <= range_rule.get("max", float('inf')):
                return v
        except:
            pass
        
        return None
    
    def normalize_rooms(self, value: Any) -> Optional[int]:
        """Apply rooms normalization rules"""
        if value is None:
            return None
        
        rules = self.rules.get("rooms", {})
        s = str(value).strip()
        
        # Handle S+N pattern (e.g., S+3 = 4 rooms)
        match = re.search(r'S\s*\+\s*(\d+)', s, re.IGNORECASE)
        if match:
            v = int(match.group(1)) + 1
            range_rule = rules.get("range", {})
            if range_rule.get("min", 0) <= v <= range_rule.get("max", 100):
                return v
        
        # Handle numbers only
        try:
            v = int(float(re.sub(r'[^\d]', '', s)))
            range_rule = rules.get("range", {})
            if range_rule.get("min", 0) <= v <= range_rule.get("max", 100):
                return v
        except:
            pass
        
        return None
    
    def normalize_transaction_type(self, value: Any) -> str:
        """Apply transaction type normalization rules"""
        if value is None:
            return "Sale"
        
        rules = self.rules.get("transaction_type", {})
        mapping = rules.get("mapping", {})
        s = str(value).strip().lower()
        
        return mapping.get(s, "Sale")
    
    def normalize_property_type(self, value: Any) -> str:
        """Apply property type normalization rules"""
        if value is None:
            return "Other"
        
        rules = self.rules.get("property_type", {})
        mapping = rules.get("mapping", {})
        s = str(value).strip().lower()
        
        return mapping.get(s, "Other")
    
    def normalize_city(self, value: Any) -> Optional[str]:
        """Apply city normalization rules"""
        if value is None:
            return None
        
        rules = self.rules.get("city", {})
        s = str(value).strip()
        
        # Remove suffixes
        for suffix in rules.get("remove_suffixes", []):
            s = s.replace(suffix, "")
        
        # Title case
        if "title_case" in rules.get("normalize", []):
            s = s.title()
        
        return s if len(s) >= 2 else None
    
    def normalize_governorate(self, value: Any) -> Optional[str]:
        """Apply governorate normalization rules"""
        if value is None:
            return None
        
        rules = self.rules.get("governorate", {})
        mapping = rules.get("mapping", {})
        s = str(value).strip().lower()
        
        return mapping.get(s, s.title())
    
    def normalize(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize all fields using rules"""
        updated = dict(metadata)
        changes = []
        
        # Price
        if metadata.get("price"):
            norm = self.normalize_price(metadata["price"])
            if norm != metadata["price"]:
                updated["price"] = norm
                changes.append("price")
        
        # Surface
        if metadata.get("surface"):
            norm = self.normalize_surface(metadata["surface"])
            if norm != metadata["surface"]:
                updated["surface"] = norm
                changes.append("surface")
        
        # Rooms
        if metadata.get("rooms"):
            norm = self.normalize_rooms(metadata["rooms"])
            if norm != metadata["rooms"]:
                updated["rooms"] = norm
                changes.append("rooms")
        
        # Transaction type
        norm = self.normalize_transaction_type(metadata.get("transaction_type"))
        if norm != metadata.get("transaction_type"):
            updated["transaction_type"] = norm
            changes.append("transaction_type")
        
        # Property type
        norm = self.normalize_property_type(metadata.get("type"))
        if norm != metadata.get("type"):
            updated["type"] = norm
            changes.append("type")
        
        # City
        if metadata.get("city"):
            norm = self.normalize_city(metadata["city"])
            if norm != metadata["city"]:
                updated["city"] = norm
                changes.append("city")
        
        # Governorate
        if metadata.get("region"):
            norm = self.normalize_governorate(metadata["region"])
            if norm != metadata["region"]:
                updated["region"] = norm
                changes.append("region")
        
        # Zone inference
        if not metadata.get("zone") and updated.get("region"):
            zone = self._infer_zone(updated["region"])
            if zone:
                updated["zone"] = zone
                changes.append("zone")
        
        # Price per m²
        if updated.get("price") and updated.get("surface") and float(updated["surface"]) > 0:
            updated["price_per_m2"] = round(float(updated["price"]) / float(updated["surface"]), 2)
        
        if changes:
            updated["normalized"] = True
            logger.debug(f"Normalized {metadata.get('property_id', '?')}: {changes}")
        
        return updated
    
    def _infer_zone(self, governorate: str) -> Optional[str]:
        """Infer zone from governorate"""
        north = {"Tunis", "Ariana", "Ben Arous", "Manouba", "Nabeul", "Zaghouan",
                 "Bizerte", "Béja", "Jendouba", "Le Kef", "Siliana"}
        east = {"Sousse", "Monastir", "Mahdia", "Sfax"}
        west = {"Kairouan", "Kasserine", "Sidi Bouzid"}
        south = {"Gabès", "Médenine", "Tataouine", "Gafsa", "Tozeur", "Kébili"}
        
        if governorate in north:
            return "north"
        if governorate in east:
            return "east"
        if governorate in west:
            return "west"
        if governorate in south:
            return "south"
        return None


# Singleton normalizer
_normalizer = None

def get_normalizer() -> RuleBasedNormalizer:
    global _normalizer
    if _normalizer is None:
        _normalizer = RuleBasedNormalizer()
    return _normalizer


def normalize(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize single record"""
    normalizer = get_normalizer()
    return normalizer.normalize(metadata)


def batch_normalize(records: list) -> list:
    """Normalize batch of records"""
    normalizer = get_normalizer()
    results = []
    changed = 0
    
    for record in records:
        metadata = record.get("metadata", record)
        updated = normalizer.normalize(metadata)
        results.append(updated)
        if updated.get("normalized"):
            changed += 1
    
    logger.info(
        f"[Normalizer] Processed {len(records)} records — "
        f"normalized {changed} ({changed/len(records)*100:.1f}%)"
    )
    return results