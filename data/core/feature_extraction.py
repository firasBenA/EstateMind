from __future__ import annotations

from typing import List, Optional

from core.models import PropertyListing


FEATURE_KEYWORDS = {
    "haut standing": ["haut standing", "hautstanding", "standing"],
    "piscine": ["piscine", "pool"],
    "jardin": ["jardin", "garden"],
    "garage": ["garage"],
    "parking": ["parking"],
    "chauffage central": ["chauffage central"],
    "climatisation": ["climatisation", "climatise", "climatiseur"],
    "meuble": ["meublé", "meuble"],
    "vue mer": ["vue sur mer", "vue mer"],
    "ascenseur": ["ascenseur", "elevator"],
    "chambre de service": ["chambre de service"],
    "cave": ["cave"],
    "terrasse": ["terrasse", "terrace"],
}


def _extract_from_text(text: str) -> List[str]:
    if not text:
        return []
    text_lower = text.lower()
    found: List[str] = []
    for label, keywords in FEATURE_KEYWORDS.items():
        if any(k in text_lower for k in keywords):
            found.append(label)
    return found


def build_feature_list(
    existing: Optional[List[str]],
    title: Optional[str],
    description: Optional[str],
    extra_text: Optional[str] = None,
) -> List[str]:
    base = list(existing or [])
    text_parts: List[str] = []
    if title:
        text_parts.append(str(title))
    if description:
        text_parts.append(str(description))
    if extra_text:
        text_parts.append(extra_text)
    combined_text = " ".join(text_parts)
    extracted = _extract_from_text(combined_text)
    combined = base + extracted
    seen = set()
    deduped: List[str] = []
    for f in combined:
        key = f.strip().lower()
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        deduped.append(f.strip())
    return deduped


def enrich_listing_features(listing: PropertyListing, extra_text: Optional[str] = None) -> PropertyListing:
    # 1. Regex-based extraction (Fast)
    listing.features = build_feature_list(
        existing=listing.features,
        title=listing.title,
        description=listing.description,
        extra_text=extra_text,
    )
    
    # 2. LLM-based extraction (Smart) - if description is long enough
    if (listing.description and len(listing.description) > 50) or (listing.title and len(listing.title) > 20):
        try:
            from preprocessing.nlp.extractor import get_extractor
            extractor = get_extractor()
            text = f"Title: {listing.title}\nDescription: {listing.description or ''}"
            llm_features = extractor.extract_features(text)
            if llm_features and isinstance(llm_features, list):
                # Merge and deduplicate
                all_features = set(listing.features)
                for f in llm_features:
                    if f and isinstance(f, str):
                        all_features.add(f.strip())
                listing.features = sorted(list(all_features))
        except Exception:
            # Silently fail if LLM is unavailable or key is wrong
            pass
            
    return listing
