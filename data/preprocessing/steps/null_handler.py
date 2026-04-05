"""
EstateMind — Null Handler

For each listing with missing fields, attempts to extract values
from the description text using LLM extractor.
Only updates fields that are genuinely null.
"""

from __future__ import annotations

from typing import Dict, Any, List, Optional
from loguru import logger

from preprocessing.nlp.extractor import get_extractor, Extractor


def handle_nulls(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Attempt to fill null fields from description text using LLM.
    """
    title = metadata.get("title", "")
    description = metadata.get("description", "")
    
    if not description and not title:
        return metadata
    
    # Build existing values dict for extractor
    existing = {
        "rooms": metadata.get("rooms"),
        "surface_area_m2": metadata.get("surface"),
        "price": metadata.get("price"),
        "city": metadata.get("city"),
        "governorate": metadata.get("region"),
        "district": metadata.get("municipalite"),
        "features": metadata.get("features"),
        "transaction_type": metadata.get("transaction_type"),
        "property_type": metadata.get("type"),
    }
    
    # Check what's missing
    missing_fields = [k for k, v in existing.items() if not v]
    if not missing_fields:
        return metadata
    
    logger.debug(f"Missing fields for {metadata.get('property_id', '?')}: {missing_fields}")
    
    # Get extractor and run extraction
    extractor = get_extractor()
    text = f"Title: {title}\nDescription: {description}"
    extracted = extractor.extract(text)
    
    if not extracted:
        return metadata
    
    # Update metadata with extracted values
    updated = dict(metadata)
    filled_fields = []
    
    # Map extracted fields to metadata keys
    field_mapping = {
        "rooms": "rooms",
        "surface": "surface",
        "price": "price",
        "city": "city",
        "governorate": "region",
        "district": "municipalite",
        "features": "features",
        "transaction_type": "transaction_type",
        "property_type": "type",
    }
    
    for extract_key, meta_key in field_mapping.items():
        if meta_key not in missing_fields:
            continue
        
        value = extracted.get(extract_key)
        if value is not None and value != "null":
            # Convert types appropriately
            if extract_key == "rooms" and isinstance(value, (int, float)):
                updated[meta_key] = int(value)
                filled_fields.append(meta_key)
            elif extract_key == "surface" and isinstance(value, (int, float)):
                updated[meta_key] = float(value)
                filled_fields.append(meta_key)
            elif extract_key == "price" and isinstance(value, (int, float)):
                updated[meta_key] = float(value)
                filled_fields.append(meta_key)
            elif isinstance(value, str) and value and value != "null":
                updated[meta_key] = value
                filled_fields.append(meta_key)
            elif extract_key == "features" and isinstance(value, list):
                updated[meta_key] = value
                filled_fields.append(meta_key)
    
    if filled_fields:
        updated["nlp_filled_fields"] = filled_fields
        updated["nlp_enriched"] = True
        logger.info(
            f"[NullHandler] {metadata.get('source_name', '?')}:"
            f"{metadata.get('property_id', '?')} — "
            f"filled {len(filled_fields)} fields: {filled_fields}"
        )
    
    return updated


def batch_handle_nulls(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Process a list of records, filling nulls where possible.
    Uses batch extraction for efficiency.
    """
    total = len(records)
    if total == 0:
        return records
    
    # First, identify which records need processing
    records_to_process = []
    indices_to_process = []
    
    for idx, record in enumerate(records):
        metadata = record.get("metadata", record)
        title = metadata.get("title", "")
        description = metadata.get("description", "")
        
        # Check if missing fields
        missing = []
        if not metadata.get("price"): missing.append("price")
        if not metadata.get("surface"): missing.append("surface")
        if not metadata.get("rooms"): missing.append("rooms")
        if not metadata.get("city"): missing.append("city")
        if not metadata.get("region"): missing.append("region")
        if not metadata.get("type"): missing.append("type")
        
        if missing and (title or description):
            records_to_process.append(f"Title: {title}\nDescription: {description}")
            indices_to_process.append(idx)
    
    if not records_to_process:
        logger.info(f"[NullHandler] No records need enrichment")
        return records
    
    # Batch extract
    extractor = get_extractor()
    extracted_batch = extractor.extract_batch(records_to_process)
    
    # Update records
    results = []
    for idx, record in enumerate(records):
        metadata = record.get("metadata", record)
        updated = dict(metadata)
        
        # Find if this record was processed
        if idx in indices_to_process:
            proc_idx = indices_to_process.index(idx)
            extracted = extracted_batch[proc_idx] if proc_idx < len(extracted_batch) else {}
            
            if extracted:
                filled = []
                
                # Apply extracted values
                if not updated.get("price") and extracted.get("price"):
                    updated["price"] = float(extracted["price"])
                    filled.append("price")
                if not updated.get("surface") and extracted.get("surface"):
                    updated["surface"] = float(extracted["surface"])
                    filled.append("surface")
                if not updated.get("rooms") and extracted.get("rooms"):
                    updated["rooms"] = int(extracted["rooms"])
                    filled.append("rooms")
                if not updated.get("city") and extracted.get("city"):
                    updated["city"] = extracted["city"]
                    filled.append("city")
                if not updated.get("region") and extracted.get("governorate"):
                    updated["region"] = extracted["governorate"]
                    filled.append("region")
                if not updated.get("type") and extracted.get("property_type"):
                    updated["type"] = extracted["property_type"]
                    filled.append("type")
                if not updated.get("features") and extracted.get("features"):
                    updated["features"] = extracted["features"]
                    filled.append("features")
                
                if filled:
                    updated["nlp_filled_fields"] = filled
                    updated["nlp_enriched"] = True
                    logger.debug(f"Filled {filled} for {updated.get('property_id', '?')}")
        
        results.append(updated)
    
    enriched_count = sum(1 for r in results if r.get("nlp_enriched"))
    logger.info(
        f"[NullHandler] Processed {total} records — "
        f"enriched {enriched_count} ({enriched_count/total*100:.1f}%)"
    )
    
    return results


def null_report(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generate a null analysis report before running handler.
    """
    key_fields = [
        "price", "surface", "rooms", "city", "region",
        "municipalite", "latitude", "longitude",
        "transaction_type", "type", "description",
    ]
    total = len(records)
    if total == 0:
        return {}
    
    null_counts = {}
    for field in key_fields:
        null_count = sum(
            1 for r in records
            if not (r.get("metadata", r) or {}).get(field)
        )
        null_counts[field] = {
            "null_count": null_count,
            "null_pct": round(null_count / total * 100, 1),
            "filled_count": total - null_count,
        }
    
    return {
        "total_records": total,
        "field_null_analysis": null_counts,
        "most_problematic": sorted(
            null_counts.items(),
            key=lambda x: x[1]["null_pct"],
            reverse=True,
        )[:5],
    }