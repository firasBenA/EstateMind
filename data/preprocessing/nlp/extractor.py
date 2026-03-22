"""
EstateMind — NLP Field Extractor

Extracts structured fields from raw French/Arabic listing descriptions.
Uses LLM via OpenRouter for smart extraction.
"""

from __future__ import annotations

import os
import re
import json
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv
import requests
from loguru import logger

# Load environment variables
load_dotenv()

class Extractor:
    def __init__(self, api_key: str = None, model: str = None):
        """Initialize extractor with API key from env if not provided"""
        self.api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        self.model = model or os.getenv("OPENROUTER_MODEL", "mistralai/mistral-small-2603")
        
        if not self.api_key:
            raise ValueError("OpenRouter API key not found. Set OPENROUTER_API_KEY in .env")
        
        self.base_url = "https://openrouter.ai/api/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        logger.info(f"LLM Extractor initialized with model: {self.model}")

    def extract(self, text: str) -> Dict[str, Any]:
        """Extract all fields from listing text using LLM"""
        if not text or len(text) < 10:
            logger.debug("Text too short for extraction")
            return {}
        
        prompt = f"""Extract structured data from this Tunisian real estate listing description.

Listing text: {text}

Return ONLY a JSON object with these exact fields:
{{
  "rooms": number or null,
  "surface": number or null,
  "price": number or null,
  "city": string or null,
  "governorate": string or null,
  "district": string or null,
  "transaction_type": "Sale" or "Rent" or null,
  "property_type": "Apartment" or "Villa" or "Land" or "Commercial" or "Other" or null,
  "features": ["feature1", "feature2"] or null
}}

Guidelines:
- Prices in TND (Tunisian Dinars)
- Surface in square meters (m²)
- Use official Tunisian governorates: Tunis, Ariana, Ben Arous, Manouba, Nabeul, Zaghouan, Bizerte, Béja, Jendouba, Le Kef, Siliana, Sousse, Monastir, Mahdia, Sfax, Kairouan, Kasserine, Sidi Bouzid, Gabès, Médenine, Tataouine, Gafsa, Tozeur, Kébili
- Features: common amenities like piscine, parking, jardin, vue mer, clim, etc.

JSON only, no other text:"""
        
        try:
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json={
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.1,
                    "max_tokens": 500
                },
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                content = result['choices'][0]['message']['content']
                
                # Clean response (remove markdown code blocks if present)
                content = self._clean_json_response(content)
                
                try:
                    extracted = json.loads(content)
                    logger.debug(f"Successfully extracted: {list(extracted.keys())}")
                    return extracted
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON: {e}\nContent: {content[:200]}")
                    return {}
            else:
                logger.error(f"OpenRouter API error: {response.status_code}")
                if response.status_code == 401:
                    logger.error("Invalid API key. Please check your OPENROUTER_API_KEY")
                return {}
                
        except requests.Timeout:
            logger.warning("LLM extraction timeout")
            return {}
        except Exception as e:
            logger.error(f"LLM extraction failed: {e}")
            return {}
    
    def extract_batch(self, texts: List[str], max_batch_size: int = 10) -> List[Dict[str, Any]]:
        """
        Extract data from multiple listings in one API call
        """
        if not texts:
            return []
        
        results = []
        for i in range(0, len(texts), max_batch_size):
            batch = texts[i:i+max_batch_size]
            batch_results = self._extract_batch_internal(batch)
            results.extend(batch_results)
            
            # Small delay between batches to avoid rate limits
            if i + max_batch_size < len(texts):
                import time
                time.sleep(0.5)
        
        return results
    
    def _extract_batch_internal(self, texts: List[str]) -> List[Dict[str, Any]]:
        """Internal batch extraction"""
        listings_text = []
        for idx, text in enumerate(texts):
            if text and len(text) > 10:
                listings_text.append(f"Listing {idx+1}: {text[:500]}")
        
        if not listings_text:
            return [{}] * len(texts)
        
        combined_text = "\n---\n".join(listings_text)
        
        prompt = f"""Extract structured data from these {len(texts)} Tunisian real estate listings.

{combined_text}

Return a JSON array with one object per listing. Each object should have:
{{
  "rooms": number or null,
  "surface": number or null,
  "price": number or null,
  "city": string or null,
  "governorate": string or null,
  "district": string or null,
  "transaction_type": "Sale" or "Rent" or null,
  "property_type": "Apartment" or "Villa" or "Land" or "Commercial" or "Other" or null,
  "features": ["feature1", "feature2"] or null
}}

JSON array only, no other text:"""
        
        try:
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json={
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.1,
                    "max_tokens": 2000
                },
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                content = result['choices'][0]['message']['content']
                content = self._clean_json_response(content)
                
                try:
                    extracted = json.loads(content)
                    if isinstance(extracted, list):
                        # Pad with empty dicts if needed
                        while len(extracted) < len(texts):
                            extracted.append({})
                        return extracted[:len(texts)]
                    else:
                        return [extracted] if extracted else [{}] * len(texts)
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse batch JSON")
                    return [{}] * len(texts)
            else:
                logger.error(f"Batch extraction API error: {response.status_code}")
                return [{}] * len(texts)
        except Exception as e:
            logger.error(f"Batch extraction error: {e}")
            return [{}] * len(texts)
    
    def _clean_json_response(self, content: str) -> str:
        """Clean JSON response from markdown formatting"""
        content = content.strip()
        if content.startswith('```json'):
            content = content[7:]
        if content.startswith('```'):
            content = content[3:]
        if content.endswith('```'):
            content = content[:-3]
        return content.strip()
    
    def extract_location_only(self, text: str) -> Dict[str, str]:
        """Extract only location fields (faster, less tokens)"""
        if not text or len(text) < 10:
            return {}
        
        prompt = f"""From this Tunisian real estate text, extract location information:
Text: {text}

Return JSON with:
- city: city name
- governorate: governorate/region  
- district: district/neighborhood

Use official Tunisian governorates: Tunis, Ariana, Ben Arous, Manouba, Nabeul, Zaghouan, Bizerte, Béja, Jendouba, Le Kef, Siliana, Sousse, Monastir, Mahdia, Sfax, Kairouan, Kasserine, Sidi Bouzid, Gabès, Médenine, Tataouine, Gafsa, Tozeur, Kébili

JSON only, no other text:"""
        
        try:
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json={
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.1,
                    "max_tokens": 150
                },
                timeout=8
            )
            
            if response.status_code == 200:
                result = response.json()
                content = result['choices'][0]['message']['content']
                content = self._clean_json_response(content)
                return json.loads(content)
            return {}
        except:
            return {}
    
    def extract_features(self, text: str) -> List[str]:
        """Extract property features only"""
        if not text:
            return []
        
        prompt = f"""From this Tunisian real estate text, list all property features mentioned:
Text: {text}

Return ONLY a JSON list of features (e.g., ["pool", "parking", "garden", "elevator", "air conditioning"]):"""
        
        try:
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json={
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.1,
                    "max_tokens": 200
                },
                timeout=8
            )
            
            if response.status_code == 200:
                result = response.json()
                content = result['choices'][0]['message']['content']
                content = self._clean_json_response(content)
                return json.loads(content)
            return []
        except:
            return []


# Singleton instance for reuse
_extractor_instance = None

def get_extractor() -> Extractor:
    """Get or create extractor instance (singleton)"""
    global _extractor_instance
    if _extractor_instance is None:
        _extractor_instance = Extractor()
    return _extractor_instance


# Convenience functions for backward compatibility
def extract_all(title: str = "", description: str = "", url: str = "", existing: Dict = None) -> Dict[str, Any]:
    """Extract all fields (compatible with existing code)"""
    extractor = get_extractor()
    text = f"{title} {description}"
    extracted = extractor.extract(text)
    
    # Map to expected field names
    result = {}
    if extracted.get("rooms"):
        result["rooms"] = {"value": extracted["rooms"], "source": "llm", "confidence": 0.85}
    if extracted.get("surface"):
        result["surface_area_m2"] = {"value": extracted["surface"], "source": "llm", "confidence": 0.85}
    if extracted.get("price"):
        result["price"] = {"value": extracted["price"], "source": "llm", "confidence": 0.85}
    if extracted.get("city"):
        result["city"] = {"value": extracted["city"], "source": "llm", "confidence": 0.85}
    if extracted.get("governorate"):
        result["governorate"] = {"value": extracted["governorate"], "source": "llm", "confidence": 0.85}
    if extracted.get("district"):
        result["district"] = {"value": extracted["district"], "source": "llm", "confidence": 0.85}
    if extracted.get("transaction_type"):
        result["transaction_type"] = {"value": extracted["transaction_type"], "source": "llm", "confidence": 0.85}
    if extracted.get("property_type"):
        result["property_type"] = {"value": extracted["property_type"], "source": "llm", "confidence": 0.85}
    if extracted.get("features"):
        result["features"] = {"value": extracted["features"], "source": "llm", "confidence": 0.85}
    
    return result