# preprocessing/nlp/extractor.py
"""
EstateMind — NLP Field Extractor
Uses local Ollama or OpenRouter for smart extraction.
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
    """LLM Extractor with local Ollama support"""
    
    FREE_MODELS = [
        "google/gemini-2.0-flash-exp:free",
        "meta-llama/llama-3.2-3b-instruct:free",
        "microsoft/phi-3-mini-128k-instruct:free",
    ]
    
    def __init__(self, api_key: str = None, model: str = None, use_local: bool = None):
        """Initialize extractor"""
        self.api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        self.model = model or os.getenv("OPENROUTER_MODEL", self.FREE_MODELS[0])
        
        # Local LLM settings
        self.use_local = use_local if use_local is not None else os.getenv("USE_LOCAL_LLM", "false").lower() == "true"
        self.local_model = os.getenv("LOCAL_LLM_MODEL", "gemma2:2b")
        self.local_url = os.getenv("LOCAL_LLM_URL", "http://localhost:11434/api/generate")
        
        # OpenRouter setup
        if self.api_key and not self.use_local:
            self.base_url = "https://openrouter.ai/api/v1/chat/completions"
            self.headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://estatemind.ai",
                "X-Title": "EstateMind",
            }
            logger.info(f"LLM Extractor initialized with OpenRouter model: {self.model}")
        elif self.use_local:
            logger.info(f"LLM Extractor initialized with local Ollama model: {self.local_model}")
        else:
            logger.warning("No LLM configured, using regex fallback only")
            self.base_url = None
            self.headers = None
    
    def _check_local_ollama(self) -> bool:
        """Check if Ollama is running"""
        try:
            resp = requests.get("http://localhost:11434/api/tags", timeout=2)
            return resp.status_code == 200
        except:
            return False
    
    def _call_local_ollama(self, prompt: str, max_tokens: int = 500) -> Optional[str]:
        """Call local Ollama instance"""
        if not self._check_local_ollama():
            logger.debug("Local Ollama not available")
            return None
        
        try:
            resp = requests.post(
                self.local_url,
                json={
                    "model": self.local_model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.1,
                        "num_predict": max_tokens,
                        "num_threads": 2  # Limit CPU usage
                    }
                },
                timeout=30
            )
            if resp.status_code == 200:
                return resp.json().get("response", "")
            else:
                logger.debug(f"Ollama error: {resp.status_code}")
                return None
        except requests.Timeout:
            logger.debug("Ollama timeout")
            return None
        except Exception as e:
            logger.debug(f"Ollama error: {e}")
            return None
    
    def _call_openrouter(self, prompt: str, max_tokens: int = 500) -> Optional[str]:
        """Call OpenRouter API"""
        if not self.api_key or not self.base_url:
            return None
        
        try:
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json={
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.1,
                    "max_tokens": max_tokens
                },
                timeout=15
            )
            
            if response.status_code == 200:
                result = response.json()
                return result['choices'][0]['message']['content']
            elif response.status_code == 402:
                logger.warning("OpenRouter payment required, try using local Ollama")
                return None
            else:
                logger.debug(f"OpenRouter API error: {response.status_code}")
                return None
                
        except requests.Timeout:
            logger.debug("OpenRouter timeout")
            return None
        except Exception as e:
            logger.debug(f"OpenRouter error: {e}")
            return None
    
    def _call_llm(self, prompt: str, max_tokens: int = 500) -> Optional[str]:
        """Call LLM with fallback: Local Ollama -> OpenRouter -> None"""
        # Try local Ollama first (if enabled)
        if self.use_local:
            result = self._call_local_ollama(prompt, max_tokens)
            if result:
                logger.debug("Used local Ollama for extraction")
                return result
        
        # Try OpenRouter as fallback
        if self.api_key:
            result = self._call_openrouter(prompt, max_tokens)
            if result:
                return result
        
        logger.debug("No LLM available")
        return None
    
    def extract(self, text: str) -> Dict[str, Any]:
        """Extract all fields from listing text using LLM"""
        if not text or len(text) < 10:
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
            content = self._call_llm(prompt, max_tokens=500)
            if not content:
                return {}
            
            content = self._clean_json_response(content)
            extracted = json.loads(content)
            logger.debug(f"Successfully extracted: {list(extracted.keys())}")
            return extracted
            
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON: {e}\nContent: {content[:200] if content else 'None'}")
            return {}
        except Exception as e:
            logger.error(f"LLM extraction failed: {e}")
            return {}
    
    def extract_batch(self, texts: List[str], max_batch_size: int = 5) -> List[Dict[str, Any]]:
        """Extract data from multiple listings"""
        if not texts:
            return []
        
        results = []
        for i in range(0, len(texts), max_batch_size):
            batch = texts[i:i+max_batch_size]
            batch_results = self._extract_batch_internal(batch)
            results.extend(batch_results)
            
            # Small delay between batches
            if i + max_batch_size < len(texts):
                import time
                time.sleep(1)
        
        return results
    
    def _extract_batch_internal(self, texts: List[str]) -> List[Dict[str, Any]]:
        """Internal batch extraction"""
        listings_text = []
        for idx, text in enumerate(texts):
            if text and len(text) > 10:
                listings_text.append(f"Listing {idx+1}: {text[:400]}")
        
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
        
        content = self._call_llm(prompt, max_tokens=2000)
        if not content:
            return [{}] * len(texts)
        
        try:
            content = self._clean_json_response(content)
            extracted = json.loads(content)
            if isinstance(extracted, list):
                while len(extracted) < len(texts):
                    extracted.append({})
                return extracted[:len(texts)]
            else:
                return [extracted] if extracted else [{}] * len(texts)
        except json.JSONDecodeError:
            logger.warning("Failed to parse batch JSON")
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
        """Extract only location fields"""
        if not text or len(text) < 10:
            return {}
        
        prompt = f"""From this Tunisian real estate text, extract location information:
Text: {text}

Return JSON with:
- city: city name
- governorate: governorate/region  
- district: district/neighborhood

Use official Tunisian governorates.

JSON only, no other text:"""
        
        content = self._call_llm(prompt, max_tokens=150)
        if not content:
            return {}
        
        try:
            content = self._clean_json_response(content)
            return json.loads(content)
        except:
            return {}
    
    def extract_features(self, text: str) -> List[str]:
        """Extract property features only"""
        if not text:
            return []
        
        prompt = f"""From this Tunisian real estate text, list all property features mentioned:
Text: {text}

Return ONLY a JSON list of features (e.g., ["pool", "parking", "garden", "elevator", "air conditioning"]):"""
        
        content = self._call_llm(prompt, max_tokens=200)
        if not content:
            return []
        
        try:
            content = self._clean_json_response(content)
            return json.loads(content)
        except:
            return []


# Singleton instance
_extractor_instance = None

def get_extractor() -> Extractor:
    """Get or create extractor instance"""
    global _extractor_instance
    if _extractor_instance is None:
        _extractor_instance = Extractor()
    return _extractor_instance


# Convenience functions
def extract_all(title: str = "", description: str = "", url: str = "", existing: Dict = None) -> Dict[str, Any]:
    """Extract all fields"""
    extractor = get_extractor()
    text = f"{title} {description}"
    extracted = extractor.extract(text)
    
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