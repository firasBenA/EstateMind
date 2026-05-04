"""LLM parameter extraction helpers for the EstateMind agent."""
import json
import logging
import os
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

CITIES = [
    "Tunis", "Sfax", "Ariana", "Sousse", "Manouba",
    "Bizerte", "Nabeul", "Monastir", "Gabes", "Gafsa",
]

PROPERTY_TYPE_MAP = {
    "apartment": "apartment",
    "appartement": "apartment",
    "flat": "apartment",
    "house": "house",
    "villa": "house",
    "maison": "house",
    "land": "land",
    "terrain": "land",
    "plot": "land",
    "commercial": "commercial",
    "bureau": "commercial",
    "office": "commercial",
    "shop": "commercial",
    "store": "commercial",
}

TRANSACTION_MAP = {
    "sale": "sale",
    "vente": "sale",
    "buy": "sale",
    "purchase": "sale",
    "sell": "sale",
    "rent": "rent",
    "rental": "rent",
    "louer": "rent",
    "location": "rent",
}

# 🔹 UPDATED: Use currently supported Groq model
DEFAULT_GROQ_MODEL = os.getenv("LLM_PRICE_EXTRACTION_MODEL", "llama-3.1-8b-instant")
DEFAULT_OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# In _call_groq(), add logging:
def _call_groq(prompt: str) -> Optional[str]:
    logger.info("🔄 [LLM_EXTRACT] Attempting Groq extraction...")  # ← NEW
    
    try:
        from groq import Groq
    except ImportError:
        logger.debug("❌ [LLM_EXTRACT] Groq client unavailable")
        return None

    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        logger.debug("❌ [LLM_EXTRACT] GROQ_API_KEY not set")
        return None

    try:
        client = Groq(api_key=api_key)
        logger.info(f"📡 [LLM_EXTRACT] Calling Groq API with model: {DEFAULT_GROQ_MODEL}")  # ← NEW
        
        response = client.chat.completions.create(
            model=DEFAULT_GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=250,
        )

        if response.choices and response.choices[0].message.content:
            logger.info("✅ [LLM_EXTRACT] Groq extraction successful")  # ← NEW
            return response.choices[0].message.content

        logger.warning("⚠️ [LLM_EXTRACT] Groq returned empty response")
        return None

    except Exception as e:
        logger.warning(f"❌ [LLM_EXTRACT] Groq extraction failed: {e}")  # ← Already there
        return None
        
def extract_predict_price_params_with_llm(message: str) -> Dict[str, Any]:
    """Extract price prediction fields from a user query using an LLM."""
    if not message or len(message.strip()) < 5:
        return {}

    prompt = _build_predict_price_prompt(message)
    raw_text = _call_llm(prompt)
    if not raw_text:
        return {}

    parsed = _extract_json(raw_text)
    if not parsed:
        return {}

    params: Dict[str, Any] = {}
    property_type = parsed.get("property_type")
    if isinstance(property_type, str):
        normalized = property_type.strip().lower()
        params["property_type"] = PROPERTY_TYPE_MAP.get(normalized, normalized)

    city = parsed.get("city")
    if isinstance(city, str):
        city_normalized = city.strip().capitalize()
        if city_normalized in CITIES:
            params["city"] = city_normalized
        else:
            params["city"] = city_normalized

    surface = parsed.get("surface")
    if surface is not None:
        try:
            params["surface"] = float(surface)
        except (ValueError, TypeError):
            pass

    rooms = parsed.get("rooms")
    if rooms is not None:
        try:
            params["rooms"] = int(round(float(rooms)))
        except (ValueError, TypeError):
            pass

    transaction_type = parsed.get("transaction_type")
    if isinstance(transaction_type, str):
        normalized = transaction_type.strip().lower()
        params["transaction_type"] = TRANSACTION_MAP.get(normalized, normalized)

    region = parsed.get("region")
    if isinstance(region, str) and region.strip():
        params["region"] = region.strip().capitalize()

    return params


def _build_predict_price_prompt(message: str) -> str:
    return (
        "Extract only JSON from the user real estate query. "
        "Return the exact fields: property_type, city, surface, rooms, transaction_type, region. "
        "If a field is missing, omit it from the JSON. "
        "Do not add any explanation. "
        "Use numeric values for surface and rooms. "
        "Example output: {\n"
        "  \"property_type\": \"apartment\",\n"
        "  \"city\": \"Sfax\",\n"
        "  \"surface\": 200.0,\n"
        "  \"rooms\": 3,\n"
        "  \"transaction_type\": \"sale\"\n"
        "}.\n"
        "Cities to normalize: Tunis, Sfax, Ariana, Sousse, Manouba, Bizerte, Nabeul, Monastir, Gabes, Gafsa.\n"
        f"Query: {message.strip()}"
    )


def _call_llm(prompt: str) -> Optional[str]:
    """Try providers in order: OpenAI → Groq → None (fallback to regex)."""
    # Try OpenAI first (if configured)
    text = _call_openai(prompt)
    if text:
        return text

    # Try Groq fallback
    text = _call_groq(prompt)
    return text


def _call_openai(prompt: str) -> Optional[str]:
    try:
        from openai import OpenAI
    except ImportError:
        logger.debug("OpenAI client unavailable for LLM extraction.")
        return None

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.debug("OPENAI_API_KEY not set; skipping OpenAI extraction.")
        return None

    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=DEFAULT_OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=250,
            temperature=0.0,
        )

        if response.choices and response.choices[0].message.content:
            return response.choices[0].message.content

        return None
    except Exception as e:
        logger.warning(f"OpenAI extraction failed: {e}", exc_info=True)
        return None


def _call_groq(prompt: str) -> Optional[str]:
    """Call Groq API with currently supported model."""
    try:
        from groq import Groq
    except ImportError:
        logger.debug("Groq client unavailable for LLM extraction.")
        return None

    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        logger.debug("GROQ_API_KEY not set; skipping Groq extraction.")
        return None

    try:
        client = Groq(api_key=api_key)
        
        # 🔹 UPDATED: Use supported model
        response = client.chat.completions.create(
            model=DEFAULT_GROQ_MODEL,  # "llama-3.1-8b-instant"
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=250,
        )

        if response.choices and response.choices[0].message.content:
            return response.choices[0].message.content

        logger.warning("Unexpected Groq response structure.")
        return None

    except Exception as e:
        logger.warning(f"Groq extraction failed: {e}", exc_info=True)
        return None


def _extract_json(text: str) -> Optional[Dict[str, Any]]:
    if not text or "{" not in text:
        return None

    try:
        start = text.index("{")
        end = text.rfind("}")
        json_text = text[start : end + 1]
        return json.loads(json_text)
    except json.JSONDecodeError:
        try:
            # Attempt to sanitize by removing code fences and trailing comments
            cleaned = text.strip().replace("```json", "").replace("```", "")
            start = cleaned.index("{")
            end = cleaned.rfind("}")
            return json.loads(cleaned[start : end + 1])
        except Exception:
            logger.warning("Failed to parse JSON from LLM text.")
            return None
    except Exception as e:
        logger.warning(f"Unexpected error parsing LLM JSON: {e}")
        return None