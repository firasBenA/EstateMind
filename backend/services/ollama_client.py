# backend/dashboard/ollama_client.py

import json
import logging
import httpx
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

OLLAMA_BASE_URL = "http://localhost:11434"
OLLAMA_MODEL = "gemma3:4b"
REQUEST_TIMEOUT = 30.0

async def check_ollama_health() -> bool:
    """Check if Ollama is running and model is available"""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{OLLAMA_BASE_URL}/api/tags")
            if response.status_code != 200:
                return False
            
            models = response.json().get("models", [])
            model_names = [m.get("name", "").split(":")[0] for m in models]
            return "gemma3" in model_names
    except Exception as e:
        logger.warning(f"Ollama health check failed: {e}")
        return False


def build_description_prompt(metadata: Dict[str, Any]) -> str:
    """Build a prompt for Ollama to generate property description"""
    
    property_type = metadata.get("property_type", "apartment")
    transaction = metadata.get("transaction", "sale")
    city = metadata.get("city", "Tunis")
    surface = metadata.get("surface_m2", "")
    rooms = metadata.get("rooms", "")
    price = metadata.get("price", "")
    
    transaction_fr = "vente" if transaction == "sale" else "location"
    
    type_fr = {
        "apartment": "appartement",
        "house": "maison",
        "villa": "villa",
        "land": "terrain",
        "commercial": "local commercial"
    }.get(property_type, property_type)
    
    prompt = f"""Tu es un agent immobilier professionnel en Tunisie. Rédige une description courte et attrayante (4-5 phrases maximum) pour une annonce immobilière.

Caractéristiques du bien:
- Type: {type_fr}
- Transaction: {transaction_fr}
- Ville: {city}
{f"- Surface: {surface} m²" if surface else ""}
{f"- Pièces: {rooms}" if rooms else ""}
{f"- Prix: {price} TND" if price else ""}

La description doit être:
- En français
- Professionnelle et engageante
- Mettre en valeur les points forts (emplacement, luminosité, confort)
- Ne pas inclure de phrases comme "Je ne peux pas" ou "Désolé"
- Courte et concise (max 150 mots)

Description:"""

    return prompt


async def generate_with_ollama(metadata: Dict[str, Any]) -> Optional[str]:
    """Generate property description using Ollama Gemma3:4b"""
    
    prompt = build_description_prompt(metadata)
    logger.info(f"Calling Ollama for description in {metadata.get('city', 'unknown')}")
    
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            response = await client.post(
                f"{OLLAMA_BASE_URL}/api/generate",
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.7,
                        "max_tokens": 300,
                    }
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                description = result.get("response", "").strip()
                logger.info(f"✅ Ollama generated description ({len(description)} chars)")
                return description
            else:
                logger.error(f"Ollama returned error: {response.status_code}")
                return None
                
    except Exception as e:
        logger.error(f"Ollama generation failed: {e}")
        return None


def generate_template_description(metadata: Dict[str, Any]) -> str:
    """Ultimate fallback: generate simple template description"""
    
    property_type = metadata.get("property_type", "apartment")
    transaction = metadata.get("transaction", "sale")
    city = metadata.get("city", "Tunis")
    surface = metadata.get("surface_m2", "")
    rooms = metadata.get("rooms", "")
    
    transaction_fr = "à vendre" if transaction == "sale" else "à louer"
    
    type_fr = {
        "apartment": "appartement",
        "house": "maison",
        "villa": "villa",
        "land": "terrain",
        "commercial": "local commercial"
    }.get(property_type, "bien immobilier")
    
    desc = f"Superbe {type_fr} {transaction_fr} situé à {city}. "
    
    if surface:
        desc += f"Ce bien de {surface} m² offre un espace de vie agréable. "
    if rooms:
        desc += f"Il se compose de {rooms} pièces. "
    
    desc += "Idéalement situé, proche de toutes les commodités. "
    desc += "N'hésitez pas à nous contacter pour plus d'informations ou pour organiser une visite."
    
    return desc