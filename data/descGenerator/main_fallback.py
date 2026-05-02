# descGenerator/main_fallback.py - With Ollama fallback support

import json
import logging
import random
import httpx
import asyncio
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
OLLAMA_URL = "http://localhost:11434"
OLLAMA_MODEL = "gemma3:4b"
USE_OLLAMA = True  # Set to False to force using template fallback

# Description templates (final fallback)
DESCRIPTION_TEMPLATES = {
    "apartment": {
        "sale": "Cet appartement charmant de {surface}m² situé à {city} dispose de {rooms} pièces. Avec son design moderne et son emplacement privilégié, il est parfait pour les familles.",
        "rent": "Bel appartement de {surface}m² à {city} avec {rooms} pièces. Disponible à la location, cette propriété offre confort et praticité."
    },
    "house": {
        "sale": "Maison spacieuse de {surface}m² située à {city}. Cette propriété avec {rooms} pièces offre un excellent espace de vie pour votre famille.",
        "rent": "Jolie maison de {surface}m² à {city} avec {rooms} pièces. Parfaite pour ceux qui recherchent calme et confort."
    },
    "land": {
        "sale": "Terrain de {surface}m² à {city}. Excellente opportunité pour construction ou investissement.",
        "rent": "Terrain de {surface}m² disponible à la location à {city}. Idéal pour divers projets."
    },
    "commercial": {
        "sale": "Espace commercial de {surface}m² à {city}. Emplacement stratégique pour votre entreprise.",
        "rent": "Local commercial de {surface}m² à louer à {city}. Parfait pour commerce ou bureau."
    }
}

HIGHLIGHTS_TEMPLATES = [
    "emplacement privilégié",
    "finition moderne",
    "excellent rapport qualité-prix",
    "proche des commodités",
    "bonne opportunité d'investissement",
    "quartier calme",
    "proche des transports",
    "agencement spacieux"
]

# Cache for Ollama responses
cache = {}

async def check_ollama_health() -> bool:
    """Check if Ollama service is running and model is available"""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            # Check if Ollama is running
            response = await client.get(f"{OLLAMA_URL}/api/tags")
            if response.status_code != 200:
                return False
            
            # Check if our model is available
            models = response.json().get("models", [])
            model_names = [m.get("name", "").split(":")[0] for m in models]
            
            if OLLAMA_MODEL.split(":")[0] not in model_names:
                logger.warning(f"Model {OLLAMA_MODEL} not found in Ollama. Available: {model_names}")
                return False
            
            return True
    except Exception as e:
        logger.warning(f"Ollama health check failed: {e}")
        return False


async def generate_with_ollama(prompt: str) -> Optional[str]:
    """Generate description using Ollama Gemma3:4b"""
    
    # Check cache first
    cache_key = hash(prompt)
    if cache_key in cache:
        logger.info("Using cached Ollama response")
        return cache[cache_key]
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{OLLAMA_URL}/api/generate",
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
                
                # Cache the result
                cache[cache_key] = description
                logger.info(f"Ollama generated description ({len(description)} chars)")
                return description
            else:
                logger.error(f"Ollama returned error: {response.status_code}")
                return None
                
    except Exception as e:
        logger.error(f"Ollama generation failed: {e}")
        return None


def generate_template_description(metadata: dict) -> tuple[str, list]:
    """Fallback: Generate description from templates"""
    property_type = metadata.get("property_type", "apartment")
    transaction = metadata.get("transaction", "sale")
    city = metadata.get("city", "Tunis")
    surface = metadata.get("surface_m2", "spacieuse")
    rooms = metadata.get("rooms", "plusieurs")
    
    if property_type not in DESCRIPTION_TEMPLATES:
        property_type = "apartment"
    
    template = DESCRIPTION_TEMPLATES[property_type].get(
        transaction,
        DESCRIPTION_TEMPLATES[property_type]["sale"]
    )
    
    description = template.format(
        surface=surface,
        city=city,
        rooms=rooms
    )
    
    selected_highlights = random.sample(
        HIGHLIGHTS_TEMPLATES,
        min(3, len(HIGHLIGHTS_TEMPLATES))
    )
    
    return description, selected_highlights


def build_ollama_prompt(metadata: dict) -> str:
    """Build a prompt for Ollama to generate a property description"""
    
    property_type = metadata.get("property_type", "apartment")
    transaction = metadata.get("transaction", "sale")
    city = metadata.get("city", "Tunis")
    surface = metadata.get("surface_m2", "spacious")
    rooms = metadata.get("rooms", "multiple")
    price = metadata.get("price", "")
    
    transaction_fr = "vente" if transaction == "sale" else "location"
    type_fr = {
        "apartment": "appartement",
        "house": "maison",
        "land": "terrain",
        "commercial": "local commercial"
    }.get(property_type, property_type)
    
    prompt = f"""Tu es un agent immobilier professionnel en Tunisie. Rédige une description courte (3-4 phrases) pour une annonce immobilière.

Caractéristiques du bien:
- Type: {type_fr}
- Transaction: {transaction_fr}
- Ville: {city}
- Surface: {surface} m²
- Pièces: {rooms}
{f"Prix: {price} TND" if price else ""}

La description doit être:
- En français
- Professionnelle et engageante
- Mettre en valeur les points forts (emplacement, luminosité, confort)
- Ne pas inclure les mots "Je ne peux pas" ou "Désolé"

Description:"""

    return prompt


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    # Check Ollama on startup
    if USE_OLLAMA:
        ollama_available = await check_ollama_health()
        if ollama_available:
            logger.info(f"✅ Ollama is available with model {OLLAMA_MODEL}")
        else:
            logger.warning(f"⚠️ Ollama not available, will use template fallback")
    yield
    logger.info("Shutting down")


app = FastAPI(
    title="AI Property Description Generator",
    description="Generates property descriptions using Ollama Gemma3 with fallback",
    lifespan=lifespan
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8000", "http://localhost:8081", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    ollama_status = await check_ollama_health() if USE_OLLAMA else False
    return {
        "status": "ok",
        "service": "description-generator-fallback",
        "ollama_available": ollama_status,
        "model": OLLAMA_MODEL if ollama_status else None
    }


@app.post("/generate-description")
async def generate_description(
    images: List[UploadFile] = File(...),
    metadata: str = Form(...)
):
    """Generate property description - tries Ollama first, then falls back to templates"""
    
    try:
        # Parse metadata
        meta = json.loads(metadata)
        
        # Determine generation method
        ollama_available = USE_OLLAMA and await check_ollama_health()
        
        description = None
        highlights = None
        
        if ollama_available:
            # Try Ollama first
            prompt = build_ollama_prompt(meta)
            logger.info(f"Generating with Ollama for {meta.get('city')}")
            
            description = await generate_with_ollama(prompt)
            
            if description:
                # Extract or generate highlights
                highlights = [
                    "emplacement idéal",
                    "bien entretenu",
                    "excellente opportunité"
                ]
                logger.info("✅ Successfully generated with Ollama")
            else:
                logger.warning("Ollama generation failed, falling back to templates")
        
        # Fallback to templates if Ollama failed or not available
        if not description:
            logger.info("Using template fallback")
            description, highlights = generate_template_description(meta)
        
        return {
            "description": description,
            "highlights": highlights,
            "tone": "professional",
            "generated_by": "ollama" if ollama_available else "template"
        }
        
    except Exception as e:
        logger.error(f"Error generating description: {e}")
        return {
            "description": f"Propriété à {meta.get('city', 'Tunis')}. Contactez-nous pour plus d'informations.",
            "highlights": ["à visiter", "bonne opportunité"],
            "tone": "simple",
            "generated_by": "emergency"
        }


@app.post("/generate-description/ollama-only")
async def generate_with_ollama_only(
    metadata: dict
):
    """Test endpoint: Only use Ollama (no fallback)"""
    if not await check_ollama_health():
        raise HTTPException(status_code=503, detail="Ollama service not available")
    
    prompt = build_ollama_prompt(metadata)
    description = await generate_with_ollama(prompt)
    
    if not description:
        raise HTTPException(status_code=500, detail="Failed to generate description")
    
    return {
        "description": description,
        "model": OLLAMA_MODEL
    }


if __name__ == "__main__":
    import uvicorn
    print(f"🚀 Starting description generator with Ollama fallback on http://localhost:8001")
    print(f"   Model: {OLLAMA_MODEL}")
    uvicorn.run(app, host="0.0.0.0", port=8001)