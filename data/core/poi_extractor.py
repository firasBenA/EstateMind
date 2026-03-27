import re
from typing import List
from core.models import POI

POI_KEYWORDS = {
    "Education": ["école", "lycée", "faculté", "université", "crèche", "jardin d'enfants", "institut"],
    "Health": ["pharmacie", "hôpital", "clinique", "médecin", "dentiste", "centre médical"],
    "Transport": ["métro", "bus", "train", "taxis", "aeroport", "station", "gare"],
    "Shopping": ["supermarché", "magasin", "boutique", "centre commercial", "marché", "carrefour", "monoprix", "aziza", "mg"],
    "Leisure": ["parc", "jardin", "plage", "mer", "café", "restaurant", "stade", "salle de sport", "piscine"],
    "Services": ["banque", "poste", "mairie", "municipalité", "police", "mosquée"]
}

def extract_pois_from_text(text: str) -> List[POI]:
    """
    Extracts POIs from text based on keywords.
    """
    if not text:
        return []
    
    text_lower = text.lower()
    found_pois = []
    seen_categories = set()
    
    for category, keywords in POI_KEYWORDS.items():
        for keyword in keywords:
            # Look for whole words or phrases
            if re.search(r'\b' + re.escape(keyword) + r'\b', text_lower):
                # We found a keyword. To avoid duplicates, we can check if we already added this category
                # or we can add specific items. 
                # For now, let's add the keyword as the name.
                
                # Check if we already have this specific keyword to avoid duplicates like "école" appearing twice
                if not any(p.name == keyword for p in found_pois):
                    found_pois.append(POI(name=keyword.capitalize(), category=category))
    
    return found_pois

def normalize_and_categorize_poi_from_text(text: str) -> List[POI]:
    if not text:
        return []
    text_lower = text.lower()
    found_pois: List[POI] = []
    for category, keywords in POI_KEYWORDS.items():
        for keyword in keywords:
            if re.search(r'\b' + re.escape(keyword) + r'\b', text_lower):
                if not any(p.name.lower() == keyword for p in found_pois):
                    found_pois.append(POI(name=keyword.capitalize(), category=category))
    return found_pois
