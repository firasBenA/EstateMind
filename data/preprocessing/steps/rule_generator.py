# data/preprocessing/steps/rule_generator.py
"""
EstateMind — Rule Generator

Uses LLM with clean examples to generate normalization rules.
If LLM fails, uses complete fallback rules.
"""

import json
import os
from typing import Dict, Any, List
from loguru import logger
from preprocessing.nlp.extractor import get_extractor


class RuleGenerator:
    """Generate normalization rules using LLM with clean examples"""
    
    def __init__(self):
        self.extractor = get_extractor()
        self.rules = {}
    
    def generate_rules_from_clean_data(self) -> Dict[str, Any]:
        """
        Generate normalization rules using clean data examples.
        If LLM fails, returns complete fallback rules.
        """
        
        # Clean examples of what GOOD data looks like
        clean_examples = [
            {"price": 850000, "surface": 145, "rooms": 4, "city": "La Marsa", "governorate": "Tunis", "transaction_type": "Sale", "property_type": "Apartment"},
            {"price": 1200000, "surface": 280, "rooms": 5, "city": "Hammamet", "governorate": "Nabeul", "transaction_type": "Sale", "property_type": "Villa"},
            {"price": 450000, "surface": 120, "rooms": 3, "city": "Sousse", "governorate": "Sousse", "transaction_type": "Sale", "property_type": "Apartment"},
            {"price": 1500, "surface": 80, "rooms": 2, "city": "Tunis", "governorate": "Tunis", "transaction_type": "Rent", "property_type": "Apartment"},
            {"price": 250000, "surface": 95, "rooms": 3, "city": "Sfax", "governorate": "Sfax", "transaction_type": "Sale", "property_type": "Apartment"}
        ]
        
        # Examples of messy data from scrapers
        messy_examples = [
            {"price": "850 000 TND", "surface": "145 m²", "rooms": "S+3", "city": "la marsa, tunis", "transaction_type": "vente", "type": "appartement"},
            {"price": "1.2M TND", "surface": "280", "rooms": "5 chambres", "city": "Hammamet | bord de mer", "transaction_type": "Sale", "type": "Villa"},
            {"price": "450k", "surface": "120m2", "rooms": "appartement 3", "city": "sousse", "transaction_type": "achat", "type": "appartement"},
            {"price": "1,500 DT", "surface": "80 mètres carrés", "rooms": "studio", "city": "tunis centre", "transaction_type": "location", "type": "studio"},
            {"price": "250.000", "surface": "95", "rooms": "3 pièces", "city": "sfax", "transaction_type": "vente", "type": "appartement"}
        ]
        
        # Try LLM first
        try:
            logger.info("Attempting to generate rules with LLM...")
            
            prompt = f"""Generate JSON normalization rules for Tunisian real estate data.

CLEAN TARGET:
{json.dumps(clean_examples, indent=2)}

MESSY INPUT:
{json.dumps(messy_examples, indent=2)}

Return this exact JSON structure with rules for each field:

{{
  "price": {{
    "remove": ["TND", "DT", "dinars", "dinar", "€", "$"],
    "thousands_sep": [" ", ","],
    "min": 500,
    "max": 50000000
  }},
  "surface": {{
    "remove": ["m²", "m2", "mètres", "metres", "m"],
    "min": 10,
    "max": 10000
  }},
  "rooms": {{
    "patterns": [
      {{"regex": "S\\\\s*\\\\+\\\\s*(\\\\d+)", "add": 1}},
      {{"regex": "(\\\\d+)\\\\s*chambres?"}},
      {{"regex": "(\\\\d+)\\\\s*pi[eè]ces?"}},
      {{"regex": "studio", "value": 1}}
    ],
    "min": 1,
    "max": 20
  }},
  "city": {{
    "split": ["|", ","],
    "take_first": true,
    "title_case": true
  }},
  "governorate": {{
    "mapping": {{
      "tunis": "Tunis",
      "ariana": "Ariana",
      "ben arous": "Ben Arous",
      "manouba": "Manouba",
      "nabeul": "Nabeul",
      "zaghouan": "Zaghouan",
      "bizerte": "Bizerte",
      "béja": "Béja",
      "jendouba": "Jendouba",
      "kef": "Le Kef",
      "siliana": "Siliana",
      "sousse": "Sousse",
      "monastir": "Monastir",
      "mahdia": "Mahdia",
      "sfax": "Sfax",
      "kairouan": "Kairouan",
      "kasserine": "Kasserine",
      "sidi bouzid": "Sidi Bouzid",
      "gabès": "Gabès",
      "médenine": "Médenine",
      "tataouine": "Tataouine",
      "gafsa": "Gafsa",
      "tozeur": "Tozeur",
      "kébili": "Kébili"
    }}
  }},
  "transaction_type": {{
    "mapping": {{
      "vente": "Sale",
      "vendre": "Sale",
      "achat": "Sale",
      "location": "Rent",
      "louer": "Rent",
      "loyer": "Rent"
    }},
    "default": "Sale"
  }},
  "property_type": {{
    "mapping": {{
      "appartement": "Apartment",
      "appart": "Apartment",
      "studio": "Apartment",
      "villa": "Villa",
      "maison": "Villa",
      "duplex": "Villa",
      "triplex": "Villa",
      "terrain": "Land",
      "bureau": "Commercial",
      "commerce": "Commercial",
      "local": "Commercial"
    }},
    "default": "Other"
  }}
}}

Return ONLY the JSON, no other text."""
            
            # Try with current model
            extracted = self.extractor.extract(prompt)
            
            if extracted and isinstance(extracted, dict) and len(extracted) >= 3:
                self.rules = extracted
                logger.info(f"✅ LLM generated rules for {len(self.rules)} fields")
                return self.rules
            else:
                logger.warning("LLM returned invalid rules, using fallback")
                # FIX: Assign fallback rules to self.rules
                self.rules = self._get_fallback_rules()
                return self.rules
                
        except Exception as e:
            logger.error(f"LLM rule generation failed: {e}")
            # FIX: Assign fallback rules to self.rules
            self.rules = self._get_fallback_rules()
            return self.rules
    
    def _get_fallback_rules(self) -> Dict:
        """Complete fallback rules that work"""
        return {
            "price": {
                "remove": ["TND", "DT", "dinars", "dinar", "€", "$"],
                "thousands_sep": [" ", ","],
                "decimal_sep": ".",
                "min": 500,
                "max": 50000000,
                "output_type": "float"
            },
            "surface": {
                "remove": ["m²", "m2", "mètres", "metres", "m"],
                "decimal_sep": ".",
                "min": 10,
                "max": 10000,
                "output_type": "float"
            },
            "rooms": {
                "patterns": [
                    {"regex": r"S\s*\+\s*(\d+)", "add": 1},
                    {"regex": r"(\d+)\s*chambres?"},
                    {"regex": r"(\d+)\s*pi[eè]ces?"},
                    {"regex": "studio", "value": 1},
                    {"regex": r"F(\d+)", "extract": 1},
                    {"regex": r"T(\d+)", "extract": 1}
                ],
                "min": 1,
                "max": 20,
                "output_type": "int"
            },
            "city": {
                "split": ["|", ","],
                "take_first": True,
                "title_case": True,
                "output_type": "string"
            },
            "governorate": {
                "mapping": {
                    "tunis": "Tunis",
                    "ariana": "Ariana",
                    "ariane": "Ariana",
                    "ben arous": "Ben Arous",
                    "benarous": "Ben Arous",
                    "manouba": "Manouba",
                    "nabeul": "Nabeul",
                    "zaghouan": "Zaghouan",
                    "bizerte": "Bizerte",
                    "béja": "Béja",
                    "beja": "Béja",
                    "jendouba": "Jendouba",
                    "kef": "Le Kef",
                    "le kef": "Le Kef",
                    "siliana": "Siliana",
                    "sousse": "Sousse",
                    "monastir": "Monastir",
                    "mahdia": "Mahdia",
                    "sfax": "Sfax",
                    "kairouan": "Kairouan",
                    "kasserine": "Kasserine",
                    "sidi bouzid": "Sidi Bouzid",
                    "gabès": "Gabès",
                    "gabes": "Gabès",
                    "médenine": "Médenine",
                    "medenine": "Médenine",
                    "tataouine": "Tataouine",
                    "gafsa": "Gafsa",
                    "tozeur": "Tozeur",
                    "kébili": "Kébili",
                    "kebili": "Kébili"
                },
                "output_type": "string"
            },
            "district": {
                "output_type": "string"
            },
            "transaction_type": {
                "mapping": {
                    "vente": "Sale",
                    "vendre": "Sale",
                    "achat": "Sale",
                    "acheter": "Sale",
                    "location": "Rent",
                    "louer": "Rent",
                    "loyer": "Rent",
                    "rent": "Rent",
                    "sale": "Sale"
                },
                "default": "Sale",
                "output_type": "string"
            },
            "property_type": {
                "mapping": {
                    "appartement": "Apartment",
                    "appart": "Apartment",
                    "studio": "Apartment",
                    "villa": "Villa",
                    "maison": "Villa",
                    "house": "Villa",
                    "duplex": "Villa",
                    "triplex": "Villa",
                    "terrain": "Land",
                    "land": "Land",
                    "bureau": "Commercial",
                    "office": "Commercial",
                    "commerce": "Commercial",
                    "local": "Commercial",
                    "commercial": "Commercial"
                },
                "default": "Other",
                "output_type": "string"
            },
            "features": {
                "keywords": {
                    "piscine": ["piscine", "swimming pool", "pool"],
                    "parking": ["parking", "garage", "box"],
                    "jardin": ["jardin", "garden", "yard"],
                    "vue mer": ["vue mer", "sea view", "ocean view"],
                    "ascenseur": ["ascenseur", "elevator", "lift"],
                    "meublé": ["meublé", "furnished"],
                    "climatisation": ["climatisation", "climatisé", "clim", "air conditioning"],
                    "chauffage": ["chauffage", "heating"],
                    "balcon": ["balcon", "balcony"],
                    "terrasse": ["terrasse", "terrace"]
                },
                "output_type": "list"
            }
        }
    
    def save_rules(self, path: str = "data/normalization_rules.json"):
        """Save generated rules to file"""
        # Get the absolute path to project root
        current_file = os.path.abspath(__file__)
        # Go up from steps/ to preprocessing/ to data/ to project root
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(current_file))))
        
        full_path = os.path.join(project_root, path)
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        
        with open(full_path, 'w', encoding='utf-8') as f:
            json.dump(self.rules, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✅ Saved rules to {full_path}")
        return full_path
    
    def load_rules(self, path: str = "data/normalization_rules.json"):
        """Load rules from file"""
        # Get the absolute path to project root
        current_file = os.path.abspath(__file__)
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(current_file))))
        
        full_path = os.path.join(project_root, path)
        
        try:
            with open(full_path, 'r', encoding='utf-8') as f:
                self.rules = json.load(f)
            logger.info(f"✅ Loaded rules from {full_path}")
            return True
        except FileNotFoundError:
            logger.warning(f"Rules file not found at {full_path}")
            return False
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse rules file: {e}")
            return False


# Singleton instance
_rules_generator = None

def get_rules_generator() -> RuleGenerator:
    """Get or create rule generator singleton"""
    global _rules_generator
    if _rules_generator is None:
        _rules_generator = RuleGenerator()
    return _rules_generator


# Convenience function
def get_rules() -> Dict:
    """Get normalization rules (generates if needed)"""
    generator = get_rules_generator()
    if not generator.rules:
        # Try to load existing rules first
        if not generator.load_rules():
            # Generate new rules
            generator.rules = generator.generate_rules_from_clean_data()
            generator.save_rules()
    return generator.rules