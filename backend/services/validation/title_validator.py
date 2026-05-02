# backend/dashboard/validation/title_validator.py

import re
import json
import requests
from typing import Dict, Any, Tuple
from django.conf import settings

class TitleValidator:
    """
    Validates real estate listing titles using:
    1. Regex patterns (fast pre-filtering)
    2. LLM (Gemma2:2b) for semantic understanding
    """
    
    # Invalid patterns (gibberish, test data)
    INVALID_PATTERNS = [
        r'^test[\d\s]*$', r'^tata$', r'^toto$', r'^titi$',
        r'^aaaa+$', r'^bbbb+$', r'^cccc+$',
        r'^qwerty$', r'^azerty$', r'^12345+$',
        r'^[a-z]{1,3}$',  # Single short word
    ]
    
    # Real estate keywords (must have at least one)
    REAL_ESTATE_KEYWORDS = [
        'appartement', 'villa', 'maison', 'terrain', 'studio', 'duplex',
        'apartment', 'house', 'land', 'office', 'commercial', 'bureau',
        'local', 'immeuble', 'building', 'chalet', 'lofe', 'penthouse',
        'rez-de-chaussée', 'rdc', 'étage', 'floor'
    ]
    
    def __init__(self):
        self.ollama_url = settings.OLLAMA_URL or "http://localhost:11434/api/generate"
        self.model = "3:4b"
    
    def validate(self, title: str) -> Tuple[bool, str, float]:
        """
        Returns: (is_valid, message, confidence)
        """
        if not title or len(title.strip()) < 5:
            return False, "Title must be at least 5 characters", 0.0
        
        if len(title) > 200:
            return False, "Title cannot exceed 200 characters", 0.0
        
        # Step 1: Regex validation (fast)
        regex_valid, regex_message = self._regex_validate(title)
        if not regex_valid:
            return False, regex_message, 0.0
        
        # Step 2: LLM validation (deep understanding)
        llm_valid, llm_message, confidence = self._llm_validate(title)
        
        return llm_valid, llm_message, confidence
    
    def _regex_validate(self, title: str) -> Tuple[bool, str]:
        """Quick regex-based validation"""
        title_lower = title.lower().strip()
        
        # Check invalid patterns
        for pattern in self.INVALID_PATTERNS:
            if re.match(pattern, title_lower):
                return False, f"Title contains invalid content: '{title}'"
        
        # Check repetitive characters (aaaaa, 11111)
        if re.search(r'(.)\1{4,}', title):
            return False, "Title contains repetitive characters that look like gibberish"
        
        # Check for at least one real estate keyword
        has_keyword = any(kw in title_lower for kw in self.REAL_ESTATE_KEYWORDS)
        if not has_keyword:
            return True, "Warning: Title may not describe a property clearly"
        
        return True, ""
    
    def _llm_validate(self, title: str) -> Tuple[bool, str, float]:
        """Deep validation using local LLM"""
        
        prompt = f"""You are a real estate listing validator. Determine if this title is a valid real estate listing.
        
Rules:
- Must describe a property (apartment, villa, land, commercial space)
- Must NOT be gibberish, test data, or random characters
- Must be descriptive (location, type, features)

Title: "{title}"

Return ONLY JSON:
{{"valid": true/false, "reason": "explanation", "confidence": 0.0-1.0, "suggested_title": "optional suggestion"}}

Examples of INVALID titles:
- "test" → {{"valid": false, "reason": "Test data", "confidence": 0.0}}
- "aaaa" → {{"valid": false, "reason": "Gibberish", "confidence": 0.0}}
- "hello" → {{"valid": false, "reason": "Not property related", "confidence": 0.1}}

Examples of VALID titles:
- "Modern apartment in La Marsa with sea view" → {{"valid": true, "reason": "Valid property description", "confidence": 0.95}}
- "Beautiful villa for sale in Sousse" → {{"valid": true, "reason": "Valid property description", "confidence": 0.95}}
"""
        
        try:
            response = requests.post(
                self.ollama_url,
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.1, "num_predict": 150}
                },
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                # Extract JSON from response
                text = result.get('response', '')
                json_match = re.search(r'\{.*\}', text, re.DOTALL)
                if json_match:
                    data = json.loads(json_match.group())
                    return (
                        data.get('valid', False),
                        data.get('reason', 'Invalid title'),
                        data.get('confidence', 0.5)
                    )
        except Exception as e:
            print(f"LLM validation failed: {e}")
        
        # Fallback: regex result with lower confidence
        return True, "Validation fallback", 0.6


# Singleton instance
_title_validator = None

def get_title_validator():
    global _title_validator
    if _title_validator is None:
        _title_validator = TitleValidator()
    return _title_validator