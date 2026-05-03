# backend/dashboard/validation/delegation_matcher.py

import re
import unicodedata
from difflib import SequenceMatcher
from typing import Dict, Any, Optional, List, Tuple
from django.db import connection

class DelegationMatcher:
    """
    Fuzzy matching for Tunisian delegations (districts/areas)
    Supports: 
    - Typo tolerance ("Mhamdia" → "Mohammedia")
    - Phonetic matching
    - Arabic/French normalization
    """
    
    # Common Tunisian spelling variations
    VARIATIONS = {
        'mhamdia': 'mohammedia',
        'mohamadia': 'mohammedia',
        'mohamedia': 'mohammedia',
        'hammam chatt': 'hammam chatt',
        'hammamchat': 'hammam chatt',
        'kram': 'le kram',
        'elkram': 'le kram',
        'bardo': 'le bardo',
        'lbardo': 'le bardo',
        'marsa': 'la marsa',
        'lamarsa': 'la marsa',
        'lac1': 'lac 1',
        'lac2': 'lac 2',
        'lac3': 'lac 3',
        'lac 1': 'lac 1',
        'lac 2': 'lac 2',
        'lac 3': 'lac 3',
        'sidibousaid': 'sidi bou said',
        'carthage': 'carthage',
        'gammarth': 'gammarth',
        'gamarth': 'gammarth',
        'rades': 'rades',
        'radés': 'rades',
        'megrine': 'megrine',
        'mégrine': 'megrine',
        'ezzahra': 'ezzahra',
        'ez zahra': 'ezzahra',
        'houmt souk': 'houmt souk',
        'houmtsouk': 'houmt souk',
        'midoun': 'midoun',
        'djerba': 'djerba midoun',
    }
    
    def __init__(self, threshold: float = 0.65):
        self.threshold = threshold
    
    def _normalize(self, text: str) -> str:
        """Normalize text for comparison"""
        if not text:
            return ""
        
        text = text.lower().strip()
        
        # Remove parentheses and content
        text = re.sub(r'\([^)]*\)', '', text)
        
        # French character normalization
        french_map = {
            'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
            'à': 'a', 'â': 'a', 'ä': 'a',
            'î': 'i', 'ï': 'i',
            'ô': 'o', 'ö': 'o',
            'ù': 'u', 'û': 'u', 'ü': 'u',
            'ç': 'c',
            'œ': 'oe', 'æ': 'ae',
        }
        
        for char, replacement in french_map.items():
            text = text.replace(char, replacement)
        
        # Remove diacritics (Arabic normalization)
        text = unicodedata.normalize('NFKD', text)
        text = ''.join(c for c in text if not unicodedata.combining(c))
        
        # Remove punctuation
        text = re.sub(r'[^\w\s]', '', text)
        
        # Remove multiple spaces
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Apply known variations
        text = self.VARIATIONS.get(text, text)
        
        return text
    
    def _similarity(self, a: str, b: str) -> float:
        """Calculate similarity between two strings (0-1)"""
        if not a or not b:
            return 0.0
        return SequenceMatcher(None, self._normalize(a), self._normalize(b)).ratio()
    
    def find_match(self, user_input: str, governorate_id: int = None) -> Optional[Dict[str, Any]]:
        """
        Find best matching delegation in database
        Returns match dictionary with id, name, value, coordinates, confidence
        """
        if not user_input or len(user_input.strip()) < 2:
            return None
        
        # Build SQL query (parameterized - SQL injection safe)
        if governorate_id:
            query = """
                SELECT d.id, d.name, d.value, d.latitude, d.longitude, 
                       g.name as governorate_name, g.id as governorate_id
                FROM delegations d
                JOIN governorates g ON d.governorate_id = g.id
                WHERE d.governorate_id = %s
            """
            params = [governorate_id]
        else:
            query = """
                SELECT d.id, d.name, d.value, d.latitude, d.longitude, 
                       g.name as governorate_name, g.id as governorate_id
                FROM delegations d
                JOIN governorates g ON d.governorate_id = g.id
            """
            params = []
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            delegations = cursor.fetchall()
        
        best_match = None
        best_score = 0.0
        
        for del_row in delegations:
            # Score against name
            name_score = self._similarity(user_input, del_row[1])  # name
            # Score against value
            value_score = self._similarity(user_input, del_row[2])  # value
            score = max(name_score, value_score)
            
            if score > best_score and score >= self.threshold:
                best_score = score
                best_match = {
                    'id': del_row[0],
                    'name': del_row[1],
                    'value': del_row[2],
                    'latitude': float(del_row[3]) if del_row[3] else None,
                    'longitude': float(del_row[4]) if del_row[4] else None,
                    'governorate': del_row[5],
                    'governorate_id': del_row[6],
                    'confidence': best_score
                }
        
        return best_match
    
    def auto_correct(self, user_input: str, governorate_id: int = None) -> Dict[str, Any]:
        """
        Auto-correct user input to nearest delegation
        Returns match result with auto-correction info
        """
        match = self.find_match(user_input, governorate_id)
        
        if match:
            return {
                'original': user_input,
                'corrected': match['name'],
                'matched': True,
                'confidence': match['confidence'],
                'delegation_id': match['id'],
                'latitude': match['latitude'],
                'longitude': match['longitude'],
                'governorate': match['governorate'],
                'message': f"Auto-corrected '{user_input}' → '{match['name']}'"
            }
        else:
            return {
                'original': user_input,
                'corrected': None,
                'matched': False,
                'confidence': 0,
                'message': f"'{user_input}' not found in database"
            }


# Singleton instance
_delegation_matcher = None

def get_delegation_matcher():
    global _delegation_matcher
    if _delegation_matcher is None:
        _delegation_matcher = DelegationMatcher()
    return _delegation_matcher