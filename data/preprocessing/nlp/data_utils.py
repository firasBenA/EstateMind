"""
Data utilities for loading and using data.ts
"""
import ast
from pathlib import Path
from typing import Dict, List, Set, Optional
from loguru import logger

class TunisianLocationData:
    """Load and manage location data from data.ts"""
    
    def __init__(self, data_ts_path: str = "data/data/data.ts"):
        self.data_path = Path(data_ts_path)
        self.governorates: Set[str] = set()
        self.delegations: Dict[str, str] = {}  # delegation -> governorate
        self.cities: Set[str] = set()
        self.postal_codes: Dict[str, List[Dict]] = {}
        
        self._load_data()
    
    def _load_data(self):
        """Load and parse data.ts"""
        if not self.data_path.exists():
            logger.warning(f"data.ts not found at {self.data_path}, using fallback")
            self._load_fallback()
            return
        
        try:
            with open(self.data_path, 'r', encoding='utf-8') as f:
                content = f.read()
                data = ast.literal_eval(content)
            
            for governorate in data:
                gov_name = governorate['Name'].strip()
                self.governorates.add(gov_name.lower())
                self.governorates.add(gov_name)  # Keep original case too
                
                for delegation in governorate.get('Delegations', []):
                    del_name = delegation['Name'].strip()
                    self.delegations[del_name.lower()] = gov_name
                    self.delegations[del_name] = gov_name
                    
                    # Extract city name from delegation (remove parentheses content)
                    if '(' in del_name:
                        city = del_name.split('(')[0].strip()
                        self.cities.add(city.lower())
                        self.cities.add(city)
                    
                    # Store postal code info
                    postal = delegation.get('PostalCode')
                    if postal:
                        if postal not in self.postal_codes:
                            self.postal_codes[postal] = []
                        self.postal_codes[postal].append({
                            'delegation': del_name,
                            'governorate': gov_name,
                            'latitude': delegation.get('Latitude'),
                            'longitude': delegation.get('Longitude')
                        })
            
            logger.info(f"Loaded {len(self.governorates)} governorates, "
                       f"{len(self.delegations)} delegations, "
                       f"{len(self.cities)} cities")
            
        except Exception as e:
            logger.error(f"Failed to load data.ts: {e}")
            self._load_fallback()
    
    def _load_fallback(self):
        """Fallback data if data.ts not available"""
        self.governorates = {
            'tunis', 'ariana', 'ben arous', 'manouba', 'nabeul', 'zaghouan',
            'bizerte', 'béja', 'jendouba', 'le kef', 'siliana', 'sousse',
            'monastir', 'mahdia', 'sfax', 'kairouan', 'kasserine', 'sidi bouzid',
            'gabès', 'médenine', 'tataouine', 'gafsa', 'tozeur', 'kébili'
        }
    
    def is_valid_governorate(self, name: str) -> bool:
        """Check if a name is a valid Tunisian governorate"""
        if not name:
            return False
        return name.lower() in self.governorates or name in self.governorates
    
    def get_governorate_for_delegation(self, delegation: str) -> Optional[str]:
        """Get governorate for a delegation/city"""
        if not delegation:
            return None
        return self.delegations.get(delegation.lower())
    
    def get_delegations_for_governorate(self, governorate: str) -> List[str]:
        """Get all delegations for a governorate"""
        return [d for d, g in self.delegations.items() if g.lower() == governorate.lower()]
    
    def get_location_by_postal(self, postal_code: str) -> Optional[Dict]:
        """Get location info from postal code"""
        if postal_code in self.postal_codes:
            return self.postal_codes[postal_code][0]
        return None


# Singleton instance
_location_data = None

def get_location_data() -> TunisianLocationData:
    """Get or create location data singleton"""
    global _location_data
    if _location_data is None:
        _location_data = TunisianLocationData()
    return _location_data