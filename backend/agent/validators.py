"""
Input validation and sanitization for agent safety.
Ensures all user inputs and tool parameters are valid.
"""
import re
from typing import Any, Dict, List, Optional
from decimal import Decimal


class InputValidator:
    """Validates and sanitizes user inputs."""

    # Whitelist of allowed tool names
    ALLOWED_TOOLS = {
        'search_listings',
        'predict_price',
        'create_listing',
        'get_analytics',
    }

    # Valid transaction types
    VALID_TRANSACTION_TYPES = {'sale', 'rent'}

    # Valid property types
    VALID_PROPERTY_TYPES = {'apartment', 'house', 'land', 'commercial'}

    # Valid reliability levels
    VALID_RELIABILITY_LEVELS = {'HIGH', 'MEDIUM', 'LOW'}

    @staticmethod
    def sanitize_string(text: str, max_length: int = 1000) -> str:
        """Remove harmful characters, limit length."""
        if not isinstance(text, str):
            return ""
        text = text.strip()[:max_length]
        # Remove control characters, keep alphanumeric + common punctuation
        text = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f]', '', text)
        return text

    @staticmethod
    def validate_price_range(min_price: Optional[float], max_price: Optional[float]) -> tuple:
        """Validate and constrain price ranges."""
        min_p = float(min_price or 0)
        max_p = float(max_price or 1_000_000_000)

        # Ensure valid range
        min_p = max(0, min_p)
        max_p = min(1_000_000_000, max_p)

        if min_p > max_p:
            min_p, max_p = max_p, min_p

        return (min_p, max_p)

    @staticmethod
    def validate_surface_range(min_surface: Optional[float], max_surface: Optional[float]) -> tuple:
        """Validate and constrain surface area ranges."""
        min_s = float(min_surface or 0)
        max_s = float(max_surface or 1_000_000)

        min_s = max(0, min_s)
        max_s = min(1_000_000, max_s)

        if min_s > max_s:
            min_s, max_s = max_s, min_s

        return (min_s, max_s)

    @staticmethod
    def validate_rooms(rooms: Optional[int]) -> Optional[int]:
        """Validate room count."""
        if rooms is None:
            return None
        rooms = int(rooms)
        return max(0, min(20, rooms))  # 0-20 rooms

    @staticmethod
    def validate_tool_name(tool_name: str) -> bool:
        """Check if tool name is in whitelist."""
        return tool_name.strip() in InputValidator.ALLOWED_TOOLS

    @staticmethod
    def validate_transaction_type(t_type: str) -> bool:
        """Check if transaction type is valid."""
        return t_type.lower() in InputValidator.VALID_TRANSACTION_TYPES

    @staticmethod
    def validate_property_type(p_type: str) -> bool:
        """Check if property type is valid."""
        return p_type.lower() in InputValidator.VALID_PROPERTY_TYPES

    @staticmethod
    def validate_listing_creation(data: Dict[str, Any]) -> tuple[bool, str]:
        """
        Validate all required fields for listing creation.
        Returns (is_valid, error_message)
        """
        required_fields = {'title', 'price', 'property_type', 'transaction_type', 'city'}
        missing = required_fields - set(data.keys())
        if missing:
            return False, f"Missing fields: {', '.join(missing)}"

        if not isinstance(data.get('title'), str) or len(data['title']) < 3:
            return False, "Title must be at least 3 characters"

        try:
            price = float(data['price'])
            if price < 0 or price > 1_000_000_000:
                return False, "Price must be between 0 and 1,000,000,000"
        except (ValueError, TypeError):
            return False, "Price must be a valid number"

        if not InputValidator.validate_property_type(data['property_type']):
            return False, f"Invalid property type: {data['property_type']}"

        if not InputValidator.validate_transaction_type(data['transaction_type']):
            return False, f"Invalid transaction type: {data['transaction_type']}"

        if not isinstance(data.get('city'), str) or len(data['city']) < 2:
            return False, "City must be at least 2 characters"

        return True, ""

    @staticmethod
    def validate_search_params(params: Dict[str, Any]) -> tuple[bool, Dict[str, Any]]:
        """
        Validate search parameters. Returns (is_valid, cleaned_params)
        """
        cleaned = {}

        # Search query
        if 'q' in params:
            cleaned['q'] = InputValidator.sanitize_string(params['q'], 200)

        # City
        if 'city' in params:
            cleaned['city'] = InputValidator.sanitize_string(params['city'], 100)

        # Region
        if 'region' in params:
            cleaned['region'] = InputValidator.sanitize_string(params['region'], 100)

        # Price range
        if 'min_price' in params or 'max_price' in params:
            min_p, max_p = InputValidator.validate_price_range(
                params.get('min_price'),
                params.get('max_price')
            )
            cleaned['min_price'] = min_p
            cleaned['max_price'] = max_p

        # Surface range
        if 'min_surface' in params or 'max_surface' in params:
            min_s, max_s = InputValidator.validate_surface_range(
                params.get('min_surface'),
                params.get('max_surface')
            )
            cleaned['min_surface'] = min_s
            cleaned['max_surface'] = max_s

        # Rooms
        if 'rooms' in params:
            cleaned['rooms'] = InputValidator.validate_rooms(params.get('rooms'))

        # Transaction type
        if 'transaction_type' in params:
            t_type = params['transaction_type'].lower()
            if InputValidator.validate_transaction_type(t_type):
                cleaned['transaction_type'] = t_type

        # Property type
        if 'property_type' in params:
            p_type = params['property_type'].lower()
            if InputValidator.validate_property_type(p_type):
                cleaned['property_type'] = p_type

        # Pagination
        if 'page' in params:
            cleaned['page'] = max(1, int(params.get('page', 1)))
        if 'page_size' in params:
            cleaned['page_size'] = max(1, min(100, int(params.get('page_size', 10))))

        return True, cleaned
