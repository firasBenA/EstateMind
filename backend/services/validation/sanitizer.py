# backend/dashboard/validation/sanitizer.py

import re
import html
import bleach
from typing import Any, Dict

# Allowed HTML tags (for description field only)
ALLOWED_TAGS = ['b', 'i', 'u', 'strong', 'em', 'br', 'p']
ALLOWED_ATTRIBUTES = {}

def sanitize_string(value: Any) -> str:
    """
    Sanitize string input to prevent XSS attacks
    - Escape HTML entities
    - Remove dangerous characters
    - Normalize unicode
    """
    if value is None:
        return ""
    
    if not isinstance(value, str):
        value = str(value)
    
    # Escape HTML entities (prevents XSS)
    value = html.escape(value)
    
    # Remove null bytes and control characters
    value = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', value)
    
    # Remove any remaining HTML/script tags
    value = bleach.clean(value, tags=[], strip=True)
    
    # Trim whitespace
    value = value.strip()
    
    # Limit length
    if len(value) > 5000:
        value = value[:5000]
    
    return value

def sanitize_html_description(value: Any) -> str:
    """
    Sanitize HTML content for description field
    Allows basic formatting tags
    """
    if value is None:
        return ""
    
    if not isinstance(value, str):
        value = str(value)
    
    # Allow basic formatting tags
    value = bleach.clean(
        value,
        tags=ALLOWED_TAGS,
        attributes=ALLOWED_ATTRIBUTES,
        strip=True
    )
    
    # Trim whitespace
    value = value.strip()
    
    # Limit length
    if len(value) > 10000:
        value = value[:10000]
    
    return value

def sanitize_listing_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitize entire listing data object
    """
    sanitized = {}
    
    # String fields
    text_fields = ['title', 'city', 'municipality', 'zone', 'region']
    for field in text_fields:
        if field in data:
            sanitized[field] = sanitize_string(data[field])
    
    # HTML description (allows basic formatting)
    if 'description' in data:
        sanitized['description'] = sanitize_html_description(data['description'])
    
    # Numeric fields (convert to float/int)
    numeric_fields = ['price', 'surface', 'rooms', 'latitude', 'longitude']
    for field in numeric_fields:
        if field in data and data[field] is not None:
            try:
                if field == 'rooms':
                    sanitized[field] = int(float(data[field]))
                else:
                    sanitized[field] = float(data[field])
            except (ValueError, TypeError):
                sanitized[field] = None
    
    # Enum fields (validate against allowed values)
    if 'transaction_type' in data:
        allowed = ['sale', 'rent']
        sanitized['transaction_type'] = data['transaction_type'] if data['transaction_type'] in allowed else 'sale'
    
    if 'property_type' in data:
        allowed = ['apartment', 'house', 'villa', 'land', 'commercial']
        sanitized['property_type'] = data['property_type'] if data['property_type'] in allowed else 'apartment'
    
    # List fields
    if 'images' in data and isinstance(data['images'], list):
        sanitized['images'] = [sanitize_string(img) for img in data['images'][:20]]  # Max 20 images
    
    # Copy other fields as-is
    for key, value in data.items():
        if key not in sanitized:
            sanitized[key] = value
    
    return sanitized