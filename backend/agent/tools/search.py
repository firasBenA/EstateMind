"""Search listings tool."""
from typing import Optional, Dict, Any, List
from django.db.models import Q  # ← Add this import!
import logging

logger = logging.getLogger(__name__)


def search_listings(
    query: Optional[str] = None,
    city: Optional[str] = None,
    region: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    min_surface: Optional[float] = None,
    max_surface: Optional[float] = None,
    rooms: Optional[int] = None,
    property_type: Optional[str] = None,
    transaction_type: Optional[str] = None,
    page: int = 1,
    page_size: int = 10,
) -> Dict[str, Any]:
    """Search for property listings with filters."""
    try:
        # 🔑 SAFE: Handle None values with defaults
        city = city.strip().lower() if city and isinstance(city, str) else None
        region = region.strip().lower() if region and isinstance(region, str) else None
        query = query.strip().lower() if query and isinstance(query, str) else ""
        property_type = property_type.lower() if property_type and isinstance(property_type, str) else None
        transaction_type = transaction_type.lower() if transaction_type and isinstance(transaction_type, str) else None

        # Import Listing from correct app
        from dashboard.models import Listing  # ← Your actual app
        
        # Build query filters
        filters = Q(should_drop=False) | Q(should_drop__isnull=True)  # ← Replace is_active
        
        if city:
            filters &= Q(city__iexact=city)
        if region:
            filters &= Q(region__iexact=region)
        if property_type:
            filters &= Q(property_type__iexact=property_type)
        if transaction_type:
            filters &= Q(transaction_type__iexact=transaction_type)
        if min_price is not None:
            filters &= Q(price__gte=min_price)
        if max_price is not None:
            filters &= Q(price__lte=max_price)
        if min_surface is not None:
            filters &= Q(surface__gte=min_surface)
        if max_surface is not None:
            filters &= Q(surface__lte=max_surface)
        if rooms is not None:
            filters &= Q(rooms__gte=rooms)
        if query:
            filters &= Q(title__icontains=query) | Q(description__icontains=query)

        # Execute query with ordering
        queryset = Listing.objects.filter(filters).order_by('-scraped_at')
        
        # Pagination
        total = queryset.count()
        pages = (total + page_size - 1) // page_size
        start = (page - 1) * page_size
        end = start + page_size
        
        results = list(queryset[start:end].values(
            "id", "title", "price", "city", "surface", "rooms", 
            "property_type", "transaction_type", "url", "images"
        ))

        return {
            "count": total,
            "pages": pages,
            "page": page,
            "results": results,
        }

    except Exception as e:
        logger.error(f"Search error: {str(e)}", exc_info=True)
        return {
            "error": f"Search failed: {str(e)}",
            "count": 0,
            "pages": 0,
            "page": 1,
            "results": [],
        }