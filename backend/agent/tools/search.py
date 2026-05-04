"""Search listings tool."""
from typing import Optional, Dict, Any, List
from django.db.models import Q
import logging

logger = logging.getLogger(__name__)

# Map user-facing property type terms to possible DB values
PROPERTY_TYPE_ALIASES = {
    "house":      ["house", "villa", "maison", "townhouse", "duplex"],
    "apartment":  ["apartment", "appartement", "flat", "studio"],
    "land":       ["land", "terrain", "plot"],
    "commercial": ["commercial", "bureau", "office", "shop", "store"],
}


def _property_type_filter(property_type: str) -> Q:
    """
    Build a Q filter that matches any known alias for a property type.
    Handles inconsistent data in the DB (e.g. 'villa' stored where 'house' expected).
    """
    aliases = PROPERTY_TYPE_ALIASES.get(property_type.lower(), [property_type])
    q = Q()
    for alias in aliases:
        q |= Q(property_type__iexact=alias)
    return q


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
        # Normalise string inputs
        city             = city.strip() if city and isinstance(city, str) else None
        region           = region.strip() if region and isinstance(region, str) else None
        query            = query.strip() if query and isinstance(query, str) else ""
        property_type    = property_type.lower().strip() if property_type else None
        transaction_type = transaction_type.lower().strip() if transaction_type else None

        from services.models import Listing

        # Base filter: exclude soft-deleted rows
        filters = Q(should_drop=False) | Q(should_drop__isnull=True)

        # City — case-insensitive
        if city:
            filters &= Q(city__iexact=city)

        # Region — case-insensitive
        if region:
            filters &= Q(region__iexact=region)

        # Property type — match any known alias (handles villa == house, etc.)
        if property_type:
            filters &= _property_type_filter(property_type)

        # Transaction type
        if transaction_type:
            filters &= Q(transaction_type__iexact=transaction_type)

        # Price range
        if min_price is not None:
            filters &= Q(price__gte=min_price)
        if max_price is not None:
            filters &= Q(price__lte=max_price)

        # Surface — NULL-safe: a listing with no surface recorded is NOT excluded
        if min_surface is not None:
            filters &= (Q(surface__gte=min_surface) | Q(surface__isnull=True))
        if max_surface is not None:
            # Only apply upper bound when surface is actually recorded
            filters &= (Q(surface__lte=max_surface) | Q(surface__isnull=True))

        # Rooms — NULL-safe
        if rooms is not None:
            filters &= (Q(rooms__gte=rooms) | Q(rooms__isnull=True))

        # Full-text keyword search
        if query:
            filters &= Q(title__icontains=query) | Q(description__icontains=query)

        # Execute
        queryset = Listing.objects.filter(filters).order_by('-scraped_at')

        total  = queryset.count()
        pages  = max(1, (total + page_size - 1) // page_size)
        start  = (page - 1) * page_size
        end    = start + page_size

        results = list(queryset[start:end].values(
            "id", "title", "price", "city", "surface", "rooms",
            "property_type", "transaction_type", "url", "images",
        ))

        logger.info(
            f"Search: city={city!r} property_type={property_type!r} "
            f"min_surface={min_surface} max_surface={max_surface} "
            f"→ {total} results"
        )

        return {
            "count":   total,
            "pages":   pages,
            "page":    page,
            "results": results,
        }

    except Exception as e:
        logger.error(f"Search error: {str(e)}", exc_info=True)
        return {
            "error":   f"Search failed: {str(e)}",
            "count":   0,
            "pages":   0,
            "page":    1,
            "results": [],
        }