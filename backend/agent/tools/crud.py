"""
CRUD operations tool - create, read, update, delete listings.
Note: Create/Update/Delete require action_confirmation=True and explicit user confirmation.
"""
from typing import Optional, Dict, Any
from dashboard.models import Listing
from agent.validators import InputValidator
from django.utils import timezone
import logging
import uuid

logger = logging.getLogger(__name__)


def create_listing(
    title: str,
    price: float,
    property_type: str,
    transaction_type: str,
    city: str,
    description: Optional[str] = None,
    surface: Optional[float] = None,
    rooms: Optional[int] = None,
    region: Optional[str] = None,
    municipality: Optional[str] = None,
    source_name: str = "admin_chat",
    action_confirmation: bool = False,
    **kwargs
) -> Dict[str, Any]:
    """
    Create a new listing.
    REQUIRES: action_confirmation=True + explicit user confirmation modal.
    
    Returns:
        {
            "success": bool,
            "listing_id": str,
            "message": str,
            "requires_confirmation": bool (if not confirmed)
        }
    """
    # Validate data
    data = {
        'title': title,
        'price': price,
        'property_type': property_type,
        'transaction_type': transaction_type,
        'city': city,
    }

    is_valid, error_msg = InputValidator.validate_listing_creation(data)
    if not is_valid:
        return {
            'success': False,
            'error': error_msg,
        }

    # Check for confirmation
    if not action_confirmation:
        return {
            'requires_confirmation': True,
            'action': 'create_listing',
            'preview': {
                'title': title,
                'price': price,
                'property_type': property_type,
                'city': city,
                'rooms': rooms,
            },
            'message': 'This will create a new listing. Please confirm to proceed.',
        }

    try:
        # Create listing
        listing = Listing.objects.create(
            id=str(uuid.uuid4()),
            title=InputValidator.sanitize_string(title),
            price=float(price),
            property_type=property_type,
            transaction_type=transaction_type,
            city=city,
            description=InputValidator.sanitize_string(description) if description else None,
            surface=float(surface) if surface else None,
            rooms=int(rooms) if rooms else None,
            region=region or "Unknown",
            municipality=municipality,
            source_name=source_name,
            currency="TND",
            scraped_at=timezone.now(),
            created_at=timezone.now(),
            normalized=True,
        )

        logger.info(f"Created listing {listing.id} via agent")

        return {
            'success': True,
            'listing_id': listing.id,
            'message': f"Listing created successfully! ID: {listing.id}",
        }

    except Exception as e:
        logger.error(f"Create listing error: {str(e)}")
        return {
            'success': False,
            'error': f"Failed to create listing: {str(e)}",
        }


def update_listing(
    listing_id: str,
    title: Optional[str] = None,
    price: Optional[float] = None,
    description: Optional[str] = None,
    surface: Optional[float] = None,
    rooms: Optional[int] = None,
    action_confirmation: bool = False,
    **kwargs
) -> Dict[str, Any]:
    """
    Update an existing listing.
    REQUIRES: action_confirmation=True + explicit user confirmation modal.
    """
    # Check for confirmation
    if not action_confirmation:
        try:
            listing = Listing.objects.get(id=listing_id)
            return {
                'requires_confirmation': True,
                'action': 'update_listing',
                'preview': {
                    'listing_id': listing_id,
                    'changes': {
                        'title': title,
                        'price': price,
                        'surface': surface,
                        'rooms': rooms,
                    }
                },
                'message': f'This will update listing {listing_id}. Please confirm.',
            }
        except Listing.DoesNotExist:
            return {'error': f'Listing {listing_id} not found'}

    try:
        listing = Listing.objects.get(id=listing_id)

        if title:
            listing.title = InputValidator.sanitize_string(title)
        if price:
            listing.price = float(price)
        if description:
            listing.description = InputValidator.sanitize_string(description)
        if surface:
            listing.surface = float(surface)
        if rooms:
            listing.rooms = int(rooms)

        listing.last_updated = timezone.now()
        listing.save()

        logger.info(f"Updated listing {listing_id} via agent")

        return {
            'success': True,
            'listing_id': listing_id,
            'message': f"Listing {listing_id} updated successfully!",
        }

    except Listing.DoesNotExist:
        return {'success': False, 'error': f'Listing {listing_id} not found'}
    except Exception as e:
        logger.error(f"Update listing error: {str(e)}")
        return {'success': False, 'error': f"Update failed: {str(e)}"}


def delete_listing(
    listing_id: str,
    action_confirmation: bool = False,
    **kwargs
) -> Dict[str, Any]:
    """
    Delete a listing (soft delete - sets should_drop=True).
    REQUIRES: action_confirmation=True + explicit user confirmation modal.
    """
    # Check for confirmation
    if not action_confirmation:
        try:
            listing = Listing.objects.get(id=listing_id)
            return {
                'requires_confirmation': True,
                'action': 'delete_listing',
                'preview': {
                    'listing_id': listing_id,
                    'title': listing.title,
                },
                'message': f'This will permanently delete listing "{listing.title}". Cannot be undone!',
            }
        except Listing.DoesNotExist:
            return {'error': f'Listing {listing_id} not found'}

    try:
        listing = Listing.objects.get(id=listing_id)
        title = listing.title

        # Soft delete
        listing.should_drop = True
        listing.last_updated = timezone.now()
        listing.save()

        logger.info(f"Deleted listing {listing_id} via agent")

        return {
            'success': True,
            'listing_id': listing_id,
            'message': f'Listing "{title}" deleted successfully.',
        }

    except Listing.DoesNotExist:
        return {'success': False, 'error': f'Listing {listing_id} not found'}
    except Exception as e:
        logger.error(f"Delete listing error: {str(e)}")
        return {'success': False, 'error': f"Delete failed: {str(e)}"}
