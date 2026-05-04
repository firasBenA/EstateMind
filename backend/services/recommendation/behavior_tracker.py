# backend/dashboard/recommendation/behavior_tracker.py
from services.models import UserBehaviorLog, UserSearchHistory
from django.utils import timezone
import logging

logger = logging.getLogger(__name__)

class BehaviorTracker:
    """
    Track all user behaviors in real-time.
    This replaces mock data and builds real training data.
    """
    
    @staticmethod
    def track_view(user, request, listing_id, duration_seconds=0, referrer=None):
        """Track when a user views a listing"""
        try:
            from services.models import Listing as Listing
            
            listing = Listing.objects.get(id=listing_id)
            
            UserBehaviorLog.objects.create(
                user=user if user.is_authenticated else None,
                session_key=request.session.session_key,
                listing=listing,
                behavior_type='view',
                duration_seconds=duration_seconds,
                referrer=referrer,
                user_agent=request.META.get('HTTP_USER_AGENT', ''),
                ip_address=BehaviorTracker._get_client_ip(request)
            )
            
            logger.info(f"Tracked view: user={user.id if user.is_authenticated else 'anon'}, listing={listing_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to track view: {e}")
            return False
    
    @staticmethod
    def track_search_click(user, request, listing_id, search_query, filters):
        """Track when a user clicks a listing from search results"""
        try:
            from services.models import Listing as Listing
            
            listing = Listing.objects.get(id=listing_id)
            
            # Save search history
            UserSearchHistory.objects.create(
                user=user if user.is_authenticated else None,
                session_key=request.session.session_key,
                search_query=search_query,
                filters=filters,
                clicked_listing=listing
            )
            
            # Save behavior
            UserBehaviorLog.objects.create(
                user=user if user.is_authenticated else None,
                session_key=request.session.session_key,
                listing=listing,
                behavior_type='search_click',
                search_query=search_query,
                referrer='search_results',
                user_agent=request.META.get('HTTP_USER_AGENT', ''),
                ip_address=BehaviorTracker._get_client_ip(request)
            )
            
            logger.info(f"Tracked search click: user={user.id if user.is_authenticated else 'anon'}")
            return True
        except Exception as e:
            logger.error(f"Failed to track search click: {e}")
            return False
    
    @staticmethod
    def track_save(user, request, listing_id):
        """Track when a user saves a listing to favorites"""
        try:
            from services.models import Listing as Listing
            
            listing = Listing.objects.get(id=listing_id)
            
            UserBehaviorLog.objects.create(
                user=user,
                session_key=request.session.session_key,
                listing=listing,
                behavior_type='save',
                user_agent=request.META.get('HTTP_USER_AGENT', ''),
                ip_address=BehaviorTracker._get_client_ip(request)
            )
            
            logger.info(f"Tracked save: user={user.id}, listing={listing_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to track save: {e}")
            return False
    
    @staticmethod
    def track_contact(user, request, listing_id):
        """Track when a user contacts an agency about a listing"""
        try:
            from services.models import Listing as Listing
            
            listing = Listing.objects.get(id=listing_id)
            
            UserBehaviorLog.objects.create(
                user=user,
                session_key=request.session.session_key,
                listing=listing,
                behavior_type='contact',
                user_agent=request.META.get('HTTP_USER_AGENT', ''),
                ip_address=BehaviorTracker._get_client_ip(request)
            )
            
            logger.info(f"Tracked contact: user={user.id}, listing={listing_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to track contact: {e}")
            return False
    
    @staticmethod
    def _get_client_ip(request):
        """Get client IP address from request"""
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[0]
        else:
            ip = request.META.get('REMOTE_ADDR')
        return ip