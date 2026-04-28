# backend/dashboard/recommendation/notification_service.py
import pusher
from django.conf import settings
from dashboard.models import UserNotification, UserBehaviorLog, Listing
from django.db.models import Q, Count
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Initialize Pusher
pusher_client = pusher.Pusher(
    app_id=settings.PUSHER_APP_ID,
    key=settings.PUSHER_KEY,
    secret=settings.PUSHER_SECRET,
    cluster=settings.PUSHER_CLUSTER,
    ssl=True
)

class NotificationService:
    """Real-time notifications using Pusher"""
    
    @staticmethod
    def send_new_listing_notification(user_id, listing_data):
        """Send notification when a new matching listing is found"""
        notification = UserNotification.objects.create(
            user_id=user_id,
            notification_type='new_listing',
            title='New Matching Property! 🏠',
            message=f"New {listing_data.get('property_type')} in {listing_data.get('city')} matches your preferences",
            listing_id=listing_data.get('id'),
            data=listing_data
        )
        
        # Send real-time via Pusher
        pusher_client.trigger(
            f'user-{user_id}',
            'new-listing',
            {
                'id': notification.id,
                'title': notification.title,
                'message': notification.message,
                'listing_id': notification.listing_id,
                'created_at': notification.created_at.isoformat(),
                'data': listing_data
            }
        )
        
        logger.info(f"Sent new listing notification to user {user_id}")
        return notification
    
    @staticmethod
    def send_price_drop_notification(user_id, listing_id, old_price, new_price):
        """Send notification when a watched listing drops in price"""
        notification = UserNotification.objects.create(
            user_id=user_id,
            notification_type='price_drop',
            title='Price Drop Alert! 💰',
            message=f"Property price dropped from {old_price:,.0f} TND to {new_price:,.0f} TND",
            listing_id=listing_id,
            data={'old_price': old_price, 'new_price': new_price}
        )
        
        pusher_client.trigger(
            f'user-{user_id}',
            'price-drop',
            {
                'id': notification.id,
                'title': notification.title,
                'message': notification.message,
                'listing_id': listing_id,
                'old_price': old_price,
                'new_price': new_price,
                'created_at': notification.created_at.isoformat()
            }
        )
        
        logger.info(f"Sent price drop notification to user {user_id}")
        return notification
    
    @staticmethod
    def send_similar_listing_notification(user_id, original_listing_id, similar_listing):
        """Send notification when a similar listing is found"""
        notification = UserNotification.objects.create(
            user_id=user_id,
            notification_type='similar_listing',
            title='Similar Property Found! 🔍',
            message=f"Found a similar property to one you liked",
            listing_id=similar_listing.get('id'),
            data={'original_listing_id': original_listing_id, 'similar_listing': similar_listing}
        )
        
        pusher_client.trigger(
            f'user-{user_id}',
            'similar-listing',
            {
                'id': notification.id,
                'title': notification.title,
                'message': notification.message,
                'listing_id': similar_listing.get('id'),
                'original_listing_id': original_listing_id,
                'created_at': notification.created_at.isoformat()
            }
        )
        
        logger.info(f"Sent similar listing notification to user {user_id}")
        return notification
    
    @staticmethod
    def check_for_new_matching_listings(user_id, preferences):
        """
        Check for new listings matching user preferences
        This should be called periodically (e.g., every hour via cron)
        """
        
        
        # Get user's viewed listings in last 7 days to avoid duplicates
        recent_views = UserBehaviorLog.objects.filter(
            user_id=user_id,
            behavior_type='view',
            created_at__gte=datetime.now() - timedelta(days=7)
        ).values_list('listing_id', flat=True)
        
        # Find new listings matching preferences
        query = Q(should_drop__is_not_true=True)
        
        if preferences.get('city'):
            query &= Q(city__iexact=preferences['city'])
        if preferences.get('property_type'):
            query &= Q(property_type=preferences['property_type'])
        if preferences.get('min_price'):
            query &= Q(price__gte=preferences['min_price'])
        if preferences.get('max_price'):
            query &= Q(price__lte=preferences['max_price'])
        
        # Exclude already viewed
        query &= ~Q(id__in=recent_views)
        
        new_listings = Listing.objects.filter(query).order_by('-created_at')[:5]
        
        for listing in new_listings:
            NotificationService.send_new_listing_notification(
                user_id,
                {
                    'id': listing.id,
                    'title': listing.title,
                    'property_type': listing.property_type,
                    'city': listing.city,
                    'price': listing.price,
                    'surface': listing.surface
                }
            )
        
        return new_listings.count()