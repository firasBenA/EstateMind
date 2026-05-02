# backend/dashboard/recommendation_views.py
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from django.db import connection
from django.db.models import Q
from services.recommendation.model_loader import RecommendationModelLoader
from services.recommendation.behavior_tracker import BehaviorTracker
from services.recommendation.notification_service import NotificationService
from services.models import Listing, UserNotification
import json



def _get_listing_images(listing_id):
    """Fetch images from image_embeddings table for a listing"""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT image_url, image_label, image_index
                FROM image_embeddings
                WHERE listing_id = %s
                ORDER BY image_index ASC
            """, [listing_id])
            rows = cursor.fetchall()
            
            images = []
            for row in rows:
                images.append({
                    'url': row[0],
                    'label': row[1] or f"Image {row[2] + 1}" if row[2] is not None else "Photo"
                })
            return images
    except Exception as e:
        print(f"Error fetching images for {listing_id}: {e}")
        return []


def _get_first_listing_image(listing_id):
    """Get first image URL only (for performance)"""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT image_url
                FROM image_embeddings
                WHERE listing_id = %s
                ORDER BY image_index ASC
                LIMIT 1
            """, [listing_id])
            row = cursor.fetchone()
            return row[0] if row else None
    except Exception:
        return None


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_similar_listings(request, listing_id):
    """
    Get similar listings for a property detail page
    Uses pre-computed similarity table + ML model
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    l.id,
                    l.title,
                    l.city,
                    l.property_type,
                    l.price,
                    l.surface,
                    ls.similarity_score,
                    ls.similarity_reason
                FROM listing_recommendation_similarities ls
                JOIN listings l ON ls.listing_id_2 = l.id
                WHERE ls.listing_id_1 = %s
                AND l.should_drop IS NOT TRUE
                ORDER BY ls.similarity_score DESC
                LIMIT 10
            """, [listing_id])
            
            similar = cursor.fetchall()
        
        # Format response with images from image_embeddings
        similar_listings = []
        for row in similar:
            listing_id = row[0]
            # Get first image from image_embeddings
            first_image = _get_first_listing_image(listing_id)
            
            similar_listings.append({
                'id': row[0],
                'title': row[1],
                'city': row[2],
                'property_type': row[3],
                'price': float(row[4]) if row[4] else 0,
                'surface': float(row[5]) if row[5] else 0,
                'image': first_image,  # Use image from image_embeddings
                'images': [{'url': first_image}] if first_image else [],  # For compatibility
                'similarity_score': float(row[6]) if row[6] else 0,
                'similarity_reason': row[7] if row[7] else ''
            })
        
        return Response({
            'listing_id': listing_id,
            'results': similar_listings,
            'strategy_used': 'precomputed',
            'count': len(similar_listings)
        })
        
    except Exception as e:
        return Response({'error': str(e)}, status=500)



@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_personalized_recommendations(request):
    """Get personalized recommendations using trained ML model"""
    try:
        limit = int(request.GET.get('limit', 10))
        viewed = []
        
        # Try to get user's viewed listings
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT listing_id
                    FROM user_behavior_log
                    WHERE user_id = %s 
                    AND behavior_type = 'view'
                    ORDER BY created_at DESC
                    LIMIT 20
                """, [request.user.id])
                viewed = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"Warning: user_behavior_log table not accessible: {e}")
            viewed = []
        
        # Fallback to popular listings
        with connection.cursor() as cursor:
            if viewed:
                placeholders = ','.join(['%s'] * len(viewed))
                query = f"""
                    SELECT 
                        l.id,
                        l.title,
                        l.city,
                        l.property_type,
                        l.price,
                        l.surface,
                        l.reliability_score
                    FROM listings l
                    WHERE l.should_drop IS NOT TRUE
                    AND l.id NOT IN ({placeholders})
                    ORDER BY l.reliability_score DESC, l.created_at DESC
                    LIMIT %s
                """
                params = viewed + [limit]
            else:
                query = """
                    SELECT 
                        l.id,
                        l.title,
                        l.city,
                        l.property_type,
                        l.price,
                        l.surface,
                        l.reliability_score
                    FROM listings l
                    WHERE l.should_drop IS NOT TRUE
                    ORDER BY l.reliability_score DESC, l.created_at DESC
                    LIMIT %s
                """
                params = [limit]
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
        
        recommendations = []
        for row in rows:
            listing_id = row[0]
            # Get first image from image_embeddings
            first_image = _get_first_listing_image(listing_id)
            
            recommendations.append({
                'id': listing_id,
                'title': row[1],
                'city': row[2],
                'property_type': row[3],
                'price': float(row[4]) if row[4] else 0,
                'surface': float(row[5]) if row[5] else 0,
                'image': first_image,  # Use image from image_embeddings
                'images': [{'url': first_image}] if first_image else [],  # For compatibility
                'reliability_score': float(row[6]) if row[6] else 0,
                'recommendation_score': 0.5
            })
        
        return Response({
            'recommendations': recommendations,
            'count': len(recommendations),
            'model_used': 'Popularity (fallback)'
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return Response({
            'recommendations': [],
            'count': 0,
            'model_used': 'none',
            'error': str(e)
        }, status=200)



@api_view(['POST'])
@permission_classes([IsAuthenticated])
def track_behavior(request):
    """
    Track user behavior in real-time
    This replaces mock data with real data
    """
    try:
        data = json.loads(request.body)
        behavior_type = data.get('behavior_type')
        listing_id = data.get('listing_id')
        duration = data.get('duration_seconds', 0)
        referrer = data.get('referrer', 'direct')
        search_query = data.get('search_query', None)
        filters = data.get('filters', {})
        
        if behavior_type == 'view':
            BehaviorTracker.track_view(request.user, request, listing_id, duration, referrer)
        elif behavior_type == 'search_click':
            BehaviorTracker.track_search_click(request.user, request, listing_id, search_query, filters)
        elif behavior_type == 'save':
            BehaviorTracker.track_save(request.user, request, listing_id)
        elif behavior_type == 'contact':
            BehaviorTracker.track_contact(request.user, request, listing_id)
        
        return Response({'status': 'tracked', 'behavior_type': behavior_type})
        
    except Exception as e:
        return Response({'error': str(e)}, status=500)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def track_search(request):
    """
    Track user search queries for recommendations
    """
    try:
        data = json.loads(request.body)
        
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO user_search_history 
                (user_id, search_query, filters, results_count, clicked_listing_id, created_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
            """, [
                request.user.id,
                data.get('search_query', ''),
                json.dumps(data.get('filters', {})),
                data.get('results_count', 0),
                data.get('clicked_listing_id')
            ])
        
        return Response({'status': 'search_tracked', 'search_query': data.get('search_query')})
        
    except Exception as e:
        return Response({'error': str(e)}, status=500)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_notifications(request):
    """Get user's notifications"""
    try:
        limit = int(request.GET.get('limit', 20))
        unread_only = request.GET.get('unread_only', 'false').lower() == 'true'
        
        try:
            # Use raw SQL since model might not be synced
            with connection.cursor() as cursor:
                unread_filter = "AND is_read = false" if unread_only else ""
                cursor.execute(f"""
                    SELECT id, notification_type, title, message, listing_id, is_read, created_at, data
                    FROM user_notifications
                    WHERE user_id = %s {unread_filter}
                    ORDER BY created_at DESC
                    LIMIT %s
                """, [request.user.id, limit])
                rows = cursor.fetchall()
                
                cursor.execute("""
                    SELECT COUNT(*) FROM user_notifications
                    WHERE user_id = %s AND is_read = false
                """, [request.user.id])
                unread_count = cursor.fetchone()[0]
        except Exception as e:
            # Table doesn't exist yet
            rows = []
            unread_count = 0
        
        notifications = []
        for row in rows:
            notifications.append({
                'id': row[0],
                'type': row[1],
                'title': row[2],
                'message': row[3],
                'listing_id': row[4],
                'is_read': row[5],
                'created_at': row[6].isoformat() if row[6] else None,
                'data': row[7] if row[7] else {}
            })
        
        return Response({
            'notifications': notifications,
            'unread_count': unread_count
        })
        
    except Exception as e:
        return Response({
            'notifications': [],
            'unread_count': 0
        }, status=200)

@api_view(['POST'])
@permission_classes([IsAuthenticated])
def mark_notification_read(request, notification_id):
    """Mark a notification as read"""
    try:
        notification = UserNotification.objects.get(id=notification_id, user=request.user)
        notification.is_read = True
        notification.save()
        
        return Response({'status': 'marked_read'})
        
    except UserNotification.DoesNotExist:
        return Response({'error': 'Notification not found'}, status=404)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def save_search_preferences(request):
    """
    Save user's search preferences for future notifications
    """
    try:
        data = json.loads(request.body)
        
        # Save to user profile or separate table
        from services.models import UserProfile
        
        profile, created = UserProfile.objects.get_or_create(user=request.user)
        profile.search_preferences = data.get('filters', {})
        profile.save()
        
        # Immediately check for matching listings
        count = NotificationService.check_for_new_matching_listings(
            request.user.id, 
            data.get('filters', {})
        )
        
        return Response({
            'status': 'preferences_saved',
            'matching_listings_found': count
        })
        
    except Exception as e:
        return Response({'error': str(e)}, status=500)