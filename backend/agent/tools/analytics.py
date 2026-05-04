"""
Analytics tool - aggregates market statistics and quality metrics.
"""
from typing import Dict, Any
from django.db.models import Avg, Count, Min, Max, Q
from services.models import Listing, AgentMetrics
from django.utils import timezone
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)


def get_analytics(
    city: str = None,
    region: str = None,
    property_type: str = None,
    days: int = 30,
) -> Dict[str, Any]:
    """
    Get market analytics and quality metrics.
    
    Args:
        city: Filter by city (optional)
        region: Filter by region (optional)
        property_type: Filter by property type (optional)
        days: Look back N days (default 30)
    
    Returns:
        {
            "total_listings": int,
            "avg_price": float,
            "avg_price_per_m2": float,
            "price_range": {"min": float, "max": float},
            "avg_surface": float,
            "avg_rooms": float,
            "fraud_rate": float,
            "outlier_rate": float,
            "duplicate_rate": float,
            "reliability_avg": float,
            "listings_this_week": int,
            "listings_this_month": int,
            "agent_metrics": {...},
            "city_stats": [...],
            "error": str (if any)
        }
    """
    try:
        # Base queryset: active, normalized listings
        qs = Listing.objects.filter(normalized=True).exclude(should_drop=True)

        # Apply filters
        if city:
            qs = qs.filter(city__icontains=city)
        if region:
            qs = qs.filter(region__icontains=region)
        if property_type:
            qs = qs.filter(property_type__icontains=property_type)

        # Date filter
        cutoff = timezone.now() - timedelta(days=days)
        qs_recent = qs.filter(scraped_at__gte=cutoff)

        total_count = qs.count()

        if total_count == 0:
            return {
                'error': 'No listings found matching criteria',
                'total_listings': 0,
            }

        # Aggregate statistics
        stats = qs.aggregate(
            avg_price=Avg('price'),
            min_price=Min('price'),
            max_price=Max('price'),
            avg_surface=Avg('surface'),
            avg_rooms=Avg('rooms'),
            avg_price_per_m2=Avg('price_per_m2'),
            avg_reliability=Avg('reliability_score'),
        )

        # Quality metrics
        fraud_count = qs.filter(is_outlier=True).count()
        duplicate_count = qs.filter(suspected_duplicate=True).count()
        outlier_count = qs.filter(is_outlier=True).count()

        # Recent activity
        one_week_ago = timezone.now() - timedelta(days=7)
        listings_this_week = qs_recent.filter(scraped_at__gte=one_week_ago).count()
        listings_this_month = qs_recent.count()

        # Agent metrics (last 10 runs)
        agent_runs = AgentMetrics.objects.all().order_by('-run_started_at')[:10]
        total_fetched = sum(run.fetched for run in agent_runs)
        total_inserted = sum(run.inserted for run in agent_runs)
        total_errors = sum(run.errors for run in agent_runs)

        # City breakdown (top 5)
        city_breakdown = (
            qs.values('city')
            .annotate(
                count=Count('id'),
                avg_price=Avg('price'),
                fraud_count=Count('id', filter=Q(is_outlier=True))
            )
            .order_by('-count')[:5]
        )

        return {
            'total_listings': total_count,
            'avg_price': float(stats['avg_price']) if stats['avg_price'] else 0,
            'min_price': float(stats['min_price']) if stats['min_price'] else 0,
            'max_price': float(stats['max_price']) if stats['max_price'] else 0,
            'price_range': {
                'min': float(stats['min_price']) if stats['min_price'] else 0,
                'max': float(stats['max_price']) if stats['max_price'] else 0,
            },
            'avg_surface': float(stats['avg_surface']) if stats['avg_surface'] else 0,
            'avg_rooms': float(stats['avg_rooms']) if stats['avg_rooms'] else 0,
            'avg_price_per_m2': float(stats['avg_price_per_m2']) if stats['avg_price_per_m2'] else 0,
            'avg_reliability_score': float(stats['avg_reliability']) if stats['avg_reliability'] else 0,
            'fraud_rate': round((fraud_count / total_count * 100), 2) if total_count > 0 else 0,
            'duplicate_rate': round((duplicate_count / total_count * 100), 2) if total_count > 0 else 0,
            'outlier_rate': round((outlier_count / total_count * 100), 2) if total_count > 0 else 0,
            'listings_this_week': listings_this_week,
            'listings_this_month': listings_this_month,
            'agent_metrics': {
                'total_fetched': total_fetched,
                'total_inserted': total_inserted,
                'total_errors': total_errors,
                'recent_runs': len(agent_runs),
            },
            'city_stats': [
                {
                    'city': item['city'],
                    'count': item['count'],
                    'avg_price': float(item['avg_price']) if item['avg_price'] else 0,
                    'fraud_count': item['fraud_count'],
                }
                for item in city_breakdown
            ],
        }

    except Exception as e:
        logger.error(f"Analytics error: {str(e)}")
        return {
            'error': f"Analytics failed: {str(e)}",
            'total_listings': 0,
        }
