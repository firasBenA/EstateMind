# backend/dashboard/urls.py

from django.urls import path
from . import views
from . import report_views
from . import similarity_views
from . import contract
from .recommendation_views import get_similar_listings, get_personalized_recommendations
from .recommendation.behavior_tracker import BehaviorTracker
from .recommendation_views import (
    get_similar_listings, 
    get_personalized_recommendations,
    track_behavior,
    track_search,
    get_notifications,
    mark_notification_read,
    save_search_preferences
)
urlpatterns = [
    # ── Public: listings ────────────────────────────────────────────────────
    path("api/listings/",          views.listings_list,   name="listings_list"),
    path("api/listings/meta/",     views.listings_meta,   name="listings_meta"),
    path("api/listings/<str:pk>/", views.listing_detail,  name="listing_detail"),
    path("api/listings/create/", views.create_listing, name="create_listing"),
    path("api/listings/<pk>/similar/",similarity_views.similar_listings,    name="similar_listings"),
    # ── Auth ─────────────────────────────────────────────────────────────────
    path("api/register/",  views.api_register, name="api_register"),
    path("api/login/",     views.api_login,    name="api_login"),
    path("api/logout/",    views.api_logout,   name="api_logout"),
    path("api/session/",   views.api_session,  name="api_session"),

    # ── Admin dashboard ───────────────────────────────────────────────────────
    path("",              views.dashboard,         name="dashboard"),
    path("api/metrics/",  views.metrics_api,       name="metrics_api"),
    path("api/eda/",      views.eda_metrics,       name="eda_metrics"),
    path("api/quality/",  views.data_quality_api,  name="data_quality_api"),

    # ── Reports (auth required) ───────────────────────────────────────────────
    path("api/reports/generate/",  report_views.generate_report, name="report_generate"),
    path("api/reports/",           report_views.list_reports,    name="report_list"),
    path("api/reports/save/",      report_views.save_report,     name="report_save"),
    path("api/reports/<int:pk>/",  report_views.get_report,      name="report_detail"),
    path("api/reports/<int:pk>/pdf/", report_views.export_report_pdf, name="report_pdf"),
    # ── Contract management (auth required) ─────────────────────────────────
    path('api/contracts/generate/', contract.generate_contract, name='generate_contract'),
    path('api/contracts/listing/<str:listing_id>/', contract.get_listing_for_contract, name='get_listing_for_contract'),
    path('api/contracts/', contract.list_contracts, name='list_contracts'),
    path('api/contracts/save/', contract.save_contract, name='save_contract'),
    path('api/contracts/<int:pk>/', contract.get_contract, name='get_contract'),
    path('api/contracts/<int:pk>/pdf/', contract.export_contract_pdf, name='export_contract_pdf'),
    path('api/contracts/<int:pk>/send/', contract.send_contract_for_signature, name='send_contract'),

    # ── Recommendation & Similar Listings ───────────────────────────────────
    path('api/recommendations/similar/<str:listing_id>/', 
         get_similar_listings, name='similar_listings'),
    path('api/recommendations/personalized/', 
         get_personalized_recommendations, name='personalized_recs'),
    
    # ── Behavior Tracking ───────────────────────────────────────────────────
    path('api/behaviors/track/', 
         track_behavior, name='track_behavior'),
    path('api/behaviors/track-search/', 
         track_search, name='track_search'),
    
    # ── Notifications ───────────────────────────────────────────────────────
    path('api/notifications/', 
         get_notifications, name='get_notifications'),
    path('api/notifications/<int:notification_id>/read/', 
         mark_notification_read, name='mark_read'),
    path('api/preferences/save/', 
         save_search_preferences, name='save_preferences'),
]