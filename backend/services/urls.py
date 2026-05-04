# dashboard/urls.py
from django.urls import path
from . import views
from . import report_views
from . import similarity_views
from . import contract
from .recommendation_views import get_similar_listings, get_personalized_recommendations
from .recommendation.behavior_tracker import BehaviorTracker
from .views_ai import generate_description, predict_price, get_model_status, compare_scenarios, get_base_prices, get_macro_forecast, predict_property, forecast_listing
from .recommendation_views import (
    get_similar_listings, 
    get_personalized_recommendations,
    track_behavior,
    track_search,
    get_notifications,
    mark_notification_read,
    save_search_preferences
)
from .view_validation import (
    validate_title_api,
    get_governorates,
    get_delegations,
    auto_correct_delegation
)

urlpatterns = [
    # ── Public: listings ────────────────────────────────────────────────────
    # IMPORTANT: Place specific paths BEFORE generic ones
    path("api/listings/create/", views.create_listing, name="create_listing"),
    path("api/listings/meta/", views.listings_meta, name="listings_meta"),
    path("api/listings/", views.listings_list, name="listings_list"),
    path("api/listings/<str:pk>/", views.listing_detail, name="listing_detail"),
    
    # Similar listings (after generic listing paths)
    path("api/listings/<str:pk>/similar/", similarity_views.similar_listings, name="similar_listings"),
    
    # ── Auth ─────────────────────────────────────────────────────────────────
    path("api/register/", views.api_register, name="api_register"),
    path("api/login/", views.api_login, name="api_login"),
    path("api/logout/", views.api_logout, name="api_logout"),
    path("api/session/", views.api_session, name="api_session"),

    # ── Admin dashboard ───────────────────────────────────────────────────────
    #path("", views.se, name="dashboard"),
    path("api/metrics/", views.metrics_api, name="metrics_api"),
    path("api/eda/", views.eda_metrics, name="eda_metrics"),
    path("api/quality/", views.data_quality_api, name="data_quality_api"),

    # ── Reports (auth required) ───────────────────────────────────────────────
    path("api/reports/generate/", report_views.generate_report, name="report_generate"),
    path("api/reports/", report_views.list_reports, name="report_list"),
    path("api/reports/save/", report_views.save_report, name="report_save"),
    path("api/reports/<int:pk>/", report_views.get_report, name="report_detail"),
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

    # ── AI & Validation endpoints ───────────────────────────────────────────
    path('api/validate-title/', validate_title_api, name='validate_title'),
    path('api/governorates/', get_governorates, name='get_governorates'),
    path('api/governorates/<int:governorate_id>/delegations/', get_delegations, name='get_delegations'),
    path('api/delegation/autocorrect/', auto_correct_delegation, name='auto_correct_delegation'),
    path("api/generate-description/", generate_description, name="generate-description"),
    path("api/predict-price/", predict_price, name="predict-price"),

    path("api/user/listings/", views.user_listings, name="user_listings"),
    path("api/user/likes/", views.user_likes, name="user_likes"),
    path("api/user/stats/", views.user_stats, name="user_stats"),
    path("api/user/activity/", views.user_activity, name="user_activity"),
    
    # ── Like/Favorite functionality ─────────────────────────────────────────
    path("api/listings/<str:listing_id>/like/", views.toggle_like, name="toggle_like"),
    # In dashboard/urls.py
    path("api/listings/<str:listing_id>/view/", views.track_listing_view, name="track_listing_view"),
    # ── Chat endpoints ─────────────────────────────────────────────────────
    path("api/chat/conversation/<str:listing_id>/", views.get_or_create_conversation, name="get_or_create_conversation"),
    path("api/chat/send/", views.send_chat_message, name="send_chat_message"),
    path("api/chat/conversations/", views.get_conversations, name="get_conversations"),
    path("api/chat/messages/<int:conversation_id>/", views.get_messages, name="get_messages"),
    path("api/pusher/auth/", views.pusher_auth, name="pusher_auth"),

    # ── Forecasting endpoints ───────────────────────────────────────────────
    path('api/listings/<str:pk>/forecast/', forecast_listing, name='listing_forecast'),
    path('api/ai/predict/', predict_property, name='ai_predict'),
    path('api/ai/compare/', compare_scenarios, name='ai_compare'),
    path('api/ai/base-prices/', get_base_prices, name='ai_base_prices'),
    path('api/ai/macro-forecast/', get_macro_forecast, name='ai_macro_forecast'),
    path('api/ai/status/', get_model_status, name='ai_status'),


        # ── Fraud detection (DSO 2.2) ─────────────────────────────────────────────
    path("api/fraud/summary/",  views.fraud_summary_api,  name="fraud_summary"),
    path("api/fraud/listings/", views.fraud_listings_api, name="fraud_listings"),
    path("api/fraud/flags/",    views.fraud_flags_api,    name="fraud_flags"),
]