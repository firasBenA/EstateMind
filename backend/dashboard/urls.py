from django.urls import path
from . import views

urlpatterns = [
    # ── Public: listings ────────────────────────────────────────────────────
    path("api/listings/",          views.listings_list,   name="listings_list"),
    path("api/listings/meta/",     views.listings_meta,   name="listings_meta"),
    path("api/listings/<str:pk>/", views.listing_detail,  name="listing_detail"),

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

    # ── Fraud detection (DSO 2.2) ─────────────────────────────────────────────
    path("api/fraud/summary/",  views.fraud_summary_api,  name="fraud_summary"),
    path("api/fraud/listings/", views.fraud_listings_api, name="fraud_listings"),
    path("api/fraud/flags/",    views.fraud_flags_api,    name="fraud_flags"),
]
