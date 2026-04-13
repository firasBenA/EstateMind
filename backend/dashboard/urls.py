from django.urls import path
from . import views
from . import report_views
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

    # ── Reports (auth required) ───────────────────────────────────────────────
    path("api/reports/generate/",  report_views.generate_report, name="report_generate"),
    path("api/reports/",           report_views.list_reports,    name="report_list"),
    path("api/reports/save/",      report_views.save_report,     name="report_save"),
    path("api/reports/<int:pk>/",  report_views.get_report,      name="report_detail"),
]