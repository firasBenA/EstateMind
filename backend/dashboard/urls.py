from django.urls import path
from . import views

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("api/metrics/", views.metrics_api, name="metrics_api"),
    path("api/eda/", views.eda_metrics, name="eda_metrics"),
    path("api/quality/", views.data_quality_api, name="data_quality_api"),
    path("api/login/", views.api_login, name="api_login"),
    path("api/logout/", views.api_logout, name="api_logout"),
    path("api/session/", views.api_session, name="api_session"),
]