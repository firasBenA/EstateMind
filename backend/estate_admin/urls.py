from django.contrib import admin
from django.http import JsonResponse
from django.urls import include, path
from django.contrib.auth import views as auth_views

from dashboard.views_ai import generate_description , predict_price

# Simple test view (defined inline)
def test_view(request):
    return JsonResponse({"routing_works": True})

urlpatterns = [
    path("admin/", admin.site.urls),
    path("accounts/login/", auth_views.LoginView.as_view(), name="login"),
    path("accounts/logout/", auth_views.LogoutView.as_view(), name="logout"),
    path("api/generate-description/", generate_description),
    path('api/predict-price/', predict_price),
    

        # ✅ TEMPORARY TEST ENDPOINT AT ROOT LEVEL
    path("api/root-test/", test_view, name="root_test"),
    path("", include("dashboard.urls")),
]

