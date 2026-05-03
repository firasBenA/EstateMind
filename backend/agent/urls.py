"""URLs for agent app."""
from django.urls import path
from . import views

urlpatterns = [
    path("", views.chat_endpoint, name="chat_endpoint"),
    path("sessions/", views.get_sessions, name="get_sessions"),
    path("sessions/<str:session_id>/", views.delete_session, name="delete_session"),
    path("sessions/<str:session_id>/messages/", views.get_session_messages, name="get_session_messages"),
]
