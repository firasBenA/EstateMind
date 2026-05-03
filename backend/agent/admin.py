"""Django admin configuration for agent app."""
from django.contrib import admin
from .models import ChatSession, ChatMessage


@admin.register(ChatSession)
class ChatSessionAdmin(admin.ModelAdmin):
    list_display = ('session_id', 'user', 'title', 'created_at', 'is_active')
    list_filter = ('is_active', 'created_at')
    search_fields = ('session_id', 'title', 'user__username')
    readonly_fields = ('session_id', 'created_at', 'updated_at')


@admin.register(ChatMessage)
class ChatMessageAdmin(admin.ModelAdmin):
    list_display = ('session', 'role', 'created_at', 'tokens_used')
    list_filter = ('role', 'created_at')
    search_fields = ('session__session_id', 'content')
    readonly_fields = ('created_at',)
