"""
Agent conversation models for persistent memory.
Stores chat sessions and messages for context retention.
"""
from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone
import json


class ChatSession(models.Model):
    """Stores individual chat sessions."""
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    session_id = models.CharField(max_length=255, unique=True)
    title = models.CharField(max_length=255, blank=True, default="New Chat")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        ordering = ['-updated_at']
        indexes = [
            models.Index(fields=['session_id']),
            models.Index(fields=['user', 'is_active']),
        ]

    def __str__(self):
        return f"{self.title} - {self.session_id}"

    def expire_old_sessions(days=30):
        """Auto-delete sessions older than N days."""
        cutoff = timezone.now() - timezone.timedelta(days=days)
        ChatSession.objects.filter(created_at__lt=cutoff).delete()


class ChatMessage(models.Model):
    """Stores individual messages in a conversation."""
    ROLE_CHOICES = [
        ('user', 'User'),
        ('assistant', 'Assistant'),
        ('tool', 'Tool Result'),
        ('error', 'Error'),
    ]

    session = models.ForeignKey(ChatSession, on_delete=models.CASCADE, related_name='messages')
    role = models.CharField(max_length=20, choices=ROLE_CHOICES)
    content = models.TextField()
    tool_calls = models.JSONField(default=list, blank=True)  # [{name, args, result}]
    tokens_used = models.IntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['created_at']
        indexes = [
            models.Index(fields=['session', 'created_at']),
        ]

    def __str__(self):
        return f"{self.role}: {self.content[:50]}..."

    @classmethod
    def from_langchain_message(cls, session, message_dict):
        """Factory method to create ChatMessage from LangChain message format."""
        return cls(
            session=session,
            role=message_dict.get('role', 'user'),
            content=message_dict.get('content', ''),
            tool_calls=message_dict.get('tool_calls', [])
        )
