# Generated migration for agent models
# This is a manual migration file for ChatSession and ChatMessage models

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='ChatSession',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('session_id', models.CharField(max_length=255, unique=True)),
                ('title', models.CharField(blank=True, default='New Chat', max_length=255)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('is_active', models.BooleanField(default=True)),
                ('user', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'ordering': ['-updated_at'],
                'indexes': [
                    models.Index(fields=['session_id'], name='agent_chats_idx1'),
                    models.Index(fields=['user', 'is_active'], name='agent_chats_idx2'),
                ],
            },
        ),
        migrations.CreateModel(
            name='ChatMessage',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('role', models.CharField(choices=[('user', 'User'), ('assistant', 'Assistant'), ('tool', 'Tool Result'), ('error', 'Error')], max_length=20)),
                ('content', models.TextField()),
                ('tool_calls', models.JSONField(blank=True, default=list)),
                ('tokens_used', models.IntegerField(default=0)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('session', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='messages', to='agent.chatsession')),
            ],
            options={
                'ordering': ['created_at'],
                'indexes': [
                    models.Index(fields=['session', 'created_at'], name='agent_msgs_idx1'),
                ],
            },
        ),
    ]
