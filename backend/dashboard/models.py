"""
dashboard/models.py

Single Supabase PostgreSQL database for everything.

Managed models (Django creates/migrates):
  - UserProfile  → user_profiles table

Unmanaged models (scraper owns the schema, Django only reads):
  - Listing      → listings table
  - AgentMetrics → agent_metrics table

Django's built-in auth_user, django_session, etc. also live in Supabase
after running: python manage.py migrate
"""
from django.db import models
from django.contrib.auth.models import User


# ── Unmanaged — scraper owns these schemas ────────────────────────────────────

class Listing(models.Model):
    # Column names match the exact schema confirmed by check_listings.py
    id                  = models.TextField(primary_key=True)   # text, not bigint
    source_name         = models.TextField(null=True)
    source_id           = models.TextField(null=True)
    url                 = models.TextField(null=True)
    title               = models.TextField(null=True)
    description         = models.TextField(null=True)
    price               = models.DecimalField(max_digits=20, decimal_places=4, null=True)
    currency            = models.TextField(null=True)
    transaction_type    = models.TextField(null=True)
    property_type       = models.TextField(null=True)    # NOT "type"
    rooms               = models.IntegerField(null=True)
    city                = models.TextField(null=True)
    municipality        = models.TextField(null=True)    # NOT "municipalite"
    zone                = models.TextField(null=True)
    region              = models.TextField(null=True)
    surface             = models.DecimalField(max_digits=20, decimal_places=4, null=True)
    features            = models.JSONField(null=True)
    images              = models.JSONField(null=True)
    images_count        = models.IntegerField(null=True)
    price_per_m2        = models.DecimalField(max_digits=20, decimal_places=4, null=True)
    latitude            = models.DecimalField(max_digits=20, decimal_places=8, null=True)
    longitude           = models.DecimalField(max_digits=20, decimal_places=8, null=True)
    poi                 = models.JSONField(null=True)
    reliability_score   = models.DecimalField(max_digits=10, decimal_places=4, null=True)
    reliability_level   = models.TextField(null=True)
    is_outlier          = models.BooleanField(null=True)
    outlier_flags       = models.JSONField(null=True)
    suspected_duplicate = models.BooleanField(null=True)
    change_type         = models.TextField(null=True)
    price_delta         = models.DecimalField(max_digits=20, decimal_places=4, null=True)
    price_delta_pct     = models.DecimalField(max_digits=10, decimal_places=4, null=True)
    has_price_history   = models.BooleanField(null=True)
    normalized          = models.BooleanField(null=True)
    nlp_enriched        = models.BooleanField(null=True)
    nlp_filled_fields   = models.JSONField(null=True)
    model_weight        = models.DecimalField(max_digits=10, decimal_places=4, null=True)
    should_drop         = models.BooleanField(null=True)
    # text_embedding skipped — vector type not supported by standard Django
    scraped_at          = models.DateTimeField(null=True)
    last_updated        = models.DateTimeField(null=True)
    created_at          = models.DateTimeField(null=True)

    class Meta:
        managed  = False
        db_table = "listings"
        ordering = ["-scraped_at"]


class AgentMetrics(models.Model):
    id                   = models.BigAutoField(primary_key=True)
    run_started_at       = models.DateTimeField(null=True)
    run_finished_at      = models.DateTimeField(null=True)
    source_name          = models.TextField(null=True)
    strategy             = models.TextField(null=True)
    fetched              = models.IntegerField(null=True)
    inserted             = models.IntegerField(null=True)
    updated              = models.IntegerField(null=True)
    unchanged            = models.IntegerField(null=True)
    errors               = models.IntegerField(null=True)
    consecutive_failures = models.IntegerField(null=True)
    disabled_until       = models.DateTimeField(null=True)

    class Meta:
        managed  = False
        db_table = "agent_metrics"
        ordering = ["-run_started_at"]


# ── Managed — Django creates this table via migrations ────────────────────────

class UserProfile(models.Model):
    ROLE_CHOICES = [
        ("particular", "Particulier"),
        ("agency",     "Agence"),
        ("analyst",    "Analyste"),
        ("admin",      "Administrateur"),
    ]

    user              = models.OneToOneField(
        User, on_delete=models.CASCADE, related_name="profile"
    )
    role              = models.CharField(max_length=20, choices=ROLE_CHOICES, default="particular")
    phone             = models.CharField(max_length=20, blank=True)
    date_of_birth     = models.DateField(null=True, blank=True)
    # Agency fields
    agency_name       = models.CharField(max_length=200, blank=True)
    matricule_fiscale = models.CharField(max_length=50, blank=True)
    created_at        = models.DateTimeField(auto_now_add=True)
    updated_at        = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "user_profiles"   # explicit name in Supabase

    def __str__(self):
        return f"{self.user.email} ({self.role})"