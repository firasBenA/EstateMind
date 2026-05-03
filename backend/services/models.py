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
class Governorate(models.Model):
    """Tunisian governorates (24 regions)"""
    name = models.CharField(max_length=100)
    name_ar = models.CharField(max_length=100, blank=True)
    value = models.CharField(max_length=50, unique=True)
    latitude = models.FloatField(null=True, blank=True)
    longitude = models.FloatField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'governorates'
        verbose_name = "Governorate"
        verbose_name_plural = "Governorates"
    
    def __str__(self):
        return self.name


class Delegation(models.Model):
    """Delegations/districts within each governorate"""
    governorate = models.ForeignKey(
        Governorate, 
        on_delete=models.CASCADE, 
        related_name='delegations'
    )
    name = models.CharField(max_length=200)
    name_ar = models.CharField(max_length=200, blank=True)
    value = models.CharField(max_length=100)
    postal_code = models.CharField(max_length=10, blank=True)
    latitude = models.FloatField(null=True, blank=True)
    longitude = models.FloatField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'delegations'
        unique_together = ['governorate', 'value']
        verbose_name = "Delegation"
        verbose_name_plural = "Delegations"
    
    def __str__(self):
        return f"{self.name} ({self.governorate.name})"
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


class SavedReport(models.Model):
    REPORT_TYPES = [
        ("market",     "Market Overview"),
        ("investment", "Investment Analysis"),
        ("portfolio",  "Portfolio Performance"),
    ]
    user        = models.ForeignKey(User, on_delete=models.CASCADE, related_name="reports")
    report_type = models.CharField(max_length=20, choices=REPORT_TYPES)
    title       = models.CharField(max_length=200)
    params      = models.JSONField(default=dict)
    content     = models.TextField()               # full markdown text
    created_at  = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "saved_reports"
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.title} ({self.user.email})"

class Contract(models.Model):
    CONTRACT_TYPES = [
        ("compromis_de_vente", "Compromis de Vente"),
        ("promesse_de_vente", "Promesse de Vente"),
        ("contrat_de_location", "Contrat de Location"),
        ("acte_de_vente", "Acte de Vente"),
    ]
    
    STATUS_CHOICES = [
        ("draft", "Draft"),
        ("sent", "Sent"),
        ("signed", "Signed"),
    ]
    
    user = models.ForeignKey(UserProfile, on_delete=models.CASCADE, related_name="contracts")
    contract_type = models.CharField(max_length=50, choices=CONTRACT_TYPES)
    title = models.CharField(max_length=255)
    params = models.JSONField(default=dict)
    content = models.TextField()
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="draft")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.title} - {self.created_at.date()}"
    
    class Meta:
        ordering = ["-created_at"]



class UserBehaviorLog(models.Model):
    """Track user behaviors"""
    user = models.ForeignKey('auth.User', on_delete=models.CASCADE, null=True, blank=True)
    listing_id = models.CharField(max_length=100)  # TEXT type in DB
    behavior_type = models.CharField(max_length=30)
    duration_seconds = models.IntegerField(default=0)
    referrer = models.CharField(max_length=200, null=True, blank=True)
    search_query = models.TextField(null=True, blank=True)
    user_agent = models.TextField(null=True, blank=True)
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'user_behavior_log'
        managed = False  # Don't let Django manage this table (already exists)


class UserSearchHistory(models.Model):
    """Track user searches"""
    user = models.ForeignKey('auth.User', on_delete=models.CASCADE, null=True, blank=True)
    session_key = models.CharField(max_length=40, null=True, blank=True)
    search_query = models.TextField()
    filters = models.JSONField(default=dict)
    results_count = models.IntegerField(default=0)
    clicked_listing_id = models.CharField(max_length=100, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'user_search_history'
        managed = False


class UserNotification(models.Model):
    """User notifications"""
    user = models.ForeignKey('auth.User', on_delete=models.CASCADE)
    notification_type = models.CharField(max_length=50)
    title = models.CharField(max_length=200)
    message = models.TextField()
    listing_id = models.CharField(max_length=100, null=True, blank=True)
    is_read = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    data = models.JSONField(default=dict)
    
    class Meta:
        db_table = 'user_notifications'
        managed = False
        ordering = ['-created_at']


class UserRecommendationsCache(models.Model):
    """Cached recommendations"""
    user = models.ForeignKey('auth.User', on_delete=models.CASCADE)
    listing_id = models.CharField(max_length=100)
    recommendation_score = models.FloatField(default=0.5)
    recommendation_type = models.CharField(max_length=50, null=True, blank=True)
    reason = models.TextField(null=True, blank=True)
    expires_at = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'user_recommendations_cache'
        managed = False


    """Tracks scraper agent performance"""
    source_name = models.CharField(max_length=100, db_index=True)
    strategy = models.CharField(max_length=50, null=True, blank=True)
    
    # Run metrics
    fetched = models.IntegerField(default=0)
    inserted = models.IntegerField(default=0)
    updated = models.IntegerField(default=0)
    unchanged = models.IntegerField(default=0)
    errors = models.IntegerField(default=0)
    
    # Timestamps
    run_started_at = models.DateTimeField(null=True, blank=True, db_index=True)
    run_finished_at = models.DateTimeField(null=True, blank=True)
    
    # Optional: tracking failures
    consecutive_failures = models.IntegerField(default=0)
    disabled_until = models.DateTimeField(null=True, blank=True)
    
    class Meta:
        ordering = ['-run_started_at']
        indexes = [
            models.Index(fields=['source_name', 'run_started_at']),
        ]
    
    def __str__(self):
        return f"{self.source_name} - {self.run_started_at}"