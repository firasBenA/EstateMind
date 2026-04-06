from django.db import models


class Listing(models.Model):
    id = models.BigAutoField(primary_key=True)
    source_name = models.TextField()
    title = models.TextField()
    price = models.FloatField(null=True)
    scraped_at = models.DateTimeField(null=True)
    region = models.TextField(null=True)
    city = models.TextField(null=True)
    municipalite = models.TextField(null=True)
    surface = models.FloatField(null=True)
    rooms = models.IntegerField(null=True)
    transaction_type = models.TextField(null=True)
    type = models.TextField(null=True)
    latitude = models.FloatField(null=True)
    longitude = models.FloatField(null=True)

    class Meta:
        managed = False
        db_table = "listings"


class AgentMetrics(models.Model):
    id = models.BigAutoField(primary_key=True)
    run_started_at = models.DateTimeField(null=True)
    run_finished_at = models.DateTimeField(null=True)
    source_name = models.TextField(null=True)
    strategy = models.TextField(null=True)
    fetched = models.IntegerField(null=True)
    inserted = models.IntegerField(null=True)
    updated = models.IntegerField(null=True)
    unchanged = models.IntegerField(null=True)
    errors = models.IntegerField(null=True)
    consecutive_failures = models.IntegerField(null=True)
    disabled_until = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "agent_metrics"

