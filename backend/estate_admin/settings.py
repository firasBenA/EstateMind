import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY    = os.environ.get("DJANGO_SECRET_KEY", "change-me-in-production")
DEBUG         = os.environ.get("DEBUG", "true").lower() == "true"
ALLOWED_HOSTS = os.environ.get("ALLOWED_HOSTS", "*").split(",")

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "corsheaders",
    "dashboard",
    "django_extensions",
]

import re
from django.utils.deprecation import MiddlewareMixin

MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",
    "estate_admin.settings.DisableCSRFOnAPI",  # ← Add this line
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "estate_admin.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "estate_admin.wsgi.application"

# ── Single database — Supabase PostgreSQL ─────────────────────────────────────
# ALL tables live here: Django auth_user, django_session, user_profiles,
# listings, agent_metrics. No SQLite. No router.
DATABASES = {
    "default": {
        "ENGINE":   "django.db.backends.postgresql",
        "NAME":     os.environ.get("PG_NAME",     "postgres"),
        "USER":     os.environ.get("PG_USER",     "postgres"),
        "PASSWORD": os.environ.get("PG_PASSWORD", ""),
        "HOST":     os.environ.get("PG_HOST",     "localhost"),
        "PORT":     os.environ.get("PG_PORT",     "5432"),
        "OPTIONS": {
            "sslmode":         os.environ.get("PG_SSLMODE", "require"),
            "connect_timeout": 10,
        },
        "CONN_MAX_AGE": 60,
    },
}

# ── CORS ───────────────────────────────────────────────────────────────────────
CORS_ALLOWED_ORIGINS = os.environ.get(
    "CORS_ALLOWED_ORIGINS",
    "http://localhost:5173,http://localhost:3000",
).split(",")
CORS_ALLOW_CREDENTIALS = True

# ── Session & CSRF ────────────────────────────────────────────────────────────
SESSION_ENGINE          = "django.contrib.sessions.backends.db"
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SECURE   = not DEBUG
SESSION_COOKIE_AGE      = 60 * 60 * 24 * 14   # 14 days

CSRF_COOKIE_HTTPONLY  = False
CSRF_COOKIE_SAMESITE  = "Lax"
CSRF_COOKIE_SECURE    = not DEBUG
CSRF_TRUSTED_ORIGINS  = os.environ.get(
    "CSRF_TRUSTED_ORIGINS",
    "http://localhost:5173,http://localhost:3000,http://localhost:8081",
).split(",")

# ── Password security ──────────────────────────────────────────────────────────
AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {
        "NAME":    "django.contrib.auth.password_validation.MinimumLengthValidator",
        "OPTIONS": {"min_length": 8},
    },
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

PASSWORD_HASHERS = [
    "django.contrib.auth.hashers.Argon2PasswordHasher",
    "django.contrib.auth.hashers.PBKDF2PasswordHasher",
]

# ── Cache ─────────────────────────────────────────────────────────────────────
CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
    }
}

LANGUAGE_CODE      = "fr-tn"
TIME_ZONE          = "Africa/Tunis"
USE_I18N           = True
USE_TZ             = True
STATIC_URL         = "/static/"
STATIC_ROOT        = BASE_DIR / "staticfiles"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

class DisableCSRFOnAPI:
    """Middleware to skip CSRF checks for /api/ endpoints"""
    def __init__(self, get_response):
        self.get_response = get_response
    
    def __call__(self, request):
        if request.path.startswith('/api/'):
            setattr(request, '_dont_enforce_csrf_checks', True)
        return self.get_response(request)