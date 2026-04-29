import os
from pathlib import Path
import sys
from dotenv import load_dotenv
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
    
# ✅ ADD THIS: Add the Project Root (EstateMind) to sys.path
# This allows imports like "from data.preprocessing..."
PROJECT_ROOT = BASE_DIR.parent 
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

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
    "agent",  # ✨ NEW: Agentic chatbot app
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


PUSHER_APP_ID = os.getenv('PUSHER_APP_ID')
PUSHER_KEY = os.getenv('PUSHER_KEY')
PUSHER_SECRET = os.getenv('PUSHER_SECRET')
PUSHER_CLUSTER = os.getenv('PUSHER_CLUSTER', 'eu')

# ── CORS ───────────────────────────────────────────────────────────────────────
CORS_ALLOWED_ORIGINS = os.environ.get(
    "CORS_ALLOWED_ORIGINS",
    "http://localhost:5173,http://localhost:3000",
).split(",")
CORS_ALLOW_CREDENTIALS = True

# ── Session & CSRF ────────────────────────────────────────────────────────────
# Session settings - increase timeout
SESSION_COOKIE_AGE = 7 * 24 * 60 * 60  # 7 days (in seconds)
SESSION_SAVE_EVERY_REQUEST = True  # Refresh session on each request
SESSION_EXPIRE_AT_BROWSER_CLOSE = False  # Don't expire when browser closes

# CSRF settings for better compatibility
CSRF_COOKIE_AGE = 7 * 24 * 60 * 60  # 7 days
CSRF_COOKIE_HTTPONLY = False
CSRF_USE_SESSIONS = False
CSRF_COOKIE_SAMESITE = 'Lax'

# Also add these for better session handling
SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')
USE_X_FORWARDED_HOST = True
USE_X_FORWARDED_PORT = True

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


# At the bottom of settings.py
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
        },
    },
    'root': {
        'handlers': ['console'],
        'level': 'INFO',  # Change to 'DEBUG' for more verbose logs
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False,
        },
        'agent': {  # Your agent module
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False,
        },
    },
}