# estate_admin/settings.py
import os
import re
from pathlib import Path
import sys
from dotenv import load_dotenv
from django.utils.deprecation import MiddlewareMixin

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

MODELS_DIR = BASE_DIR / "data"/"serie_temporelle"/ "timeseries_exports"
EXPORTS_DIR = BASE_DIR / "data"/"serie_temporelle"/ "timeseries_exports"

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
    "services",
    
    "agent",  # ✨ NEW: Agentic chatbot app
    "django_extensions",
]


# ── SigNoz / OpenTelemetry Configuration ──────────────────────────────────────
SIGNOZ_ENABLED = os.getenv("SIGNOZ_ENABLED", "False") == "True"

if SIGNOZ_ENABLED:
    try:
        from .otel_config import setup_telemetry
        setup_telemetry()
        print("✅ SigNoz telemetry enabled")
    except ImportError:
        print("⚠️ otel_config module not found")
    except Exception as e:
        print(f"⚠️ SigNoz setup failed: {e}")
else:
    print("ℹ️ SigNoz telemetry disabled")

MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",
    "config.middleware.CloseOldConnectionsMiddleware",
    "config.settings.DisableCSRFOnAPI",
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "config.urls"

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

WSGI_APPLICATION = "config.wsgi.application"

# ── Database ──────────────────────────────────────────────────────────────────
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
        "CONN_MAX_AGE":      60,    # reuse connections for 60s
        "CONN_HEALTH_CHECKS": True, # verify connection is alive before reuse
    },
}

# ── Sessions — cache-based to avoid consuming DB connections ──────────────────
SESSION_ENGINE       = "django.contrib.sessions.backends.db"
#SESSION_CACHE_ALIAS  = "default"
SESSION_COOKIE_AGE   = 7 * 24 * 60 * 60   # 7 days
SESSION_SAVE_EVERY_REQUEST     = True
SESSION_EXPIRE_AT_BROWSER_CLOSE = False


SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SECURE = False
SESSION_COOKIE_DOMAIN = None
CSRF_COOKIE_SAMESITE = "Lax"
CSRF_COOKIE_SECURE = False

# ── Cache ─────────────────────────────────────────────────────────────────────
CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        "LOCATION": "django_cache",
        "TIMEOUT": 300,
        "OPTIONS": {"MAX_ENTRIES": 1000},
    }
    
}

# ── CSRF ──────────────────────────────────────────────────────────────────────
CSRF_COOKIE_AGE      = 7 * 24 * 60 * 60
CSRF_COOKIE_HTTPONLY = False
CSRF_USE_SESSIONS    = False
CSRF_COOKIE_SAMESITE = "Lax"
CSRF_COOKIE_SECURE   = not DEBUG
CSRF_TRUSTED_ORIGINS = os.environ.get(
    "CSRF_TRUSTED_ORIGINS",
    "http://localhost:5173,http://localhost:3000,http://localhost:8081",
).split(",")

# ── CORS ──────────────────────────────────────────────────────────────────────
CORS_ALLOWED_ORIGINS = os.environ.get(
    "CORS_ALLOWED_ORIGINS",
    "http://localhost:5173,http://localhost:3000,http://localhost:8081",
).split(",")
CORS_ALLOW_CREDENTIALS = True

# ── Pusher ────────────────────────────────────────────────────────────────────
PUSHER_APP_ID = os.getenv("PUSHER_APP_ID")
PUSHER_KEY    = os.getenv("PUSHER_KEY")
PUSHER_SECRET = os.getenv("PUSHER_SECRET")
PUSHER_CLUSTER = os.getenv("PUSHER_CLUSTER", "eu")

# ── Proxy / forwarding ────────────────────────────────────────────────────────
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
USE_X_FORWARDED_HOST    = True
USE_X_FORWARDED_PORT    = True

# ── Password validation ───────────────────────────────────────────────────────
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

# ── Localisation ──────────────────────────────────────────────────────────────
LANGUAGE_CODE      = "fr-tn"
TIME_ZONE          = "Africa/Tunis"
USE_I18N           = True
USE_TZ             = True

# ── Static files ──────────────────────────────────────────────────────────────
STATIC_URL         = "/static/"
STATIC_ROOT        = BASE_DIR / "staticfiles"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"


# ── CSRF bypass for /api/ routes ──────────────────────────────────────────────
class DisableCSRFOnAPI:
    """Skip CSRF checks for /api/ endpoints."""

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if request.path.startswith("/api/"):
            setattr(request, "_dont_enforce_csrf_checks", True)
        return self.get_response(request)
    

LOGIN_URL = "/api/login/"