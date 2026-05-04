# backend/Dockerfile
FROM python:3.12-slim

# System deps needed for psycopg2, cryptography, prophet
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libpq-dev curl git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first — Docker caches this layer
# Only re-runs pip install if requirements.txt changes
COPY requirements.txt .

# Install torch CPU first from PyTorch index (no CUDA, smaller image)
RUN pip install --no-cache-dir \
    torch==2.11.0+cpu \
    --index-url https://download.pytorch.org/whl/cpu

# Install everything else
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DJANGO_SETTINGS_MODULE=config.settings

EXPOSE 8000

# Gunicorn: 3 workers to stay within Supabase connection limits
# Timeout 120s for report/contract generation (Ollama can be slow)
CMD ["gunicorn", "config.wsgi:application", \
     "--bind", "0.0.0.0:8000", \
     "--workers", "3", \
     "--timeout", "120", \
     "--keep-alive", "5", \
     "--access-logfile", "-", \
     "--error-logfile", "-"]