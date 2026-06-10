FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System packages:
# - curl: useful for Docker healthchecks
# - ca-certificates: needed for HTTPS calls
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      curl \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --upgrade pip \
 && pip install -r requirements.txt \
 && pip install debugpy

# Copy app package
COPY echorepo/ ./echorepo/

# Copy migration scripts and SQL migrations
COPY scripts/ ./scripts/
COPY migrations/ ./migrations/

# Optional: copy static if you want the image to work even without bind mounts.
# In your compose file you currently mount ./static:/app/static:ro
COPY static/ ./static/

# Compile translations.
# The `|| true` prevents the build from failing if a locale is empty/missing.
RUN pybabel compile -d /app/echorepo/translations || true

# Data dir for mounted volume
RUN mkdir -p /data/db /data/storage

# Create non-root user
RUN useradd --create-home --shell /bin/bash appuser \
 && chown -R appuser:appuser /app /data

USER appuser

EXPOSE 8000

# Default command.
# Dev/prod compose files override this with:
# wait_for_postgres.py && run_pg_migrations.py && gunicorn ...
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8000", "--timeout", "120", "--graceful-timeout", "120", "--keep-alive", "5", "echorepo.wsgi:app"]