FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir debugpy

# Copy app package
COPY echorepo/ ./echorepo/

# Copy migration scripts and SQL migrations
COPY scripts/ ./scripts/
COPY migrations/ ./migrations/

# Compile translations
# The `|| true` prevents the build from failing if a locale is empty/missing
RUN pybabel compile -d /app/echorepo/translations || true

# Data dir for your mounted volume
RUN mkdir -p /data/db

EXPOSE 8000

# Default command.
# In dev/prod compose files, you override this with:
# wait_for_postgres.py && run_pg_migrations.py && gunicorn ...
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8000", "--timeout", "120", "--graceful-timeout", "120", "--keep-alive", "5", "echorepo.wsgi:app"]
