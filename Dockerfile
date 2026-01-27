FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir debugpy

# Copy package and templates
COPY echorepo/ ./echorepo/

# Compile translations (only the app tree)
# the `|| true` prevents the build from failing if a locale is empty
RUN pybabel compile -d /app/echorepo/translations

# Data dir for your mounted volume
RUN mkdir -p /data/db

EXPOSE 8000

# Use gunicorn against the app-factory entrypoint
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8000", "--timeout", "120", "--graceful-timeout", "120", "--keep-alive", "5", "echorepo.wsgi:app"]
# (for quick local debugging you can switch to:  CMD ["python", "-m", "echorepo.wsgi"])
