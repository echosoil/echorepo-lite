FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app        

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy package and templates
COPY echorepo/ ./echorepo/

# Data dir for your mounted volume
RUN mkdir -p /data/db

EXPOSE 8000

# Use gunicorn against the app-factory entrypoint
CMD ["gunicorn", "-b", "0.0.0.0:8000", "echorepo.wsgi:app"]
# (for quick local debugging you can switch to:  CMD ["python", "-m", "echorepo.wsgi"])
