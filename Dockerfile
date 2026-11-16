FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    OSM_DATA_DIR=/data

WORKDIR /app

# Install dependencies first to leverage Docker layer caching.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Application code.
COPY app ./app
COPY wsgi.py ./wsgi.py

# Persistent SQLite directory (mounted as a volume in docker-compose).
RUN mkdir -p /data && \
    adduser --disabled-password --gecos "" --uid 10001 appuser && \
    chown -R appuser:appuser /data /app
USER appuser
VOLUME ["/data"]

EXPOSE 8000

# gthread workers: each worker serves several requests concurrently, so a
# slow upload parsing in one thread never blocks another upload or an
# export handled by a sibling thread.  Parsing happens outside any database
# write transaction, so two uploads can parse in parallel and commit in
# whatever order they finish.
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--worker-class", "gthread", "--workers", "2", "--threads", "8", "--timeout", "600", "--access-logfile", "-", "--error-logfile", "-", "wsgi:app"]