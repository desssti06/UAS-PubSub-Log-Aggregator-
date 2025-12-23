FROM python:3.11-slim

# Install PostgreSQL client untuk healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN adduser --disabled-password --gecos '' appuser && mkdir -p /app/data && chown -R appuser:appuser /app

# copy & install dependencies
COPY requirements.txt ./requirements.txt
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# copy source code
COPY src/ ./src/

# pastikan folder data tetap bisa diakses (kalau pakai volume)
RUN chmod -R 777 /app/data

# expose port dan environment
EXPOSE 8080
ENV DEDUP_DB=/app/data/dedup.db
ENV DATABASE_URL=""
ENV NUM_WORKERS=3

# switch ke user non-root
USER appuser
CMD ["python", "-m", "src.main"]

