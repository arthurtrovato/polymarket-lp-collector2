FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/data \
    HEALTH_HOST=0.0.0.0 \
    PORT=8080

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md ./
COPY polymarket_collector ./polymarket_collector
COPY scripts ./scripts
COPY deploy/container-entrypoint.sh ./deploy/container-entrypoint.sh
RUN pip install --no-cache-dir . \
    && chmod +x /app/scripts/polymarket-backup /app/deploy/container-entrypoint.sh

EXPOSE 8080
ENTRYPOINT ["/usr/bin/tini", "--", "/app/deploy/container-entrypoint.sh"]
