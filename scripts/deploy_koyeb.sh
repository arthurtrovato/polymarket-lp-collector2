#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
RCLONE_CONFIG_FILE="${RCLONE_CONFIG_FILE:-$HOME/.config/rclone/rclone.conf}"
APP_SERVICE="${KOYEB_APP_SERVICE:-polymarket-collector/collector}"
SECRET_NAME="${KOYEB_RCLONE_SECRET:-polymarket-rclone-config}"

if ! command -v koyeb >/dev/null 2>&1; then
  echo "Koyeb CLI is missing: https://www.koyeb.com/docs/build-and-deploy/cli/installation" >&2
  exit 1
fi
if [[ ! -f "$PROJECT_DIR/Dockerfile" ]]; then
  echo "Dockerfile not found in $PROJECT_DIR" >&2
  exit 1
fi
if [[ ! -s "$RCLONE_CONFIG_FILE" ]]; then
  echo "rclone config not found: $RCLONE_CONFIG_FILE" >&2
  exit 1
fi

if koyeb secrets describe "$SECRET_NAME" >/dev/null 2>&1; then
  base64 -w0 "$RCLONE_CONFIG_FILE" \
    | koyeb secrets update "$SECRET_NAME" --value-from-stdin >/dev/null
else
  base64 -w0 "$RCLONE_CONFIG_FILE" \
    | koyeb secrets create "$SECRET_NAME" --value-from-stdin >/dev/null
fi

koyeb deploy "$PROJECT_DIR" "$APP_SERVICE" \
  --archive-builder docker \
  --instance-type free \
  --regions fra \
  --ports 8080:http \
  --routes /:8080 \
  --checks 8080:http:/healthz \
  --checks-grace-period 8080=180 \
  --scale 1 \
  --env MAX_MARKETS=40 \
  --env DATA_DIR=/data \
  --env HEALTH_HOST=0.0.0.0 \
  --env PORT=8080 \
  --env ROTATION_SECONDS=300 \
  --env MAX_FILE_MIB=32 \
  --env BACKUP_INTERVAL_SECONDS=120 \
  --env BACKUP_MIN_AGE=30s \
  --env REMOVE_AFTER_UPLOAD=true \
  --env REQUIRE_REMOTE_BACKUP=true \
  --env RCLONE_REMOTE=gdrive:polymarket-data \
  --env "RCLONE_CONFIG_B64={{secret.$SECRET_NAME}}" \
  --wait \
  --wait-timeout 10m

koyeb services get "$APP_SERVICE"
