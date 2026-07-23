#!/usr/bin/env bash
set -Eeuo pipefail

DATA_DIR="${DATA_DIR:-/data}"
BACKUP_INTERVAL_SECONDS="${BACKUP_INTERVAL_SECONDS:-120}"
RCLONE_CONFIG_FILE="${RCLONE_CONFIG_FILE:-/tmp/rclone.conf}"
collector_pid=""
backup_pid=""

if ! [[ "$BACKUP_INTERVAL_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "BACKUP_INTERVAL_SECONDS must be a positive integer." >&2
  exit 1
fi

mkdir -p "$DATA_DIR"

if [[ -n "${RCLONE_CONFIG_B64:-}" ]]; then
  printf '%s' "$RCLONE_CONFIG_B64" | base64 --decode >"$RCLONE_CONFIG_FILE"
  chmod 600 "$RCLONE_CONFIG_FILE"
  export RCLONE_CONFIG="$RCLONE_CONFIG_FILE"
fi

if [[ "${REQUIRE_REMOTE_BACKUP:-false}" == "true" ]]; then
  if [[ -n "${HF_DATASET_REPO:-}" && -n "${HF_TOKEN:-}" ]]; then
    :
  elif [[ -n "${RCLONE_REMOTE:-}" && -s "$RCLONE_CONFIG_FILE" ]]; then
    :
  else
    echo "Remote backup is required but neither Hugging Face nor rclone is configured." >&2
    exit 1
  fi
fi

stop_children() {
  trap - EXIT INT TERM
  # Stop and reap the periodic uploader before the collector finalizes its
  # active files. This prevents it from racing the one-shot final backup.
  if [[ -n "$backup_pid" ]]; then
    kill -TERM "$backup_pid" 2>/dev/null || true
    wait "$backup_pid" 2>/dev/null || true
    backup_pid=""
  fi
  if [[ -n "$collector_pid" ]]; then
    kill -TERM "$collector_pid" 2>/dev/null || true
  fi
  wait "$collector_pid" 2>/dev/null || true
}
trap stop_children EXIT INT TERM

if [[ -n "${HF_DATASET_REPO:-}" ]]; then
  python -m polymarket_collector.hf_backup --loop &
  backup_pid=$!
elif [[ -n "${RCLONE_REMOTE:-}" ]]; then
  (
    while true; do
      if ! ENV_FILE=/dev/null DATA_DIR="$DATA_DIR" \
        /app/scripts/polymarket-backup; then
        echo "Backup failed; files are kept locally and will be retried." >&2
      fi
      sleep "$BACKUP_INTERVAL_SECONDS"
    done
  ) &
  backup_pid=$!
fi

polymarket-collector &
collector_pid=$!
set +e
wait "$collector_pid"
status=$?
set -e
collector_pid=""
exit "$status"
