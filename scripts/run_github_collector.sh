#!/usr/bin/env bash
set -Eeuo pipefail

# A GitHub-hosted job is capped at six hours. Stop early enough to close,
# compress, and upload the last open archives before the runner disappears.
COLLECT_SECONDS="${COLLECT_SECONDS:-20280}"

if ! [[ "$COLLECT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "COLLECT_SECONDS must be a positive integer." >&2
  exit 1
fi
if [[ -z "${HF_DATASET_REPO:-}" || -z "${HF_TOKEN:-}" ]]; then
  echo "HF_DATASET_REPO and HF_TOKEN are required." >&2
  exit 1
fi

mkdir -p "${DATA_DIR:?DATA_DIR is required}"

set +e
timeout --signal=TERM --kill-after=45s \
  "$COLLECT_SECONDS" bash deploy/container-entrypoint.sh
collector_status=$?
set -e

# GNU timeout returns 124 after deliberately stopping the long-lived service.
if [[ "$collector_status" -ne 0 && "$collector_status" -ne 124 ]]; then
  echo "Collector exited unexpectedly with status $collector_status." >&2
  exit "$collector_status"
fi

# The TERM handler has finalized the active .part files. Retry the one-shot
# backup because the runner has enough shutdown margin and local files would be
# lost when it exits.
for attempt in $(seq 1 10); do
  if BACKUP_MIN_AGE_SECONDS=0 python -m polymarket_collector.hf_backup; then
    if ! find "$DATA_DIR" -type f \
      \( -name '*.jsonl.gz' -o -name '*.jsonl.part' \) \
      -print -quit | grep -q .; then
      break
    fi
  fi
  echo "Final backup attempt $attempt left local archives; retrying." >&2
  sleep 15
done

if find "$DATA_DIR" -type f \( -name '*.jsonl.gz' -o -name '*.jsonl.part' \) \
  -print -quit | grep -q .; then
  echo "Local archives remain after the final backup." >&2
  exit 1
fi

echo "Collector window completed and all archives were backed up."
