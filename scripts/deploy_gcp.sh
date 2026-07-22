#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${1:-}"
INSTANCE="${INSTANCE_NAME:-polymarket-collector}"
ZONE="${GCP_ZONE:-us-east1-b}"
MACHINE_TYPE="${GCP_MACHINE_TYPE:-e2-micro}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "$PROJECT_ID" ]]; then
  echo "Usage: $0 GOOGLE_CLOUD_PROJECT_ID" >&2
  exit 2
fi
if ! command -v gcloud >/dev/null 2>&1; then
  echo "gcloud is required. Run this script from Google Cloud Shell or install the Google Cloud CLI." >&2
  exit 2
fi
if [[ ! -f "$ROOT_DIR/pyproject.toml" ]]; then
  echo "Project files not found next to this script." >&2
  exit 2
fi

ACTIVE_ACCOUNT="$(gcloud auth list --filter=status:ACTIVE --format='value(account)' | head -1)"
if [[ -z "$ACTIVE_ACCOUNT" ]]; then
  echo "No active Google Cloud account. Run: gcloud auth login" >&2
  exit 2
fi

gcloud config set project "$PROJECT_ID" >/dev/null
gcloud services enable compute.googleapis.com --project "$PROJECT_ID"

if ! gcloud compute instances describe "$INSTANCE" --zone "$ZONE" \
  --project "$PROJECT_ID" >/dev/null 2>&1; then
  gcloud compute instances create "$INSTANCE" \
    --project "$PROJECT_ID" \
    --zone "$ZONE" \
    --machine-type "$MACHINE_TYPE" \
    --provisioning-model STANDARD \
    --network-tier STANDARD \
    --image-family ubuntu-2404-lts-amd64 \
    --image-project ubuntu-os-cloud \
    --boot-disk-type pd-standard \
    --boot-disk-size 30GB \
    --metadata enable-oslogin=TRUE \
    --labels app=polymarket-collector
else
  echo "Instance $INSTANCE already exists; updating the collector."
fi

ARCHIVE="$(mktemp --suffix=.tar.gz)"
trap 'rm -f "$ARCHIVE"' EXIT
tar -czf "$ARCHIVE" \
  --exclude=.git --exclude=.venv --exclude=data --exclude='*.egg-info' \
  -C "$ROOT_DIR" .

gcloud compute scp "$ARCHIVE" "$INSTANCE:/tmp/polymarket-collector.tar.gz" \
  --zone "$ZONE" --project "$PROJECT_ID"
gcloud compute ssh "$INSTANCE" --zone "$ZONE" --project "$PROJECT_ID" \
  --command 'workdir=$(mktemp -d); tar -xzf /tmp/polymarket-collector.tar.gz -C "$workdir"; sudo bash "$workdir/scripts/install_vps.sh" "$workdir"'

echo
echo "Deployment complete."
echo "Connect: gcloud compute ssh $INSTANCE --zone $ZONE --project $PROJECT_ID"
echo "The collector uses public data only and has no Polymarket trading key."

