#!/usr/bin/env bash
set -euo pipefail

SOURCE_DIR="${1:-}"
INSTALL_DIR=/opt/polymarket-collector
DATA_DIR=/var/lib/polymarket-collector
ENV_FILE=/etc/polymarket-collector.env

if [[ -z "$SOURCE_DIR" || ! -f "$SOURCE_DIR/pyproject.toml" ]]; then
  echo "Usage: sudo $0 /path/to/polymarket-collector" >&2
  exit 2
fi
if [[ "${EUID}" -ne 0 ]]; then
  echo "This installer must run as root." >&2
  exit 2
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends \
  ca-certificates python3 python3-pip python3-venv rclone

if ! id polymarket >/dev/null 2>&1; then
  useradd --system --home-dir "$DATA_DIR" --create-home \
    --shell /usr/sbin/nologin polymarket
fi

mkdir -p "$INSTALL_DIR" "$DATA_DIR"
cp -a "$SOURCE_DIR/." "$INSTALL_DIR/"
python3 -m venv "$INSTALL_DIR/.venv"
"$INSTALL_DIR/.venv/bin/pip" install --no-cache-dir --upgrade pip
"$INSTALL_DIR/.venv/bin/pip" install --no-cache-dir "$INSTALL_DIR"

if [[ ! -f "$ENV_FILE" ]]; then
  cp "$INSTALL_DIR/.env.example" "$ENV_FILE"
fi
chown root:polymarket "$ENV_FILE"
chmod 0640 "$ENV_FILE"
chown -R polymarket:polymarket "$DATA_DIR"

install -m 0644 "$INSTALL_DIR/deploy/polymarket-collector.service" \
  /etc/systemd/system/polymarket-collector.service
install -m 0644 "$INSTALL_DIR/deploy/polymarket-backup.service" \
  /etc/systemd/system/polymarket-backup.service
install -m 0644 "$INSTALL_DIR/deploy/polymarket-backup.timer" \
  /etc/systemd/system/polymarket-backup.timer
install -m 0755 "$INSTALL_DIR/scripts/polymarket-backup" \
  /usr/local/sbin/polymarket-backup

systemctl daemon-reload
systemctl enable --now polymarket-collector.service

echo
echo "Collector installed."
echo "Status:  systemctl status polymarket-collector"
echo "Logs:    journalctl -u polymarket-collector -f"
echo "Health:  curl http://127.0.0.1:8080/healthz"
echo "Drive:   run 'sudo rclone config', set RCLONE_REMOTE in $ENV_FILE,"
echo "         then enable polymarket-backup.timer."

