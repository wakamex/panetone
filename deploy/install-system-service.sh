#!/bin/bash
set -eu

repo=/code/msger
service_user=mihai
service_uid=1000
user_runtime=/run/user/$service_uid
user_bus=unix:path=$user_runtime/bus
source_uv=/home/$service_user/.local/bin/uv
system_uv=/usr/local/bin/uv
system_unit=/etc/systemd/system/panetone.service
handoff_started=0

if [ "$(id -u)" -ne 0 ]; then
    echo "run with: sudo /bin/bash $repo/deploy/install-system-service.sh" >&2
    exit 2
fi

run_as_service_user() {
    /usr/sbin/runuser -u "$service_user" -- \
        env XDG_RUNTIME_DIR="$user_runtime" \
        DBUS_SESSION_BUS_ADDRESS="$user_bus" \
        "$@"
}

restore_user_service() {
    if [ "$handoff_started" -eq 1 ]; then
        systemctl disable --now panetone.service >/dev/null 2>&1 || true
        run_as_service_user systemctl --user start panetone.service || true
        echo "system service promotion failed; temporary user service restored" >&2
        handoff_started=0
    fi
}

trap restore_user_service EXIT HUP INT TERM

test -x "$source_uv"
test -f "$repo/bridge.py"
test -f "$repo/bridge.py.lock"
test -x "$repo/panetone"
test -f "$repo/deploy/panetone.service"

run_as_service_user "$source_uv" --no-config lock --check --script "$repo/bridge.py"

if run_as_service_user systemctl --user is-active --quiet panetone.service; then
    handoff_started=1
fi
run_as_service_user systemctl --user disable --now panetone.service

install -m 0755 "$source_uv" "$system_uv"
install -o "$service_user" -g "$service_user" -m 0755 \
    "$repo/panetone" "/home/$service_user/.local/bin/panetone"
install -m 0644 "$repo/deploy/panetone.service" "$system_unit"
chmod 0600 "$repo/.env"
restorecon -v "$system_uv" "$system_unit"

systemd-analyze verify "$system_unit"
systemctl daemon-reload
systemctl enable panetone.service
systemctl restart panetone.service
systemctl is-active --quiet panetone.service

trap - EXIT HUP INT TERM
handoff_started=0

sha256sum "$repo/panetone" "/home/$service_user/.local/bin/panetone"
systemctl status panetone.service --no-pager
