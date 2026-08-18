#!/bin/bash

set -euo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
binary=$repo/target/release/panetone
database=
environment=
apply=0
start_held=0
system_unit=/etc/systemd/system/panetone.service
system_binary=/usr/local/bin/panetone
system_environment=/etc/panetone/panetone.env
system_database=/var/lib/panetone/panetone.sqlite3

usage() {
    printf '%s\n' \
        "Usage: deploy/install-system-service.sh [OPTIONS]" \
        "" \
        "Options:" \
        "  --binary PATH       Exact release candidate" \
        "  --database PATH     Migrated candidate database" \
        "  --environment PATH  Credential environment file" \
        "  --apply             Install but do not start" \
        "  --start-held        Install and verify a held start" \
        "" \
        "Without --apply or --start-held this performs a side-effect-free check."
}

while (($#)); do
    case $1 in
        --binary)
            binary=$2
            shift 2
            ;;
        --database)
            database=$2
            shift 2
            ;;
        --environment)
            environment=$2
            shift 2
            ;;
        --apply)
            apply=1
            shift
            ;;
        --start-held)
            apply=1
            start_held=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            printf 'unknown argument: %s\n' "$1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

test -x "$binary"
test -f "$repo/deploy/panetone.service"
"$binary" --version
verify_root=$(mktemp -d)
trap 'rm -rf -- "$verify_root"' EXIT
install -d \
    "$verify_root/etc/systemd/system" \
    "$verify_root/etc/panetone" \
    "$verify_root/usr/local/bin" \
    "$verify_root/var/lib/panetone"
install -m 0644 "$repo/deploy/panetone.service" \
    "$verify_root/etc/systemd/system/panetone.service"
install -m 0755 "$binary" "$verify_root/usr/local/bin/panetone"
install -m 0755 /bin/true "$verify_root/usr/local/bin/wakterm"
printf '%s\n' '[Service]' 'ExecStart=/usr/local/bin/wakterm' > \
    "$verify_root/etc/systemd/system/wakterm-mux-server.service"
for target in sysinit.target basic.target shutdown.target network-online.target multi-user.target; do
    printf '%s\n' '[Unit]' 'DefaultDependencies=no' > \
        "$verify_root/etc/systemd/system/$target"
done
: > "$verify_root/etc/panetone/panetone.env"
systemd-analyze verify --root="$verify_root" panetone.service
rm -rf -- "$verify_root"
trap - EXIT

if [[ -n $database ]]; then
    test -f "$database"
    test ! -L "$database"
fi
if [[ -n $environment ]]; then
    test -f "$environment"
    test ! -L "$environment"
    environment_mode=$(stat -c '%a' "$environment")
    if [[ $environment_mode != 600 ]]; then
        printf 'environment file must have mode 0600, found %s\n' "$environment_mode" >&2
        exit 1
    fi
fi

if ((apply == 0)); then
    sha256sum "$binary" "$repo/deploy/panetone.service"
    printf 'candidate check passed; no files or services were changed\n'
    exit 0
fi

if ((EUID != 0)); then
    printf 'apply requires sudo: sudo /bin/bash %s/deploy/install-system-service.sh ...\n' "$repo" >&2
    exit 2
fi
if [[ -z $database || -z $environment ]]; then
    printf '%s\n' '--database and --environment are required for apply' >&2
    exit 2
fi
if systemctl --user --machine=mihai@ is-active --quiet panetone.service 2>/dev/null; then
    printf '%s\n' 'the user Panetone service is still active; stop it at the labeled cutover boundary' >&2
    exit 1
fi
if ! systemctl is-active --quiet wakterm-mux-server.service; then
    printf '%s\n' 'the system Wakterm mux service must be active under the same manager' >&2
    exit 1
fi
test -S /run/user/1000/wakterm/sock
if [[ -e $system_database ]]; then
    printf '%s\n' "$system_database already exists; refusing to overwrite durable state" >&2
    exit 1
fi
if [[ -e /etc/panetone || -e /var/lib/panetone || -e /usr/share/doc/panetone ]]; then
    printf '%s\n' 'candidate configuration or state directory already exists; reconcile it explicitly before install' >&2
    exit 1
fi

backup=$(mktemp -d /var/tmp/panetone-install.XXXXXX)
chmod 0700 "$backup"
installed=0

rollback_install() {
    if ((installed == 0)); then
        return
    fi
    systemctl disable --now panetone.service >/dev/null 2>&1 || true
    if [[ -f $backup/panetone ]]; then
        install -o root -g root -m 0755 "$backup/panetone" "$system_binary"
    else
        rm -f -- "$system_binary"
    fi
    if [[ -f $backup/panetone.service ]]; then
        install -o root -g root -m 0644 "$backup/panetone.service" "$system_unit"
    else
        rm -f -- "$system_unit"
    fi
    rm -rf -- /etc/panetone /var/lib/panetone /run/panetone /usr/share/doc/panetone
    systemctl daemon-reload
    printf '%s\n' 'held candidate installation failed and was removed before promotion effects' >&2
}

trap rollback_install EXIT HUP INT TERM
[[ ! -f $system_binary ]] || cp -a -- "$system_binary" "$backup/panetone"
[[ ! -f $system_unit ]] || cp -a -- "$system_unit" "$backup/panetone.service"

install -d -o root -g root -m 0755 /etc/panetone
install -d -o root -g root -m 0755 /usr/share/doc/panetone
install -d -o mihai -g mihai -m 0700 /var/lib/panetone
install -o root -g root -m 0755 "$binary" "$system_binary"
install -o root -g root -m 0644 "$repo/deploy/panetone.service" "$system_unit"
install -o root -g root -m 0644 "$repo/docs/production-operations.md" \
    /usr/share/doc/panetone/production-operations.md
install -o root -g root -m 0600 "$environment" "$system_environment"
install -o mihai -g mihai -m 0600 "$database" "$system_database"
restorecon -F "$system_binary" "$system_unit" "$system_environment" "$system_database"
systemctl daemon-reload
systemctl enable panetone.service
installed=1

commit=$(git -C "$repo" rev-parse HEAD)
binary_hash=$(sha256sum "$system_binary" | cut -d' ' -f1)
unit_hash=$(sha256sum "$system_unit" | cut -d' ' -f1)
wakterm_version=$(/usr/local/bin/wakterm --version)
python3 - "$binary_hash" "$unit_hash" "$commit" "$wakterm_version" > /var/lib/panetone/install-evidence.json <<'PY'
import json
import sys
print(json.dumps({
    "binary_sha256": sys.argv[1],
    "unit_sha256": sys.argv[2],
    "candidate_commit": sys.argv[3],
    "wakterm_version": sys.argv[4],
    "started": False,
}, sort_keys=True))
PY
chown mihai:mihai /var/lib/panetone/install-evidence.json
chmod 0600 /var/lib/panetone/install-evidence.json

if ((start_held == 1)); then
    systemctl start panetone.service
    for _attempt in {1..100}; do
        [[ -S /run/panetone/control.sock ]] && break
        sleep 0.05
    done
    test -S /run/panetone/control.sock
    status=$(runuser --user mihai -- "$system_binary" status \
        --socket /run/panetone/control.sock --json)
    python3 -c 'import json,sys; data=json.load(sys.stdin); assert data["result"]["store"]["promotion"]["delivery_hold"] is True' <<<"$status"
    python3 - "$binary_hash" "$unit_hash" "$commit" "$wakterm_version" > /var/lib/panetone/install-evidence.json <<'PY'
import json
import sys
print(json.dumps({
    "binary_sha256": sys.argv[1],
    "unit_sha256": sys.argv[2],
    "candidate_commit": sys.argv[3],
    "wakterm_version": sys.argv[4],
    "started": True,
    "delivery_hold": True,
}, sort_keys=True))
PY
    chown mihai:mihai /var/lib/panetone/install-evidence.json
    chmod 0600 /var/lib/panetone/install-evidence.json
fi

trap - EXIT HUP INT TERM
installed=0
rm -rf -- "$backup"
sha256sum "$system_binary" "$system_unit" "$system_database"
if ((start_held == 1)); then
    systemctl status panetone.service --no-pager
else
    printf '%s\n' 'candidate installed but not started; delivery remains impossible until the explicit held start'
fi
