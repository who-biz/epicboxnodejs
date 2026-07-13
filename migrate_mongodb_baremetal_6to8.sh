#!/usr/bin/env bash
#
# migrate_mongodb_baremetal_6to_8.sh
#
# Bare-metal equivalent of migrate-mongodb.sh: steps a manually installed
# (apt/dnf package) MongoDB standalone from 6.0 -> 7.0 -> 8.0 in place.
# MongoDB does not allow skipping a major version, so this script upgrades
# the installed packages one series at a time and bumps
# featureCompatibilityVersion (FCV) at each step.
#
# SCOPE / ASSUMPTIONS - read before running:
#   * Single-node standalone mongod managed by systemd (service "mongod"),
#     installed from the official mongodb-org packages on Debian/Ubuntu
#     (apt) or RHEL/Rocky/Alma (dnf/yum). Replica sets and sharded
#     clusters need MongoDB's documented rolling procedure instead.
#   * Run as root. The script stops mongod for the backup, so plan a
#     maintenance window.
#   * If mongod.conf has `security.authorization: enabled`, export
#     MONGO_ROOT_USER and MONGO_ROOT_PASSWORD before running (the FCV
#     command requires an admin user).
#
# Usage:
#   sudo MONGO_ROOT_USER=root MONGO_ROOT_PASSWORD=... ./migrate-mongodb-baremetal.sh
#   sudo ./migrate-mongodb-baremetal.sh --no-backup     # skip the tar backup
#
# Idempotent: probes the current FCV and performs only the steps still
# needed. Safe to re-run after a partial failure.

set -euo pipefail

BACKUP_DIR="/var/backups/mongodb-migration"
SERVICE="mongod"
DO_BACKUP=1
[[ "${1:-}" == "--no-backup" ]] && DO_BACKUP=0

log()  { printf '\n==> %s\n' "$*"; }
die()  { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

[[ "$(id -u)" == "0" ]] || die "run as root (sudo)."
command -v mongod  >/dev/null || die "mongod not found - is MongoDB installed from packages?"
command -v mongosh >/dev/null || die "mongosh not found - install the mongodb-mongosh package."

# ------------------------------------------------------------ environment
# Detect package manager family.
if command -v apt-get >/dev/null; then
  PKG="apt"
elif command -v dnf >/dev/null; then
  PKG="dnf"
elif command -v yum >/dev/null; then
  PKG="yum"
else
  die "no supported package manager (apt/dnf/yum) found."
fi

# dbPath from mongod.conf (YAML "dbPath:" line), with the usual defaults.
CONF="/etc/mongod.conf"
DBPATH="$(awk '$1=="dbPath:"{print $2}' "$CONF" 2>/dev/null || true)"
if [[ -z "$DBPATH" ]]; then
  [[ -d /var/lib/mongodb ]] && DBPATH=/var/lib/mongodb || DBPATH=/var/lib/mongo
fi
[[ -d "$DBPATH" ]] || die "dbPath '$DBPATH' not found - set it in $CONF or edit this script."

# mongosh auth arguments, only if credentials were provided.
MONGOSH_ARGS=(--quiet)
if [[ -n "${MONGO_ROOT_USER:-}" ]]; then
  MONGOSH_ARGS+=( -u "$MONGO_ROOT_USER" -p "$MONGO_ROOT_PASSWORD" --authenticationDatabase admin )
fi

msh() { mongosh "${MONGOSH_ARGS[@]}" --eval "$1"; }

wait_ready() {
  local i
  for i in $(seq 1 60); do
    if msh "db.adminCommand('ping').ok" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  journalctl -u "$SERVICE" -n 40 --no-pager || true
  die "mongod did not become ready."
}

get_fcv() {
  msh "db.adminCommand({getParameter:1, featureCompatibilityVersion:1}).featureCompatibilityVersion.version"
}

set_fcv() {
  log "Setting featureCompatibilityVersion to $1"
  msh "assert.commandWorked(db.adminCommand({setFeatureCompatibilityVersion:'$1', confirm:true}))" >/dev/null
}

installed_series() {
  mongod --version | awk -F'[ v.]+' '/db version/{print $3"."$4}'
}

# ---------------------------------------------------- package repo + install
# Point the official MongoDB repository at the given series (7.0 / 8.0)
# and upgrade the mongodb-org packages to it.
install_series() {
  local series="$1"
  log "Installing mongodb-org $series packages"

  if [[ "$PKG" == "apt" ]]; then
    # Repo definition per MongoDB's install docs. Adjust the distro
    # codename below if lsb_release is unavailable on your system.
    local codename arch keyfile listfile
    codename="$(lsb_release -cs 2>/dev/null || . /etc/os-release && echo "${VERSION_CODENAME}")"
    arch="$(dpkg --print-architecture)"
    keyfile="/usr/share/keyrings/mongodb-server-${series}.gpg"
    listfile="/etc/apt/sources.list.d/mongodb-org-${series}.list"

    curl -fsSL "https://www.mongodb.org/static/pgp/server-${series}.asc" \
      | gpg --dearmor -o "$keyfile"

    if [[ -f /etc/debian_version && ! -f /etc/lsb-release ]]; then
      echo "deb [signed-by=${keyfile} arch=${arch}] https://repo.mongodb.org/apt/debian ${codename}/mongodb-org/${series} main" > "$listfile"
    else
      echo "deb [signed-by=${keyfile} arch=${arch}] https://repo.mongodb.org/apt/ubuntu ${codename}/mongodb-org/${series} multiverse" > "$listfile"
    fi

    # Remove older-series repo files so apt resolves to this series only.
    find /etc/apt/sources.list.d -name 'mongodb-org-*.list' ! -name "mongodb-org-${series}.list" -delete
    apt-get update -qq
    apt-get install -y --allow-downgrades mongodb-org
  else
    local repofile="/etc/yum.repos.d/mongodb-org-${series}.repo"
    cat > "$repofile" <<EOF
[mongodb-org-${series}]
name=MongoDB Repository
baseurl=https://repo.mongodb.org/yum/redhat/\$releasever/mongodb-org/${series}/x86_64/
gpgcheck=1
enabled=1
gpgkey=https://www.mongodb.org/static/pgp/server-${series}.asc
EOF
    find /etc/yum.repos.d -name 'mongodb-org-*.repo' ! -name "mongodb-org-${series}.repo" -delete
    "$PKG" install -y mongodb-org
  fi

  systemctl daemon-reload
}

step_to() {
  local series="$1"
  if [[ "$(installed_series)" != "$series" ]]; then
    systemctl stop "$SERVICE"
    install_series "$series"
  fi
  systemctl start "$SERVICE"
  wait_ready
  set_fcv "$series"
}

# ---------------------------------------------------------------- backup
log "Stopping $SERVICE for a consistent snapshot"
systemctl stop "$SERVICE" || true

if [[ "$DO_BACKUP" == "1" ]]; then
  mkdir -p "$BACKUP_DIR"
  STAMP="$(date +%Y%m%d-%H%M%S)"
  log "Backing up $DBPATH -> $BACKUP_DIR/dbpath-$STAMP.tar.gz"
  tar czf "$BACKUP_DIR/dbpath-$STAMP.tar.gz" -C "$DBPATH" .
fi

# -------------------------------------------------------------- migration
log "Starting $SERVICE to probe current state"
systemctl start "$SERVICE"
wait_ready

FCV="$(get_fcv)"
BIN="$(installed_series)"
log "Installed mongod series: $BIN  |  data FCV: $FCV"

case "$FCV" in
  6.0)
    step_to "7.0"
    step_to "8.0"
    ;;
  7.0)
    step_to "8.0"
    ;;
  8.0)
    log "FCV is already 8.0."
    # Still make sure the binary matches the data.
    [[ "$BIN" == "8.0" ]] || step_to "8.0"
    ;;
  *)
    die "unexpected FCV '$FCV'. If this is 5.0 or older, step through the intermediate majors first."
    ;;
esac

# ------------------------------------------------------------------ verify
wait_ready
FCV="$(get_fcv)"
BIN="$(installed_series)"
log "Final state - mongod series: $BIN | FCV: $FCV"
[[ "$FCV" == "8.0" && "$BIN" == "8.0" ]] || die "verification failed; investigate before use."

log "Done. mongod 8.0 is running with FCV 8.0."
log "Optional: pin the packages so a routine 'apt upgrade' cannot jump majors:"
if [[ "$PKG" == "apt" ]]; then
  echo "    apt-mark hold mongodb-org mongodb-org-server mongodb-org-mongos mongodb-org-database mongodb-org-tools"
else
  echo "    echo 'exclude=mongodb-org*' >> /etc/yum.conf   # or use dnf versionlock"
fi
