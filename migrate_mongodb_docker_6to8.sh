#!/usr/bin/env bash
#
# migrate-mongodb.sh
#
# Single-run migration of the compose mongo_data volume from
# MongoDB 6.0 to 7.0 (single step) to 8.0. MongoDB does not allow skipping
# a major version, so this script boots intermediate vers against the same
# data volume just long enough to bump featureCompatibilityVersion (FCV),
#
# Usage:
#   ./docker_migrate_mongodb_6to8.sh     # migrate, with a tar backup first
#   ./docker_migrate_mongodb_6to8.sh --no-backup
#
# Run script from project directory (where docker-compose.yml lives)
# so the volume name properly resolves. Version checking makes this safe
# for re-runs.

set -euo pipefail

# settings

TARGET_IMAGE_70="mongo:7.0"
TARGET_IMAGE_80="mongo:8.0.26"
TMP_CONTAINER="mongo-fcv-migrate"
BACKUP_DIR="./mongo-backups"

PROJECT="${COMPOSE_PROJECT_NAME:-$(basename "$PWD" | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9_-')}"
VOLUME="${MONGO_VOLUME:-${PROJECT}_mongo_data}"

DO_BACKUP=1
[[ "${1:-}" == "--no-backup" ]] && DO_BACKUP=0

log() { printf '\n==> %s\n' "$*"; }

cleanup() { docker rm -f "$TMP_CONTAINER" >/dev/null 2>&1 || true; }
trap cleanup EXIT

# preflight

if ! docker volume inspect "$VOLUME" >/dev/null 2>&1; then
  echo "ERROR: volume '$VOLUME' not found."
  echo "Set MONGO_VOLUME=<name> or COMPOSE_PROJECT_NAME and re-run."
  echo "Existing volumes:"; docker volume ls --format '  {{.Name}}'
  exit 1
fi

log "Stopping the compose stack (mongod must not be running during migration)"
docker compose down

if [[ "$DO_BACKUP" == "1" ]]; then
  mkdir -p "$BACKUP_DIR"
  STAMP="$(date +%Y%m%d-%H%M%S)"
  log "Backing up volume '$VOLUME' -> $BACKUP_DIR/mongo_data-$STAMP.tar.gz"
  docker run --rm -v "$VOLUME":/data:ro -v "$(cd "$BACKUP_DIR" && pwd)":/backup \
    alpine tar czf "/backup/mongo_data-$STAMP.tar.gz" -C /data .
fi

# helpers

start_tmp() {
  local image="$1"
  docker run -d --name "$TMP_CONTAINER" -v "$VOLUME":/data/db "$image" >/dev/null
  local i
  for i in $(seq 1 60); do
    if docker exec "$TMP_CONTAINER" mongosh --quiet --eval "db.adminCommand('ping').ok" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "ERROR: mongod ($image) did not become ready. Logs:"
  docker logs "$TMP_CONTAINER" | tail -n 40
  exit 1
}

stop_tmp() {
  docker stop "$TMP_CONTAINER" >/dev/null
  docker rm "$TMP_CONTAINER" >/dev/null
}

get_fcv() {
  docker exec "$TMP_CONTAINER" mongosh --quiet --eval \
    "db.adminCommand({getParameter:1, featureCompatibilityVersion:1}).featureCompatibilityVersion.version"
}

set_fcv() {
  local ver="$1"
  docker exec "$TMP_CONTAINER" mongosh --quiet --eval \
    "assert.commandWorked(db.adminCommand({setFeatureCompatibilityVersion:'$ver', confirm:true}))" >/dev/null
}

# migration and version probe
log "Probing current featureCompatibilityVersion"
start_tmp "$TARGET_IMAGE_70"
FCV="$(get_fcv)"
log "Current FCV: $FCV"

case "$FCV" in
  6.0)
    log "Step 1/2: bumping FCV 6.0 -> 7.0 (on $TARGET_IMAGE_70)"
    set_fcv "7.0"
    stop_tmp
    log "Step 2/2: bumping FCV 7.0 -> 8.0 (on $TARGET_IMAGE_80)"
    start_tmp "$TARGET_IMAGE_80"
    set_fcv "8.0"
    stop_tmp
    ;;
  7.0)
    stop_tmp
    log "Already at FCV 7.0 - bumping to 8.0 (on $TARGET_IMAGE_80)"
    start_tmp "$TARGET_IMAGE_80"
    set_fcv "8.0"
    stop_tmp
    ;;
  8.0)
    stop_tmp
    log "FCV is already 8.0 - nothing to do."
    ;;
  *)
    stop_tmp
    echo "ERROR: unexpected FCV '$FCV'."
    echo "If this is 5.0 or older, step through the intermediate majors first."
    exit 1
    ;;
esac

log "Migration complete. Verifying under $TARGET_IMAGE_80"
start_tmp "$TARGET_IMAGE_80"
FCV="$(get_fcv)"
stop_tmp
log "Final FCV: $FCV"

if [[ "$FCV" != "8.0" ]]; then
  echo "ERROR: expected FCV 8.0, got '$FCV'. Do not start the stack; investigate."
  exit 1
fi

log "Done. Start the stack with: docker compose up -d --build"
