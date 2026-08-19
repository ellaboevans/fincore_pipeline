#!/bin/sh
# ============================================================
# Idempotent MinIO bootstrap for FinCore.
#
# Creates the raw and processed buckets, and optionally uploads a
# generated source partition for a given run date.
#
# Designed to run inside the minio/mc image, which is where mc and
# the cluster network are available:
#
#   docker compose run --rm \
#     -v "$PWD/scripts:/scripts:ro" \
#     -v "$PWD/data/generated:/data-import:ro" \
#     --entrypoint /bin/sh minio-init /scripts/init-minio.sh 2026-08-03
#
# Omit the run date to create the buckets only.
#
# Required environment (supplied by .env via compose):
#   MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
# Optional:
#   MINIO_ENDPOINT  (default http://minio:9000)
#   RAW_BUCKET      (default fincore-raw)
#   PROCESSED_BUCKET(default fincore-processed)
#   IMPORT_DIR      (default /data-import)
# ============================================================

set -eu

RUN_DATE="${1:-}"

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
RAW_BUCKET="${RAW_BUCKET:-fincore-raw}"
PROCESSED_BUCKET="${PROCESSED_BUCKET:-fincore-processed}"
IMPORT_DIR="${IMPORT_DIR:-/data-import}"

if [ -z "${MINIO_ROOT_USER:-}" ] || [ -z "${MINIO_ROOT_PASSWORD:-}" ]; then
    echo "ERROR: MINIO_ROOT_USER and MINIO_ROOT_PASSWORD must be set." >&2
    exit 1
fi

echo "Configuring mc alias for ${MINIO_ENDPOINT}"
mc alias set local "${MINIO_ENDPOINT}" \
    "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null

echo "Ensuring buckets exist"
mc mb --ignore-existing "local/${RAW_BUCKET}"
mc mb --ignore-existing "local/${PROCESSED_BUCKET}"

if [ -z "${RUN_DATE}" ]; then
    echo "Buckets ready. No run date supplied; skipping upload."
    exit 0
fi

# Reject anything that is not YYYY-MM-DD before it becomes a prefix.
if ! echo "${RUN_DATE}" | grep -Eq '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'; then
    echo "ERROR: run date '${RUN_DATE}' must be YYYY-MM-DD." >&2
    exit 1
fi

if [ ! -d "${IMPORT_DIR}" ]; then
    echo "ERROR: import directory ${IMPORT_DIR} is not mounted." >&2
    exit 1
fi

PARTITION="dt=${RUN_DATE}"

for SOURCE in trades market_data portfolio; do
    LOCAL_PATH="${IMPORT_DIR}/${SOURCE}/${PARTITION}"

    if [ ! -d "${LOCAL_PATH}" ]; then
        echo "ERROR: no generated data at ${LOCAL_PATH}." >&2
        echo "Run data-generator/generate_sample_data.py first." >&2
        exit 1
    fi

    echo "Uploading ${SOURCE} ${PARTITION}"
    mc cp --recursive \
        "${LOCAL_PATH}/" \
        "local/${RAW_BUCKET}/${SOURCE}/${PARTITION}/"
done

echo "MinIO initialised for ${RUN_DATE}."
