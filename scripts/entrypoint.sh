#!/bin/bash
set -x

echo "Running custom pre jupyter stack start.sh steps..."

# =============================================================================
# Credential resolution — needed for both s3fs and boto3/papermill
# =============================================================================
# GET /credentials is idempotent (returns cached creds from DB, no rotation).
# Always fetch from governance API; fall back to env vars if API unavailable.

if [ -n "${GOVERNANCE_API_URL:-}" ] && [ -n "${KBASE_AUTH_TOKEN:-}" ]; then
    # Fetch per-user credentials from governance API (idempotent — returns cached).
    # Retry up to 5 times — the service may still be starting.
    echo "Fetching MinIO credentials from governance API..."
    CRED_FETCHED=false
    for attempt in 1 2 3 4 5; do
        CRED_RESPONSE=$(curl -sL -w "\n%{http_code}" \
            -H "Authorization: Bearer ${KBASE_AUTH_TOKEN}" \
            "${GOVERNANCE_API_URL}/credentials" 2>/dev/null)
        HTTP_CODE=$(echo "$CRED_RESPONSE" | tail -1)
        BODY=$(echo "$CRED_RESPONSE" | sed '$d')
        if [ "$HTTP_CODE" = "200" ] && [ -n "$BODY" ]; then
            export AWS_ACCESS_KEY_ID=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_key',''))")
            export AWS_SECRET_ACCESS_KEY=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('secret_key',''))")
            echo "Loaded credentials from governance API"
            CRED_FETCHED=true
            break
        fi
        echo "  Attempt $attempt: HTTP $HTTP_CODE — retrying in 3s..." >&2
        sleep 3
    done
    if [ "$CRED_FETCHED" = "false" ]; then
        echo "WARNING: Governance API credential fetch failed (HTTP $HTTP_CODE)" >&2
        export AWS_ACCESS_KEY_ID="${MINIO_ACCESS_KEY:-}"
        export AWS_SECRET_ACCESS_KEY="${MINIO_SECRET_KEY:-}"
    fi
else
    export AWS_ACCESS_KEY_ID="${MINIO_ACCESS_KEY:-}"
    export AWS_SECRET_ACCESS_KEY="${MINIO_SECRET_KEY:-}"
fi

# Ensure MINIO_ENDPOINT_URL has http:// prefix
MINIO_URL="${MINIO_ENDPOINT_URL}"
if [[ ! "$MINIO_URL" =~ ^https?:// ]]; then
    MINIO_URL="http://${MINIO_URL}"
fi

# Set S3 endpoint for boto3/fsspec/papermill
# AWS_ENDPOINT_URL_S3 is the official boto3 1.28+ env var for custom S3 endpoints
export AWS_ENDPOINT_URL_S3="${MINIO_URL}"
export AWS_ENDPOINT_URL="${MINIO_URL}"
export FSSPEC_S3_ENDPOINT_URL="${MINIO_URL}"
export AWS_DEFAULT_REGION="us-east-1"

# =============================================================================
# S3 FUSE Mounts — mount MinIO paths as POSIX directories
# =============================================================================
# Layout:
#   /home/{user}/              <- s3fs -> cdm-lake/users-general-warehouse/{user}/workspace  (persistent)
#   /home/{user}/global-share/ <- s3fs -> cdm-lake/tenant-general-warehouse/globalusers/workspace (persistent)

S3FS_BUCKET="cdm-lake"
S3FS_USER_PREFIX="users-general-warehouse/${NB_USER}/workspace"
S3FS_GLOBAL_PREFIX="tenant-general-warehouse/globalusers/workspace"

# Determine SSL setting for s3fs based on MINIO_SECURE
S3FS_SSL_OPTS=""
if [[ "${MINIO_SECURE:-True}" != "True" ]]; then
    S3FS_SSL_OPTS="-o no_check_certificate"
fi

mount_s3fs() {
    local bucket_prefix="$1"
    local mountpoint="$2"
    local extra_opts="${3:-}"

    mkdir -p "${mountpoint}"

    # Resolve NB_USER uid/gid (user may not exist yet — start.sh creates it later)
    local mount_uid=$(id -u "${NB_USER}" 2>/dev/null || echo 1000)
    local mount_gid=$(id -g "${NB_USER}" 2>/dev/null || echo 100)

    local s3fs_opts="passwd_file=/tmp/.s3fs-passwd"
    s3fs_opts="${s3fs_opts},url=${MINIO_URL}"
    s3fs_opts="${s3fs_opts},use_path_request_style"
    s3fs_opts="${s3fs_opts},allow_other"
    s3fs_opts="${s3fs_opts},uid=${mount_uid}"
    s3fs_opts="${s3fs_opts},gid=${mount_gid}"
    s3fs_opts="${s3fs_opts},umask=0077"
    s3fs_opts="${s3fs_opts},endpoint=us-east-1"
    # Performance tuning: in-memory stat caching
    s3fs_opts="${s3fs_opts},max_stat_cache_size=10000"
    s3fs_opts="${s3fs_opts},stat_cache_expire=60"
    s3fs_opts="${s3fs_opts},parallel_count=10"
    s3fs_opts="${s3fs_opts},multipart_size=64"
    # compat_dir: recognize directory prefixes without explicit dir marker objects (needed for MinIO)
    s3fs_opts="${s3fs_opts},compat_dir"

    echo "Mounting s3fs: ${bucket_prefix} -> ${mountpoint}"
    if s3fs "${bucket_prefix}" "${mountpoint}" -o "${s3fs_opts}" ${S3FS_SSL_OPTS} ${extra_opts}; then
        echo "Successfully mounted ${mountpoint}"
    else
        echo "ERROR: Failed to mount ${mountpoint} — falling back to empty directory" >&2
    fi
}

if [ -n "${AWS_ACCESS_KEY_ID}" ] && [ -n "${AWS_SECRET_ACCESS_KEY}" ]; then
    # Create s3fs password file
    echo "${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}" > /tmp/.s3fs-passwd
    chmod 600 /tmp/.s3fs-passwd

    # Ensure workspace/ prefix directories exist in MinIO (s3fs needs explicit markers)
    python3 -c "
import boto3
s3 = boto3.client('s3', endpoint_url='${MINIO_URL}',
    aws_access_key_id='${AWS_ACCESS_KEY_ID}', aws_secret_access_key='${AWS_SECRET_ACCESS_KEY}',
    region_name='us-east-1')
for prefix in ['${S3FS_USER_PREFIX}/', '${S3FS_GLOBAL_PREFIX}/']:
    try:
        s3.put_object(Bucket='${S3FS_BUCKET}', Key=prefix, Body=b'')
    except Exception:
        pass
" 2>/dev/null || true

    # Mount user home directory
    mount_s3fs "${S3FS_BUCKET}:/${S3FS_USER_PREFIX}" "/home/${NB_USER}"

    # Mount global shared directory (inside home so it appears in file browser)
    mount_s3fs "${S3FS_BUCKET}:/${S3FS_GLOBAL_PREFIX}" "/home/${NB_USER}/global-share"
else
    echo "WARNING: No MinIO credentials available — skipping S3 FUSE mounts" >&2
    # Without FUSE, /home/{user}/ is a plain directory created by root.
    # Ensure the notebook user can write to it.
    mkdir -p "/home/${NB_USER}"
    chown -R "${NB_USER}":users "/home/${NB_USER}" 2>/dev/null || true
fi


exec tini -g -- /usr/local/bin/start.sh "$@"
