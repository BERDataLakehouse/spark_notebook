#!/bin/bash
set -x

echo "Running custom pre jupyter stack start.sh steps..."

# Fetch MinIO credentials from governance API (MMS) and map to AWS-compatible env vars
GOVERNANCE_URL="${GOVERNANCE_API_URL:-}"
AUTH_TOKEN="${KBASE_AUTH_TOKEN:-}"

if [ -n "$GOVERNANCE_URL" ] && [ -n "$AUTH_TOKEN" ]; then
    CRED_JSON=$(curl -sf -m 10 \
        -H "Authorization: Bearer ${AUTH_TOKEN}" \
        "${GOVERNANCE_URL%/}/credentials/" 2>/dev/null)
    if [ $? -eq 0 ] && [ -n "$CRED_JSON" ]; then
        # The unified /credentials/ endpoint returns the S3 IAM half under
        # ``s3_access_key`` / ``s3_secret_key`` (the legacy ``access_key`` /
        # ``secret_key`` fields were renamed in the Polaris/S3 refactor).
        # Read the new keys first; fall back to the legacy keys so this
        # script still works against an older MMS that hasn't been bumped
        # yet.  Empty strings stay empty after the fallback.
        export AWS_ACCESS_KEY_ID=$(echo "$CRED_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('s3_access_key') or d.get('access_key',''))")
        export AWS_SECRET_ACCESS_KEY=$(echo "$CRED_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('s3_secret_key') or d.get('secret_key',''))")
        echo "Loaded AWS credentials from governance API"
    else
        echo "WARNING: Failed to fetch credentials from governance API, falling back to env vars"
        export AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY:-}"
        export AWS_SECRET_ACCESS_KEY="${S3_SECRET_KEY:-}"
    fi
else
    # Fallback to S3_* env vars if governance API URL or auth token not set
    export AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY:-}"
    export AWS_SECRET_ACCESS_KEY="${S3_SECRET_KEY:-}"
fi

# Ensure S3_ENDPOINT_URL has a scheme prefix.  Choose http:// vs https://
# based on the same S3_SECURE flag BERDLSettings reads — the previous code
# always defaulted to http://, which silently misconfigured TLS-required
# clusters (e.g. stage / prod) and was inconsistent with how
# setup_trino_session._build_catalog_properties picks the scheme.
MINIO_URL="${S3_ENDPOINT_URL}"
if [[ ! "$MINIO_URL" =~ ^https?:// ]]; then
    case "${S3_SECURE:-false}" in
        true|True|TRUE|1|yes|Yes|YES) SCHEME="https" ;;
        *)                            SCHEME="http"  ;;
    esac
    MINIO_URL="${SCHEME}://${MINIO_URL}"
fi

# Set S3 endpoint for boto3/fsspec/papermill
# AWS_ENDPOINT_URL_S3 is the official boto3 1.28+ env var for custom S3 endpoints
export AWS_ENDPOINT_URL_S3="${MINIO_URL}"
export AWS_ENDPOINT_URL="${MINIO_URL}"
export FSSPEC_S3_ENDPOINT_URL="${MINIO_URL}"
export AWS_DEFAULT_REGION="us-east-1"

exec tini -g -- /usr/local/bin/start.sh "$@"