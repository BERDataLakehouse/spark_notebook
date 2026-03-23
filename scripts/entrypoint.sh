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
        export AWS_ACCESS_KEY_ID=$(echo "$CRED_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_key',''))")
        export AWS_SECRET_ACCESS_KEY=$(echo "$CRED_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('secret_key',''))")
        echo "Loaded AWS credentials from governance API"
    else
        echo "WARNING: Failed to fetch credentials from governance API, falling back to env vars"
        export AWS_ACCESS_KEY_ID="${MINIO_ACCESS_KEY:-}"
        export AWS_SECRET_ACCESS_KEY="${MINIO_SECRET_KEY:-}"
    fi
else
    # Fallback to MINIO env vars if governance API URL or auth token not set
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

exec tini -g -- /usr/local/bin/start.sh "$@"