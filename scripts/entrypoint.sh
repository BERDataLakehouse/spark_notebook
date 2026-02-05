#!/bin/bash
set -x

echo "Running custom pre jupyter stack start.sh steps..."

# Map MinIO credentials to AWS-compatible env vars for boto3/papermill
# Read from user's credential file if available
CRED_FILE="/home/${NB_USER}/.berdl_minio_credentials"
if [ -f "$CRED_FILE" ]; then
    export AWS_ACCESS_KEY_ID=$(cat "$CRED_FILE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_key',''))")
    export AWS_SECRET_ACCESS_KEY=$(cat "$CRED_FILE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('secret_key',''))")
    echo "Loaded AWS credentials from $CRED_FILE"
else
    # Fallback to MINIO env vars if set
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