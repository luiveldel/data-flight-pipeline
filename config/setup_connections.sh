#!/usr/bin/env bash
# Setup Airflow connections for MinIO (S3-compatible).
# Runs during airflow-init. Uses env vars: MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT

set -e

EXTRA=$(python3 -c "
import json, os
print(json.dumps({
    'endpoint_url': os.environ.get('MINIO_ENDPOINT', 'http://minio:9000'),
    'aws_access_key_id': os.environ.get('MINIO_ACCESS_KEY', 'minioadmin'),
    'aws_secret_access_key': os.environ.get('MINIO_SECRET_KEY', 'minioadmin'),
}))
")

airflow connections delete aws_default 2>/dev/null || true
airflow connections add aws_default --conn-type aws --conn-extra "$EXTRA"
echo "Created aws_default connection for MinIO"
