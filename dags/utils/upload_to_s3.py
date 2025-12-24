import os
import logging
from typing import Any

from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

logger = logging.getLogger(__name__)


def upload_to_s3(
    *,
    task_id: str,
    bucket: str,
    local_path: str,
    remote_prefix: str,
    aws_conn_id: str = AwsBaseHook.default_conn_name,
) -> Any:
    """
    Uploads all JSON files from a local directory to an S3 bucket.
    """

    @task(task_id=task_id)
    def upload_to_s3(
        *,
        bucket: str,
        local_path: str,
        remote_prefix: str,
        aws_conn_id: str = aws_conn_id,
    ) -> None:
        s3_hook = S3Hook(aws_conn_id=aws_conn_id, verify=None)
        for filename in os.listdir(local_path):
            if filename.endswith(".json"):
                local_file = os.path.join(local_path, filename)
                remote_key = f"{remote_prefix}/{filename}"
                logger.info(f"Uploading local file {local_file} to s3://{bucket}/{remote_key}")
                s3_hook.load_file(
                    filename=local_file,
                    key=remote_key,
                    bucket_name=bucket,
                    replace=True,
                )
                logger.info(f"Deleting local file {local_file}")
                os.remove(local_file)

    return upload_to_s3(
        bucket=bucket,
        local_path=local_path,
        remote_prefix=remote_prefix,
        aws_conn_id=aws_conn_id,
    )
