import os
import logging
from model import Quote, DBWriteResult
from mypy_boto3_dynamodb.service_resource import Table
from botocore.exceptions import ClientError
from typing import List, Dict, Any, Tuple
import tempfile
from mypy_boto3_s3 import S3Client


logger = logging.getLogger(__name__)

BATCH_SIZE = 25


def parse_s3_event(event: Dict[str, Any]) -> Tuple[str, str]:
    """
    Parses the S3 event to extract the bucket name and object key.
    """
    try:
        record = event["Records"][0]
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]
        logger.info(f"Parsed S3 event: bucket={bucket_name}, key={object_key}")
        return bucket_name, object_key
    except (KeyError, IndexError) as e:
        logger.error(f"Error parsing S3 event: {e}", exc_info=True)
        raise ValueError(f"Error parsing event: {e}")


def download_file_from_s3(
    s3_client: S3Client, bucket_name: str, object_key: str
) -> str:
    """
    Downloads a file from S3 to a temporary file and returns the path.
    """
    with tempfile.NamedTemporaryFile(
        mode="wb", delete=False, suffix=".csv"
    ) as temp_file:
        temp_file_path = temp_file.name
        logger.info(f"Downloading s3://{bucket_name}/{object_key} to {temp_file_path}")
        s3_client.download_fileobj(bucket_name, object_key, temp_file)
        logger.info("Download complete")
        return temp_file_path


def safe_get_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise EnvironmentError(f"Environment variable '{var_name}' is not set.")
    return value


def find_file(directory: str, filename: str) -> str | None:
    for file in os.listdir(directory):
        if file.lower() == filename.lower():
            return os.path.join(directory, file)
    return None


def extract_email(email_str: str) -> str:
    """Extract the first email from a string that may contain multiple emails."""
    if not email_str:
        return ""
    separators = [";", ",", " ", "|"]

    for sep in separators:
        if sep in email_str:
            emails = [e.strip() for e in email_str.split(sep)]
            # Return first non-empty email
            for email in emails:
                if email and "@" in email:
                    return email
            return ""
    if "@" in email_str:
        return email_str.strip()

    return ""
