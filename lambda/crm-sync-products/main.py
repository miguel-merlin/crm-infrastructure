import logging
import boto3
import tempfile
import os
from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource, Table
from mypy_boto3_s3 import S3Client
from typing import List, Dict, Any, Tuple
from utils import read_products_from_csv, safe_get_env, write_products_to_dynamo
from model import Product

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = "TABLE_NAME"


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


def handler(event, context):
    logger.info("Lambda handler started")
    logger.debug(f"Event received: {event}")

    s3_client: S3Client = boto3.client("s3")
    dynamo_db: DynamoDBServiceResource = boto3.resource("dynamodb")
    table_name = safe_get_env(TABLE_NAME)
    table: Table = dynamo_db.Table(table_name)
    try:
        bucket_name, object_key = parse_s3_event(event)
    except ValueError as e:
        logger.error(f"Invalid event structure: {str(e)}")
        return {"statusCode": 400, "body": {"error": "Invalid event structure"}}

    temp_file_path = None
    try:
        temp_file_path = download_file_from_s3(s3_client, bucket_name, object_key)
        products: List[Product] = read_products_from_csv(temp_file_path)
        logger.info(f"Read {len(products)} products from CSV")
    except Exception as e:
        logger.error(f"Error downloading or reading CSV file: {e}", exc_info=True)
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
        return {"statusCode": 500, "body": {"error": str(e)}}

    try:
        table: Table = dynamo_db.Table(table_name)
        write_result = write_products_to_dynamo(products, table)
    except Exception as e:
        logger.error(f"Error processing products: {str(e)}", exc_info=True)
        return {"statusCode": 500, "body": {"error": str(e)}}
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
            logger.info(f"Deleted temporary file {temp_file_path}")

    logger.info(
        f"Processing complete. Summary: {write_result.successful_inserts} successful, {write_result.failed_inserts} errors"
    )
    return {
        "statusCode": 200,
        "body": {
            "total": len(products),
            "successful_inserts": write_result.successful_inserts,
            "failed_inserts": write_result.failed_inserts,
        },
    }
