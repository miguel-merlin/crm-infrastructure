import boto3
import tempfile
import os
from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource, Table
from mypy_boto3_s3 import S3Client
from typing import List, Dict, Any, Tuple
from utils import read_sales_reps_from_csv, safe_get_env, write_sales_reps_to_dynamo
from model import SalesRep


TABLE_NAME = "TABLE_NAME"


def parse_s3_event(event: Dict[str, Any]) -> Tuple[str, str]:
    """
    Parses the S3 event to extract the bucket name and object key.
    """
    try:
        record = event["Records"][0]
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]
        return bucket_name, object_key
    except (KeyError, IndexError) as e:
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
        s3_client.download_fileobj(bucket_name, object_key, temp_file)
        print(
            f"Downloaded S3 object s3://{bucket_name}/{object_key} to {temp_file_path}"
        )
        return temp_file_path


def handler(event, context) -> Dict[str, Any]:
    """
    Lambda function handler to read sales reps from a CSV file and write them to a DynamoDB table.
    """
    s3_client: S3Client = boto3.client("s3")
    dynamo_db: DynamoDBServiceResource = boto3.resource("dynamodb")
    table_name = safe_get_env(TABLE_NAME)
    table: Table = dynamo_db.Table(table_name)

    try:
        bucket_name, object_key = parse_s3_event(event)
    except ValueError as e:
        print(str(e))
        return {"statusCode": 400, "body": {"error": "Invalid event structure"}}

    temp_file_path = None
    try:
        temp_file_path = download_file_from_s3(s3_client, bucket_name, object_key)
        sales_reps: List[SalesRep] = read_sales_reps_from_csv(temp_file_path)
        print(f"Read {len(sales_reps)} sales reps from CSV")
    except Exception as e:
        print(f"Error downloading or reading CSV file: {e}")
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
        return {"statusCode": 500, "body": {"error": str(e)}}

    try:
        write_result = write_sales_reps_to_dynamo(table, sales_reps)
    except Exception as e:
        print(f"Error during batch write: {e}")
        return {"statusCode": 500, "body": {"error": str(e)}}
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
            print(f"Cleaned up temporary file {temp_file_path}")

    print(
        f"\nSummary: {write_result.successful_inserts} successful, {write_result.failed_inserts} errors"
    )
    return {
        "statusCode": 200,
        "body": {
            "total": len(sales_reps),
            "successful_inserts": write_result.successful_inserts,
            "failed_inserts": write_result.failed_inserts,
        },
    }
