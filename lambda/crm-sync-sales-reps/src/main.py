import boto3
import tempfile
import os
from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource, Table
from mypy_boto3_s3 import S3Client
from typing import List, Dict, Any
from utils import read_sales_reps_from_csv, safe_get_env
from model import SalesRep


TABLE_NAME = "TABLE_NAME"


def handler(event, context) -> Dict[str, Any]:
    """
    Lambda function handler to read sales reps from a CSV file and write them to a DynamoDB table.

    :param event: Description
    :param context: Description
    :return: Description
    :rtype: Dict[str, Any]
    """
    s3_client: S3Client = boto3.client("s3")
    dynamo_db: DynamoDBServiceResource = boto3.resource("dynamodb")
    table_name = safe_get_env(TABLE_NAME)
    table: Table = dynamo_db.Table(table_name)

    try:
        record = event["Records"][0]
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]
    except (KeyError, IndexError) as e:
        print(f"Error parsing event: {e}")
        return {"statusCode": 400, "body": {"error": "Invalid event structure"}}

    temp_file_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", delete=False, suffix=".csv"
        ) as temp_file:
            temp_file_path = temp_file.name
            s3_client.download_fileobj(bucket_name, object_key, temp_file)
            print(
                f"Downloaded S3 object s3://{bucket_name}/{object_key} to {temp_file_path}"
            )
        sales_reps: List[SalesRep] = read_sales_reps_from_csv(temp_file_path)
        print(f"Read {len(sales_reps)} sales reps from CSV")
    except Exception as e:
        print(f"Error downloading or reading CSV file: {e}")
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
        return {"statusCode": 500, "body": {"error": str(e)}}

    success_count = 0
    error_count = 0
    try:
        with table.batch_writer() as batch:
            for sales_rep in sales_reps:
                try:
                    batch.put_item(Item=sales_rep.to_dynamo_item())
                    success_count += 1
                except Exception as e:
                    print(f"Error inserting sales rep {sales_rep.id}: {e}")
                    error_count += 1
    except Exception as e:
        print(f"Error during batch write: {e}")
        return {"statusCode": 500, "body": {"error": str(e)}}
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
            print(f"Cleaned up temporary file {temp_file_path}")

    print(f"\nSummary: {success_count} successful, {error_count} errors")
    return {
        "statusCode": 200,
        "body": {
            "total": len(sales_reps),
            "successful_inserts": success_count,
            "failed_inserts": error_count,
        },
    }
