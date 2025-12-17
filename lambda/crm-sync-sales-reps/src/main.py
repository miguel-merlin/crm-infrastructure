import boto3
from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource, Table
from typing import List, Dict, Any
from utils import read_sales_reps_from_csv, safe_get_env
from model import SalesRep


TABLE_NAME = "TABLE_NAME"


def handler(event, context) -> Dict[str, Any]:
    dynamo_db: DynamoDBServiceResource = boto3.resource("dynamodb")
    table_name = safe_get_env(TABLE_NAME)
    table: Table = dynamo_db.Table(table_name)
    sales_reps: List[SalesRep] = read_sales_reps_from_csv("../test/data/sales_rep.csv")
    print(f"Read {len(sales_reps)} sales reps from CSV")
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

    print(f"\nSummary: {success_count} successful, {error_count} errors")
    return {
        "statusCode": 200,
        "body": {
            "total": len(sales_reps),
            "successful_inserts": success_count,
            "failed_inserts": error_count,
        },
    }
