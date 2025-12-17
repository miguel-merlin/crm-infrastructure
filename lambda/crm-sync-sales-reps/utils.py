import os
import csv
import logging
from typing import List
from model import SalesRep, DBWriteResult
from mypy_boto3_dynamodb.service_resource import Table

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def safe_get_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        logger.error(f"Environment variable '{var_name}' is not set.")
        raise EnvironmentError(f"Environment variable '{var_name}' is not set.")
    return value


def read_sales_reps_from_csv(file_path: str) -> List[SalesRep]:
    """
    Retrieves all sales reps from a CSV file that contains AGENTE, NOMBRE, EMAIL, TEL columns.
    Returns a list of SalesRep objects.
    """
    logger.info(f"Reading sales reps from CSV file: {file_path}")
    sales_reps: List[SalesRep] = []

    try:
        with open(file_path, mode="r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if not row:
                    continue
                sales_reps.append(
                    SalesRep(
                        id=(row.get("AGENTE") or "").strip(),
                        name=(row.get("NOMBRE") or "").strip(),
                        email=(row.get("EMAIL") or "").strip(),
                    )
                )
        logger.info(f"Successfully read {len(sales_reps)} sales reps from {file_path}")
    except FileNotFoundError:
        logger.warning(f"CSV file not found: {file_path}")
        return []

    return sales_reps


def write_sales_reps_to_dynamo(
    table: Table, sales_reps: List[SalesRep]
) -> DBWriteResult:
    logger.info(f"Starting batch write of {len(sales_reps)} items to DynamoDB table '{table.name}'")
    success_count = 0
    error_count = 0
    with table.batch_writer() as batch:
        for sales_rep in sales_reps:
            try:
                batch.put_item(Item=sales_rep.to_dynamo_item())
                success_count += 1
            except Exception as e:
                logger.error(f"Error inserting sales rep {sales_rep.id}: {e}")
                error_count += 1

    logger.info(f"Batch write completed. Success: {success_count}, Failed: {error_count}")
    return DBWriteResult(
        successful_inserts=success_count,
        failed_inserts=error_count,
    )