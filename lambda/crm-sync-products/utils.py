import os
import csv
from typing import List
from model import Product, DBWriteResult
from mypy_boto3_dynamodb.service_resource import Table


def safe_get_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise EnvironmentError(f"Environment variable '{var_name}' is not set.")
    return value


def read_products_from_csv(file_path: str) -> List[Product]:
    """
    Reads products from a CSV file.
    """
    products = []
    with open(file_path, mode="r", encoding="latin-1") as csvfile:
        reader = csv.reader(csvfile)
        header_found = False
        for row in reader:
            if not row:
                continue
            if len(row) > 3 and row[3] == "Clave":
                header_found = True
                break

        if not header_found:
            return []
        for row in reader:
            if not row:
                continue
            if len(row) <= 14:
                continue

            id_ = row[3].strip()
            if not id_:
                continue

            description = row[4].strip()
            product_type = row[14].strip()

            products.append(
                Product(id=id_, description=description, product_type=product_type)
            )

    return products


def write_products_to_dynamo(products: List[Product], table: Table) -> DBWriteResult:
    """
    Writes a list of products to a DynamoDB table.
    """
    success_count = 0
    error_count = 0
    with table.batch_writer() as batch:
        for product in products:
            try:
                batch.put_item(Item=product.to_dynamo_item())
                success_count += 1
            except Exception as e:
                print(f"Error writing product {product.id} to DynamoDB: {e}")
                error_count += 1
    return DBWriteResult(successful_inserts=success_count, failed_inserts=error_count)
