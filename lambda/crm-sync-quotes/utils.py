import os
import logging
from model import Quote, DBWriteResult
from typing import List
from mypy_boto3_dynamodb.service_resource import Table


logger = logging.getLogger(__name__)


def safe_get_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise EnvironmentError(f"Environment variable '{var_name}' is not set.")
    return value


def read_quotes_from_zip(file_path: str) -> List[Quote]:
    logger.info(f"Reading quotes from ZIP file at {file_path}")
    return []


def write_quotes_to_dynamodb(quotes: List[Quote], table: Table) -> DBWriteResult:
    return DBWriteResult()
