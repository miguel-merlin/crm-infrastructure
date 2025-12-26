import os
import boto3
from mypy_boto3_s3 import S3Client
from mypy_boto3_dynamodb.service_resource import Table
import logging
from typing import List
from filter import QuoteFilter
from model import Quote
from parser import QuoteParser
from sender import QuoteEmailSender
from utils import (
    safe_get_env,
    parse_s3_event,
    download_file_from_s3,
)


logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = "TABLE_NAME"
SENDER = "SENDER_EMAIL"
DOMANAIN = "DOMAIN"
TEMPLATE_PATH = "assets/template.html"
SALES_REPS_PATH = "assets/sales_rep.csv"
EMAIL_CADENCE_DAYS = set([3, 5, 7])
ALLOW_LIST_PATH = "assets/allowlist.yaml"


def handler(event, context):
    logger.info("Lambda handler started")
    logger.debug("Received event: %s", event)
    s3_client: S3Client = boto3.client("s3")
    try:
        bucket_name, object_key = parse_s3_event(event)
    except ValueError as e:
        logger.error(f"Invalid event structure: {str(e)}")
        return {"statusCode": 400, "body": "Invalid event structure."}
    temp_file_path = None
    try:
        temp_file_path = download_file_from_s3(s3_client, bucket_name, object_key)
        parser = QuoteParser(temp_file_path, SALES_REPS_PATH)
        quotes: List[Quote] = parser.read_quotes_from_zip()
        logger.info(f"Read {len(quotes)} quotes from the file")
    except Exception as e:
        logger.error(f"Error processing file from S3: {str(e)}", exc_info=True)
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
            logger.info(f"Deleted temporary file {temp_file_path}")
        return {"statusCode": 500, "body": str(e)}

    if temp_file_path and os.path.exists(temp_file_path):
        os.unlink(temp_file_path)
        logger.info(f"Deleted temporary file {temp_file_path}")
    dynamodb = boto3.resource("dynamodb")
    transactions_table: Table = dynamodb.Table(safe_get_env(TABLE_NAME))
    quote_filter = QuoteFilter(quotes, EMAIL_CADENCE_DAYS, ALLOW_LIST_PATH)
    filtered_quotes = quote_filter.filter_quotes()
    email_sender = QuoteEmailSender(
        quotes=filtered_quotes,
        template_path=TEMPLATE_PATH,
        sender_email=safe_get_env(SENDER),
        transactions_table=transactions_table,
        domain=safe_get_env(DOMANAIN),
    )
    email_sender.send_emails()
    return {"statusCode": 200, "body": "Processing completed successfully."}
