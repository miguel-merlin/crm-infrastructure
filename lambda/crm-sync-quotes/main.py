import os
import boto3
from mypy_boto3_s3 import S3Client
from mypy_boto3_dynamodb.service_resource import Table
import logging
from typing import List, Dict
from filter import QuoteFilter
from model import Quote, RescueEmailConfig, SalesRep
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
OPT_OUT_TABLE_NAME = "OPT_OUT_TABLE_NAME"
SENDER = "SENDER_EMAIL"
DOMAIN = "DOMAIN"
ECOMMERCE_URL = "ECOMMERCE_URL"
TEMPLATE_PATH = "assets/template.html"
SALES_REPS_PATH = "assets/sales_rep.csv"
PRODUCTS_PATH = "assets/products.csv"
ALLOW_LIST_PATH = "assets/allowlist.yaml"
CUSTOM_SEND_PATH = "assets/custom_sends.yaml"

EMAIL_CADENCE_DAYS = set([7, 14, 21])
EMAIL_SUBJECT_CONFIG: Dict[int, str] = {
    7: "Estamos al pendiente, te envio detalles de to cotización",
    14: "Han pasado dos semanas, qué has pensado de tu cotización?",
    21: "Qué podemos hacer para que te decidas?",
}
EMAIL_RESCUE_DAY = 28
EMAIL_RESCUE_SUBJECT = "Cotización vencida, Intervenir, Rescatar o dar de Baja"
MANAGER_SALES_REP_ID = "1"
RESCUE_EMAIL_TEMPLATE_PATH = "assets/template_rescue.html"


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
        parser = QuoteParser(temp_file_path, SALES_REPS_PATH, PRODUCTS_PATH)
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
    opt_out_table: Table = dynamodb.Table(safe_get_env(OPT_OUT_TABLE_NAME))
    quote_filter = QuoteFilter(
        quotes,
        EMAIL_CADENCE_DAYS,
        ALLOW_LIST_PATH,
        CUSTOM_SEND_PATH,
        opt_out_table,
        EMAIL_RESCUE_DAY,
    )
    filtered_quotes, rescue_quotes = quote_filter.filter_quotes()
    logger.info(
        f"Filtered down to {len(filtered_quotes)} quotes after applying cadence and allowlist"
    )
    email_rescue_config = RescueEmailConfig(
        rescue_emails=rescue_quotes,
        subject=EMAIL_RESCUE_SUBJECT,
        sales_rep_recipient=parser.sales_reps.get(MANAGER_SALES_REP_ID)
        or SalesRep.get_empty(MANAGER_SALES_REP_ID),
        template_path=RESCUE_EMAIL_TEMPLATE_PATH,
    )
    email_sender = QuoteEmailSender(
        quotes=filtered_quotes,
        template_path=TEMPLATE_PATH,
        sender_email=safe_get_env(SENDER),
        transactions_table=transactions_table,
        domain=safe_get_env(DOMAIN),
        email_subject_config=EMAIL_SUBJECT_CONFIG,
        ecommerce_url=safe_get_env(ECOMMERCE_URL),
        rescue_email_config=email_rescue_config,
    )
    email_sender.send_emails()
    return {"statusCode": 200, "body": "Processing completed successfully."}
