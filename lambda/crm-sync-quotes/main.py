import json
import os
import boto3
from mypy_boto3_s3 import S3Client
from mypy_boto3_dynamodb.service_resource import Table
import logging
from typing import List, Dict
from filter import QuoteFilter
from model import Quote, RescueEmailConfig, SalesRep
from parser import QuoteParser
from sender import QuoteReminderSender
from utils import (
    safe_get_env,
    parse_s3_event,
    download_file_from_s3,
)
from whatsapp import WhatsAppClient, StubWhatsAppClient


logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = "TABLE_NAME"
OPT_OUT_TABLE_NAME = "OPT_OUT_TABLE_NAME"
SENDER = "SENDER_EMAIL"
DOMAIN = "DOMAIN"
ECOMMERCE_URL = "ECOMMERCE_URL"
SES_CONFIGURATION_SET = "SES_CONFIGURATION_SET"
WHATSAPP_SECRET_ARN = "WHATSAPP_SECRET_ARN"
WHATSAPP_TEMPLATE_DAY_7 = "WHATSAPP_TEMPLATE_DAY_7"
WHATSAPP_TEMPLATE_DAY_14 = "WHATSAPP_TEMPLATE_DAY_14"
WHATSAPP_TEMPLATE_DAY_21 = "WHATSAPP_TEMPLATE_DAY_21"
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


_whatsapp_client_cache: dict = {}


def _load_whatsapp_client() -> tuple:
    """
    Load the Meta WhatsApp credentials from Secrets Manager and return a
    (client, language_code) tuple. Cached on the module for warm invocations.

    On any failure (missing/malformed secret, IAM denial, network error), log
    the failure and return a StubWhatsAppClient so the cadence sender falls
    back to email for every quote. The day-28 rescue email path is unaffected.
    """
    if "client" in _whatsapp_client_cache:
        return (
            _whatsapp_client_cache["client"],
            _whatsapp_client_cache["language_code"],
        )

    try:
        secret_arn = safe_get_env(WHATSAPP_SECRET_ARN)
        sm = boto3.client("secretsmanager")
        response = sm.get_secret_value(SecretId=secret_arn)
        secret = json.loads(response["SecretString"])

        client = WhatsAppClient(
            access_token=secret["access_token"],
            phone_number_id=secret["phone_number_id"],
        )
        language_code = secret.get("language_code", "es_MX")
    except Exception:
        logger.exception(
            "Failed to load WhatsApp credentials; falling back to email for "
            "all cadence reminders. Rescue emails are unaffected."
        )
        client = StubWhatsAppClient()
        language_code = "es_MX"

    _whatsapp_client_cache["client"] = client
    _whatsapp_client_cache["language_code"] = language_code
    return client, language_code


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
    whatsapp_client, whatsapp_language_code = _load_whatsapp_client()
    whatsapp_templates: Dict[int, str] = {
        7: safe_get_env(WHATSAPP_TEMPLATE_DAY_7),
        14: safe_get_env(WHATSAPP_TEMPLATE_DAY_14),
        21: safe_get_env(WHATSAPP_TEMPLATE_DAY_21),
    }
    reminder_sender = QuoteReminderSender(
        quotes=filtered_quotes,
        template_path=TEMPLATE_PATH,
        sender_email=safe_get_env(SENDER),
        transactions_table=transactions_table,
        domain=safe_get_env(DOMAIN),
        email_subject_config=EMAIL_SUBJECT_CONFIG,
        ecommerce_url=safe_get_env(ECOMMERCE_URL),
        rescue_email_config=email_rescue_config,
        configuration_set_name=safe_get_env(SES_CONFIGURATION_SET),
        whatsapp_client=whatsapp_client,
        whatsapp_templates=whatsapp_templates,
        whatsapp_language_code=whatsapp_language_code,
    )
    reminder_sender.send_messages()
    return {"statusCode": 200, "body": "Processing completed successfully."}
