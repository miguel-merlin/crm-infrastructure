import json
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple
import boto3
from botocore.exceptions import ClientError
from utils import safe_get_env
from model import ResponseType, ResponseRecord, RequestParams, EmailTransaction
from sender import ResponseEmailSender
import logging

TABLE_NAME = safe_get_env("TABLE_NAME")
EMAIL_TRANSACTION_TABLE_NAME = safe_get_env("EMAIL_TRANSACTION_TABLE_NAME")
ENABLE_CORS = safe_get_env("ENABLE_CORS").lower() == "true"

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)
email_transaction_table = dynamodb.Table(EMAIL_TRANSACTION_TABLE_NAME)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_email_transaction_by_id(
    transaction_id: str,
) -> Tuple[Optional[EmailTransaction], Optional[str]]:
    try:
        resp = email_transaction_table.get_item(
            Key={"transaction_id": transaction_id.strip()}
        )
        item = resp.get("Item")
        if not item:
            return None, "Email transaction not found"

        tx, err = EmailTransaction.from_dynamodb_item(item)
        if err:
            return None, err

        return tx, None

    except ClientError as e:
        err = e.response.get("Error", {})
        return None, f"DynamoDB error ({err.get('Code')}): {err.get('Message')}"
    except Exception as e:
        return None, f"Unexpected error: {str(e)}"


def create_response(
    status_code: int, body: Dict[str, Any], headers: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """Create a standardized API Gateway response"""
    default_headers = {"Content-Type": "application/json"}

    if ENABLE_CORS:
        default_headers.update(
            {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "GET,OPTIONS",
            }
        )

    if headers:
        default_headers.update(headers)

    return {
        "statusCode": status_code,
        "headers": default_headers,
        "body": json.dumps(body),
    }


def save_to_dynamodb(record: ResponseRecord) -> Tuple[bool, Optional[str]]:
    try:
        table.put_item(Item=record.to_dict())
        return True, None
    except ClientError as e:
        error_code = e.response["Error"]["Code"]  # type: ignore
        error_message = e.response["Error"]["Message"]  # type: ignore
        return False, f"DynamoDB error ({error_code}): {error_message}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for prospect response tracking.

    Expected:
      Method: POST
      Body JSON:
        - id
        - response
        - email_transaction_id
    """

    if event.get("httpMethod") == "OPTIONS":
        return create_response(200, {"message": "OK"})

    if event.get("httpMethod") != "POST":
        return create_response(
            405,
            {"error": "Method not allowed", "message": "Only POST method is supported"},
        )

    params, error_message = RequestParams.from_event(event)
    if error_message:
        return create_response(
            400, {"error": "Invalid request", "message": error_message}
        )
    if not params:
        return create_response(
            400, {"error": "Invalid request", "message": "Unknown error"}
        )

    response_type = ResponseType.from_string(params.response)
    assert response_type is not None

    record = ResponseRecord(
        response_id=str(uuid.uuid4()),
        received_at=datetime.now(timezone.utc).isoformat(),
        email_transaction_id=params.email_transaction_id,
        prospect_id=params.prospect_id,
        response_type=str(response_type),
    )

    success, error = save_to_dynamodb(record)
    if not success:
        print(f"Error saving to DynamoDB: {error}")
        return create_response(
            500,
            {
                "error": "Internal server error",
                "message": "Failed to save response record",
            },
        )

    email_txn, err = get_email_transaction_by_id(params.email_transaction_id)
    if not email_txn or err:
        logger.error(f"Error retrieving email transaction: {err}")
        return create_response(
            500,
            {
                "error": "Internal server error",
                "message": "Failed to retrieve email transaction",
            },
        )
    logger.info(
        f"Retrieved email transaction: {email_txn.id} for prospect {email_txn.email_address}"
    )
    email_sender = ResponseEmailSender(
        template_path="assets/template.html",
        sender_email=safe_get_env("SENDER_EMAIL"),
    )
    try:
        email_sender.send_emails(record, email_txn)
    except Exception as e:
        logger.error(f"Error sending response email: {str(e)}", exc_info=True)

    return create_response(
        201,
        {
            "message": "Response recorded successfully",
            "data": {
                "response_id": record.response_id,
                "received_at": record.received_at,
                "prospect_id": record.prospect_id,
                "response_type": record.response_type,
            },
        },
    )
