import json
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError
from utils import safe_get_env
from model import ResponseType, ResponseRecord

TABLE_NAME = safe_get_env("TABLE_NAME")
ENABLE_CORS = safe_get_env("ENABLE_CORS").lower() == "true"

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)


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


def validate_query_params(params: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    """Validate required query parameters"""
    required_params = ["id", "response", "email_transaction_id"]

    for param in required_params:
        if not params.get(param):
            return False, f"Missing required parameter: {param}"

    # Validate response type
    response_type = ResponseType.from_string(params["response"])
    if not response_type:
        valid_types = [rt.value for rt in ResponseType]
        return False, f"Invalid response type. Must be one of: {', '.join(valid_types)}"

    # Validate IDs are not empty after stripping
    if not params["id"].strip():
        return False, "Prospect ID cannot be empty"
    if not params["email_transaction_id"].strip():
        return False, "Email transaction ID cannot be empty"

    return True, None


def save_to_dynamodb(record: ResponseRecord) -> tuple[bool, Optional[str]]:
    """Save response record to DynamoDB"""
    try:
        table.put_item(Item=record.to_dict())
        return True, None
    except ClientError as e:
        error_code = e.response["Error"]["Code"]  # type: ignore
        error_message = e.response["Error"]["Message"]  # type: ignore
        return False, f"DynamoDB error ({error_code}): {error_message}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for prospect response tracking.

    Expected query parameters:
    - id: Prospect ID
    - response: Response type (Buy, More Info, Not Interested)
    - email_transaction_id: Email transaction ID
    """

    if event.get("httpMethod") == "OPTIONS":
        return create_response(200, {"message": "OK"})

    if event.get("httpMethod") != "GET":
        return create_response(
            405,
            {"error": "Method not allowed", "message": "Only GET method is supported"},
        )

    query_params = event.get("queryStringParameters") or {}

    is_valid, error_message = validate_query_params(query_params)
    if not is_valid:
        return create_response(
            400, {"error": "Invalid request", "message": error_message}
        )

    response_type = ResponseType.from_string(query_params["response"])
    record = ResponseRecord(
        response_id=str(uuid.uuid4()),
        received_at=datetime.now(timezone.utc).isoformat(),
        email_transaction_id=query_params["email_transaction_id"].strip(),
        prospect_id=query_params["id"].strip(),
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
