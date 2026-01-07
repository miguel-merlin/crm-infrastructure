import json
import os
import time
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from utils import safe_get_env
from model import ResponseType, ResponseRecord, RequestParams, EmailTransaction
from sender import ResponseEmailSender

logger = logging.getLogger()
logger.setLevel(logging.INFO)

if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_log(level: str, message: str, *, ctx: Dict[str, Any], **fields: Any) -> None:
    """
    Emit a single-line JSON log for CloudWatch.
    """
    payload = {
        "ts": _now_iso(),
        "level": level,
        "msg": message,
        # request context always present
        "aws_request_id": ctx.get("aws_request_id"),
        "function": ctx.get("function_name"),
        "version": ctx.get("function_version"),
        "region": ctx.get("region"),
        # custom fields
        **fields,
    }
    line = json.dumps(payload, default=str, separators=(",", ":"))
    getattr(logger, level.lower(), logger.info)(line)


def _summarize_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a safe summary of the event (no big bodies, no secrets).
    Works with both API Gateway REST (v1) and HTTP API (v2) shapes.
    """
    headers = event.get("headers") or {}
    redacted_headers = {}
    for k, v in headers.items():
        lk = k.lower()
        if lk in ("authorization", "cookie", "x-api-key"):
            redacted_headers[k] = "<redacted>"
        else:
            s = str(v)
            redacted_headers[k] = s[:256] + ("…" if len(s) > 256 else "")

    body = event.get("body")
    body_len = (
        len(body)
        if isinstance(body, str)
        else (len(json.dumps(body)) if body is not None else 0)
    )

    return {
        "shape": (
            "apigw_v2"
            if "requestContext" in event
            and "http" in (event.get("requestContext") or {})
            else "apigw_v1_like"
        ),
        "httpMethod": event.get("httpMethod")
        or ((event.get("requestContext") or {}).get("http") or {}).get("method"),
        "path": event.get("path")
        or ((event.get("requestContext") or {}).get("http") or {}).get("path"),
        "queryStringParameters": event.get("queryStringParameters"),
        "isBase64Encoded": event.get("isBase64Encoded"),
        "headers": redacted_headers,
        "body_len": body_len,
    }


def _timed() -> float:
    return time.perf_counter()


TABLE_NAME = safe_get_env("TABLE_NAME")
EMAIL_TRANSACTION_TABLE_NAME = safe_get_env("EMAIL_TRANSACTION_TABLE_NAME")
OPT_OUT_TABLE_NAME = safe_get_env("OPT_OUT_TABLE_NAME")
ENABLE_CORS = safe_get_env("ENABLE_CORS").lower() == "true"

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)
email_transaction_table = dynamodb.Table(EMAIL_TRANSACTION_TABLE_NAME)
opt_out_table = dynamodb.Table(OPT_OUT_TABLE_NAME)


def create_response(
    status_code: int, body: Dict[str, Any], headers: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
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


def save_to_dynamodb(
    record: ResponseRecord, ctx: Dict[str, Any]
) -> Tuple[bool, Optional[str]]:
    t0 = _timed()
    try:
        table.put_item(
            Item=record.to_dict(),
            ConditionExpression="attribute_not_exists(email_transaction_id)",
        )
        _json_log(
            "INFO", "dynamodb_put_ok", ctx=ctx, ms=round((_timed() - t0) * 1000, 2)
        )
        return True, None

    except ClientError as e:
        err = e.response.get("Error", {}) or {}
        code = err.get("Code")
        message = err.get("Message")

        _json_log(
            "ERROR",
            "dynamodb_put_client_error",
            ctx=ctx,
            ms=round((_timed() - t0) * 1000, 2),
            error_code=code,
            error_message=message,
        )

        if code == "ConditionalCheckFailedException":
            return True, "Record already exists"  # idempotent case

        return (
            False,
            f"DynamoDB error ({code or 'UnknownCode'}): {message or 'No message'}",
        )

    except Exception:
        _json_log(
            "ERROR",
            "dynamodb_put_exception",
            ctx=ctx,
            ms=round((_timed() - t0) * 1000, 2),
        )
        logger.exception("dynamodb_put_exception_trace")  # stack trace
        return False, "Unexpected error writing to DynamoDB"


def get_email_transaction_by_id(
    transaction_id: str, ctx: Dict[str, Any]
) -> Tuple[Optional[EmailTransaction], Optional[str]]:
    t0 = _timed()
    try:
        resp = email_transaction_table.get_item(
            Key={"transaction_id": transaction_id.strip()}
        )
        item = resp.get("Item")

        _json_log(
            "INFO",
            "dynamodb_get_email_txn",
            ctx=ctx,
            ms=round((_timed() - t0) * 1000, 2),
            found=bool(item),
        )

        if not item:
            return None, "Email transaction not found"

        tx, err = EmailTransaction.from_dynamodb_item(item)
        if err:
            _json_log("ERROR", "email_txn_parse_error", ctx=ctx, parse_error=err)
            return None, err

        return tx, None

    except ClientError as e:
        err = e.response.get("Error", {}) or {}
        _json_log(
            "ERROR",
            "dynamodb_get_client_error",
            ctx=ctx,
            ms=round((_timed() - t0) * 1000, 2),
            error_code=err.get("Code"),
            error_message=err.get("Message"),
        )
        return None, f"DynamoDB error ({err.get('Code')}): {err.get('Message')}"

    except Exception:
        _json_log(
            "ERROR",
            "dynamodb_get_exception",
            ctx=ctx,
            ms=round((_timed() - t0) * 1000, 2),
        )
        logger.exception("dynamodb_get_exception_trace")
        return None, "Unexpected error reading from DynamoDB"


def save_opt_out(quote_id: str, ctx: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Record that the user is no longer interested in this specific quote."""
    t0 = _timed()
    try:
        opt_out_table.put_item(
            Item={
                "quote_id": quote_id,
                "opted_out_at": _now_iso(),
            }
        )
        _json_log(
            "INFO",
            "dynamodb_opt_out_ok",
            ctx=ctx,
            quote_id=quote_id,
            ms=round((_timed() - t0) * 1000, 2),
        )
        return True, None
    except ClientError as e:
        _json_log("ERROR", "dynamodb_opt_out_exception", ctx=ctx, quote_id=quote_id)
        logger.exception("dynamodb_opt_out_exception_trace")
        return False, f"DynamoDB ClientError: {str(e)}"
    except Exception:
        _json_log("ERROR", "dynamodb_opt_out_exception", ctx=ctx, quote_id=quote_id)
        logger.exception("dynamodb_opt_out_exception_trace")
        return False, "Unexpected error writing to DynamoDB"


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    ctx = {
        "aws_request_id": getattr(context, "aws_request_id", None),
        "function_name": os.getenv("AWS_LAMBDA_FUNCTION_NAME"),
        "function_version": os.getenv("AWS_LAMBDA_FUNCTION_VERSION"),
        "region": os.getenv("AWS_REGION"),
    }

    start = _timed()
    _json_log("INFO", "invocation_start", ctx=ctx, event=_summarize_event(event))

    try:
        # Support both API Gateway v1 and v2 method detection
        http_method = event.get("httpMethod") or (
            (event.get("requestContext") or {}).get("http") or {}
        ).get("method")

        if http_method == "OPTIONS":
            _json_log("INFO", "return_options_ok", ctx=ctx)
            return create_response(200, {"message": "OK"})

        if http_method != "POST":
            _json_log("INFO", "return_method_not_allowed", ctx=ctx, method=http_method)
            return create_response(
                405,
                {
                    "error": "Method not allowed",
                    "message": "Only POST method is supported",
                },
            )

        params, error_message = RequestParams.from_event(event)
        if error_message:
            _json_log("INFO", "return_invalid_request", ctx=ctx, reason=error_message)
            return create_response(
                400, {"error": "Invalid request", "message": error_message}
            )
        if not params:
            _json_log("INFO", "return_invalid_request", ctx=ctx, reason="Unknown error")
            return create_response(
                400, {"error": "Invalid request", "message": "Unknown error"}
            )

        _json_log(
            "INFO",
            "parsed_params",
            ctx=ctx,
            prospect_id=params.prospect_id,
            email_transaction_id=params.email_transaction_id,
            response=params.response,
        )

        response_type = ResponseType.from_string(params.response)
        if response_type is None:
            _json_log(
                "INFO",
                "return_invalid_response_type",
                ctx=ctx,
                response=params.response,
            )
            return create_response(
                400, {"error": "Invalid request", "message": "Invalid response type"}
            )

        record = ResponseRecord(
            response_id=str(uuid.uuid4()),
            received_at=datetime.now(timezone.utc).isoformat(),
            email_transaction_id=params.email_transaction_id,
            prospect_id=params.prospect_id,
            response_type=str(response_type),
        )

        ok, err = save_to_dynamodb(record, ctx)
        if not ok:
            _json_log("ERROR", "return_save_failed", ctx=ctx, error=err)
            return create_response(
                500,
                {
                    "error": "Internal server error",
                    "message": "Failed to save response record",
                },
            )
        if err:  # idempotent note
            _json_log("INFO", "idempotent_write", ctx=ctx, note=err)

        email_txn, err = get_email_transaction_by_id(params.email_transaction_id, ctx)
        if not email_txn or err:
            _json_log("ERROR", "return_email_txn_fetch_failed", ctx=ctx, error=err)
            return create_response(
                500,
                {
                    "error": "Internal server error",
                    "message": "Failed to retrieve email transaction",
                },
            )

        if response_type == ResponseType.NOT_INTERESTED:
            ok, err = save_opt_out(email_txn.quote_id, ctx)
            if not ok:
                _json_log("ERROR", "opt_out_save_failed", ctx=ctx, error=err)
            else:
                _json_log(
                    "INFO",
                    "opt_out_saved",
                    ctx=ctx,
                    quote_id=email_txn.quote_id,
                )
        _json_log(
            "INFO",
            "email_txn_loaded",
            ctx=ctx,
            email_transaction_id=email_txn.id,
        )

        email_sender = ResponseEmailSender(
            template_path="assets/template.html",
            sender_email=safe_get_env("SENDER_EMAIL"),
        )

        t0 = _timed()
        try:
            email_sender.send_emails(record, email_txn)
            _json_log(
                "INFO", "ses_send_ok", ctx=ctx, ms=round((_timed() - t0) * 1000, 2)
            )
        except Exception:
            _json_log(
                "ERROR",
                "ses_send_exception",
                ctx=ctx,
                ms=round((_timed() - t0) * 1000, 2),
            )
            logger.exception("ses_send_exception_trace")

        resp = create_response(
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

        _json_log(
            "INFO",
            "invocation_success",
            ctx=ctx,
            total_ms=round((_timed() - start) * 1000, 2),
        )
        return resp

    except Exception:
        _json_log(
            "ERROR",
            "invocation_unhandled_exception",
            ctx=ctx,
            total_ms=round((_timed() - start) * 1000, 2),
        )
        logger.exception("invocation_unhandled_exception_trace")
        return create_response(
            500, {"error": "Internal server error", "message": "Unhandled exception"}
        )
