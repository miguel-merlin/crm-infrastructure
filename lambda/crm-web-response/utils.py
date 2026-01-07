import os
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
from logging import Logger
from botocore.exceptions import ClientError
from model import ResponseRecord, EmailTransaction
from mypy_boto3_dynamodb.service_resource import Table


def safe_get_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise EnvironmentError(f"Environment variable '{var_name}' is not set.")
    return value


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_log(
    logger: Logger,
    level: str,
    message: str,
    *,
    ctx: Dict[str, Any],
    **fields: Any,
) -> None:
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


def create_response(
    enable_cors: bool,
    status_code: int,
    body: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    default_headers = {"Content-Type": "application/json"}

    if enable_cors:
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
    record: ResponseRecord, ctx: Dict[str, Any], table: Table, logger: Logger
) -> Tuple[bool, Optional[str]]:
    t0 = _timed()
    try:
        table.put_item(
            Item=record.to_dict(),
            ConditionExpression="attribute_not_exists(email_transaction_id)",
        )
        _json_log(
            logger,
            "INFO",
            "dynamodb_put_ok",
            ctx=ctx,
            ms=round((_timed() - t0) * 1000, 2),
        )
        return True, None

    except ClientError as e:
        err = e.response.get("Error", {}) or {}
        code = err.get("Code")
        message = err.get("Message")

        _json_log(
            logger,
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
            logger,
            "ERROR",
            "dynamodb_put_exception",
            ctx=ctx,
            ms=round((_timed() - t0) * 1000, 2),
        )
        logger.exception("dynamodb_put_exception_trace")  # stack trace
        return False, "Unexpected error writing to DynamoDB"


def get_email_transaction_by_id(
    transaction_id: str,
    ctx: Dict[str, Any],
    email_transaction_table: Table,
    logger: Logger,
) -> Tuple[Optional[EmailTransaction], Optional[str]]:
    t0 = _timed()
    try:
        resp = email_transaction_table.get_item(
            Key={"transaction_id": transaction_id.strip()}
        )
        item = resp.get("Item")

        _json_log(
            logger,
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
            _json_log(
                logger, "ERROR", "email_txn_parse_error", ctx=ctx, parse_error=err
            )
            return None, err

        return tx, None

    except ClientError as e:
        err = e.response.get("Error", {}) or {}
        _json_log(
            logger,
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
            logger,
            "ERROR",
            "dynamodb_get_exception",
            ctx=ctx,
            ms=round((_timed() - t0) * 1000, 2),
        )
        logger.exception("dynamodb_get_exception_trace")
        return None, "Unexpected error reading from DynamoDB"


def save_opt_out(
    quote_id: str, ctx: Dict[str, Any], opt_out_table: Table, logger: Logger
) -> Tuple[bool, Optional[str]]:
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
            logger,
            "INFO",
            "dynamodb_opt_out_ok",
            ctx=ctx,
            quote_id=quote_id,
            ms=round((_timed() - t0) * 1000, 2),
        )
        return True, None
    except ClientError as e:
        _json_log(
            logger, "ERROR", "dynamodb_opt_out_exception", ctx=ctx, quote_id=quote_id
        )
        logger.exception("dynamodb_opt_out_exception_trace")
        return False, f"DynamoDB ClientError: {str(e)}"
    except Exception:
        _json_log(
            logger, "ERROR", "dynamodb_opt_out_exception", ctx=ctx, quote_id=quote_id
        )
        logger.exception("dynamodb_opt_out_exception_trace")
        return False, "Unexpected error writing to DynamoDB"
