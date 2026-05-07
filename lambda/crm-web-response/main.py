import os
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, Any
import boto3
from utils import (
    safe_get_env,
    _timed,
    _json_log,
    _summarize_event,
    create_response,
    get_email_transaction_by_id,
    save_to_dynamodb,
    save_opt_out,
)
from model import ResponseType, ResponseRecord, RequestParams
from sender import ResponseEmailSender
from mypy_boto3_dynamodb.service_resource import Table

logger = logging.getLogger()
logger.setLevel(logging.INFO)

if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


TABLE_NAME = safe_get_env("TABLE_NAME")
EMAIL_TRANSACTION_TABLE_NAME = safe_get_env("EMAIL_TRANSACTION_TABLE_NAME")
OPT_OUT_TABLE_NAME = safe_get_env("OPT_OUT_TABLE_NAME")
ENABLE_CORS = safe_get_env("ENABLE_CORS").lower() == "true"

dynamodb = boto3.resource("dynamodb")
table: Table = dynamodb.Table(TABLE_NAME)
email_transaction_table: Table = dynamodb.Table(EMAIL_TRANSACTION_TABLE_NAME)
opt_out_table: Table = dynamodb.Table(OPT_OUT_TABLE_NAME)


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    ctx = {
        "aws_request_id": getattr(context, "aws_request_id", None),
        "function_name": os.getenv("AWS_LAMBDA_FUNCTION_NAME"),
        "function_version": os.getenv("AWS_LAMBDA_FUNCTION_VERSION"),
        "region": os.getenv("AWS_REGION"),
    }

    start = _timed()
    _json_log(
        logger, "INFO", "invocation_start", ctx=ctx, event=_summarize_event(event)
    )

    try:
        # Support both API Gateway v1 and v2 method detection
        http_method = event.get("httpMethod") or (
            (event.get("requestContext") or {}).get("http") or {}
        ).get("method")

        if http_method == "OPTIONS":
            _json_log(logger, "INFO", "return_options_ok", ctx=ctx)
            return create_response(ENABLE_CORS, 200, {"message": "OK"})

        if http_method != "POST":
            _json_log(
                logger, "INFO", "return_method_not_allowed", ctx=ctx, method=http_method
            )
            return create_response(
                ENABLE_CORS,
                405,
                {
                    "error": "Method not allowed",
                    "message": "Only POST method is supported",
                },
            )

        params, error_message = RequestParams.from_event(event)
        if error_message:
            _json_log(
                logger, "INFO", "return_invalid_request", ctx=ctx, reason=error_message
            )
            return create_response(
                ENABLE_CORS, 400, {"error": "Invalid request", "message": error_message}
            )
        if not params:
            _json_log(
                logger,
                "INFO",
                "return_invalid_request",
                ctx=ctx,
                reason="Unknown error",
            )
            return create_response(
                ENABLE_CORS,
                400,
                {"error": "Invalid request", "message": "Unknown error"},
            )

        _json_log(
            logger,
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
                logger,
                "INFO",
                "return_invalid_response_type",
                ctx=ctx,
                response=params.response,
            )
            return create_response(
                ENABLE_CORS,
                400,
                {"error": "Invalid request", "message": "Invalid response type"},
            )

        record = ResponseRecord(
            response_id=str(uuid.uuid4()),
            received_at=datetime.now(timezone.utc).isoformat(),
            email_transaction_id=params.email_transaction_id,
            prospect_id=params.prospect_id,
            response_type=str(response_type),
        )

        ok, err = save_to_dynamodb(record, ctx, table, logger)
        if not ok:
            _json_log(logger, "ERROR", "return_save_failed", ctx=ctx, error=err)
            return create_response(
                ENABLE_CORS,
                500,
                {
                    "error": "Internal server error",
                    "message": "Failed to save response record",
                },
            )
        if err:  # idempotent note
            _json_log(logger, "INFO", "idempotent_write", ctx=ctx, note=err)

        email_txn, err = get_email_transaction_by_id(
            params.email_transaction_id, ctx, email_transaction_table, logger
        )
        if not email_txn or err:
            _json_log(
                logger, "ERROR", "return_email_txn_fetch_failed", ctx=ctx, error=err
            )
            return create_response(
                ENABLE_CORS,
                500,
                {
                    "error": "Internal server error",
                    "message": "Failed to retrieve email transaction",
                },
            )

        if response_type == ResponseType.NOT_INTERESTED:
            ok, err = save_opt_out(email_txn.quote_id, ctx, opt_out_table, logger)
            if not ok:
                _json_log(logger, "ERROR", "opt_out_save_failed", ctx=ctx, error=err)
            else:
                _json_log(
                    logger,
                    "INFO",
                    "opt_out_saved",
                    ctx=ctx,
                    quote_id=email_txn.quote_id,
                )
        _json_log(
            logger,
            "INFO",
            "email_txn_loaded",
            ctx=ctx,
            email_transaction_id=email_txn.id,
        )

        email_sender = ResponseEmailSender(
            template_path="assets/template.html",
            sender_email=safe_get_env("SENDER_EMAIL"),
            configuration_set_name=safe_get_env("SES_CONFIGURATION_SET"),
        )

        t0 = _timed()
        try:
            email_sender.send_emails(record, email_txn)
            _json_log(
                logger,
                "INFO",
                "ses_send_ok",
                ctx=ctx,
                ms=round((_timed() - t0) * 1000, 2),
            )
        except Exception:
            _json_log(
                logger,
                "ERROR",
                "ses_send_exception",
                ctx=ctx,
                ms=round((_timed() - t0) * 1000, 2),
            )
            logger.exception("ses_send_exception_trace")

        resp = create_response(
            ENABLE_CORS,
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
            logger,
            "INFO",
            "invocation_success",
            ctx=ctx,
            total_ms=round((_timed() - start) * 1000, 2),
        )
        return resp

    except Exception:
        _json_log(
            logger,
            "ERROR",
            "invocation_unhandled_exception",
            ctx=ctx,
            total_ms=round((_timed() - start) * 1000, 2),
        )
        logger.exception("invocation_unhandled_exception_trace")
        return create_response(
            ENABLE_CORS,
            500,
            {"error": "Internal server error", "message": "Unhandled exception"},
        )
