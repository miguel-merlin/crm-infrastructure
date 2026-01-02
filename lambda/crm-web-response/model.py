from dataclasses import dataclass, asdict
from enum import Enum
from typing import Optional, Tuple, Dict, Any
import base64
import json


class ResponseType(Enum):
    BUY = "Buy"
    MORE_INFO = "More Info"
    NOT_INTERESTED = "Not Interested"

    def __str__(self) -> str:
        return self.value

    @classmethod
    def from_string(cls, value: str | None) -> Optional["ResponseType"]:
        """
        Accepts:
          - "Buy"
          - "buy"
          - "MORE_INFO"
          - "More Info"
          - "not interested"
        """
        if value is None:
            return None
        key = value.strip().upper().replace(" ", "_")
        try:
            return cls[key]
        except KeyError:
            return None


@dataclass(frozen=True)
class RequestParams:
    prospect_id: str
    email_transaction_id: str
    response: str

    @classmethod
    def from_event(
        cls, event: Dict[str, Any]
    ) -> Tuple[Optional["RequestParams"], Optional[str]]:
        """
        Parse JSON body from API Gateway/Lambda proxy event.
        Supports base64-encoded bodies.
        Expected JSON:
          {
            "prospect_id": "...",
            "email_transaction_id": "...",
            "response": "Buy" | "More Info" | "Not Interested"
          }
        """
        raw_body = event.get("body")
        if not raw_body:
            return None, "Missing request body"

        try:
            if event.get("isBase64Encoded"):
                decoded = base64.b64decode(raw_body).decode("utf-8")
                payload = json.loads(decoded)
            else:
                payload = json.loads(raw_body)
        except (ValueError, UnicodeDecodeError):
            return None, "Request body must be valid JSON"

        prospect_id = (payload.get("prospect_id") or "").strip()
        email_transaction_id = (payload.get("email_transaction_id") or "").strip()
        response = (payload.get("response") or "").strip()

        params = cls(
            prospect_id=prospect_id,
            email_transaction_id=email_transaction_id,
            response=response,
        )

        ok, err = params.validate()
        if not ok:
            return None, err

        return params, None

    def validate(self) -> Tuple[bool, Optional[str]]:
        if not self.prospect_id:
            return False, "Missing required field: id"
        if not self.email_transaction_id:
            return False, "Missing required field: email_transaction_id"
        if not self.response:
            return False, "Missing required field: response"

        rt = ResponseType.from_string(self.response)
        if not rt:
            valid_types = [rt.value for rt in ResponseType]
            return (
                False,
                f"Invalid response type. Must be one of: {', '.join(valid_types)}",
            )

        return True, None


class EmailStatus(Enum):
    NO_RESPONSE = "No Response"
    SENT = "Sent"

    def __str__(self) -> str:
        return self.value

    @classmethod
    def from_string(cls, value: Optional[str]) -> Optional["EmailStatus"]:
        if not value:
            return None
        # match by enum value (e.g. "Sent") or by name (e.g. "SENT")
        for s in cls:
            if value == s.value:
                return s
        try:
            return cls[value.strip().upper()]
        except KeyError:
            return None


@dataclass
class SalesRep:
    id: str
    name: str
    email: str
    phone_number: str

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Tuple[Optional["SalesRep"], Optional[str]]:
        if not isinstance(d, dict):
            return None, "sales_rep must be an object"

        rep_id = (d.get("id") or "").strip()
        name = (d.get("name") or "").strip()
        email = (d.get("email") or "").strip()
        phone = (d.get("phone_number") or "").strip()
        if not (rep_id or name or email or phone):
            return None, None

        missing = [
            k
            for k, v in [
                ("id", rep_id),
                ("name", name),
                ("email", email),
                ("phone_number", phone),
            ]
            if not v
        ]
        if missing:
            return None, f"sales_rep missing fields: {', '.join(missing)}"

        return cls(id=rep_id, name=name, email=email, phone_number=phone), None


@dataclass
class EmailTransaction:
    id: str
    quote_id: str
    email_address: str
    sent_at: str
    status: EmailStatus
    sales_rep: Optional[SalesRep] = None

    @classmethod
    def from_dynamodb_item(
        cls, item: Dict[str, Any]
    ) -> Tuple[Optional["EmailTransaction"], Optional[str]]:
        """
        Convert a DynamoDB item (dict from boto3) into an EmailTransaction.
        Expected keys in DynamoDB:
          - transaction_id
          - quote_id
          - email_address
          - sent_at
          - status
        Optional:
          - sales_rep (object)
        """
        if not isinstance(item, dict):
            return None, "DynamoDB item must be a dict"

        tx_id = (item.get("transaction_id") or item.get("id") or "").strip()
        quote_id = (item.get("quote_id") or "").strip()
        email_address = (item.get("email_address") or "").strip()
        sent_at = (item.get("sent_at") or "").strip()
        status_raw = item.get("status")

        if isinstance(status_raw, str):
            status_raw = status_raw.strip()
        else:
            status_raw = ""

        status = EmailStatus.from_string(status_raw)
        if not status:
            valid = ", ".join([s.value for s in EmailStatus])
            return None, f"Invalid or missing status. Must be one of: {valid}"

        missing = []
        if not tx_id:
            missing.append("transaction_id")
        if not quote_id:
            missing.append("quote_id")
        if not email_address:
            missing.append("email_address")
        if not sent_at:
            missing.append("sent_at")
        if missing:
            return (
                None,
                f"Missing required fields in DynamoDB item: {', '.join(missing)}",
            )

        sales_rep_obj = None
        if "sales_rep" in item and item["sales_rep"] is not None:
            sales_rep_obj, err = SalesRep.from_dict(item["sales_rep"])
            if err:
                return None, err

        return (
            cls(
                id=tx_id,
                quote_id=quote_id,
                email_address=email_address,
                sent_at=sent_at,
                status=status,
                sales_rep=sales_rep_obj,
            ),
            None,
        )


@dataclass
class ResponseRecord:
    response_id: str
    received_at: str
    email_transaction_id: str
    prospect_id: str
    response_type: str

    def to_dict(self) -> dict:
        return asdict(self)
