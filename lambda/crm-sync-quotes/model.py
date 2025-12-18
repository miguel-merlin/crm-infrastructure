from dataclasses import dataclass
from enum import Enum
from shlex import quote


class QuoteStatus(Enum):
    CANCELLED = "Cancelada"
    ORDERED = "Pedida"
    SENT = "Emitida"

    def __str__(self) -> str:
        return self.value


@dataclass
class Prospect:
    id: str
    name: str
    email: str


@dataclass
class Quote:
    id: str
    prospect: Prospect
    sales_rep_id: str
    item_ids: list[str]
    amount: float
    status: QuoteStatus
    created_at: str

    def to_dynamodb_item(self) -> dict:
        return {
            "quote_id": self.id,
            "prospect_id": self.prospect.id,
            "prospect_name": self.prospect.name,
            "prospect_email": self.prospect.email,
            "sales_rep_id": self.sales_rep_id,
            "item_ids": self.item_ids,
            "amount": self.amount,
            "status": self.status.value,
            "created_at": self.created_at,
        }


class EmailStatus(Enum):
    NO_RESPONSE = "No Response"
    SENT = "Sent"

    def __str__(self) -> str:
        return self.value


@dataclass
class EmailTransaction:
    id: str
    quote_id: str
    email_address: str
    sent_at: str
    status: EmailStatus

    def to_dynamodb_item(self) -> dict:
        return {
            "transaction_id": self.id,
            "quote_id": self.quote_id,
            "email_address": self.email_address,
            "sent_at": self.sent_at,
            "status": self.status.value,
        }
