from dataclasses import dataclass
from enum import Enum


class QuoteStatus(Enum):
    CANCELLED = "Cancelada"
    ORDERED = "Pedida"
    SENT = "Emitida"


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


@dataclass
class DBWriteResult:
    successful_inserts: int = 0
    failed_inserts: int = 0
