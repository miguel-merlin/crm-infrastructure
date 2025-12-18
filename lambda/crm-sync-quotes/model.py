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
class SalesRep:
    id: str
    name: str
    email: str


@dataclass
class Product:
    id: str
    description: str
    quantity: int
    price: float


@dataclass
class Quote:
    id: str
    prospect: Prospect
    sales_rep: SalesRep
    items: list[Product]
    amount: float
    status: QuoteStatus
    created_at: str


@dataclass
class DBWriteResult:
    successful_inserts: int = 0
    failed_inserts: int = 0
