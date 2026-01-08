from dataclasses import dataclass
from enum import Enum


class QuoteStatus(Enum):
    CANCELLED = "Cancelada"
    ORDERED = "Pedida"
    SENT = "Emitida"

    def __str__(self) -> str:
        return self.value


class CustomerType(Enum):
    PROSPECT = "P"
    CLIENT = "C"

    def __str__(self) -> str:
        return self.value


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
    phone_number: str

    def to_dynamodb_item(self) -> dict:
        return {
            "sales_rep_id": self.id,
            "name": self.name,
            "email": self.email,
            "phone_number": self.phone_number,
        }


@dataclass
class BaseProduct:
    product_id: str
    description: str
    product_type: str

    @staticmethod
    def get_empty(product_id: str) -> "BaseProduct":
        return BaseProduct(product_id=product_id, description="", product_type="")


@dataclass
class Product(BaseProduct):
    quantity: int
    price: float
    vat_perc: float
    vat: float
    total_price: float
    discount_1: float = 0.0
    discount_2: float = 0.0

    def to_dynamodb_item(self) -> dict:
        return {
            "item_id": self.product_id,
            "description": self.description,
            "quantity": self.quantity,
            "price": self.price,
            "vat_perc": self.vat_perc,
            "total_price": self.total_price,
        }


@dataclass
class Quote:
    id: str
    customer_type: CustomerType
    prospect: Prospect
    sales_rep: SalesRep
    products: list[Product]
    amount: float
    status: QuoteStatus
    created_at: str

    def to_dynamodb_item(self) -> dict:
        return {
            "quote_id": self.id,
            "customer_type": self.customer_type.value,
            "prospect_id": self.prospect.id,
            "prospect_name": self.prospect.name,
            "prospect_email": self.prospect.email,
            "sales_rep": self.sales_rep.to_dynamodb_item(),
            "products": [product.to_dynamodb_item() for product in self.products],
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
    sales_rep: SalesRep

    def to_dynamodb_item(self) -> dict:
        return {
            "transaction_id": self.id,
            "quote_id": self.quote_id,
            "email_address": self.email_address,
            "sent_at": self.sent_at,
            "status": self.status.value,
            "sales_rep": self.sales_rep.to_dynamodb_item(),
        }
