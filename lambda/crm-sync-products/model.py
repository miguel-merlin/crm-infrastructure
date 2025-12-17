from dataclasses import dataclass


@dataclass
class Product:
    id: str = ""
    description: str = ""
    product_type: str = ""

    def to_dynamo_item(self) -> dict:
        return {
            "id": {"S": self.id},
            "description": {"S": self.description},
            "product_type": {"S": self.product_type},
        }


@dataclass
class DBWriteResult:
    successful_inserts: int = 0
    failed_inserts: int = 0
