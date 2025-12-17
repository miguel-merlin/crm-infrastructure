from dataclasses import dataclass


@dataclass
class SalesRep:
    id: str = ""
    name: str = ""
    email: str = ""

    def to_dynamo_item(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "email": self.email,
        }
