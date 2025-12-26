from dataclasses import dataclass, asdict
from enum import Enum
from typing import Optional, Dict, Any


class ResponseType(Enum):
    BUY = "Buy"
    MORE_INFO = "More Info"
    NOT_INTERESTED = "Not Interested"

    def __str__(self) -> str:
        return self.value

    @classmethod
    def from_string(cls, value: str) -> Optional["ResponseType"]:
        """Case-insensitive lookup of ResponseType"""
        value_upper = value.upper().replace(" ", "_")
        try:
            return cls[value_upper]
        except KeyError:
            return None


@dataclass
class ResponseRecord:
    response_id: str
    received_at: str
    email_transaction_id: str
    prospect_id: str
    response_type: str

    def to_dict(self) -> dict:
        return asdict(self)
