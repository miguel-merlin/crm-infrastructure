from model import Quote
from typing import List, Set
from datetime import datetime
from pdb import set_trace as TRACE


class QuoteFilter:
    def __init__(self, quotes: List[Quote], email_cadence_config: Set[int]) -> None:
        self.quotes = quotes
        self.email_cadence_config = email_cadence_config

    def filter_quotes(self) -> List[Quote]:
        """Filter quotes based on the email cadence configuration."""
        filtered_quotes = []
        now = datetime.now()
        for quote in self.quotes:
            TRACE()
            days_since_creation = (now - datetime.fromisoformat(quote.created_at)).days
            if days_since_creation in self.email_cadence_config:
                filtered_quotes.append(quote)
        return filtered_quotes
