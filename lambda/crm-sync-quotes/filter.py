from model import Quote
from typing import List, Set
from datetime import datetime
import yaml
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class QuoteFilter:
    def __init__(
        self, quotes: List[Quote], email_cadence_config: Set[int], allowlist_path: str
    ) -> None:
        self.quotes = quotes
        self.email_cadence_config = email_cadence_config
        self.allow_list_set = self._parse_allowlist(allowlist_path)

    def _parse_allowlist(self, allow_list_path: str) -> Set[int]:
        """Parse the allowlist file to get a set of allowed quote IDs."""

        try:
            with open(allow_list_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                allowed_ids = set(data.get("ids", []))
                return allowed_ids
        except Exception as e:
            logger.error(f"Error reading allowlist file: {e}", exc_info=True)
            return set()

    def filter_quotes(self) -> List[Quote]:
        """Filter quotes based on the email cadence configuration."""
        filtered_quotes = []
        now = datetime.now()
        for quote in self.quotes:
            days_since_creation = (now - datetime.fromisoformat(quote.created_at)).days
            if days_since_creation in self.email_cadence_config:
                filtered_quotes.append(quote)
        return filtered_quotes
