from model import Quote, CustomerType
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
        self.prospect_allowlist, self.customer_allowlist = self._parse_allowlist(
            allowlist_path
        )

    def _parse_allowlist(self, allow_list_path: str) -> tuple[set[str], set[str]]:
        try:
            with open(allow_list_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}

                prospect_raw = data.get("prospect_ids") or []
                customer_raw = data.get("customer_ids") or []

                prospect_ids = set(map(str, prospect_raw))
                customer_ids = set(map(str, customer_raw))

                logger.info(
                    "Parsed allowlist: %d prospect_ids, %d customer_ids",
                    len(prospect_ids),
                    len(customer_ids),
                )

                return prospect_ids, customer_ids

        except Exception as e:
            logger.error("Error reading allowlist file: %s", e, exc_info=True)
            return set(), set()

    def filter_quotes(self) -> List[Quote]:
        """Filter quotes based on cadence + allowlist by customer type.

        Rule change:
        - If prospect allowlist is empty (missing/empty YAML), allow *all* prospects.
        - Customers (CLIENT) still require allowlist match.
        """
        filtered_quotes: List[Quote] = []
        now = datetime.now()

        # If YAML had no prospect_ids (or file missing/invalid), allow all prospects.
        prospect_allow_all = len(self.prospect_allowlist) == 0

        for quote in self.quotes:
            days_since_creation = (now - datetime.fromisoformat(quote.created_at)).days
            if days_since_creation not in self.email_cadence_config:
                continue

            if quote.customer_type == CustomerType.PROSPECT:
                if (
                    not prospect_allow_all
                    and quote.prospect.id not in self.prospect_allowlist
                ):
                    continue

            elif quote.customer_type == CustomerType.CLIENT:
                if quote.prospect.id not in self.customer_allowlist:
                    continue

            else:
                continue

            logger.info(
                "Allowed quote=%s customer_type=%s prospect_id=%s (prospect_allow_all=%s)",
                quote.id,
                quote.customer_type.value,
                quote.prospect.id,
                prospect_allow_all,
            )
            filtered_quotes.append(quote)

        return filtered_quotes
