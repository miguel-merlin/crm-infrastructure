from model import Quote, CustomerType
from typing import List, Set
from datetime import datetime
import yaml
import logging
from mypy_boto3_dynamodb.service_resource import Table
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class QuoteFilter:
    def __init__(
        self,
        quotes: List[Quote],
        email_cadence_config: Set[int],
        allowlist_path: str,
        custom_send_path: str,
        opt_out_table: Table,
    ) -> None:
        self.quotes = quotes
        self.email_cadence_config = email_cadence_config
        self.prospect_allowlist, self.customer_allowlist = self._parse_allowlist(
            allowlist_path
        )
        self.opt_out_table = opt_out_table
        self.custom_send_ids = self._parse_custom_sends(custom_send_path)

    def _parse_custom_sends(self, custom_send_path: str) -> Set[str]:
        try:
            with open(custom_send_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
                custom_send_raw = data.get("ids") or []
                custom_send_ids = set(map(str, custom_send_raw))
                logger.info(
                    "Parsed custom sends: %d quote_ids",
                    len(custom_send_ids),
                )
                print("Custom send IDs:", custom_send_ids)
                return custom_send_ids
        except Exception as e:
            logger.error("Error reading custom sends file: %s", e, exc_info=True)
            return set()

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

    def _is_opted_out(self, quote_id: str) -> bool:
        """Check if the quote ID is in the opt-out table."""
        try:
            response = self.opt_out_table.get_item(Key={"quote_id": quote_id})
            if "Item" in response:
                logger.info("Quote ID %s is opted out", quote_id)
                return True
            return False
        except ClientError as e:
            logger.error(
                "Error checking opt-out status for quote ID %s: %s",
                quote_id,
                e,
                exc_info=True,
            )
            return False

    def filter_quotes(self) -> List[Quote]:
        """Filter quotes based on cadence + allowlist by customer type.

        Rule change:
        - If prospect allowlist is empty (missing/empty YAML), allow *all* prospects.
        - Customers (CLIENT) still require allowlist match.
        """
        filtered_quotes: List[Quote] = []
        now = datetime.now()

        # If YAML had no prospect_ids or customer_ids (or file missing/invalid), allow all prospects.
        prospect_allow_all = len(self.prospect_allowlist) == 0
        client_allow_all = len(self.customer_allowlist) == 0

        for quote in self.quotes:
            if quote.id in self.custom_send_ids:
                filtered_quotes.append(quote)
                logger.info(
                    "Allowed custom send quote=%s customer_type=%s prospect_id=%s",
                    quote.id,
                    quote.customer_type.value,
                    quote.prospect.id,
                )
                continue
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
                if (
                    not client_allow_all
                    and quote.prospect.id not in self.customer_allowlist
                ):
                    continue

            else:
                continue

            if self._is_opted_out(quote.id):
                continue

            logger.info(
                "Allowed quote=%s customer_type=%s prospect_id=%s quote_status=%s (prospect_allow_all=%s, client_allow_all=%s)",
                quote.id,
                quote.customer_type.value,
                quote.prospect.id,
                quote.status.value,
                prospect_allow_all,
                client_allow_all,
            )
            filtered_quotes.append(quote)

        return filtered_quotes
