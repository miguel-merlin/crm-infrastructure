from model import Quote, CustomerType, QuoteStatus
from typing import List, Set, Tuple
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
        email_rescue_day: int,
    ) -> None:
        self.quotes = quotes
        self.email_cadence_config = email_cadence_config
        self.prospect_allowlist, self.customer_allowlist = self._parse_allowlist(
            allowlist_path
        )
        self.email_rescue_day = email_rescue_day
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

    def _is_eligible_for_email(self, quote: Quote, days_since: int) -> bool:
        """Encapsulates the filtering rules for readability."""
        if quote.status != QuoteStatus.SENT or self._is_opted_out(quote.id):
            return False

        is_cadence_day = days_since in self.email_cadence_config
        is_rescue_day = days_since == self.email_rescue_day
        if not (is_cadence_day or is_rescue_day):
            return False

        allowlist = (
            self.prospect_allowlist
            if quote.customer_type == CustomerType.PROSPECT
            else (
                self.customer_allowlist
                if quote.customer_type == CustomerType.CLIENT
                else None
            )
        )
        if allowlist is None:
            return False

        if len(allowlist) > 0 and quote.prospect.id not in allowlist:
            return False

        return True

    def filter_quotes(self) -> Tuple[List[Quote], List[Quote]]:
        filtered_quotes: List[Quote] = []
        rescue_quotes: List[Quote] = []
        now = datetime.now()

        for quote in self.quotes:
            if quote.id in self.custom_send_ids and quote.status == QuoteStatus.SENT:
                filtered_quotes.append(quote)
                continue

            days_since = (now - datetime.fromisoformat(quote.created_at)).days
            if not self._is_eligible_for_email(quote, days_since):
                continue

            if days_since == self.email_rescue_day:
                rescue_quotes.append(quote)
                logger.info(
                    "Quote %s added to rescue emails (day %d)", quote.id, days_since
                )
            else:
                filtered_quotes.append(quote)

        return filtered_quotes, rescue_quotes
