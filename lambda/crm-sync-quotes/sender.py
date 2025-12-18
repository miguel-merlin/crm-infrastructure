import boto3
from mypy_boto3_dynamodb.service_resource import Table
from typing import List, Set, Dict
from datetime import datetime
from model import Quote, EmailTransaction, EmailStatus
from jinja2 import Template
import logging
import uuid

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class QuoteEmailSender:
    def __init__(
        self,
        quotes: List[Quote],
        email_cadence_config: Set[int],
        template_path: str,
        sender_email: str,
        transactions_table: Table,
    ) -> None:
        self.quotes = quotes
        self.email_cadence_config = email_cadence_config
        self.ses_client = boto3.client("ses")
        self.sender_email = sender_email
        self.transactions_table = transactions_table
        try:
            with open(template_path, "r", encoding="utf-8") as f:
                template_content = f.read()
            self.template: Template = Template(template_content)
        except Exception as e:
            raise ValueError(f"Error reading email template: {str(e)}") from e

    def _filter_quotes(self) -> Dict[int, Quote]:
        """Filter quotes based on the email cadence configuration."""
        filtered_quotes = {}
        now = datetime.now()
        for quote in self.quotes:
            days_since_creation = (now - datetime.fromisoformat(quote.created_at)).days
            if days_since_creation in self.email_cadence_config:
                filtered_quotes[days_since_creation] = quote
        return filtered_quotes

    def _render_template(self, quote: Quote, transaction_id: str) -> str:
        """Render the email template with quote data."""
        return self.template.render(
            quote_id=quote.id,
            prospect_name=quote.prospect.name,
            amount=quote.amount,
            status=str(quote.status),
            created_at=quote.created_at,
            transaction_id=transaction_id,
        )

    def _batch_write_transactions(self, transactions: List[EmailTransaction]) -> None:
        """Batch write email transactions to DynamoDB."""
        with self.transactions_table.batch_writer() as batch:
            for transaction in transactions:
                batch.put_item(Item=transaction.to_dynamodb_item())

    def send_emails(self) -> None:
        """Send emails for the filtered quotes."""
        filtered_quotes: Dict[int, Quote] = self._filter_quotes()
        email_transactions: List[EmailTransaction] = []
        for quote in filtered_quotes.values():
            transaction_id = str(uuid.uuid4())
            rendered_email = self._render_template(quote, transaction_id)
            body_text = "Los detalles de tu cotización están adjuntos."
            try:
                response = self.ses_client.send_email(
                    Source=self.sender_email,
                    Destination={"ToAddresses": [quote.prospect.email]},
                    Message={
                        "Subject": {
                            "Data": f"Detalles de tu cotización {quote.id}",
                            "Charset": "UTF-8",
                        },
                        "Body": {
                            "Text": {"Data": body_text, "Charset": "UTF-8"},
                            "Html": {"Data": rendered_email, "Charset": "UTF-8"},
                        },
                    },
                )
                logger.info(
                    f"Email sent to {quote.prospect.email} for quote {quote.id}, MessageId: {response['MessageId']}"
                )
                email_transaction = EmailTransaction(
                    id=transaction_id,
                    quote_id=quote.id,
                    email_address=quote.prospect.email,
                    sent_at=datetime.now().isoformat(),
                    status=EmailStatus.SENT,
                )
                email_transactions.append(email_transaction)
            except Exception as e:
                logger.error(
                    f"Error sending email to {quote.prospect.email} for quote {quote.id}: {str(e)}",
                    exc_info=True,
                )
        if email_transactions:
            self._batch_write_transactions(email_transactions)
            logger.info(
                f"Wrote {len(email_transactions)} email transactions to DynamoDB"
            )
