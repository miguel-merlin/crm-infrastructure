import boto3
from mypy_boto3_dynamodb.service_resource import Table
from typing import List, Dict
from datetime import datetime
from model import (
    Quote,
    MessageTransaction,
    MessageChannel,
    EmailStatus,
    RescueEmailConfig,
)
from jinja2 import Environment, select_autoescape
import logging
import uuid

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_SUBJECT = "Detalles de tu cotización"


def _format_money(value) -> str:
    try:
        # "$1,234.56"
        return f"${float(value):,.2f}"
    except Exception:
        return str(value)


def _format_percent(value) -> str:
    try:
        v = float(value)
        if v <= 0:
            return "—"
        # show as 10% or 10.5% depending on input
        if v.is_integer():
            return f"{int(v)}%"
        return f"{v:.2f}%"
    except Exception:
        return "—"


class QuoteReminderSender:
    def __init__(
        self,
        quotes: List[Quote],
        template_path: str,
        sender_email: str,
        transactions_table: Table,
        domain: str,
        email_subject_config: Dict[int, str],
        ecommerce_url: str,
        rescue_email_config: RescueEmailConfig,
        configuration_set_name: str,
    ) -> None:
        self.quotes = quotes
        self.ses_client = boto3.client("ses")
        self.sender_email = sender_email
        self.transactions_table = transactions_table
        self.domain = domain
        self.email_subject_config = email_subject_config
        self.ecommerce_url = ecommerce_url
        self.rescue_email_config = rescue_email_config
        self.configuration_set_name = configuration_set_name
        self.jinja_env = Environment(
            autoescape=select_autoescape(["html", "xml"]),
        )
        self.jinja_env.filters["money"] = _format_money
        self.jinja_env.filters["percent"] = _format_percent
        try:
            with open(template_path, "r", encoding="utf-8") as f:
                template_content = f.read()
            self.template = self.jinja_env.from_string(template_content)

            with open(rescue_email_config.template_path, "r", encoding="utf-8") as f:
                rescue_template_content = f.read()
            self.rescue_template = self.jinja_env.from_string(rescue_template_content)
        except Exception as e:
            raise ValueError(f"Error reading email template: {str(e)}") from e

    def _render_template(self, quote: Quote, transaction_id: str) -> str:
        """Render the email template with quote data."""
        has_discount_1, has_discount_2 = quote.has_discounts()
        return self.template.render(
            quote_id=quote.id,
            prospect_name=quote.prospect.name,
            amount=quote.amount,
            status=str(quote.status),
            created_at=quote.created_at,
            transaction_id=transaction_id,
            domain=self.domain,
            prospect_id=quote.prospect.id,
            products=quote.products,
            total_vat=quote.compute_total_vat(),
            has_discount_1=has_discount_1,
            has_discount_2=has_discount_2,
            ecommerce_url=self.ecommerce_url,
        )

    def _render_rescue_template(self, quote: Quote) -> str:
        """Render the rescue email template with quote data."""
        has_discount_1, has_discount_2 = quote.has_discounts()

        return self.rescue_template.render(
            quote_id=quote.id,
            prospect_name=quote.prospect.name,
            sales_rep=quote.sales_rep,
            sales_rep_name=quote.sales_rep.name,
            amount=quote.amount,
            status=str(quote.status),
            created_at=quote.created_at,
            domain=self.domain,
            prospect_id=quote.prospect.id,
            products=quote.products,
            total_vat=quote.compute_total_vat(),
            has_discount_1=has_discount_1,
            has_discount_2=has_discount_2,
        )

    def _batch_write_transactions(self, transactions: List[MessageTransaction]) -> None:
        """Batch write email transactions to DynamoDB."""
        with self.transactions_table.batch_writer() as batch:
            for transaction in transactions:
                batch.put_item(Item=transaction.to_dynamodb_item())

    def send_messages(self) -> None:
        """Send emails for the filtered quotes."""
        email_transactions: List[MessageTransaction] = []
        for quote in self.quotes:
            transaction_id = str(uuid.uuid4())
            rendered_email = self._render_template(quote, transaction_id)
            body_text = "Los detalles de tu cotización están adjuntos."
            day_diff = (datetime.now() - datetime.fromisoformat(quote.created_at)).days
            subject = (
                self.email_subject_config.get(day_diff, DEFAULT_SUBJECT)
                + f" - {quote.id}"
            )
            try:
                response = self.ses_client.send_email(
                    Source=self.sender_email,
                    Destination={
                        "ToAddresses": [quote.prospect.email],
                        "CcAddresses": [quote.sales_rep.email],
                    },
                    Message={
                        "Subject": {
                            "Data": subject,
                            "Charset": "UTF-8",
                        },
                        "Body": {
                            "Text": {"Data": body_text, "Charset": "UTF-8"},
                            "Html": {"Data": rendered_email, "Charset": "UTF-8"},
                        },
                    },
                    ConfigurationSetName=self.configuration_set_name,
                    Tags=[
                        {"Name": "EmailType", "Value": "quote-followup"},
                        {"Name": "SalesRepId", "Value": quote.sales_rep.id or "unknown"},
                    ],
                )
                logger.info(
                    f"Email sent to {quote.prospect.email} for quote {quote.id}, MessageId: {response['MessageId']}"
                )
                email_transaction = MessageTransaction(
                    id=transaction_id,
                    quote_id=quote.id,
                    channel=MessageChannel.EMAIL,
                    email_address=quote.prospect.email,
                    phone=quote.prospect.phone,
                    sent_at=datetime.now().isoformat(),
                    status=EmailStatus.SENT,
                    sales_rep=quote.sales_rep,
                )
                email_transactions.append(email_transaction)
            except Exception as e:
                logger.error(
                    f"Error sending email to {quote.prospect.email} for quote {quote.id}: {str(e)}",
                    exc_info=True,
                )

        for rescue_quote in self.rescue_email_config.rescue_emails:
            if self.rescue_email_config.sales_rep_recipient.is_empty():
                logger.warning(
                    f"Skipping rescue email for quote {rescue_quote.id} because sales rep recipient is empty"
                )
                continue
            try:
                rendered_rescue_email = self._render_rescue_template(rescue_quote)
                unique_recipients = list(set([
                    self.rescue_email_config.sales_rep_recipient.email,
                    rescue_quote.sales_rep.email]))
                subject = f"{self.rescue_email_config.subject} - {rescue_quote.id}"
                response = self.ses_client.send_email(
                    Source=self.sender_email,
                    Destination={
                        "ToAddresses": unique_recipients,
                    },
                    Message={
                        "Subject": {
                            "Data": subject,
                            "Charset": "UTF-8",
                        },
                        "Body": {
                            "Text": {
                                "Data": f"Rescate para cotización {rescue_quote.id}",
                                "Charset": "UTF-8",
                            },
                            "Html": {
                                "Data": rendered_rescue_email,
                                "Charset": "UTF-8",
                            },
                        },
                    },
                    ConfigurationSetName=self.configuration_set_name,
                    Tags=[
                        {"Name": "EmailType", "Value": "quote-rescue"},
                        {"Name": "SalesRepId", "Value": rescue_quote.sales_rep.id or "unknown"},
                    ],
                )
                logger.info(
                    f"Rescue email sent for quote {rescue_quote.id}, MessageId: {response['MessageId']}"
                )
            except Exception as e:
                logger.error(
                    f"Error sending rescue email for quote {rescue_quote.id}: {str(e)}",
                    exc_info=True,
                )
        if email_transactions:
            self._batch_write_transactions(email_transactions)
            logger.info(
                f"Wrote {len(email_transactions)} email transactions to DynamoDB"
            )
