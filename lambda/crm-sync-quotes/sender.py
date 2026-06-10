import boto3
from mypy_boto3_dynamodb.service_resource import Table
from typing import List, Dict, Optional
from datetime import datetime
from model import (
    Quote,
    MessageTransaction,
    MessageChannel,
    EmailStatus,
    RescueEmailConfig,
)
from jinja2 import Environment, select_autoescape
from whatsapp import WhatsAppClient, WhatsAppSendError
import logging
import uuid

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_SUBJECT = "Detalles de tu cotización"


def _format_money(value) -> str:
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return str(value)


def _format_percent(value) -> str:
    try:
        v = float(value)
        if v <= 0:
            return "—"
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
        whatsapp_client: WhatsAppClient,
        whatsapp_templates: Dict[int, str],
        whatsapp_language_code: str = "es_MX",
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
        self.whatsapp_client = whatsapp_client
        self.whatsapp_templates = whatsapp_templates
        self.whatsapp_language_code = whatsapp_language_code
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

    def _batch_write_transactions(
        self, transactions: List[MessageTransaction]
    ) -> None:
        with self.transactions_table.batch_writer() as batch:
            for transaction in transactions:
                batch.put_item(Item=transaction.to_dynamodb_item())

    def _try_send_whatsapp(
        self, quote: Quote, day_diff: int, transaction_id: str
    ) -> None:
        """Raises WhatsAppSendError if anything goes wrong."""
        template_name = self.whatsapp_templates.get(day_diff)
        if not template_name:
            raise WhatsAppSendError(
                f"No WhatsApp template configured for day {day_diff}"
            )
        tracking_url = f"{self.domain}/r/{transaction_id}"
        params = [
            quote.prospect.name,
            quote.id,
            _format_money(quote.amount),
            tracking_url,
        ]
        self.whatsapp_client.send_template(
            to=quote.prospect.phone,
            template_name=template_name,
            language_code=self.whatsapp_language_code,
            params=params,
        )

    def _send_email(
        self,
        quote: Quote,
        day_diff: int,
        transaction_id: str,
        fallback_from: Optional[MessageChannel],
    ) -> None:
        rendered_email = self._render_template(quote, transaction_id)
        body_text = "Los detalles de tu cotización están adjuntos."
        subject = (
            self.email_subject_config.get(day_diff, DEFAULT_SUBJECT)
            + f" - {quote.id}"
        )
        tags = [
            {"Name": "EmailType", "Value": "quote-followup"},
            {"Name": "SalesRepId", "Value": quote.sales_rep.id or "unknown"},
        ]
        if fallback_from is not None:
            tags.append({"Name": "Fallback", "Value": fallback_from.value})

        response = self.ses_client.send_email(
            Source=self.sender_email,
            Destination={
                "ToAddresses": [quote.prospect.email],
                "CcAddresses": [quote.sales_rep.email],
            },
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Text": {"Data": body_text, "Charset": "UTF-8"},
                    "Html": {"Data": rendered_email, "Charset": "UTF-8"},
                },
            },
            ConfigurationSetName=self.configuration_set_name,
            Tags=tags,
        )
        logger.info(
            "Email sent to %s for quote %s, MessageId: %s",
            quote.prospect.email,
            quote.id,
            response["MessageId"],
        )

    def _send_cadence_message(self, quote: Quote) -> Optional[MessageTransaction]:
        transaction_id = str(uuid.uuid4())
        day_diff = (datetime.now() - datetime.fromisoformat(quote.created_at)).days

        attempted_wa = False
        if quote.prospect.phone is not None:
            attempted_wa = True
            try:
                self._try_send_whatsapp(quote, day_diff, transaction_id)
                return MessageTransaction(
                    id=transaction_id,
                    quote_id=quote.id,
                    channel=MessageChannel.WHATSAPP,
                    email_address=quote.prospect.email,
                    sent_at=datetime.now().isoformat(),
                    status=EmailStatus.SENT,
                    sales_rep=quote.sales_rep,
                    phone=quote.prospect.phone,
                )
            except WhatsAppSendError as e:
                logger.warning(
                    "WhatsApp send failed for quote %s, falling back to email: %s",
                    quote.id,
                    e,
                )
            except Exception as e:
                logger.exception(
                    "Unexpected error in WhatsApp send for quote %s, "
                    "falling back to email",
                    quote.id,
                )

        try:
            self._send_email(
                quote,
                day_diff,
                transaction_id,
                fallback_from=MessageChannel.WHATSAPP if attempted_wa else None,
            )
            return MessageTransaction(
                id=transaction_id,
                quote_id=quote.id,
                channel=MessageChannel.EMAIL,
                email_address=quote.prospect.email,
                sent_at=datetime.now().isoformat(),
                status=EmailStatus.SENT,
                sales_rep=quote.sales_rep,
                phone=quote.prospect.phone,
                fallback_from=(
                    MessageChannel.WHATSAPP if attempted_wa else None
                ),
            )
        except Exception as e:
            logger.error(
                "Error sending email for quote %s: %s",
                quote.id,
                e,
                exc_info=True,
            )
            return None

    def _send_rescue_email(self, rescue_quote: Quote) -> None:
        rendered_rescue_email = self._render_rescue_template(rescue_quote)
        unique_recipients = list(
            set(
                [
                    self.rescue_email_config.sales_rep_recipient.email,
                    rescue_quote.sales_rep.email,
                ]
            )
        )
        subject = f"{self.rescue_email_config.subject} - {rescue_quote.id}"
        response = self.ses_client.send_email(
            Source=self.sender_email,
            Destination={"ToAddresses": unique_recipients},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
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
                {
                    "Name": "SalesRepId",
                    "Value": rescue_quote.sales_rep.id or "unknown",
                },
            ],
        )
        logger.info(
            "Rescue email sent for quote %s, MessageId: %s",
            rescue_quote.id,
            response["MessageId"],
        )

    def send_messages(self) -> None:
        transactions: List[MessageTransaction] = []
        for quote in self.quotes:
            tx = self._send_cadence_message(quote)
            if tx is not None:
                transactions.append(tx)

        for rescue_quote in self.rescue_email_config.rescue_emails:
            if self.rescue_email_config.sales_rep_recipient.is_empty():
                logger.warning(
                    "Skipping rescue email for quote %s because sales rep "
                    "recipient is empty",
                    rescue_quote.id,
                )
                continue
            try:
                self._send_rescue_email(rescue_quote)
            except Exception as e:
                logger.error(
                    "Error sending rescue email for quote %s: %s",
                    rescue_quote.id,
                    e,
                    exc_info=True,
                )

        if transactions:
            try:
                self._batch_write_transactions(transactions)
                logger.info(
                    "Wrote %d message transactions to DynamoDB",
                    len(transactions),
                )
            except Exception:
                logger.exception(
                    "Failed to write %d message transactions to DynamoDB. "
                    "Sends already succeeded; manual reconciliation may be needed.",
                    len(transactions),
                )
