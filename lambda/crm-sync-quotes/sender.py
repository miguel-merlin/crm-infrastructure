import boto3
from typing import List, Set, Dict
from datetime import datetime
from model import Quote
from jinja2 import Template
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class QuoteEmailSender:
    def __init__(
        self,
        quotes: List[Quote],
        email_cadence_config: Set[int],
        template_path: str,
        sender_email: str,
    ) -> None:
        self.quotes = quotes
        self.email_cadence_config = email_cadence_config
        self.ses_client = boto3.client("ses")
        self.sender_email = sender_email
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

    def _render_template(self, quote: Quote) -> str:
        """Render the email template with quote data."""
        return self.template.render(
            quote_id=quote.id,
            prospect_name=quote.prospect.name,
            amount=quote.amount,
            status=str(quote.status),
            created_at=quote.created_at,
        )

    def send_emails(self) -> None:
        """Send emails for the filtered quotes."""
        filtered_quotes: Dict[int, Quote] = self._filter_quotes()
        for quote in filtered_quotes.values():
            rendered_email = self._render_template(quote)
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
            except Exception as e:
                logger.error(
                    f"Error sending email to {quote.prospect.email} for quote {quote.id}: {str(e)}",
                    exc_info=True,
                )
