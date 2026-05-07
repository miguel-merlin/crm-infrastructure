import boto3
from typing import Tuple
from model import EmailTransaction, ResponseRecord, ResponseType
from jinja2 import Template
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

RESPONSE_LABELS = {
    ResponseType.BUY: "CLIENTE QUIERE COMPRAR",
    ResponseType.MORE_INFO: "CLIENTE QUIERE MÁS INFORMACIÓN",
    ResponseType.NOT_INTERESTED: "CLIENTE NO ESTÁ INTERESADO",
}

RESPONSE_KEYS = {
    ResponseType.BUY: "buy",
    ResponseType.MORE_INFO: "more_info",
    ResponseType.NOT_INTERESTED: "not_interested",
}


class ResponseEmailSender:
    def __init__(
        self,
        template_path: str,
        sender_email: str,
        configuration_set_name: str,
    ) -> None:
        self.ses_client = boto3.client("ses")
        self.sender_email = sender_email
        self.configuration_set_name = configuration_set_name
        try:
            with open(template_path, "r", encoding="utf-8") as f:
                template_content = f.read()
            self.template: Template = Template(template_content)
        except Exception as e:
            raise ValueError(f"Error reading email template: {str(e)}") from e

    def _normalize_response(self, raw: str) -> Tuple[str, str]:
        """
        Returns (response_key, response_label)
        """
        rt = ResponseType.from_string(raw)
        if not rt:
            # fallback
            return "unknown", (raw or "RESPUESTA NO RECONOCIDA").upper()

        return RESPONSE_KEYS[rt], RESPONSE_LABELS[rt]

    def _render_template(
        self, response_record: ResponseRecord, email_transaction: EmailTransaction
    ) -> str:
        """Render the email template with quote data."""
        if not email_transaction.sales_rep:
            raise ValueError(
                "EmailTransaction must have a SalesRep to render template."
            )
        response_key, response_label = self._normalize_response(
            response_record.response_type
        )
        return self.template.render(
            sales_rep_name=email_transaction.sales_rep.name,
            quote_id=email_transaction.quote_id,
            response_key=response_key,
            response_label=response_label,
        )

    def send_emails(
        self, response_record: ResponseRecord, email_transaction: EmailTransaction
    ) -> None:
        """Send emails for the filtered quotes."""
        rendered_email = self._render_template(response_record, email_transaction)
        body_text = "Ha recibido una respuesta a su cotización."
        if not email_transaction.sales_rep:
            raise ValueError("EmailTransaction must have a SalesRep to send email.")
        try:
            response = self.ses_client.send_email(
                Source=self.sender_email,
                Destination={"ToAddresses": [email_transaction.sales_rep.email]},
                Message={
                    "Subject": {
                        "Data": f"To cotización {email_transaction.quote_id} necesita ser atendida",
                        "Charset": "UTF-8",
                    },
                    "Body": {
                        "Text": {"Data": body_text, "Charset": "UTF-8"},
                        "Html": {"Data": rendered_email, "Charset": "UTF-8"},
                    },
                },
                ConfigurationSetName=self.configuration_set_name,
                Tags=[{"Name": "EmailType", "Value": "response-notification"}],
            )
            logger.info(
                f"Email sent to {email_transaction.sales_rep.email} for quote {email_transaction.quote_id}, MessageId: {response['MessageId']}"
            )
        except Exception as e:
            logger.error(
                f"Error sending email to {email_transaction.sales_rep.email} for quote {email_transaction.quote_id}: {str(e)}",
                exc_info=True,
            )
