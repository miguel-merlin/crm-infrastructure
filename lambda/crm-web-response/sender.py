import boto3
from model import EmailTransaction, ResponseRecord
from jinja2 import Template
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ResponseEmailSender:
    def __init__(
        self,
        template_path: str,
        sender_email: str,
    ) -> None:
        self.ses_client = boto3.client("ses")
        self.sender_email = sender_email
        try:
            with open(template_path, "r", encoding="utf-8") as f:
                template_content = f.read()
            self.template: Template = Template(template_content)
        except Exception as e:
            raise ValueError(f"Error reading email template: {str(e)}") from e

    def _render_template(
        self, response_record: ResponseRecord, email_transaction: EmailTransaction
    ) -> str:
        """Render the email template with quote data."""
        if not email_transaction.sales_rep:
            raise ValueError(
                "EmailTransaction must have a SalesRep to render template."
            )
        return self.template.render(
            sales_rep_name=email_transaction.sales_rep.name,
            quote_id=response_record.email_transaction_id,
            response=response_record.response_type,
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
                        "Data": f"Respuesta recibida para la cotización {email_transaction.quote_id}",
                        "Charset": "UTF-8",
                    },
                    "Body": {
                        "Text": {"Data": body_text, "Charset": "UTF-8"},
                        "Html": {"Data": rendered_email, "Charset": "UTF-8"},
                    },
                },
            )
            logger.info(
                f"Email sent to {email_transaction.sales_rep.email} for quote {email_transaction.quote_id}, MessageId: {response['MessageId']}"
            )
        except Exception as e:
            logger.error(
                f"Error sending email to {email_transaction.sales_rep.email} for quote {email_transaction.quote_id}: {str(e)}",
                exc_info=True,
            )
