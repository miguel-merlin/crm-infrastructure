import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

from sender import QuoteReminderSender
from model import (
    Quote,
    Prospect,
    SalesRep,
    QuoteStatus,
    CustomerType,
    RescueEmailConfig,
)
from whatsapp import WhatsAppSendError


def _make_quote(*, phone, days_ago=7, quote_id="Q-1"):
    created_at = (datetime.now() - timedelta(days=days_ago)).isoformat()
    return Quote(
        id=quote_id,
        customer_type=CustomerType.PROSPECT,
        prospect=Prospect(id="p-1", name="Alice", email="a@x.com", phone=phone),
        sales_rep=SalesRep(
            id="8", name="Rep", email="rep@x.com", phone_number="811"
        ),
        products=[],
        amount=1234.56,
        status=QuoteStatus.SENT,
        created_at=created_at,
    )


class TestReminderRouting(unittest.TestCase):
    def setUp(self):
        # Patch open() before constructor reads template files.
        self.patcher = patch(
            "builtins.open",
            MagicMock(
                return_value=MagicMock(
                    __enter__=MagicMock(
                        return_value=MagicMock(read=MagicMock(return_value=""))
                    ),
                    __exit__=MagicMock(return_value=False),
                )
            ),
        )
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

        self.wa_client = MagicMock()
        self.transactions_table = MagicMock()
        self.batch_ctx = MagicMock()
        self.transactions_table.batch_writer.return_value.__enter__.return_value = (
            self.batch_ctx
        )

        self.rescue_cfg = RescueEmailConfig(
            rescue_emails=[],
            subject="rescue",
            sales_rep_recipient=SalesRep.get_empty("1"),
            template_path="assets/template_rescue.html",
        )

        self.wa_templates = {
            7: "reminder_day_7",
            14: "reminder_day_14",
            21: "reminder_day_21",
        }

    def _build_sender(self, quotes):
        sender = QuoteReminderSender(
            quotes=quotes,
            template_path="assets/template.html",
            sender_email="from@x.com",
            transactions_table=self.transactions_table,
            domain="https://www.example.com",
            email_subject_config={7: "s7", 14: "s14", 21: "s21"},
            ecommerce_url="https://shop.example.com",
            rescue_email_config=self.rescue_cfg,
            configuration_set_name="cfg-set",
            whatsapp_client=self.wa_client,
            whatsapp_templates=self.wa_templates,
        )
        sender.ses_client = MagicMock()
        sender.ses_client.send_email.return_value = {"MessageId": "ses-1"}
        return sender

    def test_phone_present_and_whatsapp_success_no_email_sent(self):
        quote = _make_quote(phone="+528112345678")
        sender = self._build_sender([quote])
        self.wa_client.send_template.return_value = {
            "messages": [{"id": "wamid.x"}]
        }

        sender.send_messages()

        self.wa_client.send_template.assert_called_once()
        sender.ses_client.send_email.assert_not_called()
        put_calls = self.batch_ctx.put_item.call_args_list
        self.assertEqual(len(put_calls), 1)
        item = put_calls[0].kwargs["Item"]
        self.assertEqual(item["channel"], "whatsapp")
        self.assertEqual(item["phone"], "+528112345678")
        self.assertNotIn("fallback_from", item)

    def test_no_phone_falls_to_email_no_whatsapp_attempted(self):
        quote = _make_quote(phone=None)
        sender = self._build_sender([quote])

        sender.send_messages()

        self.wa_client.send_template.assert_not_called()
        sender.ses_client.send_email.assert_called_once()
        item = self.batch_ctx.put_item.call_args.kwargs["Item"]
        self.assertEqual(item["channel"], "email")
        self.assertNotIn("phone", item)
        self.assertNotIn("fallback_from", item)

    def test_whatsapp_failure_falls_back_to_email(self):
        quote = _make_quote(phone="+528112345678")
        sender = self._build_sender([quote])
        self.wa_client.send_template.side_effect = WhatsAppSendError("Meta down")

        sender.send_messages()

        self.wa_client.send_template.assert_called_once()
        sender.ses_client.send_email.assert_called_once()
        item = self.batch_ctx.put_item.call_args.kwargs["Item"]
        self.assertEqual(item["channel"], "email")
        self.assertEqual(item["phone"], "+528112345678")
        self.assertEqual(item["fallback_from"], "whatsapp")

    def test_email_fallback_send_includes_fallback_tag(self):
        quote = _make_quote(phone="+528112345678")
        sender = self._build_sender([quote])
        self.wa_client.send_template.side_effect = WhatsAppSendError("Meta down")

        sender.send_messages()

        call = sender.ses_client.send_email.call_args
        tags = call.kwargs["Tags"]
        tag_names = {t["Name"]: t["Value"] for t in tags}
        self.assertEqual(tag_names.get("Fallback"), "whatsapp")

    def test_unmapped_day_routes_to_email(self):
        # day_diff = 7 is mapped; remove from templates to force unmapped case
        quote = _make_quote(phone="+528112345678", days_ago=7)
        sender = self._build_sender([quote])
        sender.whatsapp_templates = {}  # nothing mapped

        sender.send_messages()

        self.wa_client.send_template.assert_not_called()
        sender.ses_client.send_email.assert_called_once()


if __name__ == "__main__":
    unittest.main()
