import unittest

from model import (
    MessageChannel,
    MessageTransaction,
    EmailStatus,
    SalesRep,
)


class TestMessageTransaction(unittest.TestCase):
    def setUp(self):
        self.rep = SalesRep(
            id="1", name="Rep", email="r@x.com", phone_number="8112345678"
        )

    def test_email_channel_item_has_email_fields(self):
        tx = MessageTransaction(
            id="tx-1",
            quote_id="q-1",
            channel=MessageChannel.EMAIL,
            email_address="customer@example.com",
            phone=None,
            sent_at="2026-06-10T12:00:00",
            status=EmailStatus.SENT,
            sales_rep=self.rep,
        )
        item = tx.to_dynamodb_item()
        self.assertEqual(item["transaction_id"], "tx-1")
        self.assertEqual(item["quote_id"], "q-1")
        self.assertEqual(item["channel"], "email")
        self.assertEqual(item["email_address"], "customer@example.com")
        self.assertNotIn("phone", item)
        self.assertNotIn("fallback_from", item)
        self.assertEqual(item["status"], "Sent")

    def test_whatsapp_channel_item_has_phone(self):
        tx = MessageTransaction(
            id="tx-2",
            quote_id="q-2",
            channel=MessageChannel.WHATSAPP,
            email_address="customer@example.com",
            phone="+528112345678",
            sent_at="2026-06-10T12:00:00",
            status=EmailStatus.SENT,
            sales_rep=self.rep,
        )
        item = tx.to_dynamodb_item()
        self.assertEqual(item["channel"], "whatsapp")
        self.assertEqual(item["phone"], "+528112345678")
        self.assertEqual(item["email_address"], "customer@example.com")

    def test_fallback_from_recorded_when_set(self):
        tx = MessageTransaction(
            id="tx-3",
            quote_id="q-3",
            channel=MessageChannel.EMAIL,
            email_address="customer@example.com",
            phone="+528112345678",
            sent_at="2026-06-10T12:00:00",
            status=EmailStatus.SENT,
            sales_rep=self.rep,
            fallback_from=MessageChannel.WHATSAPP,
        )
        item = tx.to_dynamodb_item()
        self.assertEqual(item["fallback_from"], "whatsapp")
        self.assertEqual(item["channel"], "email")


if __name__ == "__main__":
    unittest.main()
