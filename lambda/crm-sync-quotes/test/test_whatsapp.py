import json
import unittest
from unittest.mock import patch, MagicMock
from urllib.error import HTTPError

from whatsapp import WhatsAppClient, WhatsAppSendError


def _fake_response(status: int, body: dict) -> MagicMock:
    resp = MagicMock()
    resp.status = status
    resp.read.return_value = json.dumps(body).encode("utf-8")
    resp.__enter__ = MagicMock(return_value=resp)
    resp.__exit__ = MagicMock(return_value=False)
    return resp


class TestWhatsAppClient(unittest.TestCase):
    def setUp(self):
        self.client = WhatsAppClient(
            access_token="test-token", phone_number_id="987654321"
        )

    @patch("whatsapp.urllib.request.urlopen")
    def test_send_template_builds_expected_request(self, mock_urlopen):
        mock_urlopen.return_value = _fake_response(
            200, {"messages": [{"id": "wamid.abc"}]}
        )

        result = self.client.send_template(
            to="+528112345678",
            template_name="reminder_day_7",
            language_code="es_MX",
            params=["Alice", "Q-100", "$1,234.56", "https://example.com/t/abc"],
        )

        self.assertEqual(result["messages"][0]["id"], "wamid.abc")
        request = mock_urlopen.call_args[0][0]
        self.assertEqual(
            request.full_url,
            "https://graph.facebook.com/v21.0/987654321/messages",
        )
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.headers["Authorization"], "Bearer test-token")
        self.assertEqual(request.headers["Content-type"], "application/json")
        payload = json.loads(request.data.decode("utf-8"))
        self.assertEqual(payload["messaging_product"], "whatsapp")
        self.assertEqual(payload["to"], "+528112345678")
        self.assertEqual(payload["type"], "template")
        self.assertEqual(payload["template"]["name"], "reminder_day_7")
        self.assertEqual(payload["template"]["language"]["code"], "es_MX")
        components = payload["template"]["components"]
        self.assertEqual(len(components), 1)
        self.assertEqual(components[0]["type"], "body")
        param_texts = [p["text"] for p in components[0]["parameters"]]
        self.assertEqual(
            param_texts,
            ["Alice", "Q-100", "$1,234.56", "https://example.com/t/abc"],
        )

    @patch("whatsapp.urllib.request.urlopen")
    def test_non_2xx_raises(self, mock_urlopen):
        mock_urlopen.side_effect = HTTPError(
            url="https://graph.facebook.com/v21.0/987654321/messages",
            code=400,
            msg="Bad Request",
            hdrs=None,
            fp=None,
        )
        with self.assertRaises(WhatsAppSendError):
            self.client.send_template(
                to="+528112345678",
                template_name="x",
                language_code="es_MX",
                params=[],
            )

    @patch("whatsapp.urllib.request.urlopen")
    def test_meta_error_payload_raises(self, mock_urlopen):
        mock_urlopen.return_value = _fake_response(
            200,
            {
                "error": {
                    "message": "Invalid template",
                    "code": 132000,
                }
            },
        )
        with self.assertRaises(WhatsAppSendError) as ctx:
            self.client.send_template(
                to="+528112345678",
                template_name="x",
                language_code="es_MX",
                params=[],
            )
        self.assertIn("132000", str(ctx.exception))

    @patch("whatsapp.urllib.request.urlopen")
    def test_network_exception_wrapped(self, mock_urlopen):
        mock_urlopen.side_effect = OSError("connection refused")
        with self.assertRaises(WhatsAppSendError):
            self.client.send_template(
                to="+528112345678",
                template_name="x",
                language_code="es_MX",
                params=[],
            )


    @patch("whatsapp.urllib.request.urlopen")
    def test_empty_messages_list_does_not_raise(self, mock_urlopen):
        # Meta can return {"messages": []} for unsupported phones; must not
        # raise IndexError - the caller's fallback contract depends on this.
        mock_urlopen.return_value = _fake_response(200, {"messages": []})
        result = self.client.send_template(
            to="+528112345678",
            template_name="x",
            language_code="es_MX",
            params=[],
        )
        self.assertEqual(result, {"messages": []})

    def test_init_rejects_non_digit_phone_number_id(self):
        with self.assertRaises(ValueError):
            WhatsAppClient(access_token="t", phone_number_id="abc")

    def test_init_rejects_empty_phone_number_id(self):
        with self.assertRaises(ValueError):
            WhatsAppClient(access_token="t", phone_number_id="")


class TestStubWhatsAppClient(unittest.TestCase):
    def test_send_template_always_raises(self):
        from whatsapp import StubWhatsAppClient

        stub = StubWhatsAppClient()
        with self.assertRaises(WhatsAppSendError):
            stub.send_template(
                to="+528112345678",
                template_name="any",
                language_code="es_MX",
                params=["a", "b"],
            )


if __name__ == "__main__":
    unittest.main()
