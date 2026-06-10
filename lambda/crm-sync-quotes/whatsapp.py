import json
import logging
import urllib.error
import urllib.request
from typing import List

logger = logging.getLogger(__name__)


META_API_VERSION = "v21.0"


class WhatsAppSendError(Exception):
    """Raised when a WhatsApp send fails for any reason."""


class WhatsAppClient:
    def __init__(
        self,
        access_token: str,
        phone_number_id: str,
        timeout_seconds: int = 10,
    ) -> None:
        self.access_token = access_token
        self.phone_number_id = phone_number_id
        self.timeout_seconds = timeout_seconds

    def _endpoint(self) -> str:
        return (
            f"https://graph.facebook.com/{META_API_VERSION}/"
            f"{self.phone_number_id}/messages"
        )

    def send_template(
        self,
        to: str,
        template_name: str,
        language_code: str,
        params: List[str],
    ) -> dict:
        """
        Send an approved WhatsApp template message.
        Returns the parsed JSON response on success.
        Raises WhatsAppSendError on any failure.
        """
        payload = {
            "messaging_product": "whatsapp",
            "to": to,
            "type": "template",
            "template": {
                "name": template_name,
                "language": {"code": language_code},
                "components": [
                    {
                        "type": "body",
                        "parameters": [
                            {"type": "text", "text": p} for p in params
                        ],
                    }
                ],
            },
        }

        request = urllib.request.Request(
            url=self._endpoint(),
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
        )
        request.add_header("Authorization", f"Bearer {self.access_token}")
        request.add_header("Content-type", "application/json")

        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout_seconds
            ) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace") if e.fp else ""
            raise WhatsAppSendError(
                f"Meta HTTP {e.code}: {err_body or e.reason}"
            ) from e
        except (urllib.error.URLError, OSError) as e:
            raise WhatsAppSendError(f"Network error calling Meta: {e}") from e

        try:
            parsed = json.loads(body)
        except ValueError as e:
            raise WhatsAppSendError(
                f"Non-JSON response from Meta: {body[:200]}"
            ) from e

        if "error" in parsed:
            err = parsed["error"]
            raise WhatsAppSendError(
                f"Meta error code {err.get('code')}: {err.get('message')}"
            )

        if "messages" in parsed:
            msg_id = parsed["messages"][0].get("id")
            logger.info("WhatsApp message sent to %s, id=%s", to, msg_id)

        return parsed
