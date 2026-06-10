# WhatsApp Reminders Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route prospect-facing reminders on days 7/14/21 through the Meta WhatsApp Business Cloud API, with automatic email fallback when no phone is available or WhatsApp fails at runtime. Day-28 sales-rep rescue email stays as email.

**Architecture:** Unified `QuoteReminderSender` (replacing `QuoteEmailSender`) dispatches per quote between WhatsApp and SES email. Phone numbers are parsed from `clientes.DBF` / `PROSPECT.DBF` and normalized to E.164 (Mexican default). A new `WhatsAppClient` wraps the Meta Cloud API using `urllib`. Credentials come from AWS Secrets Manager; template names come from Lambda env vars. Transactions go to the existing DynamoDB table with a new `channel` field.

**Tech Stack:** Python 3.13 (Lambda), `unittest` for tests, `boto3` for AWS, `urllib` for HTTP (no new pip deps), AWS CDK (TypeScript) for infra.

**Spec:** [docs/superpowers/specs/2026-06-10-whatsapp-reminders-design.md](../specs/2026-06-10-whatsapp-reminders-design.md)

**Commit convention:** Match the repo's existing style — `feat:`, `fix:`, `chore:`, `docs:` prefixes; subject under 70 chars; no Co-Authored-By trailer.

**File structure (final state):**

| Path | Status | Responsibility |
|---|---|---|
| `lambda/crm-sync-quotes/phone.py` | NEW | DBF phone field → E.164 normalization |
| `lambda/crm-sync-quotes/whatsapp.py` | NEW | Meta Cloud API client, `WhatsAppSendError` |
| `lambda/crm-sync-quotes/model.py` | MODIFY | `Prospect.phone`, `MessageChannel`, `MessageTransaction` |
| `lambda/crm-sync-quotes/parser.py` | MODIFY | Read DBF phone fields, set `Prospect.phone` |
| `lambda/crm-sync-quotes/sender.py` | MODIFY | Rename to `QuoteReminderSender`, add routing |
| `lambda/crm-sync-quotes/main.py` | MODIFY | Load secret, build `WhatsAppClient`, wire deps |
| `lambda/crm-sync-quotes/test/test_phone.py` | NEW | Normalization tests |
| `lambda/crm-sync-quotes/test/test_whatsapp.py` | NEW | Client request shape + error tests |
| `lambda/crm-sync-quotes/test/test_model.py` | NEW | `MessageTransaction.to_dynamodb_item` shape |
| `lambda/crm-sync-quotes/test/test_sender.py` | REWRITE (file empty today) | Routing decision-table tests |
| `lib/constructs/crm-ingestion-construct.ts` | MODIFY | Secrets Manager secret, IAM grant, env vars |
| `lib/crm-infra-stack.ts` | MODIFY | Pass WA template env vars + secret ARN |

**Running tests during this plan:**

```bash
cd lambda/crm-sync-quotes
source .venv/bin/activate
python -m unittest discover -s test -t .
```

Run individual test files with `python -m unittest test.test_phone -v`.

---

## Task 1: Phone normalization module

**Files:**
- Create: `lambda/crm-sync-quotes/phone.py`
- Create: `lambda/crm-sync-quotes/test/test_phone.py`

- [ ] **Step 1.1: Write the failing test file**

Create `lambda/crm-sync-quotes/test/test_phone.py`:

```python
import unittest

from phone import normalize_to_e164


class TestNormalizeToE164(unittest.TestCase):
    def test_movil_with_ladam_happy_path(self):
        # MOVIL 10 digits, no LADAM needed
        self.assertEqual(
            normalize_to_e164("8112345678", "", "", "", "", ""),
            "+528112345678",
        )

    def test_movil_with_short_movil_and_ladam(self):
        # 7-digit MOVIL + 3-digit LADAM => 10-digit national => +52 prefix
        self.assertEqual(
            normalize_to_e164("1234567", "811", "", "", "", ""),
            "+528111234567",
        )

    def test_movil_already_country_prefixed(self):
        self.assertEqual(
            normalize_to_e164("528112345678", "", "", "", "", ""),
            "+528112345678",
        )

    def test_movil_with_punctuation_stripped(self):
        self.assertEqual(
            normalize_to_e164("(811) 234-5678", "", "", "", "", ""),
            "+528112345678",
        )

    def test_falls_through_to_tel1_when_movil_empty(self):
        self.assertEqual(
            normalize_to_e164("", "", "1234567", "", "", "811"),
            "+528111234567",
        )

    def test_falls_through_to_tel2_when_tel1_unrecoverable(self):
        # TEL1 missing LADA -> rejected; TEL2 has its own LADA pair
        self.assertEqual(
            normalize_to_e164("", "", "1234567", "8112345678", "", ""),
            "+528112345678",
        )

    def test_falls_through_to_tel3(self):
        self.assertEqual(
            normalize_to_e164("", "", "", "", "8112345678", ""),
            "+528112345678",
        )

    def test_seven_digit_landline_without_lada_returns_none(self):
        # The "5121-855" sample from the design doc - rejected, not guessed
        self.assertIsNone(normalize_to_e164("", "", "5121855", "", "", ""))

    def test_all_empty_returns_none(self):
        self.assertIsNone(normalize_to_e164("", "", "", "", "", ""))

    def test_leading_zeros_stripped(self):
        self.assertEqual(
            normalize_to_e164("08112345678", "", "", "", "", ""),
            "+528112345678",
        )

    def test_non_digit_garbage_returns_none(self):
        self.assertIsNone(normalize_to_e164("abc---", "", "", "", "", ""))

    def test_too_few_digits_after_concat_returns_none(self):
        # 4 digits total - cannot be a valid Mexican mobile
        self.assertIsNone(normalize_to_e164("1234", "", "", "", "", ""))

    def test_too_many_digits_returns_none(self):
        # 15 digits is out of range
        self.assertIsNone(normalize_to_e164("123456789012345", "", "", "", "", ""))

    def test_custom_default_country_code(self):
        self.assertEqual(
            normalize_to_e164(
                "5551234567", "", "", "", "", "", default_country_code="1"
            ),
            "+15551234567",
        )


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 1.2: Run test to verify it fails**

```bash
cd lambda/crm-sync-quotes
source .venv/bin/activate
python -m unittest test.test_phone -v
```

Expected: `ModuleNotFoundError: No module named 'phone'`.

- [ ] **Step 1.3: Write `phone.py` to make tests pass**

Create `lambda/crm-sync-quotes/phone.py`:

```python
import re
from typing import Optional, List, Tuple


_DIGITS_RE = re.compile(r"\D+")


def _digits_only(s: str) -> str:
    return _DIGITS_RE.sub("", s or "")


def _try_candidate(
    number: str, area_code: str, default_country_code: str
) -> Optional[str]:
    """Return E.164 string or None for one (number, area_code) pair."""
    digits = _digits_only(number)
    if not digits:
        return None

    expected_len_with_cc = len(default_country_code) + 10

    # Already country-prefixed?
    if (
        len(digits) == expected_len_with_cc
        and digits.startswith(default_country_code)
    ):
        return f"+{digits}"

    # Concatenate area code + number, strip leading zeros
    combined = (_digits_only(area_code) + digits).lstrip("0")

    if len(combined) == 10:
        return f"+{default_country_code}{combined}"

    if (
        len(combined) == expected_len_with_cc
        and combined.startswith(default_country_code)
    ):
        return f"+{combined}"

    return None


def normalize_to_e164(
    movil: str,
    ladam: str,
    tel1: str,
    tel2: str,
    tel3: str,
    lada: str,
    default_country_code: str = "52",
) -> Optional[str]:
    """
    Return an E.164 string (e.g. '+528112345678') or None if no phone field
    yields a recoverable Mexican mobile number.

    Candidate priority:
      1. (MOVIL, LADAM)
      2. (TEL1, LADA)
      3. (TEL2, LADA)
      4. (TEL3, LADA)
    """
    candidates: List[Tuple[str, str]] = [
        (movil, ladam),
        (tel1, lada),
        (tel2, lada),
        (tel3, lada),
    ]
    for number, area_code in candidates:
        result = _try_candidate(number, area_code, default_country_code)
        if result is not None:
            return result
    return None
```

- [ ] **Step 1.4: Run tests to verify they pass**

```bash
python -m unittest test.test_phone -v
```

Expected: 14 tests pass.

- [ ] **Step 1.5: Commit**

```bash
git add lambda/crm-sync-quotes/phone.py lambda/crm-sync-quotes/test/test_phone.py
git commit -m "feat: add phone normalization to E.164 for WhatsApp"
```

---

## Task 2: Add `phone` field to `Prospect`

**Files:**
- Modify: `lambda/crm-sync-quotes/model.py:24-27`

- [ ] **Step 2.1: Update `Prospect` dataclass**

Replace the `Prospect` dataclass in `lambda/crm-sync-quotes/model.py`:

```python
@dataclass
class Prospect:
    id: str
    name: str
    email: str
    phone: Optional[str] = None
```

Also add the import at the top of the file (if `Optional` is not already imported):

```python
from typing import Tuple, Optional
```

- [ ] **Step 2.2: Verify the module still imports**

```bash
cd lambda/crm-sync-quotes
source .venv/bin/activate
python -c "from model import Prospect; p = Prospect(id='1', name='x', email='y'); print(p.phone)"
```

Expected output: `None`

- [ ] **Step 2.3: Commit**

```bash
git add lambda/crm-sync-quotes/model.py
git commit -m "feat: add optional phone field to Prospect model"
```

---

## Task 3: Parser reads DBF phone fields

**Files:**
- Modify: `lambda/crm-sync-quotes/parser.py:182-202`

- [ ] **Step 3.1: Import the normalizer**

Add at the top of `lambda/crm-sync-quotes/parser.py`, near the other imports:

```python
from phone import normalize_to_e164
```

- [ ] **Step 3.2: Update `_parse_prospect_from_prospect_dbf`**

Replace the method at `parser.py:182-192` with:

```python
def _parse_prospect_from_prospect_dbf(
    self, prospect_rec: Dict
) -> Optional[Prospect]:
    """Parse a prospect record into a Prospect object."""
    cve_pros = prospect_rec.get("CVE_PROS")
    nom_pros = prospect_rec.get("NOM_PROS", "").strip()
    email_pros = prospect_rec.get("EMAIL_PROS", "").strip()
    email = self.email_extractor.extract_first_or_empty(email_pros)
    if not email:
        return None
    phone = normalize_to_e164(
        movil=str(prospect_rec.get("MOVIL_PROS") or "").strip(),
        ladam=str(prospect_rec.get("LADAM_PROS") or "").strip(),
        tel1=str(prospect_rec.get("TEL1_PROS") or "").strip(),
        tel2=str(prospect_rec.get("TEL2_PROS") or "").strip(),
        tel3=str(prospect_rec.get("TEL3_PROS") or "").strip(),
        lada=str(prospect_rec.get("LADA_PROS") or "").strip(),
    )
    return Prospect(
        id=str(cve_pros).strip(), name=nom_pros, email=email, phone=phone
    )
```

- [ ] **Step 3.3: Update `_parse_prospect_from_cliente_dbf`**

Replace the method at `parser.py:194-202` with:

```python
def _parse_prospect_from_cliente_dbf(self, client_rec: Dict) -> Optional[Prospect]:
    """Parse a client record into a Prospect object."""
    cve_cte = client_rec.get("CVE_CTE")
    nom_cte = client_rec.get("NOM_CTE", "").strip()
    email_cte = client_rec.get("EMAIL_CTE", "").strip()
    email = self.email_extractor.extract_first_or_empty(email_cte)
    if not email:
        return None
    phone = normalize_to_e164(
        movil=str(client_rec.get("MOVIL_CTE") or "").strip(),
        ladam=str(client_rec.get("LADAM_CTE") or "").strip(),
        tel1=str(client_rec.get("TEL1_CTE") or "").strip(),
        tel2=str(client_rec.get("TEL2_CTE") or "").strip(),
        tel3=str(client_rec.get("TEL3_CTE") or "").strip(),
        lada=str(client_rec.get("LADA_CTE") or "").strip(),
    )
    return Prospect(
        id=str(cve_cte).strip(), name=nom_cte, email=email, phone=phone
    )
```

- [ ] **Step 3.4: Smoke-test the parser against a real DBF**

```bash
cd lambda/crm-sync-quotes
source .venv/bin/activate
python -c "
from dbfread import DBF
from parser import QuoteParser
from phone import normalize_to_e164

# Walk a real DBF to confirm phone parsing populates non-empty values.
dbf_path = '../../cdk.out/asset.127c56c432e93d4c09743eddc99583a9ed58fc9ec6bf6ccd011114482c1bc931/test/data/PROSPECT.DBF'
for rec in DBF(dbf_path, encoding='latin1', ignore_missing_memofile=True):
    phone = normalize_to_e164(
        movil=str(rec.get('MOVIL_PROS') or '').strip(),
        ladam=str(rec.get('LADAM_PROS') or '').strip(),
        tel1=str(rec.get('TEL1_PROS') or '').strip(),
        tel2=str(rec.get('TEL2_PROS') or '').strip(),
        tel3=str(rec.get('TEL3_PROS') or '').strip(),
        lada=str(rec.get('LADA_PROS') or '').strip(),
    )
    if phone:
        print(f'{rec.get(\"CVE_PROS\")}: {phone}')
        break
print('done')
"
```

Expected: prints at least one `cve_pros: +52xxxxxxxxxx` line OR `done` if no prospect in the sample DBF has a usable phone. Either is acceptable — we're only verifying the parser does not crash.

- [ ] **Step 3.5: Commit**

```bash
git add lambda/crm-sync-quotes/parser.py
git commit -m "feat: parse phone fields from DBF prospects and clients"
```

---

## Task 4: `MessageChannel` enum and `MessageTransaction` model

**Files:**
- Modify: `lambda/crm-sync-quotes/model.py:119-144` (replace `EmailTransaction` block)
- Modify: `lambda/crm-sync-quotes/sender.py:5,114,122,158` (rename usages)
- Create: `lambda/crm-sync-quotes/test/test_model.py`

- [ ] **Step 4.1: Write the failing test file**

Create `lambda/crm-sync-quotes/test/test_model.py`:

```python
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
```

- [ ] **Step 4.2: Run test to verify it fails**

```bash
python -m unittest test.test_model -v
```

Expected: `ImportError: cannot import name 'MessageChannel' from 'model'`.

- [ ] **Step 4.3: Update `model.py` — replace `EmailTransaction` block**

In `lambda/crm-sync-quotes/model.py`, replace the existing `EmailTransaction` dataclass (the block currently spanning roughly `class EmailStatus(Enum):` through the end of `EmailTransaction.to_dynamodb_item`) with:

```python
class EmailStatus(Enum):
    NO_RESPONSE = "No Response"
    SENT = "Sent"

    def __str__(self) -> str:
        return self.value


class MessageChannel(Enum):
    EMAIL = "email"
    WHATSAPP = "whatsapp"

    def __str__(self) -> str:
        return self.value


@dataclass
class MessageTransaction:
    id: str
    quote_id: str
    channel: MessageChannel
    email_address: str
    phone: Optional[str]
    sent_at: str
    status: EmailStatus
    sales_rep: SalesRep
    fallback_from: Optional[MessageChannel] = None

    def to_dynamodb_item(self) -> dict:
        item: dict = {
            "transaction_id": self.id,
            "quote_id": self.quote_id,
            "channel": self.channel.value,
            "email_address": self.email_address,
            "sent_at": self.sent_at,
            "status": self.status.value,
            "sales_rep": self.sales_rep.to_dynamodb_item(),
        }
        if self.phone is not None:
            item["phone"] = self.phone
        if self.fallback_from is not None:
            item["fallback_from"] = self.fallback_from.value
        return item
```

Delete the old `EmailTransaction` dataclass entirely.

- [ ] **Step 4.4: Update `sender.py` to use the new symbol names (keep build green)**

In `lambda/crm-sync-quotes/sender.py`:

1. Change the import on line 5 from:
   ```python
   from model import Quote, EmailTransaction, EmailStatus, RescueEmailConfig
   ```
   to:
   ```python
   from model import (
       Quote,
       MessageTransaction,
       MessageChannel,
       EmailStatus,
       RescueEmailConfig,
   )
   ```

2. Find every occurrence of `EmailTransaction` in `sender.py` and replace it with `MessageTransaction`. There are four: the type hint in `_batch_write_transactions`, the local list type hint in `send_emails`, the constructor call inside the try-block, and any other annotation.

3. The existing constructor call:
   ```python
   email_transaction = EmailTransaction(
       id=transaction_id,
       quote_id=quote.id,
       email_address=quote.prospect.email,
       sent_at=datetime.now().isoformat(),
       status=EmailStatus.SENT,
       sales_rep=quote.sales_rep,
   )
   ```
   becomes:
   ```python
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
   ```
   (This is a temporary state — Task 6 rewrites this method entirely.)

- [ ] **Step 4.5: Run tests**

```bash
python -m unittest test.test_model test.test_phone -v
```

Expected: all 17 tests pass (3 model + 14 phone).

- [ ] **Step 4.6: Verify `sender.py` still imports cleanly**

```bash
python -c "import sender; print(sender.QuoteEmailSender)"
```

Expected: prints the class repr without `ImportError`.

- [ ] **Step 4.7: Commit**

```bash
git add lambda/crm-sync-quotes/model.py lambda/crm-sync-quotes/sender.py lambda/crm-sync-quotes/test/test_model.py
git commit -m "feat: add MessageChannel and MessageTransaction models"
```

---

## Task 5: WhatsApp client (Meta Cloud API)

**Files:**
- Create: `lambda/crm-sync-quotes/whatsapp.py`
- Create: `lambda/crm-sync-quotes/test/test_whatsapp.py`

- [ ] **Step 5.1: Write the failing test file**

Create `lambda/crm-sync-quotes/test/test_whatsapp.py`:

```python
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


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 5.2: Run test to verify it fails**

```bash
python -m unittest test.test_whatsapp -v
```

Expected: `ModuleNotFoundError: No module named 'whatsapp'`.

- [ ] **Step 5.3: Write the WhatsApp client**

Create `lambda/crm-sync-quotes/whatsapp.py`:

```python
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
```

- [ ] **Step 5.4: Run tests**

```bash
python -m unittest test.test_whatsapp -v
```

Expected: 4 tests pass.

- [ ] **Step 5.5: Commit**

```bash
git add lambda/crm-sync-quotes/whatsapp.py lambda/crm-sync-quotes/test/test_whatsapp.py
git commit -m "feat: add Meta WhatsApp Cloud API client"
```

---

## Task 6: `QuoteReminderSender` — rename + routing logic

This task is the heart of the change. We're splitting it into sub-tasks: rename first (no behavior change), then add WhatsApp routing.

**Files:**
- Modify: `lambda/crm-sync-quotes/sender.py` (full rewrite)
- Modify: `lambda/crm-sync-quotes/main.py:10,92` (import + instantiation)
- Modify: `lambda/crm-sync-quotes/test/test_sender.py` (currently empty)

### 6a. Rename `QuoteEmailSender` → `QuoteReminderSender`

- [ ] **Step 6a.1: Rename the class in `sender.py`**

In `lambda/crm-sync-quotes/sender.py`, change `class QuoteEmailSender:` to `class QuoteReminderSender:`. Also rename the `send_emails` method to `send_messages`.

- [ ] **Step 6a.2: Update the import + instantiation in `main.py`**

In `lambda/crm-sync-quotes/main.py`:
- Line 10: change `from sender import QuoteEmailSender` to `from sender import QuoteReminderSender`.
- Line 92: change `email_sender = QuoteEmailSender(` to `reminder_sender = QuoteReminderSender(`.
- Line 103: change `email_sender.send_emails()` to `reminder_sender.send_messages()`.

- [ ] **Step 6a.3: Verify imports still work**

```bash
cd lambda/crm-sync-quotes
source .venv/bin/activate
python -c "from sender import QuoteReminderSender; from main import handler; print('ok')"
```

Expected: prints `ok`.

- [ ] **Step 6a.4: Commit**

```bash
git add lambda/crm-sync-quotes/sender.py lambda/crm-sync-quotes/main.py
git commit -m "refactor: rename QuoteEmailSender to QuoteReminderSender"
```

### 6b. Add WhatsApp routing

- [ ] **Step 6b.1: Write the failing routing test file**

Create `lambda/crm-sync-quotes/test/test_sender.py` (file is currently empty):

```python
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
    Product,
    RescueEmailConfig,
    MessageChannel,
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
        # day_diff = 14 is mapped; pick day 7 and remove from templates
        quote = _make_quote(phone="+528112345678", days_ago=7)
        sender = self._build_sender([quote])
        sender.whatsapp_templates = {}  # nothing mapped

        sender.send_messages()

        self.wa_client.send_template.assert_not_called()
        sender.ses_client.send_email.assert_called_once()


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 6b.2: Run test to verify it fails**

```bash
python -m unittest test.test_sender -v
```

Expected: tests fail because `QuoteReminderSender.__init__` does not yet accept `whatsapp_client` / `whatsapp_templates`.

- [ ] **Step 6b.3: Rewrite `sender.py` with routing logic**

Replace the full contents of `lambda/crm-sync-quotes/sender.py` with:

```python
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
                    phone=quote.prospect.phone,
                    sent_at=datetime.now().isoformat(),
                    status=EmailStatus.SENT,
                    sales_rep=quote.sales_rep,
                )
            except WhatsAppSendError as e:
                logger.warning(
                    "WhatsApp send failed for quote %s, falling back to email: %s",
                    quote.id,
                    e,
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
                phone=quote.prospect.phone,
                sent_at=datetime.now().isoformat(),
                status=EmailStatus.SENT,
                sales_rep=quote.sales_rep,
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
            self._batch_write_transactions(transactions)
            logger.info(
                "Wrote %d message transactions to DynamoDB", len(transactions)
            )
```

- [ ] **Step 6b.4: Run sender tests**

```bash
python -m unittest test.test_sender -v
```

Expected: 5 tests pass.

- [ ] **Step 6b.5: Run the entire test suite to verify nothing else broke**

```bash
python -m unittest discover -s test -t . -v
```

Expected: all tests pass (phone: 14, model: 3, sender: 5, whatsapp: 4, extractor: existing ~15).

- [ ] **Step 6b.6: Commit**

```bash
git add lambda/crm-sync-quotes/sender.py lambda/crm-sync-quotes/test/test_sender.py
git commit -m "feat: route reminders via WhatsApp with email fallback"
```

---

## Task 7: Lambda wiring — load secret + build `WhatsAppClient`

**Files:**
- Modify: `lambda/crm-sync-quotes/main.py`

- [ ] **Step 7.1: Update `main.py` to load Meta credentials from Secrets Manager and wire the `WhatsAppClient`**

Apply these edits to `lambda/crm-sync-quotes/main.py`:

1. Add new imports near the top (after the existing imports):

```python
import json
from whatsapp import WhatsAppClient
```

2. Add new env-var constants alongside the existing constants near `TABLE_NAME`:

```python
WHATSAPP_SECRET_ARN = "WHATSAPP_SECRET_ARN"
WHATSAPP_TEMPLATE_DAY_7 = "WHATSAPP_TEMPLATE_DAY_7"
WHATSAPP_TEMPLATE_DAY_14 = "WHATSAPP_TEMPLATE_DAY_14"
WHATSAPP_TEMPLATE_DAY_21 = "WHATSAPP_TEMPLATE_DAY_21"
```

3. Add a module-level cache and a helper, immediately above `def handler(event, context):`:

```python
_whatsapp_client_cache: dict = {}


def _load_whatsapp_client() -> tuple[WhatsAppClient, str]:
    """
    Load the Meta WhatsApp credentials from Secrets Manager and return a
    (client, language_code) tuple. Cached on the module for warm invocations.
    """
    if "client" in _whatsapp_client_cache:
        return (
            _whatsapp_client_cache["client"],
            _whatsapp_client_cache["language_code"],
        )

    secret_arn = safe_get_env(WHATSAPP_SECRET_ARN)
    sm = boto3.client("secretsmanager")
    response = sm.get_secret_value(SecretId=secret_arn)
    secret = json.loads(response["SecretString"])

    client = WhatsAppClient(
        access_token=secret["access_token"],
        phone_number_id=secret["phone_number_id"],
    )
    language_code = secret.get("language_code", "es_MX")

    _whatsapp_client_cache["client"] = client
    _whatsapp_client_cache["language_code"] = language_code
    return client, language_code
```

4. Inside `handler`, just before the `QuoteEmailSender`/`QuoteReminderSender` instantiation, add:

```python
    whatsapp_client, whatsapp_language_code = _load_whatsapp_client()
    whatsapp_templates: Dict[int, str] = {
        7: safe_get_env(WHATSAPP_TEMPLATE_DAY_7),
        14: safe_get_env(WHATSAPP_TEMPLATE_DAY_14),
        21: safe_get_env(WHATSAPP_TEMPLATE_DAY_21),
    }
```

5. Update the `QuoteReminderSender(...)` call to pass the new arguments:

```python
    reminder_sender = QuoteReminderSender(
        quotes=filtered_quotes,
        template_path=TEMPLATE_PATH,
        sender_email=safe_get_env(SENDER),
        transactions_table=transactions_table,
        domain=safe_get_env(DOMAIN),
        email_subject_config=EMAIL_SUBJECT_CONFIG,
        ecommerce_url=safe_get_env(ECOMMERCE_URL),
        rescue_email_config=email_rescue_config,
        configuration_set_name=safe_get_env(SES_CONFIGURATION_SET),
        whatsapp_client=whatsapp_client,
        whatsapp_templates=whatsapp_templates,
        whatsapp_language_code=whatsapp_language_code,
    )
```

- [ ] **Step 7.2: Verify the lambda module still imports**

```bash
cd lambda/crm-sync-quotes
source .venv/bin/activate
python -c "import main; print('handler =', main.handler)"
```

Expected: prints `handler = <function handler at 0x...>` with no import error.

- [ ] **Step 7.3: Run the full test suite to confirm nothing broke**

```bash
python -m unittest discover -s test -t . -v
```

Expected: same set of tests pass as in Task 6.

- [ ] **Step 7.4: Commit**

```bash
git add lambda/crm-sync-quotes/main.py
git commit -m "feat: wire WhatsApp client from Secrets Manager in handler"
```

---

## Task 8: CDK — Secrets Manager secret + IAM grant + env vars

**Files:**
- Modify: `lib/constructs/crm-ingestion-construct.ts`
- Modify: `lib/crm-infra-stack.ts`

- [ ] **Step 8.1: Add the Secrets Manager secret + grant + env vars in the construct**

In `lib/constructs/crm-ingestion-construct.ts`:

1. Add the import near the other CDK imports at the top:

```typescript
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
```

2. Inside the `CrmIngestion` class constructor, immediately after `this.bucket` is created and before `this.processor` is created, add:

```typescript
    const whatsappSecret = new secretsmanager.Secret(this, "WhatsAppSecret", {
      secretName: "crm/whatsapp/meta",
      description:
        "Meta WhatsApp Cloud API credentials (access_token, phone_number_id, language_code). Populate manually in the AWS console.",
    });
```

3. Extend the Lambda's `environment` block to include the secret ARN:

```typescript
      environment: {
        TABLE_NAME: this.table.tableName,
        WHATSAPP_SECRET_ARN: whatsappSecret.secretArn,
        ...props.lambdaEnvVars,
      },
```

4. After the existing `this.bucket.grantRead(this.processor);` line, add:

```typescript
    whatsappSecret.grantRead(this.processor);
```

5. Add an `CfnOutput` near the existing outputs:

```typescript
    new cdk.CfnOutput(this, "WhatsAppSecretArn", {
      value: whatsappSecret.secretArn,
      description:
        "Secrets Manager ARN for Meta WhatsApp credentials (populate in console)",
    });
```

- [ ] **Step 8.2: Pass the WhatsApp template env vars from the stack**

In `lib/crm-infra-stack.ts`, extend the `lambdaEnvVars` object passed to `CrmIngestion`:

```typescript
      lambdaEnvVars: {
        SENDER_EMAIL: "contacto@" + DOMAIN,
        DOMAIN: "https://" + SUBDOMAIN + "." + DOMAIN,
        OPT_OUT_TABLE_NAME: optOutsTable.tableName,
        ECOMMERCE_URL: ECOMMERCE_URL,
        SES_CONFIGURATION_SET: SES_CONFIGURATION_SET,
        WHATSAPP_TEMPLATE_DAY_7: "reminder_day_7",
        WHATSAPP_TEMPLATE_DAY_14: "reminder_day_14",
        WHATSAPP_TEMPLATE_DAY_21: "reminder_day_21",
      },
```

(The template names must match what's approved in Meta Business Manager. The values shown here are placeholders to be updated once the actual template names are approved.)

- [ ] **Step 8.3: Run `cdk synth` to verify the stack compiles**

```bash
npx cdk synth --quiet
```

Expected: synth completes without errors. The diff should include a new `AWS::SecretsManager::Secret` resource and the new env vars on the Lambda.

- [ ] **Step 8.4: Sanity-check the CloudFormation diff**

```bash
npx cdk diff
```

Expected: new `WhatsAppSecret` resource, new IAM policy statement granting `secretsmanager:GetSecretValue` to the Lambda role, three new env vars on the processor Lambda.

- [ ] **Step 8.5: Commit**

```bash
git add lib/constructs/crm-ingestion-construct.ts lib/crm-infra-stack.ts
git commit -m "feat: provision WhatsApp secret and env vars for sync-quotes lambda"
```

---

## Task 9: Post-deploy manual integration check

This task is not code — it's the runbook the operator follows after `cdk deploy`.

- [ ] **Step 9.1: Populate the Secrets Manager secret**

In the AWS console, open Secrets Manager → `crm/whatsapp/meta` → "Retrieve secret value" → "Edit" → paste:

```json
{
  "access_token": "<Meta access token>",
  "phone_number_id": "<Meta phone number ID>",
  "language_code": "es_MX"
}
```

- [ ] **Step 9.2: Confirm the three approved templates exist in Meta Business Manager**

In Meta Business Manager → WhatsApp Manager → Message Templates, verify the three templates (`reminder_day_7`, `reminder_day_14`, `reminder_day_21`, or whatever names were configured in `crm-infra-stack.ts`) are approved with one body component that takes four `{{1..4}}` text parameters in order: `prospect_name`, `quote_id`, `amount`, `tracking_url`.

- [ ] **Step 9.3: One-off end-to-end test**

Pick a single quote in DynamoDB whose prospect has a known mobile number you control. Upload a single-quote ZIP to the ingestion bucket and confirm:

- CloudWatch logs show `WhatsApp message sent to +52... id=wamid....`
- The DynamoDB transactions table has a new item with `channel="whatsapp"`.
- You received the WhatsApp message on the test phone.

- [ ] **Step 9.4: One-off fallback test**

Repeat the test with a quote whose prospect has no phone (or a malformed phone) and confirm:
- CloudWatch logs do NOT show a WhatsApp send.
- The DynamoDB item has `channel="email"`, no `phone`, no `fallback_from`.
- The email arrives as before.

---

## Self-Review (run by plan author after writing)

**Spec coverage:**

| Spec section | Plan task(s) |
|---|---|
| Phone normalization | Task 1 |
| `Prospect.phone` model field | Task 2 |
| Parser reads DBF phone fields | Task 3 |
| `MessageChannel` / `MessageTransaction` | Task 4 |
| `WhatsAppClient` (Meta Cloud API) | Task 5 |
| `QuoteReminderSender` rename + routing | Task 6 (6a + 6b) |
| Lambda secret loading + wiring | Task 7 |
| CDK Secrets Manager + env vars + IAM | Task 8 |
| Failure / fallback semantics | Tested explicitly in Task 6b |
| Day-28 rescue email unchanged | Preserved in Task 6b (`_send_rescue_email`) |
| Operator runbook | Task 9 |

**Placeholder scan:** No TBDs or "implement later" markers. Every code step has full code. The template names in Task 8 are explicitly flagged as placeholders to be updated once Meta approves real templates — that's a known runbook step, not a code gap.

**Type / name consistency:**
- `QuoteReminderSender` (Task 6a) and `send_messages` consistent across Tasks 6, 7.
- `MessageTransaction` constructor signature in Task 4 matches its callers in Task 6b.
- `whatsapp_client`, `whatsapp_templates`, `whatsapp_language_code` parameter names match across sender (6b), main.py (7), and tests (6b).
- `MessageChannel.EMAIL`/`WHATSAPP` enum values (`"email"`/`"whatsapp"`) match the test assertions in Tasks 4 and 6b.
- `WhatsAppClient.send_template` signature in Task 5 matches the call site in Task 6b.
