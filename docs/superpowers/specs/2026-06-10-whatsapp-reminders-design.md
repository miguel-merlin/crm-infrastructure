# WhatsApp Reminders — Design

**Date:** 2026-06-10
**Status:** Approved (pre-implementation)
**Owner:** miguel-merlin

## Goal

Replace the prospect-facing reminder emails on days 7, 14, and 21 with WhatsApp messages sent through the Meta WhatsApp Business Cloud API. Fall back to the existing SES email path when WhatsApp is impossible (no usable phone number) or fails at runtime. Leave the day-28 sales-rep rescue email unchanged.

## Scope

In scope:
- New prospect phone parsing from `clientes.DBF` / `PROSPECT.DBF`.
- New `WhatsAppClient` (Meta Cloud API) used by the existing `crm-sync-quotes` Lambda.
- Unified `QuoteReminderSender` (replacing `QuoteEmailSender`) that dispatches per quote between WhatsApp and email.
- DynamoDB transactions record a `channel` field so the unified history is readable per quote.
- CDK additions: a Secrets Manager secret for Meta credentials, three env vars for template names, IAM grant.

Out of scope (follow-ups):
- Meta delivery webhook (sent/delivered/read/failed callbacks).
- WhatsApp-initiated opt-out (e.g., honoring "STOP" replies).
- WhatsApp open/click metrics in the CloudWatch dashboard.
- Backfilling existing transaction items with a `channel` field.

## Routing rule

Per quote, per invocation:

| Phone normalizes? | WhatsApp API result | Action | Transaction recorded |
|---|---|---|---|
| No | — | Email send | `channel=EMAIL`, `phone=None`, `fallback_from=None` |
| Yes | 2xx | WhatsApp send. **No email is sent.** | `channel=WHATSAPP`, `phone=...`, `fallback_from=None` |
| Yes | Non-2xx / exception | WhatsApp logged as failed, then email send | `channel=EMAIL`, `phone=...`, `fallback_from=WHATSAPP` |
| No | — and email send fails | Error logged | Nothing recorded (matches today) |

A successful WhatsApp send terminates the flow for that quote. The WhatsApp/email choice is strictly either-or.

No retries on WhatsApp failure — one attempt, then email fallback.

## Phone normalization

New module: `lambda/crm-sync-quotes/phone.py`.

Single entry point:

```python
def normalize_to_e164(
    movil: str, ladam: str,
    tel1: str, tel2: str, tel3: str, lada: str,
    default_country_code: str = "52",
) -> Optional[str]
```

Algorithm:

1. Build candidate list, in priority order: `(MOVIL, LADAM)`, `(TEL1, LADA)`, `(TEL2, LADA)`, `(TEL3, LADA)`.
2. For each `(number, area_code)`:
   - Strip every non-digit from both. Skip if `number` is empty.
   - If `number` is already 12 digits starting with `52`, treat as fully qualified.
   - Else concatenate `area_code + number`, then strip leading zeros.
   - If the result is 10 digits, prepend `52`.
   - If the final result is 12 digits and starts with `52`, accept as `+<12 digits>`.
   - Otherwise discard.
3. Return the first accepted candidate as a `+E.164` string, else `None`.

The algorithm is deliberately strict — 7-digit landline-only numbers without a LADA are rejected rather than guessed. Rejected prospects fall through to the email path.

## Model changes

`Prospect` gains an optional E.164 phone:

```python
@dataclass
class Prospect:
    id: str
    name: str
    email: str
    phone: Optional[str] = None
```

`EmailTransaction` is renamed `MessageTransaction` and gains channel metadata. `email_address` is preserved; `phone` is added alongside it. The `channel` field tells which one was used.

```python
class MessageChannel(Enum):
    EMAIL = "email"
    WHATSAPP = "whatsapp"

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
```

DynamoDB item adds `channel`, `phone`, optional `fallback_from`. Existing items (without these fields) remain readable; readers should treat missing `channel` as `EMAIL`.

## Parser changes

`QuoteParser._parse_prospect_from_prospect_dbf` and `_parse_prospect_from_cliente_dbf` are extended to read the phone fields and call `phone.normalize_to_e164`:

- For prospects: `MOVIL_PROS`, `LADAM_PROS`, `TEL1_PROS`, `TEL2_PROS`, `TEL3_PROS`, `LADA_PROS`.
- For clients: `MOVIL_CTE`, `LADAM_CTE`, `TEL1_CTE`, `TEL2_CTE`, `TEL3_CTE`, `LADA_CTE`.

The result is assigned to `Prospect.phone`. A `None` here means the prospect must take the email path.

## `QuoteReminderSender`

Replaces `QuoteEmailSender`. Same public surface (`send_messages()`), plus two new dependencies injected from `main.py`:

```python
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
        whatsapp_templates: Dict[int, str],   # {7: "...", 14: "...", 21: "..."}
    ): ...
```

Per-quote method `_send_cadence_message(quote)`:

```
day_diff = days since quote.created_at
transaction_id = uuid4()
attempted_wa = False

if quote.prospect.phone is not None:
    attempted_wa = True
    try:
        self._send_whatsapp(quote, day_diff, transaction_id)
        record MessageTransaction(channel=WHATSAPP, phone=quote.prospect.phone, ...)
        return
    except WhatsAppSendError as e:
        logger.warning("WA send failed for quote %s, falling back to email: %s",
                       quote.id, e)

self._send_email(quote, day_diff, transaction_id)
record MessageTransaction(
    channel=EMAIL,
    email_address=quote.prospect.email,
    phone=quote.prospect.phone,
    fallback_from=MessageChannel.WHATSAPP if attempted_wa else None,
)
```

`_send_email` is the existing SES send logic, lifted from `QuoteEmailSender.send_emails` and parameterized by `transaction_id`. Existing SES tags stay (`EmailType=quote-followup`, `SalesRepId=...`). A new tag `Fallback=whatsapp` is added when the email is a WhatsApp-fallback send, so the dashboard can split fallback volume.

`_send_whatsapp(quote, day_diff, transaction_id)`:
- Looks up `whatsapp_templates[day_diff]`. If the day has no mapped template, raise `WhatsAppSendError` immediately (caller falls back to email).
- Builds template params: `prospect_name`, `quote_id`, formatted `amount`, and a tracking URL `domain + "/" + transaction_id`.
- Calls `whatsapp_client.send_template(to=quote.prospect.phone, template_name=..., language_code=..., params=[...])`.
- Raises `WhatsAppSendError` on any non-2xx HTTP status, network error, or Meta error payload.

Day-28 rescue logic (`self.rescue_email_config.rescue_emails`) is **untouched** — still email-only to the manager + assigned sales rep.

Transactions are batched and written at the end of `send_messages()`, same as today.

## WhatsApp client

New module: `lambda/crm-sync-quotes/whatsapp.py`.

```python
class WhatsAppSendError(Exception): ...

class WhatsAppClient:
    def __init__(self, access_token: str, phone_number_id: str, timeout_seconds: int = 10): ...

    def send_template(
        self,
        to: str,
        template_name: str,
        language_code: str,
        params: List[str],
    ) -> dict:
        """POST to https://graph.facebook.com/v21.0/{phone_number_id}/messages.
        Returns the parsed JSON on success; raises WhatsAppSendError otherwise."""
```

Implementation uses `urllib.request` to avoid adding a pip dependency. Logs the Meta `messages[0].id` on success and the Meta error code on failure.

## Secrets & infra

Secret name: `crm/whatsapp/meta`. JSON payload:

```json
{
  "access_token": "...",
  "phone_number_id": "...",
  "language_code": "es_MX"
}
```

CDK changes in `lib/constructs/crm-ingestion-construct.ts`:

- Declare a new `secretsmanager.Secret` (CDK creates it empty; the value is populated out-of-band in the AWS console).
- Pass `WHATSAPP_SECRET_ARN` as a Lambda env var.
- Call `secret.grantRead(this.processor)`.
- Pass three template env vars:
  - `WHATSAPP_TEMPLATE_DAY_7`
  - `WHATSAPP_TEMPLATE_DAY_14`
  - `WHATSAPP_TEMPLATE_DAY_21`

IAM: Lambda role gets `secretsmanager:GetSecretValue` scoped to the secret ARN. No new SES permissions.

`main.py` reads the secret once at cold start via `boto3.client("secretsmanager").get_secret_value(...)`, caches it on the module, and builds the `WhatsAppClient` from it. Templates dict is built from the three env vars.

## Logging & metrics

WhatsApp attempts log: `quote_id`, `phone`, HTTP status, Meta `message_id` (success) or Meta error code (failure).

Email-as-fallback sends use the existing SES configuration set and CloudWatch destination, and add a `Fallback=whatsapp` SES tag so the dashboard can isolate fallback volume.

No new CloudWatch metrics for WhatsApp in this change — deferred to the webhook follow-up.

## Failure semantics

- One WhatsApp attempt per quote per invocation. No retries.
- WhatsApp 2xx terminates the flow — email is **not** sent.
- WhatsApp non-2xx or exception → email send, with `fallback_from=WHATSAPP` recorded.
- Email send failures match today's behavior — logged, no transaction recorded.

## Opt-out

The existing `crm-email-opt-outs` table is checked by `QuoteFilter._is_opted_out(quote_id)` **before** channel routing, so an opted-out quote skips both channels. No new opt-out store.

Known gap: a prospect who replies "STOP" on WhatsApp will not be auto-opted-out. That requires the Meta delivery webhook, which is a follow-up.

## Testing strategy

- **Unit:** `phone.normalize_to_e164` — MOVIL+LADAM happy path, TEL fallback, malformed/empty, already-prefixed, leading zeros, 7-digit landline rejection.
- **Unit:** `WhatsAppClient.send_template` — mock `urllib`; assert request shape; assert `WhatsAppSendError` on non-2xx and on Meta error payloads.
- **Unit:** `QuoteReminderSender._send_cadence_message` — table-driven test mirroring the routing table (no phone, WA success, WA failure → email fallback, with/without `fallback_from`).
- **Integration (manual, one-off):** send a real Meta message to a known WhatsApp number with each of the three approved templates before flipping on prod.

## Follow-ups (explicitly out of scope)

- Meta delivery webhook → CloudWatch metrics + auto-opt-out on "STOP".
- Backfill `channel=email` on legacy DynamoDB items.
- WhatsApp template approval workflow / template-content versioning.
