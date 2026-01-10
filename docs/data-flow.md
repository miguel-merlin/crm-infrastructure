# Data Flow Documentation

This document traces the lifecycle of data through the CRM Infrastructure system.

## Flow 1: Quote Ingestion & Outreach

This process begins manually (upload) and ends with an email being sent and recorded.

1.  **Input Source**:
    *   Administrator uploads a `ZIP` file to the Ingestion S3 Bucket.
    *   **Content**: The ZIP must contain:
        *   `cotizac.DBF` (Quote Headers)
        *   `cotizad.DBF` (Quote Items)
        *   `clientes.DBF` (Existing Clients)
        *   `prospect.DBF` (Prospects)

2.  **Processing (Lambda)**:
    *   **Trigger**: S3 Event Notification calls `crm-sync-quotes`.
    *   **Parsing**:
        *   `cotizac.DBF` is iterated row-by-row.
        *   For each quote, items are fetched from `cotizad.DBF`.
        *   Product details are enriched using the bundled `assets/products.csv`.
        *   Customer details are resolved from `clientes.DBF` OR `prospect.DBF` based on `TIPO_CTE` ('C' or 'P').
    *   **Filtering**:
        *   **Date Check**: System calculates `Days = Today - QuoteDate`.
        *   **Rule**: `Days` must exist in Configured Set `{16}`.
        *   **Opt-Out Check**: System queries `crm-email-opt-outs` by `quote_id`. If a record exists, the email is skipped.
        *   **Allowlist Check**:
            *   **Clients**: ID must exist in `assets/allowlist.yaml`.
            *   **Prospects**: ID must exist in `assets/allowlist.yaml`, UNLESS the list is empty, in which case ALL prospects are allowed.
    *   **Sales Rep Resolution**:
        *   The Quote's Agent ID (`CVE_AGE`) is matched against `assets/sales_rep.csv` bundled with the Lambda.

3.  **Output Actions**:
    *   **Email**: An HTML email is generated (using `assets/template.html`) and sent to the prospect via Amazon SES.
    *   **Persistence**: A record is written to **DynamoDB** (`crm-quotes-emails-transactions`).
        *   *Key Data Saved*: `transaction_id`, `quote_id`, `email_address`, `status`, `sales_rep`.

---

## Flow 2: Prospect Response

This process begins when a user clicks a link in the email.

1.  **User Action**:
    *   User lands on the landing page (`hidrorey.info`) hosted on CloudFront/S3.
    *   Frontend sends `POST` request to API Gateway based on the click-to-action on the email (e.g., Buy, More Info, Opt Out).

2.  **API Handling (Lambda)**:
    *   **Trigger**: API Gateway invokes `crm-web-response`.
    *   **Validation**: Validates `response` enum type and required IDs.

3.  **Data Correlation & Persistence**:
    *   Lambda receives `email_transaction_id` from the request.
    *   It queries **DynamoDB** (`crm-quotes-emails-transactions`) to find the original sent email.
    *   **Persistence**: A new record is written to **DynamoDB** (`crm-api-responses`) logging the specific response using `email_transaction_id` as PK.
    *   **Opt-Out Logic**: If the response is "Opt Out", a record is added to `crm-email-opt-outs` using the `quote_id` from the transaction.

4.  **Output Actions**:
    *   **Notification**:
        *   The system uses the `sales_rep` data from the original transaction.
        *   An email is sent via SES to the Sales Rep notifying them of the prospect's action.

---

## Flow 3: Incoming Email Forwarding

Incoming emails sent to `contacto@hidrorey.info` are captured by SES Receipt Rules and forwarded to `contacto@hidrorey.mx` for human processing.

---

## Data Model Relationships

```text
[Legacy DBF Data] 
       |
       v
[DynamoDB: Transactions] <----------+
    PK: transaction_id               |
    Data: quote_id, email, rep       |
       |                             |
       | (Reference)                 | (Reference via email_transaction_id)
       v                             |
[DynamoDB: Opt-Outs]           [DynamoDB: Responses]
    PK: quote_id                  PK: email_transaction_id
                                  Data: response_type, timestamp
```
