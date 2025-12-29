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
        *   Customer details are resolved from `clientes.DBF` OR `prospect.DBF` based on `TIPO_CTE` ('C' or 'P').
    *   **Filtering**:
        *   **Date Check**: System calculates `Days = Today - QuoteDate`.
        *   **Rule**: `Days` must exist in Configured Set `{3, 5, 8}`.
        *   **Allowlist Check**: The Customer ID must exist in `assets/allowlist.yaml`.
    *   **Sales Rep Resolution**:
        *   The Quote's Agent ID (`CVE_AGE`) is matched against `assets/sales_rep.csv` bundled with the Lambda.

3.  **Output Actions**:
    *   **Email**: An HTML email is generated (using `assets/template.html`) and sent to the prospect.
    *   **Persistence**: A record is written to **DynamoDB** (`crm-quotes-emails-transactions`).
        *   *Key Data Saved*: `transaction_id`, `quote_id`, `email_address`, `status`, `sales_rep`.

---

## Flow 2: Prospect Response

This process begins when a user clicks a link in the email.

1.  **User Action**:
    *   User lands on the static website (CloudFront/S3).
    *   User selects an option (e.g., "I'm interested").
    *   Frontend sends `POST` request to API Gateway.

2.  **API Handling (Lambda)**:
    *   **Trigger**: API Gateway invokes `crm-web-response`.
    *   **Validation**: Decoder handles standard JSON or Base64-encoded bodies. Validates `response` enum type.

3.  **Data Correlation**:
    *   Lambda receives `email_transaction_id` from the request.
    *   It queries **DynamoDB** (`crm-quotes-emails-transactions`) to find the original sent email.
    *   *Purpose*: To know WHO responded to WHICH quote and WHO the sales rep is.

4.  **Output Actions**:
    *   **Persistence**: A new record is written to **DynamoDB** (`crm-api-responses`) logging the specific response.
    *   **Notification**:
        *   The system uses the `sales_rep` data from the original transaction.
        *   An email is sent to the Sales Rep: *"Prospect X has responded 'Buy' to Quote Y"*.

---

## Data Model Relationships

```text
[Legacy DBF Data] 
       |
       v
[DynamoDB: Transactions]
    PK: transaction_id
    Data: quote_id, email, sales_rep_snapshot
       ^
       | (Reference via email_transaction_id)
       |
[DynamoDB: Responses]
    PK: response_id
    Data: response_type (Buy/Info/No), timestamp
```
