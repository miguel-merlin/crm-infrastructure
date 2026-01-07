# Low-Level Component Details

This document provides a technical deep dive into the individual components of the CRM Infrastructure.

## 1. Infrastructure Stack (`CrmInfraStack`)

The root CDK stack (`lib/crm-infra-stack.ts`) orchestrates three main constructs:

### A. Quote Ingestion Construct (`CrmIngestion`)
*   **Source**: `lib/constructs/crm-ingestion-construct.ts`
*   **Resources**:
    *   **S3 Bucket**: Stores uploaded ZIP files containing DBF data. Configured to auto-delete objects on stack destruction.
    *   **DynamoDB Table** (`crm-quotes-emails-transactions`):
        *   **Partition Key**: `transaction_id` (String)
        *   **GSI**: `by_quote_id` (PK: `quote_id`)
    *   **Lambda Function** (`Processor`):
        *   **Runtime**: Python 3.13 (bundled from `lambda/crm-sync-quotes`)
        *   **Memory**: 1024 MB
        *   **Timeout**: 5 minutes
        *   **Trigger**: S3 `OBJECT_CREATED` event.

### B. API Response Construct (`ApiResponse`)
*   **Source**: `lib/constructs/crm-api-response-construct.ts`
*   **Resources**:
    *   **DynamoDB Table** (`crm-api-responses`):
        *   **Partition Key**: `response_id` (String)
    *   **Lambda Function** (`Handler`):
        *   **Runtime**: Python 3.11 (source: `lambda/crm-web-response`)
        *   **Timeout**: 10 seconds
    *   **API Gateway**: REST API acting as a proxy to the Lambda function.

### C. Website Construct (`Website`)
*   **Source**: `lib/constructs/crm-web-construct.ts`
*   **Resources**:
    *   **S3 Bucket**: Hosts static `index.html`.
    *   **CloudFront Distribution**: Serves the bucket content via HTTPS. Configured with a custom 404 error response pointing to `index.html`.

---

## 2. Lambda Functions

### A. Quote Processor (`crm-sync-quotes`)
*   **Path**: `lambda/crm-sync-quotes/`
*   **Entry Point**: `main.py` -> `handler`
*   **Dependencies**: `dbfread`, `boto3`, `pyyaml`.
*   **Key Modules**:
    *   `parser.py`: Handles ZIP extraction and DBF parsing.
        *   **Expected Files**: `cotizac.DBF` (Headers), `cotizad.DBF` (Items), `clientes.DBF`, `prospect.DBF`.
        *   **Logic**: Joins headers with items and customer data (Client or Prospect). Enriches product data using bundled `assets/products.csv`.
    *   `filter.py`: Applies business logic.
        *   **Cadence**: Checks if `(Today - QuoteDate)` matches configured days `{16}`.
        *   **Allowlist**: Checks `assets/allowlist.yaml`. If `prospect_ids` is empty/missing, ALL prospects are allowed. Customers (`CLIENT`) always require an explicit match.
    *   `sender.py`: Handles email generation and sending (logic assumed based on name/context).

### B. Response Handler (`crm-web-response`)
*   **Path**: `lambda/crm-web-response/`
*   **Entry Point**: `main.py` -> `handler`
*   **Input Schema** (JSON Body):
    ```json
    {
        "id": "prospect_id_string",
        "email_transaction_id": "uuid_string",
        "response": "Buy" | "More Info" | "Not Interested"
    }
    ```
*   **Logic**:
    1.  Validates input `RequestParams`.
    2.  Writes a new record to `crm-api-responses` with a UUID `response_id`.
    3.  Fetches the original email context from `crm-quotes-emails-transactions` using `email_transaction_id`.
    4.  Triggers `ResponseEmailSender` to notify the sales rep.

---

## 3. Data Storage

### DynamoDB: Email Transactions
Used to track every email sent to a prospect.
*   **Table Name**: `crm-quotes-emails-transactions`
*   **Schema**:
    *   `transaction_id` (PK): UUID
    *   `quote_id`: The ID of the quote from the DBF file.
    *   `email_address`: Recipient email.
    *   `sent_at`: ISO timestamp.
    *   `status`: Enum (e.g., "Sent").
    *   `sales_rep`: Map/Object containing rep details.

### DynamoDB: API Responses
Used to track user feedback.
*   **Table Name**: `crm-api-responses`
*   **Schema**:
    *   `response_id` (PK): UUID
    *   `received_at`: ISO timestamp.
    *   `email_transaction_id`: Reference to the transaction table.
    *   `prospect_id`: ID of the prospect/client.
    *   `response_type`: Enum ("Buy", "More Info", "Not Interested").
