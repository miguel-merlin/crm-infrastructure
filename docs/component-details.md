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
        *   **Permissions**: Read/Write to Transactions table, Read from S3 bucket, SES SendEmail/SendRawEmail for `hidrorey.info`.

### B. API Response Construct (`ApiResponse`)
*   **Source**: `lib/constructs/crm-api-response-construct.ts`
*   **Resources**:
    *   **DynamoDB Table** (`crm-api-responses`):
        *   **Partition Key**: `email_transaction_id` (String)
    *   **Lambda Function** (`Handler`):
        *   **Runtime**: Python 3.11 (source: `lambda/crm-web-response`)
        *   **Timeout**: 10 seconds
        *   **Permissions**: Read/Write to Responses and Transactions tables, SES SendEmail/SendRawEmail.
    *   **API Gateway**: REST API acting as a proxy to the Lambda function. Configured with CORS enabled and request throttling (50/60).

### C. Website Construct (`Website`)
*   **Source**: `lib/constructs/crm-web-construct.ts`
*   **Resources**:
    *   **S3 Bucket**: Hosts static `index.html` and assets.
    *   **CloudFront Distribution**: Serves the bucket content via HTTPS. Configured with a custom 404 error response pointing to `index.html`.
    *   **Route 53**: A-Record alias pointing to the CloudFront distribution for the custom domain (`hidrorey.info`).
    *   **ACM**: SSL Certificate used for secure HTTPS communication.

### D. Email Forwarding (`EmailForwardingRuleSet`)
*   **Source**: `lib/crm-infra-stack.ts` (via `@seeebiii/ses-email-forwarding`)
*   **Resources**:
    *   **SES Receipt Rule Set**: Captures incoming emails to `contacto@hidrorey.info` and forwards them to a designated target email (`contacto@hidrorey.mx`).

### E. Opt-Outs Management
*   **DynamoDB Table** (`crm-email-opt-outs`):
    *   **Partition Key**: `quote_id` (String)
    *   **Purpose**: Stores IDs of quotes or customers who have opted out of further communications.
    *   **Permissions**: Both `Processor` and `Handler` Lambdas have Read/Write access.

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
        *   **Opt-Out Check**: Verifies if the `quote_id` exists in `crm-email-opt-outs` before sending.
        *   **Allowlist**: Checks `assets/allowlist.yaml`. If `prospect_ids` is empty/missing, ALL prospects are allowed. Customers (`CLIENT`) always require an explicit match.
    *   `sender.py`: Handles email generation and sending via SES.

### B. Response Handler (`crm-web-response`)
*   **Path**: `lambda/crm-web-response/`
*   **Entry Point**: `main.py` -> `handler`
*   **Input Schema** (JSON Body):
    ```json
    {
        "id": "prospect_id_string",
        "email_transaction_id": "uuid_string",
        "response": "Buy" | "More Info" | "Not Interested" | "Opt Out"
    }
    ```
*   **Logic**:
    1.  Validates input `RequestParams`.
    2.  Writes a new record to `crm-api-responses` using `email_transaction_id`.
    3.  Fetches the original email context from `crm-quotes-emails-transactions` using `email_transaction_id`.
    4.  If response is "Opt Out", it records the `quote_id` in `crm-email-opt-outs`.
    5.  Triggers `ResponseEmailSender` to notify the sales rep via SES.

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
    *   `email_transaction_id` (PK): UUID (Reference to Transactions table)
    *   `received_at`: ISO timestamp.
    *   `prospect_id`: ID of the prospect/client.
    *   `response_type`: Enum ("Buy", "More Info", "Not Interested", "Opt Out").

### DynamoDB: Opt-Outs
Used to prevent sending emails to specific quotes.
*   **Table Name**: `crm-email-opt-outs`
*   **Schema**:
    *   `quote_id` (PK): String
