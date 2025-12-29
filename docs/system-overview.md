# High-Level System Overview

## Introduction

The **CRM Infrastructure** system is an automated platform designed to streamline the sales follow-up process. Its primary function is to digest legacy quote data (from DBF files), manage communication cadences with prospects and clients, and track their responses via a web interface.

## Core Objectives

1.  **Automated Ingestion**: Seamlessly ingest and parse legacy quote data uploaded to the cloud.
2.  **Intelligent Filtering**: Apply business rules (allowlists, cadence schedules) to determine which prospects should receive follow-up emails.
3.  **Engagement Tracking**: Capture user responses ("Buy", "More Info", "Not Interested") from email links and store them for analysis.
4.  **Sales Loop Closure**: Automatically notify sales representatives when a prospect interacts with a quote.

## System Architecture

The system is built entirely on **AWS Serverless** architecture using the AWS Cloud Development Kit (CDK).

### High-Level Diagram

```mermaid
graph LR
    User[Administrator] -- Uploads ZIP --> S3[Ingestion S3 Bucket]
    S3 -- Triggers --> Lambda1[Quote Processor Lambda]
    Lambda1 -- Reads/Parses --> DBF[Legacy DBF Files]
    Lambda1 -- Filters & Sends --> Email[SES / SMTP]
    Lambda1 -- Stores Tx --> DDB1[Transactions DynamoDB]
    
    Prospect[Prospect/Client] -- Clicks Link --> Web[Landing Page]
    Web -- POST Request --> APIG[API Gateway]
    APIG -- Proxies --> Lambda2[Response Handler Lambda]
    Lambda2 -- Records Response --> DDB2[Responses DynamoDB]
    Lambda2 -- Reads Tx --> DDB1
    Lambda2 -- Notifies --> SalesRep[Sales Representative]
```

## Key Subsystems

### 1. Ingestion & Processing Pipeline
*   **Role**: Converts raw data into actionable communication.
*   **Mechanism**: Listens for file uploads, parses proprietary DBF formats, and filters quotes based on strict business logic (time-since-creation, explicit allowlists).

### 2. API & Response Handling
*   **Role**: The "listening" ear of the system.
*   **Mechanism**: A RESTful API that accepts feedback from prospects. It correlates the response with the original email transaction to provide context to the sales team.

### 3. Frontend Landing Page
*   **Role**: The user interface for prospects.
*   **Mechanism**: A static website hosted on S3 and distributed via CloudFront, providing a fast and secure endpoint for users to submit their interest.

## Technology Stack

*   **Infrastructure as Code**: AWS CDK (TypeScript).
*   **Compute**: AWS Lambda (Python 3.13/3.11).
*   **Storage**: AWS S3 (Files), AWS DynamoDB (Data).
*   **Networking**: AWS API Gateway, Amazon CloudFront.
*   **Language**: Python (Backend logic), TypeScript (Infrastructure).
