import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import CrmIngestion from "./constructs/crm-ingestion-construct";
import ApiResponse from "./constructs/crm-api-response-construct";
import Website from "./constructs/crm-web-construct";
import { Construct } from "constructs";

const DOMAIN = "hidrorey.info";
const SUBDOMAIN = "www";
export class CrmInfraStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const emailTransactionsTable = new dynamodb.Table(this, "Table", {
      tableName: "crm-quotes-emails-transactions",
      partitionKey: {
        name: "transaction_id",
        type: dynamodb.AttributeType.STRING,
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const crmIngestion = new CrmIngestion(this, "QuotesIngestion", {
      table: emailTransactionsTable,
      codePath: "./lambda/crm-sync-quotes",
      lambdaEnvVars: {
        SENDER_EMAIL: "contacto@" + DOMAIN,
        DOMAIN: "https://" + SUBDOMAIN + "." + DOMAIN,
      },
      globalSecondaryIndexes: [
        {
          indexName: "by_quote_id",
          partitionKeyName: "quote_id",
        },
      ],
    });

    new ApiResponse(this, "ApiResponse", {
      transactionsTable: emailTransactionsTable,
      tableName: "crm-api-responses",
      lambdaCodePath: "./lambda/crm-web-response",
      enableCors: true,
      lambdaEnvVars: {
        EMAIL_TRANSACTION_TABLE_NAME: crmIngestion.table.tableName,
        SENDER_EMAIL: "contacto@" + DOMAIN,
      },
    });

    new Website(this, "LandingPage", {
      bucketName: "crm-landing-page-bucket",
      indexFile: "index.html",
      errorFile: "index.html",
      domainConfig: {
        domainName: DOMAIN,
        subdomainName: "www",
        certificateArn:
          "arn:aws:acm:us-east-1:183631317390:certificate/b66da7de-bbc5-4968-bbe4-37fbe222b283",
      },
    });
  }
}
