import * as cdk from "aws-cdk-lib";
import CrmIngestion from "./constructs/crm-ingestion-construct";
import ApiResponse from "./constructs/crm-api-response-construct";
import { Construct } from "constructs";

const DOMAIN = "hidrorey.info";
export class CrmInfraStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    new CrmIngestion(this, "QuotesIngestion", {
      tableName: "crm-quotes-emails-transactions",
      partitionKeyName: "transaction_id",
      codePath: "./lambda/crm-sync-quotes",
      lambdaEnvVars: {
        SENDER_EMAIL: "contacto@" + DOMAIN,
        DOMAIN: DOMAIN,
      },
      globalSecondaryIndexes: [
        {
          indexName: "by_quote_id",
          partitionKeyName: "quote_id",
        },
      ],
    });

    new ApiResponse(this, "ApiResponse", {
      tableName: "crm-api-responses",
      lambdaCodePath: "./lambda/crm-web-response",
      enableCors: true,
    });
  }
}
