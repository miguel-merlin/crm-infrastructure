import * as cdk from "aws-cdk-lib";
import CrmIngestion from "./crm-ingestion-construct";
import { Construct } from "constructs";

export class CrmInfraStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    new CrmIngestion(this, "QuotesIngestion", {
      partitionKeyName: "quoteId",
      codePath: "lambda/quotes-processor",
      tableNameEnvName: "QUOTES_TABLE_NAME",
    });

    new CrmIngestion(this, "ProductsIngestion", {
      partitionKeyName: "productId",
      codePath: "lambda/products-processor",
      tableNameEnvName: "PRODUCTS_TABLE_NAME",
    });

    new CrmIngestion(this, "SalesRepsIngestion", {
      partitionKeyName: "salesRepId",
      codePath: "lambda/sales-reps-processor",
      tableNameEnvName: "SALES_REP_TABLE_NAME",
    });
  }
}
