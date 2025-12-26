import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import { Construct } from "constructs";

interface CrmIngestionProps {
  tableName: string;
  partitionKeyName: string;
  codePath: string;
  lambdaEnvVars?: { [key: string]: string };
  globalSecondaryIndexes?: {
    indexName: string;
    partitionKeyName: string;
    sortKeyName?: string;
  }[];
}

export default class CrmIngestion extends Construct {
  constructor(scope: Construct, id: string, props: CrmIngestionProps) {
    super(scope, id);

    const bucket = new s3.Bucket(this, "Bucket", {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    const table = new dynamodb.Table(this, "Table", {
      tableName: props.tableName,
      partitionKey: {
        name: props.partitionKeyName,
        type: dynamodb.AttributeType.STRING,
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    if (props.globalSecondaryIndexes) {
      props.globalSecondaryIndexes.forEach((gsi) => {
        table.addGlobalSecondaryIndex({
          indexName: gsi.indexName,
          partitionKey: {
            name: gsi.partitionKeyName,
            type: dynamodb.AttributeType.STRING,
          },
          sortKey: gsi.sortKeyName
            ? {
                name: gsi.sortKeyName,
                type: dynamodb.AttributeType.STRING,
              }
            : undefined,
        });
      });
    }

    const processor = new lambda.Function(this, "Processor", {
      runtime: lambda.Runtime.PYTHON_3_13,
      handler: "main.handler",
      code: lambda.Code.fromAsset(props.codePath),
      environment: {
        TABLE_NAME: table.tableName,
        ...props.lambdaEnvVars,
      },
    });

    table.grantReadWriteData(processor);

    bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.LambdaDestination(processor)
    );
  }
}
