import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as iam from "aws-cdk-lib/aws-iam";
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
  public readonly bucket: s3.Bucket;
  public readonly table: dynamodb.Table;
  public readonly processor: lambda.Function;
  public readonly uploadRole: iam.Role;

  constructor(scope: Construct, id: string, props: CrmIngestionProps) {
    super(scope, id);

    this.bucket = new s3.Bucket(this, "Bucket", {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    this.table = new dynamodb.Table(this, "Table", {
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
        this.table.addGlobalSecondaryIndex({
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

    this.processor = new lambda.Function(this, "Processor", {
      runtime: lambda.Runtime.PYTHON_3_13,
      handler: "main.handler",
      code: lambda.Code.fromAsset(props.codePath, {
        bundling: {
          image: lambda.Runtime.PYTHON_3_10.bundlingImage,
          command: [
            "bash",
            "-c",
            "pip install -r requirements.txt --platform manylinux2014_x86_64 --only-binary=:all: -t /asset-output && cp -au . /asset-output",
          ],
        },
      }),
      timeout: cdk.Duration.minutes(5),
      memorySize: 1024,
      description: "Processes CRM ingestion files from S3 to DynamoDB",
      environment: {
        TABLE_NAME: this.table.tableName,
        ...props.lambdaEnvVars,
      },
    });

    this.table.grantReadWriteData(this.processor);

    this.bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.LambdaDestination(this.processor)
    );
  }
}
