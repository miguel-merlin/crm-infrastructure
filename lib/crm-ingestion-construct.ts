import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import { Construct } from "constructs";

interface CrmIngestionProps {
  partitionKeyName: string;
  codePath: string;
  tableNameEnvName: string;
}

export default class CrmIngestion extends Construct {
  constructor(scope: Construct, id: string, props: CrmIngestionProps) {
    super(scope, id);

    const bucket = new s3.Bucket(this, "Bucket", {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    const table = new dynamodb.Table(this, "Table", {
      partitionKey: {
        name: props.partitionKeyName,
        type: dynamodb.AttributeType.STRING,
      },
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const processor = new lambda.Function(this, "Processor", {
      runtime: lambda.Runtime.NODEJS_14_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset(props.codePath),
      environment: {
        [props.tableNameEnvName]: table.tableName,
      },
    });

    table.grantReadWriteData(processor);

    bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.LambdaDestination(processor)
    );
  }
}
