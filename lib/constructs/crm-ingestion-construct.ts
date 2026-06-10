import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as iam from "aws-cdk-lib/aws-iam";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import { Construct } from "constructs";

interface CrmIngestionProps {
  table: dynamodb.Table;
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

    const whatsappSecret = new secretsmanager.Secret(this, "WhatsAppSecret", {
      secretName: "crm/whatsapp/meta",
      description:
        "Meta WhatsApp Cloud API credentials (access_token, phone_number_id, language_code). Populate manually in the AWS console.",
    });

    this.table = props.table;

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
        WHATSAPP_SECRET_ARN: whatsappSecret.secretArn,
        ...props.lambdaEnvVars,
      },
    });

    this.table.grantReadWriteData(this.processor);
    this.bucket.grantRead(this.processor);
    whatsappSecret.grantRead(this.processor);

    this.bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.LambdaDestination(this.processor)
    );

    this.processor.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["ses:SendEmail", "ses:SendRawEmail"],
        resources: [
          "arn:aws:ses:us-west-1:183631317390:identity/hidrorey.info",
          "arn:aws:ses:us-west-1:183631317390:configuration-set/crm-emails",
        ],
      })
    );

    new cdk.CfnOutput(this, "IngestionBucketName", {
      value: this.bucket.bucketName,
      description: "S3 bucket used for CRM ingestion uploads",
    });

    new cdk.CfnOutput(this, "IngestionBucketArn", {
      value: this.bucket.bucketArn,
      description: "S3 bucket ARN for CRM ingestion uploads",
    });

    new cdk.CfnOutput(this, "IngestionTableName", {
      value: this.table.tableName,
      description: "DynamoDB table for CRM quotes/emails transactions",
    });

    new cdk.CfnOutput(this, "IngestionTableArn", {
      value: this.table.tableArn,
      description: "DynamoDB table ARN for CRM quotes/emails transactions",
    });

    new cdk.CfnOutput(this, "IngestionProcessorName", {
      value: this.processor.functionName,
      description: "Lambda name for CRM ingestion processing",
    });

    new cdk.CfnOutput(this, "IngestionProcessorArn", {
      value: this.processor.functionArn,
      description: "Lambda ARN for CRM ingestion processing",
    });

    new cdk.CfnOutput(this, "WhatsAppSecretArn", {
      value: whatsappSecret.secretArn,
      description:
        "Secrets Manager ARN for Meta WhatsApp credentials (populate in console)",
    });
  }
}
