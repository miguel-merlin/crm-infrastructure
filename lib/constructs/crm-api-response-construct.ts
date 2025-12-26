import * as lambda from "aws-cdk-lib/aws-lambda";
import * as apigateway from "aws-cdk-lib/aws-apigateway";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as path from "path";

export interface ApiResponseProps {
  /**
   * The path to the directory containing the Python lambda code.
   * Defaults to '../lambda' relative to the executing code if not provided.
   */
  lambdaCodePath?: string;

  /**
   * Whether to enable CORS headers in the Lambda environment variables.
   * Defaults to true.
   */
  enableCors?: boolean;

  /**
   * Optional name for the DynamoDB table.
   * If not provided, a unique name will be generated.
   */
  tableName?: string;
}

export default class ApiResponse extends Construct {
  public readonly table: dynamodb.Table;
  public readonly handler: lambda.Function;
  public readonly api: apigateway.RestApi;

  constructor(scope: Construct, id: string, props: ApiResponseProps = {}) {
    super(scope, id);

    this.table = new dynamodb.Table(this, "Table", {
      tableName: props.tableName,
      partitionKey: {
        name: "response_id",
        type: dynamodb.AttributeType.STRING,
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Change to RETAIN for production
    });

    const codePath = props.lambdaCodePath || path.join(__dirname, "../lambda");

    this.handler = new lambda.Function(this, "Handler", {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: "main.handler",
      code: lambda.Code.fromAsset(codePath),
      environment: {
        TABLE_NAME: this.table.tableName,
        ENABLE_CORS: String(props.enableCors ?? true),
      },
      timeout: cdk.Duration.seconds(10),
    });

    this.table.grantWriteData(this.handler);

    this.api = new apigateway.LambdaRestApi(this, "Api", {
      handler: this.handler,
      proxy: true,
      description: `Prospect API for ${id}`,
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: apigateway.Cors.ALL_METHODS,
      },
    });
  }
}
