import * as cdk from "aws-cdk-lib";
import * as cloudwatch from "aws-cdk-lib/aws-cloudwatch";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as ses from "aws-cdk-lib/aws-ses";
import CrmIngestion from "./constructs/crm-ingestion-construct";
import ApiResponse from "./constructs/crm-api-response-construct";
import Website from "./constructs/crm-web-construct";
import { EmailForwardingRuleSet } from "@seeebiii/ses-email-forwarding";
import { Construct } from "constructs";

const DOMAIN = "hidrorey.info";
const SUBDOMAIN = "www";
const FWD_EMAIL = "contacto@hidrorey.mx";
const ECOMMERCE_URL = "https://hidrolavadoras.com/";
const SES_CONFIGURATION_SET = "crm-emails";
export class CrmInfraStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const emailConfigSet = new ses.ConfigurationSet(this, "EmailConfigSet", {
      configurationSetName: SES_CONFIGURATION_SET,
      reputationMetrics: true,
    });

    const emailTransactionsTable = new dynamodb.Table(this, "Table", {
      tableName: "crm-quotes-emails-transactions",
      partitionKey: {
        name: "transaction_id",
        type: dynamodb.AttributeType.STRING,
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const optOutsTable = new dynamodb.Table(this, "OptOutsTable", {
      tableName: "crm-email-opt-outs",
      partitionKey: {
        name: "quote_id",
        type: dynamodb.AttributeType.STRING,
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    new ses.CfnConfigurationSetEventDestination(
      this,
      "SesCloudWatchEventDestination",
      {
        configurationSetName: emailConfigSet.configurationSetName,
        eventDestination: {
          name: "cloudwatch-metrics",
          enabled: true,
          matchingEventTypes: [
            "send",
            "reject",
            "bounce",
            "complaint",
            "delivery",
            "open",
            "click",
            "renderingFailure",
            "deliveryDelay",
          ],
          cloudWatchDestination: {
            dimensionConfigurations: [
              {
                dimensionName: "EmailType",
                dimensionValueSource: "messageTag",
                defaultDimensionValue: "default",
              },
              {
                dimensionName: "SalesRepId",
                dimensionValueSource: "messageTag",
                defaultDimensionValue: "unknown",
              },
            ],
          },
        },
      }
    );

    this.buildEmailMetricsDashboard();

    const crmIngestion = new CrmIngestion(this, "QuotesIngestion", {
      table: emailTransactionsTable,
      codePath: "./lambda/crm-sync-quotes",
      lambdaEnvVars: {
        SENDER_EMAIL: "contacto@" + DOMAIN,
        DOMAIN: "https://" + SUBDOMAIN + "." + DOMAIN,
        OPT_OUT_TABLE_NAME: optOutsTable.tableName,
        ECOMMERCE_URL: ECOMMERCE_URL,
        SES_CONFIGURATION_SET: SES_CONFIGURATION_SET,
      },
      globalSecondaryIndexes: [
        {
          indexName: "by_quote_id",
          partitionKeyName: "quote_id",
        },
      ],
    });
    optOutsTable.grantReadWriteData(crmIngestion.processor);

    const webApiTracking = new ApiResponse(this, "ApiResponse", {
      transactionsTable: emailTransactionsTable,
      tableName: "crm-api-responses",
      lambdaCodePath: "./lambda/crm-web-response",
      enableCors: true,
      lambdaEnvVars: {
        EMAIL_TRANSACTION_TABLE_NAME: crmIngestion.table.tableName,
        SENDER_EMAIL: "contacto@" + DOMAIN,
        OPT_OUT_TABLE_NAME: optOutsTable.tableName,
        SES_CONFIGURATION_SET: SES_CONFIGURATION_SET,
      },
    });
    optOutsTable.grantReadWriteData(webApiTracking.handler);

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

    new EmailForwardingRuleSet(this, "EmailForwardingRuleSet", {
      enableRuleSet: true,
      emailForwardingProps: [
        {
          domainName: DOMAIN,
          fromPrefix: "contacto",
          emailMappings: [
            {
              receivePrefix: "contacto",
              targetEmails: [FWD_EMAIL],
            },
          ],
        },
      ],
    });
  }

  private buildEmailMetricsDashboard(): void {
    const dashboard = new cloudwatch.Dashboard(this, "EmailMetricsDashboard", {
      dashboardName: "crm-email-metrics",
      defaultInterval: cdk.Duration.days(7),
    });

    const sesSearch = (
      metricName: string,
      label: string
    ): cloudwatch.MathExpression =>
      new cloudwatch.MathExpression({
        expression: `SEARCH('{AWS/SES,EmailType,SalesRepId} MetricName="${metricName}"', 'Sum', 86400)`,
        label,
        period: cdk.Duration.days(1),
        usingMetrics: {},
      });

    const totalsRow = ["Send", "Delivery", "Bounce", "Complaint", "Open", "Click"].map(
      (metricName) =>
        new cloudwatch.SingleValueWidget({
          title: `${metricName} (7d)`,
          metrics: [
            new cloudwatch.MathExpression({
              expression: `SUM(SEARCH('{AWS/SES,EmailType,SalesRepId} MetricName="${metricName}"', 'Sum', 86400))`,
              label: metricName,
              period: cdk.Duration.days(7),
              usingMetrics: {},
            }),
          ],
          width: 4,
          height: 4,
        })
    );

    const perRepSends = new cloudwatch.GraphWidget({
      title: "Sends per sales rep (30d)",
      left: [sesSearch("Send", "Send")],
      width: 12,
      height: 6,
    });

    const perRepOpens = new cloudwatch.GraphWidget({
      title: "Opens per sales rep (30d)",
      left: [sesSearch("Open", "Open")],
      width: 12,
      height: 6,
    });

    const perRepClicks = new cloudwatch.GraphWidget({
      title: "Clicks per sales rep (30d)",
      left: [sesSearch("Click", "Click")],
      width: 12,
      height: 6,
    });

    const bouncesByEmailType = new cloudwatch.GraphWidget({
      title: "Bounces by email type (30d)",
      left: [sesSearch("Bounce", "Bounce")],
      width: 12,
      height: 6,
    });

    const complaintsByEmailType = new cloudwatch.GraphWidget({
      title: "Complaints by email type (30d)",
      left: [sesSearch("Complaint", "Complaint")],
      width: 12,
      height: 6,
    });

    const reputation = new cloudwatch.GraphWidget({
      title: "SES reputation",
      left: [
        new cloudwatch.Metric({
          namespace: "AWS/SES",
          metricName: "Reputation.BounceRate",
          statistic: "Average",
          period: cdk.Duration.hours(1),
        }),
        new cloudwatch.Metric({
          namespace: "AWS/SES",
          metricName: "Reputation.ComplaintRate",
          statistic: "Average",
          period: cdk.Duration.hours(1),
        }),
      ],
      width: 24,
      height: 6,
    });

    dashboard.addWidgets(...totalsRow);
    dashboard.addWidgets(perRepSends, perRepOpens);
    dashboard.addWidgets(perRepClicks, bouncesByEmailType);
    dashboard.addWidgets(complaintsByEmailType);
    dashboard.addWidgets(reputation);

    new cdk.CfnOutput(this, "EmailMetricsDashboardUrl", {
      value: `https://${this.region}.console.aws.amazon.com/cloudwatch/home?region=${this.region}#dashboards:name=${dashboard.dashboardName}`,
      description: "CloudWatch dashboard with per-sales-rep SES email metrics",
    });
  }
}
