import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import * as cloudfont from "aws-cdk-lib/aws-cloudfront";
import * as origins from "aws-cdk-lib/aws-cloudfront-origins";
import * as certificatemanager from "aws-cdk-lib/aws-certificatemanager";
import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import * as route53 from "aws-cdk-lib/aws-route53";

export interface DomainConfig {
  /** The root domain name (e.g., example.com).
   * There must be an associated hosted zone in Route 53 for this domain.
   */
  domainName: string;
  /** The subdomain name */
  subdomainName: string;
  /** The ARN of the SSL certificate to use for the domain. */
  certificateArn: string;
}

export interface WebsiteProps {
  /** The name of the S3 bucket that will host the website content. */
  bucketName: string;

  /** The path to the index document that will be served as the default page. */
  indexFile: string;

  /** The path to the error document that will be served when an error occurs. */
  errorFile: string;

  /** Optional configuration for custom domain setup. */
  domainConfig?: DomainConfig;

  /** Optional path to a custom 404 page. If not specified, the error file will be used. */
  notFoundResponsePagePath?: string;
}

export default class Website extends Construct {
  public readonly bucket: s3.Bucket;
  public readonly distribution: cloudfont.Distribution;

  constructor(scope: Construct, id: string, props: WebsiteProps) {
    super(scope, id);
    this.bucket = new s3.Bucket(this, props.bucketName, {
      websiteIndexDocument: props.indexFile,
      websiteErrorDocument: props.errorFile,
      publicReadAccess: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ACLS_ONLY,
      accessControl: s3.BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
      encryption: s3.BucketEncryption.S3_MANAGED,
    });
    const oai = new cloudfont.OriginAccessIdentity(
      this,
      `${props.bucketName}-OAI`
    );
    this.bucket.addToResourcePolicy(
      new iam.PolicyStatement({
        actions: ["s3:GetObject"],
        resources: [this.bucket.arnForObjects("*")],
        principals: [
          new iam.CanonicalUserPrincipal(
            oai.cloudFrontOriginAccessIdentityS3CanonicalUserId
          ),
        ],
      })
    );

    this.distribution = new cloudfont.Distribution(
      this,
      `${props.bucketName}-distribution`,
      {
        defaultBehavior: {
          origin: new origins.S3StaticWebsiteOrigin(this.bucket),
          viewerProtocolPolicy:
            cloudfont.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
        },
        errorResponses: [
          {
            httpStatus: 404,
            responseHttpStatus: 404,
            responsePagePath: props.notFoundResponsePagePath || `/404.html`,
            ttl: cdk.Duration.minutes(30),
          },
        ],
        priceClass: cloudfont.PriceClass.PRICE_CLASS_100,
        ...(props.domainConfig
          ? {
              domainNames: [this._getFullDomainName(props.domainConfig)],
              certificate: this._getCertificate(
                props.domainConfig.certificateArn
              ),
            }
          : {}),
      }
    );

    if (props.domainConfig) {
      const hostedZone = route53.HostedZone.fromLookup(this, "HostedZone", {
        domainName: props.domainConfig.domainName,
      });
      const domainARecord = new route53.ARecord(this, "DomainARecord", {
        zone: hostedZone,
        recordName: this._getFullDomainName(props.domainConfig),
        target: cdk.aws_route53.RecordTarget.fromAlias(
          new cdk.aws_route53_targets.CloudFrontTarget(this.distribution)
        ),
      });
      domainARecord.node.addDependency(this.distribution);
    }

    new s3deploy.BucketDeployment(this, `${props.bucketName}-deployment`, {
      sources: [s3deploy.Source.asset("ui")],
      destinationBucket: this.bucket,
      destinationKeyPrefix: "",
      distribution: this.distribution,
      distributionPaths: ["/*"],
    });

    new cdk.CfnOutput(this, "cloudfront-website-url", {
      value: this.distribution.distributionDomainName,
      description: "CloudFront Distribution Domain Name",
    });

    new cdk.CfnOutput(this, "LandingBucketName", {
      value: this.bucket.bucketName,
      description: "S3 bucket name for the CRM landing page",
    });

    new cdk.CfnOutput(this, "LandingBucketArn", {
      value: this.bucket.bucketArn,
      description: "S3 bucket ARN for the CRM landing page",
    });

    new cdk.CfnOutput(this, "LandingDistributionId", {
      value: this.distribution.distributionId,
      description: "CloudFront distribution ID for the landing page",
    });

    new cdk.CfnOutput(this, "LandingDistributionDomain", {
      value: this.distribution.distributionDomainName,
      description: "CloudFront domain for the landing page",
    });

    new cdk.CfnOutput(this, "s3-website-url", {
      value: this.bucket.bucketWebsiteUrl,
      description: "S3 Bucket Website URL",
    });

    if (props.domainConfig) {
      new cdk.CfnOutput(this, "website-url", {
        value: this.distribution.domainName,
        description: "Website URL",
      });
    }
  }

  private _getFullDomainName(domainConfig: DomainConfig): string {
    return domainConfig.subdomainName
      ? `${domainConfig.subdomainName}.${domainConfig.domainName}`
      : domainConfig.domainName;
  }

  private _getCertificate(arn: string): certificatemanager.ICertificate {
    return certificatemanager.Certificate.fromCertificateArn(
      this,
      `website-cert`,
      arn
    );
  }
}
