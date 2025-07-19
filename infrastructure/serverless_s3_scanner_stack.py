"""
AWS CDK Stack for Serverless S3 Scanner

This stack creates all AWS resources needed for the S3 Scanner application:
- S3 Bucket for storing and scanning files
- Lambda Function for scanning S3 objects
- SNS Topic for email notifications
- IAM Role with least privilege permissions
- Automated deployment of sample files

Author: DevOps Student Portfolio
Version: 1.0.0
"""

from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    Tags,
    aws_s3 as s3,
    aws_s3_deployment as s3_deployment,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subscriptions,
    CfnOutput
)
from constructs import Construct
import os
import sys

# Configuration values (imported from config.py or defaults)
try:
    # Try to import config
    current_dir = os.path.dirname(os.path.abspath(__file__))
    lambda_dir = os.path.join(current_dir, '..', 'lambda')
    lambda_dir = os.path.abspath(lambda_dir)
    sys.path.insert(0, lambda_dir)

    import config
    print(f" Successfully imported config from: {lambda_dir}")

    # Use imported config
    LAMBDA_TIMEOUT = config.LAMBDA_TIMEOUT
    LAMBDA_MEMORY = config.LAMBDA_MEMORY
    NOTIFICATION_EMAIL = config.NOTIFICATION_EMAIL
    LOG_LEVEL = config.LOG_LEVEL
    APP_NAME = config.APP_NAME
    APP_VERSION = config.APP_VERSION
    SNS_TOPIC_NAME = config.SNS_TOPIC_NAME

except ImportError as e:
    print(f"⚠️  Using hardcoded config values (import failed: {e})")

    # Hardcoded fallback values
    LAMBDA_TIMEOUT = 30
    LAMBDA_MEMORY = 128
    NOTIFICATION_EMAIL = "sahar283@gmail.com"
    LOG_LEVEL = "INFO"
    APP_NAME = "Serverless S3 Scanner"
    APP_VERSION = "1.0.0"
    SNS_TOPIC_NAME = "S3ScannerNotifications"


class ServerlessS3ScannerStack(Stack):
    """
    Main CDK Stack for the Serverless S3 Scanner application.

    Creates and configures all AWS resources needed for the application:
    - S3 Bucket with sample files
    - Lambda Function for S3 scanning
    - SNS Topic for notifications
    - IAM Role with appropriate permissions
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create resources in CORRECTED dependency order
        self.bucket = self._create_s3_bucket()
        self.bucket_deployment = self._upload_sample_files()
        self.sns_topic = self._create_sns_topic()
        self.lambda_role = self._create_lambda_role()
        self.lambda_function = self._create_lambda_function()
        self.sns_subscription = self._create_sns_subscription()

        self._create_outputs()

    def _create_s3_bucket(self) -> s3.Bucket:
        """
        Create S3 bucket for storing files that will be scanned by Lambda.

        Uses account ID in bucket name to ensure global uniqueness.
        Configured with appropriate settings for a portfolio project.

        Returns:
            s3.Bucket: The created S3 bucket instance
        """
        # Generate unique bucket name using account ID
        bucket_name = f"serverless-s3-scanner-files-{self.account}"

        # Create S3 bucket with appropriate configuration
        bucket = s3.Bucket(
            self,
            "S3ScannerBucket",
            bucket_name=bucket_name,

            # Security settings
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,

            # Lifecycle settings for cost optimization
            versioned=False,  # Keep it simple for portfolio project

            # Cleanup policy - important for portfolio project
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,  # Clean up files when stack is deleted

            # Monitoring and compliance
            event_bridge_enabled=False  # Not needed for this use case
        )

        # Add tags using Tags.of() method (compatible with all CDK versions)
        Tags.of(bucket).add("Project", "ServerlessS3Scanner")
        Tags.of(bucket).add("Environment", "Portfolio")
        Tags.of(bucket).add("Owner", "DevOpsStudent")

        return bucket

    def _upload_sample_files(self) -> s3_deployment.BucketDeployment:
        """
        Upload sample files from local sample_files/ directory to S3 bucket.

        This deployment runs during CDK deploy and uploads all files from
        the sample_files/ directory to the S3 bucket root.

        Returns:
            s3_deployment.BucketDeployment: The deployment construct
        """
        # Get the path to sample_files directory
        sample_files_path = os.path.join(
            os.path.dirname(__file__),
            '..',
            'sample_files'
        )

        # Verify sample_files directory exists
        if not os.path.exists(sample_files_path):
            raise ValueError(f"sample_files directory not found at: {sample_files_path}")

        # Create bucket deployment to upload files
        bucket_deployment = s3_deployment.BucketDeployment(
            self,
            "S3ScannerBucketDeployment",

            # Source and destination
            sources=[s3_deployment.Source.asset(sample_files_path)],
            destination_bucket=self.bucket,

            # Deployment configuration
            prune=True,  # Remove files that are not in source
            retain_on_delete=False,  # Clean up when stack is deleted

            # Access control
            access_control=s3.BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,

            # Cache control for uploaded files
            cache_control=[
                s3_deployment.CacheControl.max_age(Duration.hours(1))
            ],

            # Metadata for uploaded files
            metadata={
                "project": "serverless-s3-scanner",
                "uploaded-by": "cdk-deployment"
            }
        )

        return bucket_deployment

    def _create_lambda_role(self) -> iam.Role:
        """
        Create IAM role for Lambda function with least privilege permissions.

        The role includes only the minimum permissions needed:
        - Read access to the specific S3 bucket
        - Publish access to the specific SNS topic
        - CloudWatch Logs access for monitoring

        Returns:
            iam.Role: The created IAM role for Lambda
        """
        # Create IAM role for Lambda function
        lambda_role = iam.Role(
            self,
            "S3ScannerLambdaRole",

            # Basic role configuration
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            role_name=f"S3ScannerLambdaRole-{self.account}",
            description="IAM role for S3 Scanner Lambda with least privilege permissions",

            # Attach basic Lambda execution role
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ]
        )

        # Add tags using Tags.of() method
        Tags.of(lambda_role).add("Project", "ServerlessS3Scanner")
        Tags.of(lambda_role).add("Environment", "Portfolio")
        Tags.of(lambda_role).add("Purpose", "LambdaExecution")

        # Add S3 read permissions for the specific bucket
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                resources=[
                    self.bucket.bucket_arn,
                    f"{self.bucket.bucket_arn}/*"
                ]
            )
        )

        # Add SNS publish permissions (will be refined after SNS topic creation)
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "sns:Publish"
                ],
                resources=[
                    f"arn:aws:sns:{self.region}:{self.account}:*S3ScannerNotifications*"
                ]
            )
        )

        return lambda_role

    def _create_lambda_function(self) -> lambda_.Function:
        """
        Create Lambda function for S3 scanning with proper configuration.

        Deploys the s3_scanner.py and config.py code with all necessary
        environment variables and settings.

        Returns:
            lambda_.Function: The created Lambda function
        """
        # Get Lambda source directory
        lambda_source_path = os.path.join(
            os.path.dirname(__file__),
            '..',
            'lambda'
        )

        # Create Lambda function
        lambda_function = lambda_.Function(
            self,
            "S3ScannerLambdaFunction",

            # Code configuration
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="s3_scanner.lambda_handler",
            code=lambda_.Code.from_asset(lambda_source_path),

            # Function configuration
            function_name="S3Scanner",
            description="Serverless S3 Scanner - Lists objects and sends notifications",
            timeout=Duration.seconds(LAMBDA_TIMEOUT),
            memory_size=LAMBDA_MEMORY,

            # IAM role
            role=self.lambda_role,

            # Environment variables
            environment={
                "AWS_ACCOUNT_ID": self.account,
                "S3_BUCKET_NAME": self.bucket.bucket_name,
                "SNS_TOPIC_ARN": self.sns_topic.topic_arn,
                "NOTIFICATION_EMAIL": NOTIFICATION_EMAIL,
                "LOG_LEVEL": LOG_LEVEL,
                "APP_NAME": APP_NAME,
                "APP_VERSION": APP_VERSION
            },

            # Monitoring and debugging
            tracing=lambda_.Tracing.ACTIVE  # Enable X-Ray tracing
        )

        # Add tags using Tags.of() method
        Tags.of(lambda_function).add("Project", "ServerlessS3Scanner")
        Tags.of(lambda_function).add("Environment", "Portfolio")
        Tags.of(lambda_function).add("Language", "Python")

        return lambda_function

    def _create_sns_topic(self) -> sns.Topic:
        """
        Create SNS topic for email notifications.

        Returns:
            sns.Topic: The created SNS topic
        """
        # Create SNS topic
        sns_topic = sns.Topic(
            self,
            "S3ScannerSNSTopic",

            # Topic configuration
            topic_name=SNS_TOPIC_NAME,
            display_name="S3 Scanner Notifications",

            # Message settings
            fifo=False  # Standard topic for email notifications
        )

        # Add tags using Tags.of() method
        Tags.of(sns_topic).add("Project", "ServerlessS3Scanner")
        Tags.of(sns_topic).add("Environment", "Portfolio")
        Tags.of(sns_topic).add("Purpose", "EmailNotifications")

        return sns_topic

    def _create_sns_subscription(self) -> sns_subscriptions.EmailSubscription:
        """
        Create email subscription for SNS topic.

        Returns:
            sns_subscriptions.EmailSubscription: The created subscription
        """
        # Create email subscription
        email_subscription = sns_subscriptions.EmailSubscription(
            NOTIFICATION_EMAIL
        )

        # Add subscription to topic
        self.sns_topic.add_subscription(email_subscription)

        return email_subscription

    def _create_outputs(self) -> None:
        """
        Create CloudFormation outputs for easy access to resource information.
        """
        # S3 Bucket outputs
        CfnOutput(
            self,
            "S3BucketName",
            value=self.bucket.bucket_name,
            description="Name of the S3 bucket containing files to scan"
        )

        CfnOutput(
            self,
            "S3BucketArn",
            value=self.bucket.bucket_arn,
            description="ARN of the S3 bucket"
        )

        # Lambda function outputs
        CfnOutput(
            self,
            "LambdaFunctionName",
            value=self.lambda_function.function_name,
            description="Name of the Lambda function"
        )

        CfnOutput(
            self,
            "LambdaFunctionArn",
            value=self.lambda_function.function_arn,
            description="ARN of the Lambda function"
        )

        # SNS topic outputs
        CfnOutput(
            self,
            "SNSTopicName",
            value=self.sns_topic.topic_name,
            description="Name of the SNS topic"
        )

        CfnOutput(
            self,
            "SNSTopicArn",
            value=self.sns_topic.topic_arn,
            description="ARN of the SNS topic for notifications"
        )

        # Email notification info
        CfnOutput(
            self,
            "NotificationEmail",
            value=NOTIFICATION_EMAIL,
            description="Email address that will receive notifications"
        )