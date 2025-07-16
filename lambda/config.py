"""
Configuration file for Serverless S3 Scanner Lambda Function
Contains all the settings and constants used across the application
"""

import os
import boto3
import logging
from datetime import datetime

# AWS Configuration
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
AWS_ACCOUNT_ID = os.environ.get('AWS_ACCOUNT_ID', '')

# S3 Configuration
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', f'serverless-s3-scanner-files-{AWS_ACCOUNT_ID}')

# SNS Configuration
SNS_TOPIC_NAME = os.environ.get('SNS_TOPIC_NAME', 'S3ScannerNotifications')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', '')

# Email Configuration
NOTIFICATION_EMAIL = os.environ.get('NOTIFICATION_EMAIL', 'sahar283@gmail.com')

# Lambda Configuration
LAMBDA_FUNCTION_NAME = 'S3Scanner'
LAMBDA_TIMEOUT = 30  # seconds
LAMBDA_MEMORY = 128  # MB

# Application Settings
APP_NAME = 'Serverless S3 Scanner'
APP_VERSION = '1.0.0'
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')

# Setup logger for this module
logger = logging.getLogger(__name__)

# AWS Clients (initialized once for performance)
def get_s3_client():
    """Get S3 client with error handling"""
    try:
        logger.info(f"Creating S3 client for region: {AWS_REGION}")
        client = boto3.client('s3', region_name=AWS_REGION)
        logger.info("S3 client created successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to create S3 client: {str(e)}", exc_info=True)
        raise

def get_sns_client():
    """Get SNS client with error handling"""
    try:
        logger.info(f"Creating SNS client for region: {AWS_REGION}")
        client = boto3.client('sns', region_name=AWS_REGION)
        logger.info("SNS client created successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to create SNS client: {str(e)}", exc_info=True)
        raise

# Message Templates
EMAIL_SUBJECT_TEMPLATE = f"[{APP_NAME}] S3 Bucket Scan Results"

EMAIL_BODY_TEMPLATE = """
Hello,

The Serverless S3 Scanner has completed a scan of your S3 bucket.

Scan Details:
- Bucket Name: {bucket_name}
- Scan Time: {scan_time}
- Total Objects Found: {object_count}
- Scan Duration: {duration} seconds

Objects Found:
{object_list}

This is an automated message from the Serverless S3 Scanner.
Application Version: {app_version}

Best regards,
Serverless S3 Scanner
"""

# Logging Configuration
def setup_logging():
    """Setup logging configuration"""
    log_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Return logger for the calling module
    return logging.getLogger(__name__)

# Validation Functions
def validate_config():
    """Validate that all required configuration is present"""
    logger.info("Validating configuration...")

    required_vars = {
        'AWS_REGION': AWS_REGION,
        'S3_BUCKET_NAME': S3_BUCKET_NAME,
        'NOTIFICATION_EMAIL': NOTIFICATION_EMAIL
    }

    missing_vars = []
    for var_name, var_value in required_vars.items():
        if not var_value or var_value == '':
            missing_vars.append(var_name)

    if missing_vars:
        error_msg = f"Missing required configuration: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info("Configuration validation passed")
    return True

# Utility Functions
def get_current_timestamp():
    """Get current timestamp in ISO format"""
    timestamp = datetime.utcnow().isoformat() + 'Z'
    logger.debug(f"Generated timestamp: {timestamp}")
    return timestamp

def format_object_list(objects):
    """Format S3 objects list for email"""
    if not objects:
        logger.info("No objects found in bucket")
        return "No objects found in the bucket."

    logger.info(f"Formatting {len(objects)} objects for email")
    formatted_list = []

    for obj in objects:
        size_mb = round(obj.get('Size', 0) / (1024 * 1024), 2)
        last_modified = obj.get('LastModified', 'Unknown')

        if isinstance(last_modified, datetime):
            last_modified = last_modified.strftime('%Y-%m-%d %H:%M:%S UTC')

        formatted_list.append(f"  • {obj['Key']} ({size_mb} MB) - Modified: {last_modified}")

    logger.debug(f"Formatted {len(formatted_list)} objects successfully")
    return '\n'.join(formatted_list)

# Constants for error handling
class ErrorMessages:
    BUCKET_NOT_FOUND = "S3 bucket not found"
    ACCESS_DENIED = "Access denied to S3 bucket"
    SNS_PUBLISH_FAILED = "Failed to publish message to SNS"
    INVALID_CONFIG = "Invalid configuration"
    GENERAL_ERROR = "An unexpected error occurred"

# Feature Flags
FEATURES = {
    'enable_detailed_logging': True,
    'include_object_metadata': True,
    'send_empty_bucket_notification': True,
    'validate_email_format': True
}