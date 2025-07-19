"""
Serverless S3 Scanner Lambda Function

Scans S3 bucket objects and sends email notifications via SNS.
Designed for manual triggering and portfolio demonstration.

Dependencies: boto3, config module
Environment Variables: AWS_REGION, S3_BUCKET_NAME, SNS_TOPIC_ARN, NOTIFICATION_EMAIL
"""

import time
from typing import Dict, List, Any
from botocore.exceptions import ClientError, NoCredentialsError

# Import local configuration module
import config
import logging
logger = logging.getLogger(__name__)



def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for S3 scanning.

    Scans S3 bucket, formats results, and sends SNS notification.
    Returns JSON response with scan results or error details.

    # TODO: Add CloudWatch metrics for monitoring (option)
    """
    # Setup logging for this execution
    config.setup_logging()
    logger.info("=" * 50)
    logger.info(f"Starting {config.APP_NAME} v{config.APP_VERSION}")
    logger.info(f"Request ID: {context.aws_request_id if context else 'local-test'}")
    logger.info("=" * 50)

    # Record start time for performance tracking
    start_time = time.time()

    try:
        # Validate configuration before proceeding
        logger.info("Validating configuration...")
        config.validate_config()
        logger.info("Configuration validation successful")

        # Perform S3 bucket scan
        logger.info("Starting S3 bucket scan...")
        scan_results = scan_s3_bucket()
        logger.info(f"S3 scan completed successfully. Found {scan_results['object_count']} objects")

        # Calculate total execution time
        execution_time = round(time.time() - start_time, 2)
        scan_results['scan_duration'] = execution_time

        # Send notification with scan results
        logger.info("Sending notification...")
        send_notification(scan_results)
        logger.info("Notification sent successfully")

        # Prepare success response
        response = {
            "statusCode": 200,
            "message": f"S3 scan completed successfully. Found {scan_results['object_count']} objects in {scan_results['bucket_name']}",
            "scanResults": {
                "bucketName": scan_results['bucket_name'],
                "objectCount": scan_results['object_count'],
                "scanDuration": execution_time,
                "timestamp": config.get_current_timestamp()
            }
        }

        logger.info("Lambda execution completed successfully")
        logger.info(f"Total execution time: {execution_time} seconds")
        return response

    except ValueError as e:
        # Configuration validation errors
        error_msg = f"Configuration error: {str(e)}"
        logger.error(error_msg)
        return {
            "statusCode": 500,
            "message": config.ErrorMessages.INVALID_CONFIG,
            "error": error_msg
        }

    except ClientError as e:
        # AWS service errors (S3, SNS, etc.)
        error_code = e.response['Error']['Code']
        error_msg = e.response['Error']['Message']

        if error_code == 'NoSuchBucket':
            logger.error(f"S3 bucket not found: {config.S3_BUCKET_NAME}")
            return {
                "statusCode": 500,
                "message": config.ErrorMessages.BUCKET_NOT_FOUND,
                "error": f"Bucket '{config.S3_BUCKET_NAME}' does not exist"
            }
        elif error_code == 'AccessDenied':
            logger.error(f"Access denied to S3 bucket: {config.S3_BUCKET_NAME}")
            return {
                "statusCode": 500,
                "message": config.ErrorMessages.ACCESS_DENIED,
                "error": f"Insufficient permissions for bucket '{config.S3_BUCKET_NAME}'"
            }
        else:
            logger.error(f"AWS ClientError: {error_code} - {error_msg}")
            return {
                "statusCode": 500,
                "message": config.ErrorMessages.GENERAL_ERROR,
                "error": f"AWS Error: {error_code} - {error_msg}"
            }

    except NoCredentialsError:
        # AWS credentials errors
        error_msg = "AWS credentials not found or invalid"
        logger.error(error_msg)
        return {
            "statusCode": 500,
            "message": "Authentication error",
            "error": error_msg
        }

    except Exception as e:
        # Catch-all for unexpected errors
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            "statusCode": 500,
            "message": config.ErrorMessages.GENERAL_ERROR,
            "error": error_msg
        }


def scan_s3_bucket() -> Dict[str, Any]:
    """
    Scan S3 bucket and retrieve object metadata.

    Handles pagination automatically for buckets with >1000 objects.
    Returns dict with bucket_name, object_count, objects list, and timestamp.

    """
    logger.info(f"Scanning S3 bucket: {config.S3_BUCKET_NAME}")

    # Initialize S3 client
    s3_client = config.get_s3_client()

    # Initialize variables for pagination
    objects = []
    continuation_token = None

    try:
        while True:
            # Prepare list_objects_v2 parameters
            list_params = {
                'Bucket': config.S3_BUCKET_NAME,
                'MaxKeys': 1000  # Maximum objects per API call
            }

            # Add continuation token for pagination if available
            if continuation_token:
                list_params['ContinuationToken'] = continuation_token

            logger.debug(f"Calling list_objects_v2 with params: {list_params}")

            # Call S3 API to list objects
            response = s3_client.list_objects_v2(**list_params)

            # Extract objects from response if any exist
            if 'Contents' in response:
                batch_objects = response['Contents']
                objects.extend(batch_objects)
                logger.debug(f"Retrieved {len(batch_objects)} objects in this batch")
            else:
                logger.info("No objects found in the bucket")

            # Check if there are more objects to retrieve
            if response.get('IsTruncated', False):
                continuation_token = response.get('NextContinuationToken')
                logger.debug(f"More objects available, continuing with token: {continuation_token}")
            else:
                logger.debug("All objects retrieved, pagination complete")
                break

    except ClientError as e:
        logger.error(f"Failed to scan S3 bucket: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during S3 scan: {e}")
        raise

    # Prepare scan results
    scan_results = {
        "bucket_name": config.S3_BUCKET_NAME,
        "object_count": len(objects),
        "objects": objects,
        "scan_timestamp": config.get_current_timestamp()
    }

    logger.info(f"S3 scan completed: {len(objects)} objects found")
    return scan_results


def send_notification(scan_results: Dict[str, Any]) -> None:
    """
    Send email notification with scan results via SNS.

    Formats results using email templates and publishes to SNS topic.
    Logs message ID on success.
    """
    logger.info("Preparing email notification...")

    # Validate required scan_results keys
    required_keys = ['bucket_name', 'object_count', 'objects', 'scan_duration']
    missing_keys = [key for key in required_keys if key not in scan_results]

    if missing_keys:
        error_msg = f"Missing required keys in scan_results: {missing_keys}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    try:
        # Initialize SNS client
        sns_client = config.get_sns_client()

        # Format object list for email body
        formatted_object_list = config.format_object_list(scan_results['objects'])

        # Prepare email subject using template
        email_subject = config.EMAIL_SUBJECT_TEMPLATE
        logger.debug(f"Email subject: {email_subject}")

        # Prepare email body using template with scan data
        email_body = config.EMAIL_BODY_TEMPLATE.format(
            bucket_name=scan_results['bucket_name'],
            scan_time=scan_results.get('scan_timestamp', config.get_current_timestamp()),
            object_count=scan_results['object_count'],
            duration=scan_results['scan_duration'],
            object_list=formatted_object_list,
            app_version=config.APP_VERSION
        )

        logger.debug("Email body formatted successfully")

        # Determine SNS topic ARN
        if config.SNS_TOPIC_ARN:
            topic_arn = config.SNS_TOPIC_ARN
        else:
            # Construct ARN if not provided in environment
            topic_arn = f"arn:aws:sns:{config.AWS_REGION}:{config.AWS_ACCOUNT_ID}:{config.SNS_TOPIC_NAME}"

        logger.info(f"Publishing message to SNS topic: {topic_arn}")

        # Publish message to SNS
        response = sns_client.publish(
            TopicArn=topic_arn,
            Subject=email_subject,
            Message=email_body
        )

        # Log success with message ID
        message_id = response.get('MessageId', 'unknown')
        logger.info(f"SNS message published successfully. Message ID: {message_id}")

        # Log notification details for debugging
        logger.debug(f"Notification sent to: {config.NOTIFICATION_EMAIL}")
        logger.debug(f"Object count in notification: {scan_results['object_count']}")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = e.response['Error']['Message']
        logger.error(f"SNS ClientError: {error_code} - {error_msg}")

        if error_code == 'NotFound':
            raise ClientError(
                error_response={'Error': {'Code': 'TopicNotFound', 'Message': f"SNS topic not found: {topic_arn}"}},
                operation_name='Publish'
            )
        else:
            raise

    except Exception as e:
        logger.error(f"Failed to send notification: {str(e)}", exc_info=True)
        raise


def format_scan_results(objects: List[Dict[str, Any]], duration: float) -> str:
    """
    Format scan results into a human-readable summary string.
    Returns formatted summary with object count, total size, and duration.
    """
    object_count = len(objects)
    total_size_bytes = sum(obj.get('Size', 0) for obj in objects)
    total_size_mb = round(total_size_bytes / (1024 * 1024), 2)

    return f"Scan Summary: {object_count} objects, {total_size_mb} MB total, completed in {duration} seconds"


# Lambda function metadata for monitoring and debugging
__version__ = config.APP_VERSION
__author__ = "DevOps Student Portfolio"
__description__ = "Serverless S3 Scanner Lambda Function"