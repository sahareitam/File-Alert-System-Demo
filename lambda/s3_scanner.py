"""
Serverless S3 Scanner Lambda Function

This module contains the main Lambda function that scans an S3 bucket for objects,
collects metadata about the files, and sends an email notification with scan results
via Amazon SNS. The function is designed to be triggered manually for demonstration
purposes in a DevOps portfolio project.

The Lambda function performs the following operations:
1. Validates configuration and initializes AWS clients
2. Scans the configured S3 bucket to list all objects
3. Collects object metadata (name, size, last modified date)
4. Formats the scan results into a human-readable report
5. Publishes the results to an SNS topic for email notification
6. Returns a JSON response with execution details

Dependencies:
    - boto3: AWS SDK for Python
    - config: Local configuration module with AWS clients and settings

Environment Variables:
    - AWS_REGION: AWS region for resource operations
    - S3_BUCKET_NAME: Target S3 bucket to scan
    - SNS_TOPIC_ARN: SNS topic ARN for notifications
    - NOTIFICATION_EMAIL: Email address for notifications

Author: DevOps Student Portfolio Project
Version: 1.0.0
"""

import json
import time
from datetime import datetime
from typing import Dict, List, Any
from botocore.exceptions import ClientError, NoCredentialsError

# Import local configuration module
import config


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main AWS Lambda handler function for the S3 Scanner.

    This is the entry point for the Lambda function. It orchestrates the entire
    S3 scanning process, from initialization to notification delivery. The function
    handles all errors gracefully and ensures proper logging throughout execution.

    The function flow:
    1. Setup logging and validate configuration
    2. Initialize start time for performance tracking
    3. Scan the S3 bucket for objects
    4. Format and send notification with results
    5. Return execution summary

    Args:
        event (Dict[str, Any]): AWS Lambda event object containing trigger information.
                               For manual invocations, this is typically empty or contains
                               test data. The function doesn't rely on specific event data.
        context (Any): AWS Lambda context object containing runtime information such as
                      function name, version, remaining time, and request ID.

    Returns:
        Dict[str, Any]: JSON response containing execution results with the following structure:
            {
                "statusCode": int,           # HTTP status code (200 for success, 500 for error)
                "message": str,              # Human-readable status message
                "scanResults": {             # Detailed scan results (only on success)
                    "bucketName": str,       # Name of the scanned S3 bucket
                    "objectCount": int,      # Number of objects found
                    "scanDuration": float,   # Scan duration in seconds
                    "timestamp": str         # ISO timestamp of scan completion
                },
                "error": str                 # Error details (only on failure)
            }

    Raises:
        The function catches all exceptions internally and returns error details
        in the response rather than raising them. This ensures Lambda doesn't
        retry failed executions unnecessarily.
    """
    # Setup logging for this execution
    logger = config.setup_logging()
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
    Scan the configured S3 bucket and retrieve metadata for all objects.

    This function connects to the S3 service and performs a comprehensive scan
    of the configured bucket. It retrieves essential metadata for each object
    including the object key (filename), size in bytes, and last modified timestamp.
    The function handles pagination automatically to ensure all objects are retrieved
    regardless of bucket size.

    The function uses the S3 list_objects_v2 API which is more efficient than
    the legacy list_objects API and provides better performance for large buckets.

    Returns:
        Dict[str, Any]: Dictionary containing scan results with the following structure:
            {
                "bucket_name": str,          # Name of the scanned S3 bucket
                "object_count": int,         # Total number of objects found
                "objects": List[Dict],       # List of object metadata dictionaries
                "scan_timestamp": str        # ISO timestamp when scan was performed
            }

            Each object in the "objects" list contains:
            {
                "Key": str,                  # Object key (filename/path)
                "Size": int,                 # Object size in bytes
                "LastModified": datetime     # Last modification timestamp
            }

    Raises:
        ClientError: If there are issues accessing the S3 bucket, such as:
                    - NoSuchBucket: The specified bucket doesn't exist
                    - AccessDenied: Insufficient permissions to read the bucket
                    - InvalidBucketName: The bucket name is malformed

        Exception: For other unexpected errors during the S3 operation

    Example:
        >>> results = scan_s3_bucket()
        >>> print(f"Found {results['object_count']} objects in {results['bucket_name']}")
        Found 3 objects in my-bucket
    """
    logger = config.setup_logging()
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
    Send email notification with S3 scan results via Amazon SNS.

    This function formats the scan results into a human-readable email message
    and publishes it to the configured SNS topic. The email includes a summary
    of the scan (bucket name, object count, scan duration) and a detailed list
    of all objects found with their metadata.

    The function uses predefined email templates from the config module to ensure
    consistent formatting and professional appearance. It handles both empty
    buckets and buckets with multiple objects gracefully.

    Args:
        scan_results (Dict[str, Any]): Dictionary containing scan results from scan_s3_bucket().
                                      Must include the following keys:
                                      - bucket_name (str): Name of the scanned bucket
                                      - object_count (int): Number of objects found
                                      - objects (List[Dict]): List of object metadata
                                      - scan_duration (float): Scan duration in seconds

    Returns:
        None: This function doesn't return a value but logs the SNS message ID on success.

    Raises:
        ClientError: If there are issues with the SNS service, such as:
                    - InvalidTopicArn: The SNS topic ARN is invalid or doesn't exist
                    - AuthorizationError: Insufficient permissions to publish to the topic
                    - ThrottlingException: Too many requests to SNS

        ValueError: If required keys are missing from scan_results parameter

        Exception: For other unexpected errors during SNS publishing

    Example:
        >>> scan_results = {
        ...     "bucket_name": "my-bucket",
        ...     "object_count": 3,
        ...     "objects": [...],
        ...     "scan_duration": 1.25
        ... }
        >>> send_notification(scan_results)
        # Sends email notification via SNS
    """
    logger = config.setup_logging()
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

    This utility function creates a concise, formatted summary of the S3 scan
    results that can be used in logs, console output, or other text-based
    reporting. The summary includes object count, total size, and scan duration.

    Args:
        objects (List[Dict[str, Any]]): List of S3 object metadata dictionaries.
                                       Each dictionary should contain 'Size' key.
        duration (float): Scan duration in seconds.

    Returns:
        str: Formatted summary string containing:
             - Total number of objects
             - Total size in MB (rounded to 2 decimal places)
             - Scan duration in seconds

    Example:
        >>> objects = [{'Size': 1024}, {'Size': 2048}]
        >>> summary = format_scan_results(objects, 1.5)
        >>> print(summary)
        Scan Summary: 2 objects, 0.00 MB total, completed in 1.5 seconds
    """
    object_count = len(objects)
    total_size_bytes = sum(obj.get('Size', 0) for obj in objects)
    total_size_mb = round(total_size_bytes / (1024 * 1024), 2)

    return f"Scan Summary: {object_count} objects, {total_size_mb} MB total, completed in {duration} seconds"


# Lambda function metadata for monitoring and debugging
__version__ = config.APP_VERSION
__author__ = "DevOps Student Portfolio"
__description__ = "Serverless S3 Scanner Lambda Function"