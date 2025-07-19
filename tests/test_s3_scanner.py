"""
Unit tests for lambda/s3_scanner.py

Tests the main Lambda function and its helper functions.
These tests use mocking to avoid actual AWS service calls during testing.
"""

import os
import sys
from datetime import datetime
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

# Add the lambda directory to the path for importing s3_scanner and config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../lambda'))
import s3_scanner

class TestLambdaHandler:
    """Test the main lambda_handler function"""

    @patch('s3_scanner.logger')
    @patch('s3_scanner.send_notification')
    @patch('s3_scanner.scan_s3_bucket')
    @patch('s3_scanner.config.validate_config')
    @patch('s3_scanner.config.setup_logging')
    def test_lambda_handler_success(self, mock_setup_logging, mock_validate_config,
                                    mock_scan_s3_bucket, mock_send_notification, mock_logger):
        """
        Test the complete successful execution flow of lambda_handler.

        This test mocks all external dependencies and verifies:
        1. Proper function call sequence
        2. Correct response structure
        3. Success status code and message
        4. Scan results formatting
        """
        # Setup mocks
        mock_setup_logging.return_value = None
        mock_validate_config.return_value = True

        # Mock S3 scan results
        mock_scan_results = {
            'bucket_name': 'test-bucket',
            'object_count': 3,
            'objects': [
                {
                    'Key': 'document1.txt',
                    'Size': 1024,
                    'LastModified': datetime(2025, 1, 15, 10, 30, 45)
                },
                {
                    'Key': 'document2.json',
                    'Size': 2048,
                    'LastModified': datetime(2025, 1, 15, 11, 15, 20)
                },
                {
                    'Key': 'document3.csv',
                    'Size': 512,
                    'LastModified': datetime(2025, 1, 15, 12, 0, 0)
                }
            ],
            'scan_timestamp': '2025-01-15T10:30:45Z'
        }
        mock_scan_s3_bucket.return_value = mock_scan_results
        mock_send_notification.return_value = None

        # Mock Lambda context
        mock_context = Mock()
        mock_context.aws_request_id = 'test-request-123'

        # Test data
        test_event = {"test": "data"}  # Event with some data

        # Execute the function
        result = s3_scanner.lambda_handler(test_event, mock_context)

        # Verify function calls
        mock_setup_logging.assert_called_once()
        mock_validate_config.assert_called_once()
        mock_scan_s3_bucket.assert_called_once()
        mock_send_notification.assert_called_once()

        # Verify send_notification was called with scan results including duration
        notification_args = mock_send_notification.call_args[0][0]
        assert notification_args['bucket_name'] == 'test-bucket'
        assert notification_args['object_count'] == 3
        assert 'scan_duration' in notification_args
        assert isinstance(notification_args['scan_duration'], float)

        # Verify response structure
        assert isinstance(result, dict)
        assert 'statusCode' in result
        assert 'message' in result
        assert 'scanResults' in result

        # Verify success response
        assert result['statusCode'] == 200
        assert 'successfully' in result['message'].lower()
        assert '3 objects' in result['message']
        assert 'test-bucket' in result['message']

        # Verify scan results structure
        scan_results = result['scanResults']
        assert scan_results['bucketName'] == 'test-bucket'
        assert scan_results['objectCount'] == 3
        assert isinstance(scan_results['scanDuration'], float)
        assert 'timestamp' in scan_results

        # Verify no error field in success response
        assert 'error' not in result

        # Verify logging calls
        assert mock_logger.info.call_count >= 3  # Multiple info logs expected

    @patch('s3_scanner.logger')
    @patch('s3_scanner.config.validate_config')
    @patch('s3_scanner.config.setup_logging')
    def test_lambda_handler_config_error(self, mock_setup_logging, mock_validate_config, mock_logger):
        """
        Test lambda_handler behavior when configuration validation fails.

        This test verifies that the Lambda function handles configuration errors
        gracefully and returns a proper error response when required environment
        variables are missing or invalid.

        The test mocks config.validate_config() to raise a ValueError,
        simulating missing configuration like AWS_REGION or NOTIFICATION_EMAIL.
        """
        # Setup mocks
        mock_setup_logging.return_value = None

        # Make validate_config raise a ValueError (simulating missing config)
        mock_validate_config.side_effect = ValueError("Missing required configuration: NOTIFICATION_EMAIL")

        # Mock Lambda context
        mock_context = Mock()
        mock_context.aws_request_id = 'test-request-456'

        # Test data
        test_event = {"test": "data"}

        # Execute the function
        result = s3_scanner.lambda_handler(test_event, mock_context)

        # Verify function calls
        mock_setup_logging.assert_called_once()
        mock_validate_config.assert_called_once()

        # Verify error response structure
        assert isinstance(result, dict)
        assert 'statusCode' in result
        assert 'message' in result
        assert 'error' in result

        # Verify error response content
        assert result['statusCode'] == 500
        assert result['message'] == "Invalid configuration"  # From config.ErrorMessages.INVALID_CONFIG
        assert "Configuration error" in result['error']
        assert "Missing required configuration" in result['error']
        assert "NOTIFICATION_EMAIL" in result['error']

        # Verify no scanResults in error response
        assert 'scanResults' not in result

        # Verify logging calls
        assert mock_logger.info.call_count >= 2  # Should have some initial logs
        assert mock_logger.error.call_count >= 1  # Should log the error

    @patch('s3_scanner.logger')
    @patch('s3_scanner.scan_s3_bucket')
    @patch('s3_scanner.config.validate_config')
    @patch('s3_scanner.config.setup_logging')
    def test_lambda_handler_s3_bucket_not_found(self, mock_setup_logging, mock_validate_config,
                                               mock_scan_s3_bucket, mock_logger):
        """
        Test lambda_handler behavior when S3 bucket doesn't exist.

        This test verifies that the Lambda function handles S3 bucket not found errors
        gracefully and returns a proper error response. This is one of the most common
        runtime errors in S3-based applications.

        The test mocks scan_s3_bucket() to raise a ClientError with 'NoSuchBucket' error code,
        simulating the scenario where the configured S3 bucket doesn't exist in AWS.
        """
        # Setup mocks
        mock_setup_logging.return_value = None
        mock_validate_config.return_value = True

        # Create a realistic S3 ClientError for "bucket not found"
        s3_error = ClientError(
            error_response={
                'Error': {
                    'Code': 'NoSuchBucket',
                    'Message': 'The specified bucket does not exist'
                }
            },
            operation_name='ListObjectsV2'
        )

        # Make scan_s3_bucket raise the S3 error
        mock_scan_s3_bucket.side_effect = s3_error

        # Mock Lambda context
        mock_context = Mock()
        mock_context.aws_request_id = 'test-request-789'

        # Test data
        test_event = {"test": "data"}

        # Execute the function
        result = s3_scanner.lambda_handler(test_event, mock_context)

        # Verify function call sequence
        mock_setup_logging.assert_called_once()
        mock_validate_config.assert_called_once()
        mock_scan_s3_bucket.assert_called_once()

        # Verify error response structure
        assert isinstance(result, dict)
        assert 'statusCode' in result
        assert 'message' in result
        assert 'error' in result

        # Verify specific S3 bucket not found error response
        assert result['statusCode'] == 500
        assert result['message'] == "S3 bucket not found"  # From config.ErrorMessages.BUCKET_NOT_FOUND
        assert "does not exist" in result['error']

        # Verify bucket name is included in error message
        # (The actual bucket name comes from config.S3_BUCKET_NAME)
        assert "Bucket" in result['error']

        # Verify no scanResults in error response
        assert 'scanResults' not in result

        # Verify logging - should log the S3 error
        assert mock_logger.info.call_count >= 2  # Initial logs
        assert mock_logger.error.call_count >= 1  # Error logging


class TestScanS3Bucket:
    """Test the scan_s3_bucket function"""

    @patch('s3_scanner.logger')
    @patch('s3_scanner.config.get_s3_client')
    @patch('s3_scanner.config.setup_logging')
    def test_scan_s3_bucket_with_pagination(self, mock_setup_logging, mock_get_s3_client, mock_logger):
        """
        Test scan_s3_bucket function with pagination handling.

        This test verifies that the scan_s3_bucket function correctly handles
        S3 pagination when there are more objects than can be returned in a single
        list_objects_v2 API call (AWS returns max 1000 objects per call).

        The test mocks two pages of S3 responses:
        - Page 1: 2 objects + IsTruncated=True (more objects available)
        - Page 2: 1 object + IsTruncated=False (no more objects)

        This ensures the function collects all objects across multiple API calls.
        """
        # Setup mocks
        mock_setup_logging.return_value = None

        # Create mock S3 client
        mock_s3_client = Mock()
        mock_get_s3_client.return_value = mock_s3_client

        # Mock S3 responses - simulate pagination
        # Page 1: 2 objects, more available (IsTruncated=True)
        first_page_response = {
            'Contents': [
                {
                    'Key': 'document1.txt',
                    'Size': 1024,
                    'LastModified': datetime(2025, 1, 15, 10, 30, 45)
                },
                {
                    'Key': 'document2.json',
                    'Size': 2048,
                    'LastModified': datetime(2025, 1, 15, 11, 15, 20)
                }
            ],
            'IsTruncated': True,  # More objects available
            'NextContinuationToken': 'token-123'  # Token for next page
        }

        # Page 2: 1 object, no more available (IsTruncated=False)
        second_page_response = {
            'Contents': [
                {
                    'Key': 'document3.csv',
                    'Size': 512,
                    'LastModified': datetime(2025, 1, 15, 12, 0, 0)
                }
            ],
            'IsTruncated': False  # No more objects
        }

        # Configure mock to return different responses on consecutive calls
        mock_s3_client.list_objects_v2.side_effect = [first_page_response, second_page_response]

        # Execute the function
        result = s3_scanner.scan_s3_bucket()

        # Verify S3 client was called twice (pagination)
        assert mock_s3_client.list_objects_v2.call_count == 2

        # Verify first call parameters (no continuation token)
        first_call_args = mock_s3_client.list_objects_v2.call_args_list[0]
        first_call_kwargs = first_call_args[1]
        assert first_call_kwargs['Bucket'] == 'serverless-s3-scanner-files-'  # From config
        assert first_call_kwargs['MaxKeys'] == 1000
        assert 'ContinuationToken' not in first_call_kwargs

        # Verify second call parameters (with continuation token)
        second_call_args = mock_s3_client.list_objects_v2.call_args_list[1]
        second_call_kwargs = second_call_args[1]
        assert second_call_kwargs['Bucket'] == 'serverless-s3-scanner-files-'  # From config
        assert second_call_kwargs['MaxKeys'] == 1000
        assert second_call_kwargs['ContinuationToken'] == 'token-123'

        # Verify result structure
        assert isinstance(result, dict)
        assert 'bucket_name' in result
        assert 'object_count' in result
        assert 'objects' in result
        assert 'scan_timestamp' in result

        # Verify all objects were collected from both pages
        assert result['object_count'] == 3  # 2 from first page + 1 from second page
        assert len(result['objects']) == 3

        # Verify specific objects are present
        object_keys = [obj['Key'] for obj in result['objects']]
        assert 'document1.txt' in object_keys
        assert 'document2.json' in object_keys
        assert 'document3.csv' in object_keys

        # Verify bucket name
        assert result['bucket_name'] == 'serverless-s3-scanner-files-'  # From config

        # Verify timestamp format
        assert result['scan_timestamp'].endswith('Z')  # ISO format with Z suffix

        # Verify logging calls
        assert mock_logger.info.call_count >= 2  # Should log scanning progress


class TestSendNotification:
    """Test the send_notification function"""

    @patch('s3_scanner.logger')
    @patch('s3_scanner.config.get_sns_client')
    @patch('s3_scanner.config.setup_logging')
    def test_send_notification_success(self, mock_setup_logging, mock_get_sns_client, mock_logger):
        """
        Test send_notification function with successful SNS publishing.

        This test verifies that the send_notification function correctly formats
        scan results into an email message and publishes it to SNS. It tests the
        end-to-end notification flow including email template formatting and
        SNS integration.

        The test mocks the SNS client and verifies that:
        1. The email is formatted correctly using config templates
        2. SNS publish is called with correct parameters
        3. The function handles the SNS response properly
        """
        # Setup mocks
        mock_setup_logging.return_value = None

        # Create mock SNS client
        mock_sns_client = Mock()
        mock_get_sns_client.return_value = mock_sns_client

        # Mock SNS publish response
        mock_sns_response = {
            'MessageId': 'test-message-id-123',
            'ResponseMetadata': {
                'RequestId': 'test-request-id-456',
                'HTTPStatusCode': 200
            }
        }
        mock_sns_client.publish.return_value = mock_sns_response

        # Test scan results data
        test_scan_results = {
            'bucket_name': 'test-bucket-name',
            'object_count': 2,
            'objects': [
                {
                    'Key': 'test-file1.txt',
                    'Size': 1024,
                    'LastModified': datetime(2025, 1, 15, 10, 30, 45)
                },
                {
                    'Key': 'test-file2.json',
                    'Size': 2048,
                    'LastModified': datetime(2025, 1, 15, 11, 15, 20)
                }
            ],
            'scan_duration': 1.25,
            'scan_timestamp': '2025-01-15T10:30:45Z'
        }

        # Mock config values that will be used in email formatting
        with patch('s3_scanner.config.EMAIL_SUBJECT_TEMPLATE', '[Test] S3 Scanner Results'):
            with patch('s3_scanner.config.EMAIL_BODY_TEMPLATE',
                       'Bucket: {bucket_name}\nObjects: {object_count}\nDuration: {duration}s\nFiles:\n{object_list}'):
                with patch('s3_scanner.config.SNS_TOPIC_ARN', 'arn:aws:sns:us-east-1:123456789:TestTopic'):
                    with patch('s3_scanner.config.APP_VERSION', '1.0.0'):
                        with patch('s3_scanner.config.format_object_list') as mock_format_objects:
                            # Mock the object list formatting
                            mock_format_objects.return_value = "• test-file1.txt (0.0 MB)\n• test-file2.json (0.0 MB)"

                            # Execute the function
                            s3_scanner.send_notification(test_scan_results)

        # Verify SNS client was obtained
        mock_get_sns_client.assert_called_once()

        # Verify format_object_list was called with the objects
        mock_format_objects.assert_called_once_with(test_scan_results['objects'])

        # Verify SNS publish was called
        mock_sns_client.publish.assert_called_once()

        # Verify SNS publish parameters
        publish_call_args = mock_sns_client.publish.call_args[1]  # Get keyword arguments

        # Check Topic ARN
        assert publish_call_args['TopicArn'] == 'arn:aws:sns:us-east-1:123456789:TestTopic'

        # Check Subject
        assert publish_call_args['Subject'] == '[Test] S3 Scanner Results'

        # Check Message content includes key information
        message_body = publish_call_args['Message']
        assert 'test-bucket-name' in message_body
        assert '2' in message_body  # object count
        assert '1.25' in message_body  # duration
        assert 'test-file1.txt' in message_body  # file names
        assert 'test-file2.json' in message_body

        # Verify logging calls
        assert mock_logger.info.call_count >= 2  # Should log preparation and success
        assert mock_logger.debug.call_count >= 1  # Should log debug info


# Helper functions for testing (fixing "Unresolved reference" issues)
def scan_s3_bucket():
    """Mock version of scan_s3_bucket for testing reference"""
    return s3_scanner.scan_s3_bucket()


def send_notification(scan_results):
    """Mock version of send_notification for testing reference"""
    return s3_scanner.send_notification(scan_results)