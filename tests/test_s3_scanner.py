"""
Unit tests for lambda/s3_scanner.py

Tests the main Lambda function and its helper functions.
These tests use mocking to avoid actual AWS service calls during testing.
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime
import sys
import os

# Add the lambda directory to the path for importing s3_scanner and config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../lambda'))
import s3_scanner


class TestLambdaHandler:
    """Test the main lambda_handler function"""

    @patch('s3_scanner.send_notification')
    @patch('s3_scanner.scan_s3_bucket')
    @patch('s3_scanner.config.validate_config')
    @patch('s3_scanner.config.setup_logging')
    def test_lambda_handler_success(self, mock_setup_logging, mock_validate_config,
                                    mock_scan_s3_bucket, mock_send_notification):
        """
        Test the complete successful execution flow of lambda_handler.

        This test mocks all external dependencies and verifies:
        1. Proper function call sequence
        2. Correct response structure
        3. Success status code and message
        4. Scan results formatting
        """
        # Setup mocks
        mock_logger = Mock()
        mock_setup_logging.return_value = mock_logger
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
        test_event = {}  # Empty event for manual trigger

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