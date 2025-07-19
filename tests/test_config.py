"""
Unit tests for lambda/config.py
Tests configuration validation, utility functions, and error handling
"""

import pytest
from unittest.mock import patch, Mock
from datetime import datetime

# Import the config module
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../lambda'))
import config
import logging


class TestConfigValidation:
    """Test configuration validation functions"""

    def test_validate_config_success(self):
        """Test that validation passes with all required config"""
        with patch.dict(os.environ, {
            'AWS_REGION': 'us-east-1',
            'AWS_ACCOUNT_ID': '123456789012',
            'NOTIFICATION_EMAIL': 'test@example.com'
        }):
            # Reload config to pick up new environment variables
            import importlib
            importlib.reload(config)

            # Should not raise any exception
            result = config.validate_config()
            assert result is True

    def test_validate_config_missing_region(self):
        """Test that validation fails when AWS_REGION is missing"""
        with patch.dict(os.environ, {
            'AWS_REGION': '',
            'AWS_ACCOUNT_ID': '123456789012',
            'NOTIFICATION_EMAIL': 'test@example.com'
        }):
            import importlib
            importlib.reload(config)

            with pytest.raises(ValueError) as exc_info:
                config.validate_config()

            assert "Missing required configuration" in str(exc_info.value)
            assert "AWS_REGION" in str(exc_info.value)

    def test_validate_config_missing_email(self):
        """Test that validation fails when email is missing"""
        with patch.dict(os.environ, {
            'AWS_REGION': 'us-east-1',
            'AWS_ACCOUNT_ID': '123456789012',
            'NOTIFICATION_EMAIL': ''
        }):
            import importlib
            importlib.reload(config)

            with pytest.raises(ValueError) as exc_info:
                config.validate_config()

            assert "NOTIFICATION_EMAIL" in str(exc_info.value)

    def test_validate_config_multiple_missing(self):
        """Test that validation reports all missing configuration"""
        with patch.dict(os.environ, {
            'AWS_REGION': '',
            'AWS_ACCOUNT_ID': '',
            'NOTIFICATION_EMAIL': ''
        }):
            import importlib
            importlib.reload(config)

            with pytest.raises(ValueError) as exc_info:
                config.validate_config()

            error_msg = str(exc_info.value)
            assert "AWS_REGION" in error_msg
            assert "NOTIFICATION_EMAIL" in error_msg


class TestUtilityFunctions:
    """Test utility functions"""

    def test_get_current_timestamp_format(self):
        """Test that timestamp is in correct ISO format"""
        timestamp = config.get_current_timestamp()

        # Should end with 'Z'
        assert timestamp.endswith('Z')

        # Should be parseable as ISO format
        # Remove 'Z' and parse
        datetime.fromisoformat(timestamp[:-1])

    def test_format_object_list_empty(self):
        """Test formatting empty object list"""
        result = config.format_object_list([])
        assert result == "No objects found in the bucket."

    def test_format_object_list_single_object(self):
        """Test formatting single S3 object"""
        test_objects = [{
            'Key': 'test-file.txt',
            'Size': 1024,
            'LastModified': datetime(2025, 1, 15, 10, 30, 45)
        }]

        result = config.format_object_list(test_objects)

        assert "test-file.txt" in result
        assert "0.0 MB" in result  # 1024 bytes = 0.0 MB when rounded
        assert "2025-01-15 10:30:45 UTC" in result
        assert "•" in result  # Bullet point

    def test_format_object_list_multiple_objects(self):
        """Test formatting multiple S3 objects"""
        test_objects = [
            {
                'Key': 'file1.txt',
                'Size': 1048576,  # 1 MB
                'LastModified': datetime(2025, 1, 15, 10, 30, 45)
            },
            {
                'Key': 'file2.json',
                'Size': 2097152,  # 2 MB
                'LastModified': datetime(2025, 1, 15, 11, 30, 45)
            }
        ]

        result = config.format_object_list(test_objects)

        # Check both files are included
        assert "file1.txt" in result
        assert "file2.json" in result
        assert "1.0 MB" in result
        assert "2.0 MB" in result

        # Check formatting
        lines = result.split('\n')
        assert len(lines) == 2
        assert all(line.strip().startswith('•') for line in lines)

    def test_format_object_list_unknown_last_modified(self):
        """Test formatting object with unknown last modified date"""
        test_objects = [{
            'Key': 'test-file.txt',
            'Size': 1024,
            'LastModified': 'Unknown'
        }]

        result = config.format_object_list(test_objects)
        assert "Modified: Unknown" in result


class TestAWSClientFactory:
    """Test AWS client factory functions"""

    @patch('config.boto3.client')
    def test_get_s3_client_success(self, mock_boto_client):
        """Test successful S3 client creation"""
        mock_client = Mock()
        mock_boto_client.return_value = mock_client

        result = config.get_s3_client()

        # Verify boto3.client was called correctly
        mock_boto_client.assert_called_once_with('s3', region_name=config.AWS_REGION)

        # Verify returned client
        assert result == mock_client

    @patch('config.boto3.client')
    def test_get_s3_client_failure(self, mock_boto_client):
        """Test S3 client creation failure"""
        mock_boto_client.side_effect = Exception("Credentials error")

        with pytest.raises(Exception) as exc_info:
            config.get_s3_client()

        assert "Credentials error" in str(exc_info.value)

    @patch('config.boto3.client')
    def test_get_sns_client_success(self, mock_boto_client):
        """Test successful SNS client creation"""
        mock_client = Mock()
        mock_boto_client.return_value = mock_client

        result = config.get_sns_client()

        # Verify boto3.client was called correctly
        mock_boto_client.assert_called_once_with('sns', region_name=config.AWS_REGION)

        # Verify returned client
        assert result == mock_client

    @patch('config.boto3.client')
    def test_get_sns_client_failure(self, mock_boto_client):
        """Test SNS client creation failure"""
        mock_boto_client.side_effect = Exception("Network error")

        with pytest.raises(Exception) as exc_info:
            config.get_sns_client()

        assert "Network error" in str(exc_info.value)


class TestConstants:
    """Test constants and configuration values"""

    def test_email_templates_exist(self):
        """Test that email templates are defined"""
        assert config.EMAIL_SUBJECT_TEMPLATE
        assert config.EMAIL_BODY_TEMPLATE
        assert "Serverless S3 Scanner" in config.EMAIL_SUBJECT_TEMPLATE
        assert "{bucket_name}" in config.EMAIL_BODY_TEMPLATE
        assert "{object_count}" in config.EMAIL_BODY_TEMPLATE

    def test_error_messages_exist(self):
        """Test that error message constants exist"""
        assert hasattr(config.ErrorMessages, 'BUCKET_NOT_FOUND')
        assert hasattr(config.ErrorMessages, 'ACCESS_DENIED')
        assert hasattr(config.ErrorMessages, 'SNS_PUBLISH_FAILED')
        assert hasattr(config.ErrorMessages, 'INVALID_CONFIG')
        assert hasattr(config.ErrorMessages, 'GENERAL_ERROR')

    def test_feature_flags_exist(self):
        """Test that feature flags are defined"""
        assert isinstance(config.FEATURES, dict)
        assert 'enable_detailed_logging' in config.FEATURES
        assert 'include_object_metadata' in config.FEATURES
        assert 'send_empty_bucket_notification' in config.FEATURES
        assert 'validate_email_format' in config.FEATURES

    def test_lambda_configuration_values(self):
        """Test Lambda configuration constants"""
        assert config.LAMBDA_FUNCTION_NAME == 'S3Scanner'
        assert config.LAMBDA_TIMEOUT == 30
        assert config.LAMBDA_MEMORY == 128
        assert config.APP_NAME == 'Serverless S3 Scanner'
        assert config.APP_VERSION == '1.0.0'


class TestLoggingSetup:
    """Test logging configuration"""

    @patch('config.logging.basicConfig')
    def test_setup_logging(self, mock_basic_config):
        """Test logging setup function"""
        result = config.setup_logging()

        # Verify basicConfig was called
        mock_basic_config.assert_called_once()

        # Verify return value
        assert result is None
        mock_basic_config.assert_called_once()
        call_args, call_kwargs = mock_basic_config.call_args
        assert call_kwargs['level'] == getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)
        assert 'format' in call_kwargs
        assert 'datefmt' in call_kwargs


# Test runner
if __name__ == '__main__':
    pytest.main([__file__, '-v'])