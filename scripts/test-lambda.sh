#!/bin/bash

# Manual Lambda test script
# This script invokes the S3Scanner Lambda function and shows the results

echo "Testing S3Scanner Lambda Function..."
echo "======================================"

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI is not installed"
    echo "Please install AWS CLI first: https://aws.amazon.com/cli/"
    exit 1
fi

# Check if AWS is configured
if ! aws sts get-caller-identity &> /dev/null; then
    echo "Error: AWS CLI is not configured"
    echo "Please run: aws configure"
    exit 1
fi

# Set variables
FUNCTION_NAME="S3Scanner"
REGION="eu-west-1"
OUTPUT_FILE="lambda-response.json"

echo "Function: $FUNCTION_NAME"
echo "Region: $REGION"
echo ""

# Invoke the Lambda function
echo "Invoking Lambda function..."
aws lambda invoke \
    --function-name $FUNCTION_NAME \
    --region $REGION \
    --payload '{}' \
    --cli-binary-format raw-in-base64-out \
    $OUTPUT_FILE

# Check if the invoke was successful
if [ $? -eq 0 ]; then
    echo ""
    echo "Lambda invocation successful!"
    echo ""
    echo "Response from Lambda:"
    echo "====================="
    cat $OUTPUT_FILE | python3 -m json.tool
    echo ""
    echo "Check your email (sahar283@gmail.com) for scan results!"
    echo ""
    echo "Response saved to: $OUTPUT_FILE"
else
    echo ""
    echo "Lambda invocation failed!"
    echo "Check the error message above"
    exit 1
fi