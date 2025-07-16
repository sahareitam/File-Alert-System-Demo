#!/bin/bash
# Environment Check Script for Serverless S3 Scanner Project

echo "=== Serverless S3 Scanner - Environment Check ==="
echo ""

# Check Python version
echo "1. Python Version:"
python3 --version
echo ""

# Check AWS CLI
echo "2. AWS CLI:"
aws --version
echo ""

# Check AWS credentials (without showing sensitive data)
echo "3. AWS Credentials Configuration:"
aws configure list
echo ""

# Check AWS account ID and region
echo "4. AWS Account Information:"
aws sts get-caller-identity --query 'Account' --output text 2>/dev/null
aws configure get region
echo ""

# Check CDK
echo "5. AWS CDK:"
cdk --version
echo ""

# Check Node.js (required for CDK)
echo "6. Node.js:"
node --version
echo ""

# Check npm
echo "7. npm:"
npm --version
echo ""

# Check if CDK is bootstrapped
echo "8. CDK Bootstrap Status:"
echo "Run this command to check if CDK is bootstrapped in your account:"
echo "cdk bootstrap"
echo ""

echo "=== Environment Check Complete ==="