import aws_cdk as core
import aws_cdk.assertions as assertions

from serverless_s3_scanner.serverless_s3_scanner_stack import ServerlessS3ScannerStack

# example tests. To run these tests, uncomment this file along with the example
# resource in serverless_s3_scanner/serverless_s3_scanner_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = ServerlessS3ScannerStack(app, "serverless-s3-scanner")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
