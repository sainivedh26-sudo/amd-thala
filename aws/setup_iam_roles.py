# setup_iam_roles.py
import boto3
import json
import os
from dotenv import load_dotenv

load_dotenv()

# AWS credentials are automatically loaded from .env
os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')
os.environ['AWS_SESSION_TOKEN'] = os.getenv('AWS_SESSION_TOKEN')
os.environ['AWS_DEFAULT_REGION'] = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

iam = boto3.client('iam')
account_id = boto3.client('sts').get_caller_identity()['Account']

# Trust policy for Kendra
trust_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "kendra.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}

# Create role for Kendra index
print("Creating IAM role for Kendra Index...")
try:
    role_response = iam.create_role(
        RoleName='KendraIndexRole',
        AssumeRolePolicyDocument=json.dumps(trust_policy),
        Description='Role for Kendra Index'
    )
    role_arn = role_response['Role']['Arn']
    print(f"✓ Role created: {role_arn}")
except iam.exceptions.EntityAlreadyExistsException:
    role_arn = f"arn:aws:iam::{account_id}:role/KendraIndexRole"
    print(f"✓ Role already exists: {role_arn}")

# Attach policy to role
policy_document = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "kendra:*",
                "cloudwatch:PutMetricAlarm",
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "*"
        }
    ]
}

print("Attaching policy to role...")
iam.put_role_policy(
    RoleName='KendraIndexRole',
    PolicyName='KendraIndexPolicy',
    PolicyDocument=json.dumps(policy_document)
)
print("✓ Policy attached")

print(f"\nSave this role ARN: {role_arn}")
