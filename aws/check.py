import boto3
from dotenv import load_dotenv
load_dotenv()
import os
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.getenv('AWS_SESSION_TOKEN')
)

# Use S3
# response = s3_client.list_buckets()
# print(response)

s3_client.upload_file(
    Filename="C:\\Users\\saini\\Downloads\\reddit.jpg",
    Bucket='thala-images',
    Key='images/reddit.jpg',
    ExtraArgs={'ACL': 'public-read'}  # Make it publicly accessible
)
print("Upload successful!")


# Delete the image from S3