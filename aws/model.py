import boto3
import json
import os
from dotenv import load_dotenv

load_dotenv()

# Create a Bedrock Runtime client
client = boto3.client('bedrock-runtime', region_name='us-east-2')

# Use the inference profile ID instead of the base model ID
model_id = 'us.meta.llama3-3-70b-instruct-v1:0'  # Prefix with 'us.' for inference profile

# Define your prompt
user_message = "What is machine learning?"

# Create the request using Converse API
response = client.converse(
    modelId=model_id,  # Now uses inference profile
    messages=[
        {
            'role': 'user',
            'content': [{'text': user_message}]
        }
    ],
    inferenceConfig={
        'maxTokens': 512,
        'temperature': 0.5,
        'topP': 0.9
    }
)

# Print the response
print(response['output']['message']['content'][0]['text'])

# import boto3
# import json
# import os
# from dotenv import load_dotenv

# load_dotenv()

# # Set the API key from environment variable
# os.environ['AWS_BEARER_TOKEN_BEDROCK'] = os.getenv('AWS_BEARER_TOKEN_BEDROCK')

# # Create a Bedrock Runtime client
# client = boto3.client('bedrock-runtime', region_name='us-east-2')

# # Model ID for Llama 3.3 70B Instruct
# model_id = 'meta.llama3-3-70b-instruct-v1:0'

# # Define your prompt
# user_message = "What is machine learning?"

# # Create the request using Converse API (recommended)
# response = client.converse(
#     modelId=model_id,
#     messages=[
#         {
#             'role': 'user',
#             'content': [{'text': user_message}]
#         }
#     ],
#     inferenceConfig={
#         'maxTokens': 512,
#         'temperature': 0.5,
#         'topP': 0.9
#     }
# )

# # Print the response
# print(response['output']['message']['content'][0]['text'])
