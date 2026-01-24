import boto3
import json

# Initialize Textract client
textract_client = boto3.client('textract', region_name='us-east-2')

# Method 1: Extract text from image bytes
def extract_text_from_image(image_path):
    """Extract text from a local image file"""
    with open(image_path, 'rb') as image_file:
        image_bytes = image_file.read()
    
    # Call Textract API
    response = textract_client.detect_document_text(
        Document={'Bytes': image_bytes}
    )
    
    # Extract and print text
    extracted_text = []
    for block in response['Blocks']:
        if block['BlockType'] == 'LINE':
            extracted_text.append(block['Text'])
    
    return '\n'.join(extracted_text)

# Method 2: Extract from S3 object
def extract_text_from_s3(bucket_name, object_key):
    """Extract text from image stored in S3"""
    response = textract_client.detect_document_text(
        Document={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': object_key
            }
        }
    )
    
    extracted_text = []
    for block in response['Blocks']:
        if block['BlockType'] == 'LINE':
            extracted_text.append(block['Text'])
    
    return '\n'.join(extracted_text)

# Usage
if __name__ == "__main__":
    # Local file extraction
    text = extract_text_from_image("C:\\Users\\saini\\Downloads\\testingaws.png")
    print("Extracted Text:")
    print(text)
