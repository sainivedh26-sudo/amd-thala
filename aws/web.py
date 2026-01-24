import requests
import json

# Replace with your actual Function URL from the output above
LAMBDA_URL = "https://oj4j6xjjvv7xlgg5sxzzf7essq0ahhox.lambda-url.us-east-2.on.aws/"

def extract_websites(description):
    """Send request to Lambda and get extracted website data"""
    
    payload = {
        'query': description
    }
    
    print(f"Sending request with query: {description}\n")
    
    response = requests.post(LAMBDA_URL, json=payload)
    
    if response.status_code == 200:
        data = response.json()
        print(f"✓ Found {data['results_found']} relevant websites\n")
        print(json.dumps(data, indent=2))
    else:
        print(f"Error: {response.status_code}")
        print(response.text)

# Usage
extract_websites("database connection pool reset")
