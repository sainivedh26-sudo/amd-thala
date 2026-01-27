import os
import json
import time
from datetime import datetime
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
from dotenv import load_dotenv

load_dotenv()

def get_opensearch_client():
    # OPENSEARCH_HOST should be ONLY the domain, no https:// prefix
    # e.g.: mq0f51ervmima24i2pp7.us-east-2.aoss.amazonaws.com
    host = os.getenv("OPENSEARCH_HOST")
    if not host:
        raise ValueError("OPENSEARCH_HOST env not set (e.g. mq0f51ervmima24i2pp7.us-east-2.aoss.amazonaws.com)")
    
    region = os.getenv("AWS_REGION", "us-east-2")

    session = boto3.Session()
    creds = session.get_credentials()
    if not creds:
        raise ValueError("No AWS credentials found. Configure AWS CLI or set env vars.")

    awsauth = AWS4Auth(
        creds.access_key,
        creds.secret_key,
        region,
        "aoss",
        session_token=creds.token,
    )

    client = OpenSearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=30,
        max_retries=2,
        retry_on_timeout=True,
    )
    return client

# import os
# import json
# import time
# from datetime import datetime
# from opensearchpy import OpenSearch, RequestsHttpConnection
# from requests_aws4auth import AWS4Auth
# import boto3
# from dotenv import load_dotenv
# load_dotenv()
# def get_opensearch_client():
#     # Expect OPENSEARCH_HOST like: abcdefg123456.us-east-2.aoss.amazonaws.com (no https://)
#     host = os.getenv("OPENSEARCH_HOST")
#     if not host:
#         raise ValueError("OPENSEARCH_HOST env not set (e.g. abc.us-east-2.aoss.amazonaws.com)")
#     region = os.getenv("AWS_REGION", "us-east-2")

#     session = boto3.Session()
#     creds = session.get_credentials()
#     if not creds:
#         raise ValueError("No AWS credentials found. Configure AWS CLI or set env vars.")

#     awsauth = AWS4Auth(
#         creds.access_key,
#         creds.secret_key,
#         region,
#         "aoss",
#         session_token=creds.token,
#     )

#     client = OpenSearch(
#         hosts=[{"host": host, "port": 443, "scheme": "https"}],
#         http_auth=awsauth,
#         use_ssl=True,
#         verify_certs=True,
#         connection_class=RequestsHttpConnection,
#         timeout=30,
#         max_retries=2,
#         retry_on_timeout=True,
#     )
#     return client

def main():
    index_name = os.getenv("AOSS_INDEX", "thala_knowledge")
    client = get_opensearch_client()

    # Create index if it doesn't exist
    if not client.indices.exists(index=index_name):
        mapping = {
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {
                "properties": {
                    "text": {"type": "text"},
                    "source": {"type": "keyword"},
                    "status": {"type": "keyword"},
                    "category": {"type": "keyword"},
                    "severity": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                }
            },
        }
        resp = client.indices.create(index=index_name, body=mapping)
        print("Index created:", json.dumps(resp, indent=2))
    else:
        print(f"Index '{index_name}' already exists")

    # Index one test document
    doc_id = f"test-{int(time.time())}"
    doc = {
        "text": "OpenSearch Serverless connectivity test from Thala",
        "source": "test",
        "status": "Open",
        "category": "Application",
        "severity": "Low",
        "timestamp": datetime.utcnow().isoformat(),
    }
    put = client.index(index=index_name, id=doc_id, body=doc)
    print("Indexed doc:", json.dumps(put, indent=2))

    # Search back
    res = client.search(
        index=index_name,
        body={"query": {"match": {"text": "connectivity"}}},
        size=1,
    )
    hits = res.get("hits", {}).get("hits", [])
    print("Search hits:", json.dumps(hits, indent=2))

if __name__ == "__main__":
    main()