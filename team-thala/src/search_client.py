import os
from dotenv import load_dotenv

load_dotenv()

ES_BACKEND = os.getenv('SEARCH_BACKEND', 'elasticsearch').lower()

def get_search_client():
    """
    Return a search client for Elasticsearch (default) or OpenSearch.
    Configure via env:
      SEARCH_BACKEND=elasticsearch|opensearch
      ES_HOST=https://localhost:9200
      ES_USER=elastic
      ES_PASSWORD=...
      ES_CA_CERT=path/to/http_ca.crt (optional)

      For OpenSearch Serverless with SigV4, set:
      OPENSEARCH_HOST=https://<endpoint>
      AWS_REGION=us-east-2
    """
    if ES_BACKEND == 'opensearch_serverless':
        # OpenSearch Serverless with SigV4 (AOSS)
        from opensearchpy import OpenSearch, RequestsHttpConnection
        from requests_aws4auth import AWS4Auth
        import boto3
        host = os.getenv('OPENSEARCH_HOST')
        if not host:
            raise ValueError('OPENSEARCH_HOST is required when SEARCH_BACKEND=opensearch_serverless')
        # remove https:// if provided
        host = host.replace('https://', '').replace('http://', '')
        region = os.getenv('AWS_REGION', 'us-east-2')
        session = boto3.Session()
        creds = session.get_credentials()
        if not creds:
            raise ValueError('AWS credentials not found for SigV4 (AOSS)')
        awsauth = AWS4Auth(
            creds.access_key,
            creds.secret_key,
            region,
            'aoss',
            session_token=creds.token,
        )
        return OpenSearch(
            hosts=[{"host": host, "port": 443, "scheme": "https"}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=30,
        )
    if ES_BACKEND == 'opensearch':
        # OpenSearch (managed). For Serverless with SigV4, use requests-sigv4.
        from opensearchpy import OpenSearch
        host = os.getenv('OPENSEARCH_HOST')
        if not host:
            raise ValueError('OPENSEARCH_HOST is required when SEARCH_BACKEND=opensearch')
        # Basic auth mode (managed domain) fallback
        user = os.getenv('ES_USER')
        password = os.getenv('ES_PASSWORD')
        ca = os.getenv('ES_CA_CERT')
        verify = bool(ca) or os.getenv('ES_VERIFY_CERTS', 'false').lower() == 'true'
        return OpenSearch(
            hosts=[host],
            http_auth=(user, password) if user and password else None,
            verify_certs=verify,
            ca_certs=ca if ca else None,
        )
    else:
        # Elasticsearch default
        from elasticsearch import Elasticsearch
        host = os.getenv('ES_HOST', 'https://localhost:9200')
        user = os.getenv('ES_USER', 'elastic')
        password = os.getenv('ES_PASSWORD', '')
        ca = os.getenv('ES_CA_CERT')
        return Elasticsearch(
            hosts=[host],
            ca_certs=ca if ca else None,
            verify_certs=bool(ca),
            basic_auth=(user, password) if user else None,
        )



