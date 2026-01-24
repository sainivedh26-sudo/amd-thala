# lambda_function.py
import boto3
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os

# Initialize clients
kendra_client = boto3.client('kendra', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-2'))

# Use your Kendra Index ID from Part 3
KENDRA_INDEX_ID = ''

def search_relevant_websites(query):
    """
    Search Kendra index for relevant websites
    """
    try:
        print(f"Searching Kendra for: {query}")
        
        response = kendra_client.query(
            IndexId=KENDRA_INDEX_ID,
            QueryText=query,
            PageSize=10
        )
        
        results = []
        for item in response.get('ResultItems', []):
            result = {
                'title': item.get('DocumentTitle', ''),
                'excerpt': item.get('DocumentExcerpt', {}).get('Text', '') if item.get('DocumentExcerpt') else '',
                'source': item.get('DocumentURI', ''),
                'relevance_score': item.get('ScoreAttributes', {}).get('ScoreConfidence', 0)
            }
            results.append(result)
            print(f"  Found: {result['title']} ({result['source']})")
        
        return results
    
    except Exception as e:
        print(f"Kendra search error: {str(e)}")
        return []


def crawl_website(url):
    """
    Crawl a single website and extract relevant content
    """
    try:
        print(f"Crawling: {url}")
        
        response = requests.get(url, timeout=10, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Remove script and style elements
        for script in soup(['script', 'style']):
            script.decompose()
        
        # Extract content
        extracted = {
            'url': url,
            'title': soup.title.string if soup.title else 'N/A',
            'main_content': soup.get_text(separator='\n', strip=True)[:3000],
            'headings': [h.get_text().strip() for h in soup.find_all(['h1', 'h2', 'h3'])][:10],
            'code_blocks': [code.get_text() for code in soup.find_all('code')][:5],
            'crawl_timestamp': datetime.now().isoformat()
        }
        
        print(f"  ✓ Successfully crawled")
        return extracted
    
    except Exception as e:
        print(f"  ✗ Error: {str(e)}")
        return {
            'url': url,
            'error': f"Failed to crawl: {str(e)}"
        }


def lambda_handler(event, context):
    """
    Main Lambda handler
    """
    try:
        print("="*60)
        print("Lambda Handler Started")
        print("="*60)
        
        # Parse request
        if isinstance(event, str):
            event = json.loads(event)
        
        # Get query from different possible locations
        body = event.get('body')
        if body and isinstance(body, str):
            body = json.loads(body)
        
        query = None
        if body:
            query = body.get('query')
        
        if not query:
            query_params = event.get('queryStringParameters')
            if query_params:
                query = query_params.get('query')
        
        if not query:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing "query" parameter'}),
                'headers': {'Content-Type': 'application/json'}
            }
        
        print(f"Processing query: {query}\n")
        
        # Step 1: Search Kendra
        print("STEP 1: Searching Kendra Index")
        print("-" * 60)
        search_results = search_relevant_websites(query)
        
        if not search_results:
            print("No results found in Kendra")
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'No relevant websites found in Kendra index'}),
                'headers': {'Content-Type': 'application/json'}
            }
        
        print(f"Found {len(search_results)} results\n")
        
        # Step 2: Crawl websites
        print("STEP 2: Crawling Websites")
        print("-" * 60)
        crawled_data = []
        
        for idx, result in enumerate(search_results, 1):
            print(f"\n[{idx}/{len(search_results)}]")
            crawled = crawl_website(result['source'])
            crawled['kendra_relevance_score'] = result['relevance_score']
            crawled['kendra_excerpt'] = result['excerpt']
            
            crawled_data.append(crawled)
        
        print(f"\n\nSTEP 3: Preparing Response")
        print("-" * 60)
        
        # Return results
        response_data = {
            'query': query,
            'results_found': len(crawled_data),
            'data': crawled_data
        }
        
        print(f"Returning {len(crawled_data)} results\n")
        
        return {
            'statusCode': 200,
            'body': json.dumps(response_data, indent=2),
            'headers': {'Content-Type': 'application/json'}
        }
    
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}),
            'headers': {'Content-Type': 'application/json'}
        }


# For local testing
if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    # Test locally
    test_event = {
        'body': json.dumps({'query': 'database connection pool reset'})
    }
    
    result = lambda_handler(test_event, None)
    print("\n" + "="*60)
    print("RESULT")
    print("="*60)
    print(json.dumps(json.loads(result['body']), indent=2))
