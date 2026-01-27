"""
Script to clear all incidents from OpenSearch/Elasticsearch
This will delete all documents from the thala_knowledge index
"""
import os
import sys
import logging
from dotenv import load_dotenv

# Add team-thala/src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'team-thala', 'src'))

from search_client import get_search_client

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clear_all_incidents():
    """Delete all documents from thala_knowledge index"""
    try:
        es = get_search_client()
        index_name = "thala_knowledge"
        
        # Check if index exists
        if not es.indices.exists(index=index_name):
            logger.info(f"Index '{index_name}' does not exist. Nothing to clear.")
            return
        
        # Count documents first
        count_result = es.count(index=index_name)
        total_docs = count_result['count']
        logger.info(f"Found {total_docs} documents in index '{index_name}'")
        
        if total_docs == 0:
            logger.info("Index is already empty. Nothing to delete.")
            return
        
        # Confirm deletion
        print(f"\n⚠️  WARNING: This will delete ALL {total_docs} documents from '{index_name}' index!")
        response = input("Are you sure you want to continue? (yes/no): ")
        
        if response.lower() != 'yes':
            logger.info("Operation cancelled.")
            return
        
        # Delete all documents using delete_by_query
        logger.info("Deleting all documents...")
        
        # For OpenSearch Serverless, we need to use delete_by_query
        # But AOSS might not support delete_by_query, so we'll try to delete by querying all docs
        
        try:
            # Try delete_by_query first (works for Elasticsearch and managed OpenSearch)
            delete_result = es.delete_by_query(
                index=index_name,
                body={"query": {"match_all": {}}}
            )
            deleted_count = delete_result.get('deleted', 0)
            logger.info(f"✅ Successfully deleted {deleted_count} documents using delete_by_query")
        except Exception as e:
            logger.warning(f"delete_by_query failed: {e}")
            logger.info("Trying alternative method: deleting index and recreating it...")
            
            # Alternative: Delete and recreate index
            # Get the mapping first
            try:
                mapping = es.indices.get_mapping(index=index_name)
                index_mapping = mapping[index_name]['mappings']
                
                # Delete index
                es.indices.delete(index=index_name)
                logger.info(f"Deleted index '{index_name}'")
                
                # Recreate index with same mapping
                es.indices.create(
                    index=index_name,
                    body={"mappings": index_mapping}
                )
                logger.info(f"Recreated index '{index_name}' with original mapping")
                logger.info("✅ Successfully cleared all data by recreating index")
            except Exception as recreate_error:
                logger.error(f"Error recreating index: {recreate_error}")
                logger.error("Index was deleted but could not be recreated. You may need to recreate it manually.")
        
        # Verify deletion - wait for AOSS eventual consistency
        import time
        logger.info("Waiting for eventual consistency (OpenSearch Serverless may take a few seconds)...")
        time.sleep(5)  # Wait 5 seconds for AOSS
        
        # Try counting multiple times
        for attempt in range(3):
            try:
                final_count = es.count(index=index_name)['count']
                logger.info(f"Verification attempt {attempt + 1}: {final_count} documents")
                if final_count == 0:
                    break
                if attempt < 2:
                    time.sleep(3)  # Wait 3 more seconds
            except Exception as e:
                logger.warning(f"Count check failed: {e}")
                time.sleep(2)
        
        # Also try a search to verify
        try:
            search_result = es.search(index=index_name, body={"query": {"match_all": {}}, "size": 1})
            search_count = search_result['hits']['total']['value']
            logger.info(f"Search verification: {search_count} documents found")
        except Exception as e:
            logger.warning(f"Search verification failed: {e}")
            search_count = final_count
        
        if final_count == 0 and search_count == 0:
            logger.info("🎉 All incidents have been cleared successfully!")
        elif final_count == 0:
            logger.info("✅ Count shows 0 documents. Index is cleared!")
        elif search_count == 0:
            logger.info("✅ Search shows 0 documents. Index is cleared (count may be cached)!")
        else:
            logger.warning(f"⚠️  Warning: {final_count} documents still found. This may be due to AOSS eventual consistency.")
            logger.info("💡 Try running the script again in a few seconds, or check the index manually.")
            
    except Exception as e:
        logger.error(f"Error clearing incidents: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    print("=" * 60)
    print("OpenSearch/Elasticsearch Data Cleaner")
    print("=" * 60)
    print()
    clear_all_incidents()

