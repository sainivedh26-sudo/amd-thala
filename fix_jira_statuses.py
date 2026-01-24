"""
Fix existing Jira tickets in Elasticsearch
Updates status from "To Do" to "Open" so they appear in /thala issues
"""
from elasticsearch import Elasticsearch
import os
import sys
import io

# Fix Unicode encoding for Windows console
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Connect to Elasticsearch
es = Elasticsearch(
    hosts=["https://localhost:9200"],
    ca_certs="D:\\elasticsearch-9.1.5-windows-x86_64\\elasticsearch-9.1.5\\config\\certs\\http_ca.crt",
    verify_certs=False,
    basic_auth=("elastic", "NHyNwkjjOmO1GwUBU54_")
)

def fix_jira_statuses():
    """Update all Jira tickets with non-standard statuses to 'Open'"""
    
    # Find all documents with Jira source and non-Open status
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"source": "jira"}},
                    {"bool": {
                        "must_not": {"term": {"status": "Open"}}
                    }}
                ]
            }
        }
    }
    
    result = es.search(
        index="thala_knowledge",
        body=query,
        size=1000
    )
    
    total = result['hits']['total']['value']
    print(f"Found {total} Jira tickets with non-Open status")
    
    if total == 0:
        print("No tickets to fix!")
        return
    
    fixed = 0
    for hit in result['hits']['hits']:
        doc_id = hit['_id']
        current_status = hit['_source'].get('status', '')
        
        # Normalize Jira statuses to Open (unless already Resolved)
        if current_status.lower() in ['done', 'resolved', 'closed', 'cancelled']:
            new_status = 'Resolved'
        else:
            new_status = 'Open'
        
        # Update the document
        es.update(
            index="thala_knowledge",
            id=doc_id,
            body={
                "doc": {
                    "status": new_status
                }
            }
        )
        
        issue_id = hit['_source'].get('issue_id', 'unknown')
        print(f"[FIXED] {issue_id}: {current_status} -> {new_status}")
        fixed += 1
    
    print(f"\n[DONE] Fixed {fixed} Jira tickets!")
    print("Now they should appear in /thala issues")

if __name__ == "__main__":
    try:
        fix_jira_statuses()
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()

