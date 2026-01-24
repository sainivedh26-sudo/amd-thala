"""
Manually mark deleted Jira tickets as Resolved in Elasticsearch
Use this if you've deleted tickets in Jira but they still show as Open
"""
from elasticsearch import Elasticsearch
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

def mark_jira_deleted(issue_keys):
    """
    Mark specific Jira issues as Resolved (deleted)
    
    Args:
        issue_keys: List of Jira issue keys (e.g., ['KAN-21', 'KAN-20'])
    """
    if not issue_keys:
        print("No issue keys provided!")
        return
    
    print(f"Marking {len(issue_keys)} Jira issues as Resolved (deleted)...")
    print("=" * 60)
    
    updated = 0
    for issue_key in issue_keys:
        # Search for all documents with this issue_id
        query = {
            "query": {
                "term": {"issue_id": issue_key}
            },
            "size": 100
        }
        
        result = es.search(index="thala_knowledge", body=query)
        total = result['hits']['total']['value']
        
        if total == 0:
            print(f"[SKIP] {issue_key}: Not found in Elasticsearch")
            continue
        
        # Update all matching documents
        for hit in result['hits']['hits']:
            doc_id = hit['_id']
            current_status = hit['_source'].get('status', '')
            
            if current_status == 'Resolved':
                print(f"[SKIP] {issue_key} (doc {doc_id}): Already Resolved")
                continue
            
            # Mark as Resolved
            es.update(
                index="thala_knowledge",
                id=doc_id,
                body={
                    "doc": {
                        "status": "Resolved",
                        "resolution_text": "Issue deleted from Jira",
                        "resolved_by": "System (Manual)",
                        "resolved_at": "2025-11-02T00:00:00Z"
                    }
                }
            )
            print(f"[FIXED] {issue_key} (doc {doc_id}): {current_status} -> Resolved")
            updated += 1
    
    print("=" * 60)
    print(f"[DONE] Updated {updated} document(s)")
    print("Deleted tickets should no longer appear in /thala issues")

if __name__ == "__main__":
    # Add issue keys here that you've deleted in Jira
    deleted_issues = [
        'KAN-21',  # Add your deleted issue keys here
        'KAN-20',  # You can add multiple
        # 'KAN-18',
        # 'KAN-5',
    ]
    
    if len(sys.argv) > 1:
        # Allow command-line arguments
        deleted_issues = sys.argv[1:]
    
    print("Jira Deletion Marker")
    print("=" * 60)
    print(f"Issues to mark as deleted: {', '.join(deleted_issues)}")
    print()
    
    try:
        mark_jira_deleted(deleted_issues)
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()



