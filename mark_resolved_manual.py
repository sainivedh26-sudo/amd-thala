"""
Manually mark a Jira ticket as Resolved with resolution text
"""
from elasticsearch import Elasticsearch
import sys
import io
from datetime import datetime

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

def mark_resolved(issue_id, resolution_text, resolved_by="User (Manual)"):
    """
    Mark a Jira ticket as Resolved
    
    Args:
        issue_id: Jira issue key (e.g., "KAN-21")
        resolution_text: Resolution description
        resolved_by: Who resolved it
    """
    print(f"Marking {issue_id} as Resolved...")
    print(f"Resolution: {resolution_text}")
    print("=" * 80)
    
    # Search for all documents with this issue_id
    query = {
        "query": {
            "term": {"issue_id": issue_id}
        },
        "size": 100
    }
    
    result = es.search(index="thala_knowledge", body=query)
    total = result['hits']['total']['value']
    
    if total == 0:
        print(f"[ERROR] {issue_id}: Not found in Elasticsearch")
        return False
    
    updated = 0
    resolved_at = datetime.utcnow().isoformat()
    
    # Update all matching documents
    for hit in result['hits']['hits']:
        doc_id = hit['_id']
        current_status = hit['_source'].get('status', '')
        
        if current_status == 'Resolved':
            print(f"[SKIP] {issue_id} (doc {doc_id}): Already Resolved")
            continue
        
        # Mark as Resolved
        es.update(
            index="thala_knowledge",
            id=doc_id,
            body={
                "doc": {
                    "status": "Resolved",
                    "resolution_text": resolution_text,
                    "resolved_by": resolved_by,
                    "resolved_at": resolved_at
                }
            }
        )
        print(f"[FIXED] {issue_id} (doc {doc_id}): {current_status} -> Resolved")
        updated += 1
    
    print("=" * 80)
    print(f"[DONE] Updated {updated} document(s)")
    print(f"{issue_id} should no longer appear in /thala issues")
    return True

if __name__ == "__main__":
    # Mark KAN-21 as resolved
    mark_resolved(
        issue_id="KAN-21",
        resolution_text="Issue with RBAC - wrong policy pushed by mistake. Rolled back the change. Should work now.",
        resolved_by="SAI NIVEDH (Slack resolution)"
    )



