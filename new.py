from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer
from flask import Flask, request, jsonify
import numpy as np
import xgboost as xgb
import pandas as pd
import schedule
import threading
import time
import logging
from datetime import datetime
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('thala_prediction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Connect to Elasticsearch
es = Elasticsearch(
    hosts=["https://localhost:9200"],
    ca_certs="D:\\elasticsearch-9.1.5-windows-x86_64\\elasticsearch-9.1.5\\config\\certs\\http_ca.crt",
    verify_certs=False,
    basic_auth=("elastic", "NHyNwkjjOmO1GwUBU54_")
)

# Define the enhanced index mapping
mapping = {
    "mappings": {
        "properties": {
            "text": {"type": "text"},
            "embedding": {"type": "dense_vector", "dims": 384},
            "timestamp": {"type": "date"},
            "status": {"type": "keyword"},
            "incident_likelihood": {"type": "keyword"},
            "source": {"type": "keyword"},
            "resolution_text": {"type": "text"},
            "resolved_by": {"type": "keyword"},
            "resolved_at": {"type": "date"},
            "issue_id": {"type": "keyword"}
        }
    }
}

# Create index if not exists
try:
    es.indices.create(index="thala_knowledge", body=mapping)
    logger.info("Created thala_knowledge index")
except Exception as e:
    logger.info(f"Index already exists or error: {e}")

# Initialize sentence transformer model
model = SentenceTransformer('all-MiniLM-L6-v2')

# Global XGBoost model
xgb_model = None

def load_and_train_initial_model():
    """Load or train XGBoost model - checks for saved model and new data"""
    global xgb_model
    try:
        # First, try to load existing model
        if os.path.exists('xgboost_incident.json'):
            logger.info("Found existing model file, loading...")
            xgb_model = xgb.XGBClassifier()
            xgb_model.load_model('xgboost_incident.json')
            logger.info("Loaded existing XGBoost model from file")
            
            # Check if there's new labeled data to retrain
            try:
                query = {"query": {"exists": {"field": "incident_likelihood"}}}
                response = es.search(index="thala_knowledge", body=query, size=1)
                if response['hits']['total']['value'] > 0:
                    logger.info("Found labeled data in Elasticsearch, triggering retrain...")
                    export_and_retrain()
            except Exception as e:
                logger.warning(f"Could not check for new data: {e}")
            
            return
        
        # No saved model, train from initial data
        logger.info("No saved model found, training from initial_data.csv...")
        if os.path.exists('initial_data.csv'):
            df = pd.read_csv('initial_data.csv')
            X = model.encode(df['text'].tolist(), convert_to_numpy=True)
            y = (df['incident_likelihood'] == 'Likely').astype(int)
            
            xgb_model = xgb.XGBClassifier(
                objective='binary:logistic',
                n_estimators=100,
                max_depth=3,
                learning_rate=0.1,
                random_state=42
            )
            xgb_model.fit(X, y)
            logger.info(f"Initial XGBoost model trained with {len(df)} samples")
            
            # Save model
            xgb_model.save_model('xgboost_incident.json')
            logger.info("Model saved to xgboost_incident.json")
        else:
            logger.warning("initial_data.csv not found, skipping initial training")
    except Exception as e:
        logger.error(f"Error in load_and_train_initial_model: {e}")

def export_and_retrain():
    """Export labeled data from Elasticsearch and retrain model"""
    global xgb_model
    try:
        logger.info("=" * 60)
        logger.info("Starting auto-training process...")
        
        # Scroll through all documents with incident_likelihood labels
        query = {
            "query": {
                "exists": {
                    "field": "incident_likelihood"
                }
            }
        }
        
        # Get all labeled documents
        response = es.search(index="thala_knowledge", body=query, size=1000, scroll='2m')
        
        if 'hits' not in response or 'hits' not in response['hits']:
            logger.warning("No labeled data found in Elasticsearch")
            return
        
        scroll_id = response.get('_scroll_id')
        hits = response['hits']['hits']
        
        all_hits = list(hits)
        
        # Continue scrolling if there are more results
        while len(hits) > 0 and scroll_id:
            try:
                response = es.scroll(scroll_id=scroll_id, scroll='2m')
                scroll_id = response.get('_scroll_id')
                hits = response['hits']['hits']
                all_hits.extend(hits)
            except Exception as scroll_error:
                logger.warning(f"Scroll completed or error: {scroll_error}")
                break
        
        logger.info(f"Found {len(all_hits)} labeled documents in Elasticsearch")
        
        # Extract texts and labels
        texts = []
        labels = []
        for hit in all_hits:
            source = hit.get('_source', {})
            if 'text' in source and 'incident_likelihood' in source:
                texts.append(source['text'])
                labels.append(source['incident_likelihood'])
        
        # Combine with initial data if available
        if os.path.exists('initial_data.csv'):
            initial_df = pd.read_csv('initial_data.csv')
            texts.extend(initial_df['text'].tolist())
            labels.extend(initial_df['incident_likelihood'].tolist())
            logger.info(f"Added {len(initial_df)} samples from initial_data.csv")
        
        if len(texts) < 5:
            logger.warning(f"Not enough labeled data for retraining ({len(texts)} samples, need at least 5)")
            return
        
        logger.info(f"Total training samples: {len(texts)}")
        logger.info(f"Generating embeddings for {len(texts)} texts...")
        
        # Generate embeddings and train
        X = model.encode(texts, convert_to_numpy=True)
        y = np.array([1 if label == 'Likely' else 0 for label in labels])
        
        logger.info(f"Training distribution - Likely: {sum(y)}, Not Likely: {len(y) - sum(y)}")
        
        xgb_model = xgb.XGBClassifier(
            objective='binary:logistic',
            n_estimators=100,
            max_depth=3,
            learning_rate=0.1,
            random_state=42
        )
        xgb_model.fit(X, y)
        xgb_model.save_model('xgboost_incident.json')
        
        logger.info(f"[SUCCESS] Model retrained successfully with {len(texts)} samples")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error during auto-training: {e}")
        import traceback
        logger.error(traceback.format_exc())

def schedule_auto_training():
    """Schedule auto-training to run every hour"""
    schedule.every(1).hours.do(export_and_retrain)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

# Index embeddings function
def index_embeddings(texts, embeddings, timestamp=None, status=None, incident_likelihood=None, source=None, issue_id=None):
    """Index text with embeddings and metadata"""
    for text, embedding in zip(texts, embeddings):
        doc = {
            "text": text,
            "embedding": embedding,
            "timestamp": timestamp or datetime.utcnow().isoformat(),
            "status": status or "Open",
            "source": source or "unknown"
        }
        if incident_likelihood:
            doc["incident_likelihood"] = incident_likelihood
        if issue_id:
            doc["issue_id"] = issue_id
        
        es.index(index="thala_knowledge", body=doc)
        logger.info(f"Indexed document: {text[:50]}... [ID: {issue_id}]" if issue_id else f"Indexed document: {text[:50]}...")

# Flask app
app = Flask(__name__)

@app.route('/index', methods=['POST'])
def index():
    """Index new text data with embeddings"""
    try:
        data = request.get_json()
        texts = data.get('texts', [])
        timestamp = data.get('timestamp')
        status = data.get('status', 'Open')
        incident_likelihood = data.get('incident_likelihood')
        source = data.get('source', 'unknown')
        issue_id = data.get('issue_id')
        
        if not texts:
            return jsonify({"error": "No texts provided"}), 400
        
        embeddings = model.encode(texts, convert_to_numpy=True).tolist()
        index_embeddings(texts, embeddings, timestamp, status, incident_likelihood, source, issue_id)
        
        return jsonify({
            "message": "Indexed successfully",
            "texts": texts,
            "timestamp": timestamp,
            "status": status
        })
    except Exception as e:
        logger.error(f"Error in /index endpoint: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/update_status', methods=['POST'])
def update_status():
    """Update status of an existing document in Elasticsearch"""
    try:
        data = request.get_json()
        original_issue_id = data.get('original_issue_id')
        status = data.get('status')
        resolution_text = data.get('resolution_text')
        
        if not original_issue_id:
            return jsonify({"error": "No original_issue_id provided"}), 400
        
        # Search for the original document by issue_id field (exact match)
        search_query = {
            "query": {
                "term": {
                    "issue_id": original_issue_id
                }
            },
            "size": 10
        }
        
        response = es.search(index="thala_knowledge", body=search_query)
        
        if response['hits']['total']['value'] > 0:
            # Update all matching documents
            updated_count = 0
            for hit in response['hits']['hits']:
                doc_id = hit['_id']
                update_body = {
                    "doc": {
                        "status": status
                    }
                }
                
                # Add resolution metadata if provided
                if resolution_text:
                    update_body["doc"]["resolution_text"] = resolution_text
                
                # Add resolved_by and resolved_at from request
                if data.get('resolved_by'):
                    update_body["doc"]["resolved_by"] = data.get('resolved_by')
                if data.get('resolved_at'):
                    update_body["doc"]["resolved_at"] = data.get('resolved_at')
                
                es.update(index="thala_knowledge", id=doc_id, body=update_body)
                updated_count += 1
                logger.info(f"Updated document {doc_id} status to {status} (resolved by: {data.get('resolved_by', 'unknown')})")
            
            return jsonify({
                "message": f"Updated {updated_count} document(s)",
                "original_issue_id": original_issue_id,
                "new_status": status
            })
        else:
            logger.warning(f"No documents found for issue_id: {original_issue_id}")
            return jsonify({
                "message": "No documents found to update",
                "original_issue_id": original_issue_id
            }), 404
            
    except Exception as e:
        logger.error(f"Error in /update_status endpoint: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/predict_incident', methods=['POST'])
def predict_incident():
    """Predict incident likelihood for a query"""
    try:
        data = request.get_json()
        query = data.get('query', '')
        
        if not query:
            return jsonify({"error": "No query provided"}), 400
        
        if xgb_model is None:
            return jsonify({"error": "Model not trained yet"}), 503
        
        # Generate embedding for query
        query_embedding = model.encode([query], convert_to_numpy=True)
        
        # Predict
        prediction_proba = xgb_model.predict_proba(query_embedding)[0]
        prediction = xgb_model.predict(query_embedding)[0]
        
        # prediction_proba[0] = probability of "Not Likely" (class 0)
        # prediction_proba[1] = probability of "Likely" (class 1)
        prob_not_likely = float(prediction_proba[0])
        prob_likely = float(prediction_proba[1])
        
        if prediction == 1:
            incident_likelihood = "Likely"
            confidence = prob_likely
        else:
            incident_likelihood = "Not Likely"
            confidence = prob_not_likely
        
        logger.info(f"Prediction for '{query}': {incident_likelihood} (confidence: {confidence:.2f}) [Proba: Not Likely={prob_not_likely:.2f}, Likely={prob_likely:.2f}]")
        
        return jsonify({
            "query": query,
            "incident_likelihood": incident_likelihood,
            "confidence": round(confidence, 2)
        })
    except Exception as e:
        logger.error(f"Error in /predict_incident endpoint: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/search', methods=['POST'])
def search():
    """Search for similar incidents and include prediction"""
    try:
        data = request.get_json()
        query = data.get('query', '')
        top_k = data.get('top_k', 5)
        
        if not query:
            return jsonify({"error": "No query provided"}), 400
        
        query_embedding = model.encode([query], convert_to_numpy=True).tolist()[0]
        
        # Search for similar documents
        response = es.search(
            index="thala_knowledge",
            body={
                "size": top_k,
                "query": {
                    "script_score": {
                        "query": {"match_all": {}},
                        "script": {
                            "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                            "params": {"query_vector": query_embedding}
                        }
                    }
                }
            }
        )
        
        hits = response['hits']['hits']
        results = []
        for hit in hits:
            result = {
                "text": hit['_source']['text'],
                "score": hit['_score'],
                "timestamp": hit['_source'].get('timestamp'),
                "status": hit['_source'].get('status'),
                "incident_likelihood": hit['_source'].get('incident_likelihood'),
                "resolution_text": hit['_source'].get('resolution_text'),
                "resolved_by": hit['_source'].get('resolved_by'),
                "resolved_at": hit['_source'].get('resolved_at'),
                "issue_id": hit['_source'].get('issue_id')
            }
            results.append(result)
        
        # Deduplicate by issue_id - keep only the most recent version of each issue
        seen_issues = {}
        for r in results:
            issue_id = r.get('issue_id')
            if issue_id:
                # Keep the one with latest timestamp, or with complete resolution info
                if issue_id not in seen_issues:
                    seen_issues[issue_id] = r
                else:
                    existing = seen_issues[issue_id]
                    # Prefer complete resolution info
                    r_has_complete = (r.get('resolution_text') and r.get('resolved_by') and r.get('resolved_at'))
                    existing_has_complete = (existing.get('resolution_text') and existing.get('resolved_by') and existing.get('resolved_at'))
                    
                    if r_has_complete and not existing_has_complete:
                        seen_issues[issue_id] = r
                    elif existing_has_complete and not r_has_complete:
                        pass  # Keep existing
                    else:
                        # Both same completeness, keep more recent
                        try:
                            r_time = r.get('timestamp', '')
                            existing_time = existing.get('timestamp', '')
                            if r_time > existing_time:
                                seen_issues[issue_id] = r
                        except:
                            pass
        
        results = list(seen_issues.values())
        
        # Simple sorting: Resolved + Complete Info first, then everything else by score
        def sort_key(r):
            # Check if this is a complete resolved incident
            is_resolved = r.get('status') == 'Resolved'
            has_resolution_text = r.get('resolution_text') is not None and r.get('resolution_text') != ''
            has_resolved_by = r.get('resolved_by') is not None and r.get('resolved_by') != ''
            has_resolved_at = r.get('resolved_at') is not None and r.get('resolved_at') != ''
            has_complete_info = has_resolution_text and has_resolved_by and has_resolved_at
            
            # Return tuple for sorting: (is_complete_resolved, semantic_score)
            # Python sorts tuples element by element
            # True > False in Python, so complete resolved incidents come first
            # Then within each group, sort by semantic score (higher first)
            return (is_resolved and has_complete_info, r.get('score', 0))
        
        results.sort(key=sort_key, reverse=True)
        
        # Get prediction for the query
        prediction = None
        if xgb_model is not None:
            query_emb = model.encode([query], convert_to_numpy=True)
            pred_proba = xgb_model.predict_proba(query_emb)[0]
            pred = xgb_model.predict(query_emb)[0]
            
            prob_not_likely = float(pred_proba[0])
            prob_likely = float(pred_proba[1])
            
            if pred == 1:
                incident_likelihood = "Likely"
                confidence = prob_likely
            else:
                incident_likelihood = "Not Likely"
                confidence = prob_not_likely
            
            prediction = {
                "query": query,
                "incident_likelihood": incident_likelihood,
                "confidence": round(confidence, 2)
            }
        
        return jsonify({
            "results": results,
            "prediction": prediction
        })
    except Exception as e:
        logger.error(f"Error in /search endpoint: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "model_loaded": xgb_model is not None,
        "elasticsearch_connected": es.ping()
    })

def initialize_app():
    """Initialize the application"""
    logger.info("Initializing ITSM Incident Prediction System...")
    
    # Load and train initial model
    load_and_train_initial_model()
    
    # Start auto-training scheduler in background thread
    scheduler_thread = threading.Thread(target=schedule_auto_training, daemon=True)
    scheduler_thread.start()
    logger.info("Auto-training scheduler started (runs every hour)")

if __name__ == '__main__':
    initialize_app()
    app.run(host='0.0.0.0', port=5000, debug=False)
