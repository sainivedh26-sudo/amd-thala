"""
Thala AI Jira Integration - Custom Panel/Web Integration
Provides REST API endpoints that can be embedded in Jira as:
1. Connect App (iframe panels)
2. Browser extension
3. Custom automation rules
"""

import os
import logging
from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
import requests
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)  # Enable CORS for Jira domain
logger = logging.getLogger(__name__)

# Flask API URL (your existing backend)
FLASK_API_URL = os.getenv("FLASK_API_URL", "http://localhost:5000")
JIRA_URL = os.getenv("JIRA_URL", "")

# ============================================================================
# HTML TEMPLATES FOR JIRA PANELS
# ============================================================================

JIRA_PANEL_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Thala AI - Similar Incidents</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
            font-size: 14px;
            line-height: 1.5;
            color: #172B4D;
            background: #FFFFFF;
            padding: 16px;
        }
        .header {
            display: flex;
            align-items: center;
            margin-bottom: 16px;
            padding-bottom: 12px;
            border-bottom: 2px solid #DFE1E6;
        }
        .header h2 {
            font-size: 18px;
            font-weight: 600;
            color: #0052CC;
        }
        .header .badge {
            margin-left: 8px;
            padding: 2px 8px;
            background: #0052CC;
            color: white;
            border-radius: 3px;
            font-size: 11px;
            font-weight: 600;
        }
        .search-box {
            margin-bottom: 16px;
        }
        .search-box input {
            width: 100%;
            padding: 8px 12px;
            border: 2px solid #DFE1E6;
            border-radius: 3px;
            font-size: 14px;
        }
        .search-box input:focus {
            outline: none;
            border-color: #0052CC;
        }
        .prediction-card {
            padding: 12px;
            margin-bottom: 16px;
            border-radius: 3px;
            border-left: 4px solid;
        }
        .prediction-card.likely {
            background: #FFEBE6;
            border-color: #DE350B;
        }
        .prediction-card.unlikely {
            background: #E3FCEF;
            border-color: #00875A;
        }
        .prediction-card .title {
            font-weight: 600;
            margin-bottom: 4px;
        }
        .prediction-card .confidence {
            font-size: 12px;
            color: #5E6C84;
        }
        .incident-card {
            border: 1px solid #DFE1E6;
            border-radius: 3px;
            padding: 12px;
            margin-bottom: 12px;
            background: #FAFBFC;
            transition: all 0.2s;
        }
        .incident-card:hover {
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            transform: translateY(-1px);
        }
        .incident-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
        }
        .incident-status {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 3px;
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
        }
        .incident-status.resolved {
            background: #E3FCEF;
            color: #006644;
        }
        .incident-status.open {
            background: #FFEBE6;
            color: #BF2600;
        }
        .incident-score {
            font-size: 12px;
            color: #5E6C84;
            font-weight: 600;
        }
        .incident-text {
            margin-bottom: 8px;
            color: #172B4D;
            font-size: 13px;
        }
        .incident-resolution {
            margin-top: 8px;
            padding: 8px;
            background: #FFF;
            border-left: 3px solid #00875A;
            font-size: 12px;
        }
        .incident-resolution .label {
            font-weight: 600;
            color: #00875A;
            margin-bottom: 4px;
        }
        .incident-meta {
            display: flex;
            justify-content: space-between;
            font-size: 11px;
            color: #5E6C84;
            margin-top: 8px;
        }
        .loading {
            text-align: center;
            padding: 24px;
            color: #5E6C84;
        }
        .error {
            padding: 12px;
            background: #FFEBE6;
            border-left: 4px solid #DE350B;
            color: #DE350B;
            border-radius: 3px;
        }
        .empty {
            text-align: center;
            padding: 24px;
            color: #5E6C84;
        }
        button {
            background: #0052CC;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 3px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 600;
        }
        button:hover {
            background: #0065FF;
        }
        button:disabled {
            background: #DFE1E6;
            cursor: not-allowed;
        }
    </style>
</head>
<body>
    <div class="header">
        <h2>🤖 Thala AI</h2>
        <span class="badge">BETA</span>
    </div>
    
    <div class="search-box">
        <input type="text" id="searchInput" placeholder="Search similar incidents..." />
    </div>
    
    <div id="predictionResult"></div>
    <div id="resultsContainer">
        <div class="loading">Loading similar incidents...</div>
    </div>

    <script>
        const API_URL = '{{ api_url }}';
        const ISSUE_SUMMARY = '{{ issue_summary }}';
        const ISSUE_DESCRIPTION = '{{ issue_description }}';
        
        let debounceTimer;
        
        // Auto-search on page load with issue data
        window.addEventListener('DOMContentLoaded', () => {
            const query = ISSUE_SUMMARY + ' ' + ISSUE_DESCRIPTION;
            if (query.trim()) {
                searchIncidents(query);
            }
        });
        
        // Search on input with debounce
        document.getElementById('searchInput').addEventListener('input', (e) => {
            clearTimeout(debounceTimer);
            debounceTimer = setTimeout(() => {
                const query = e.target.value.trim();
                if (query) {
                    searchIncidents(query);
                }
            }, 500);
        });
        
        async function searchIncidents(query) {
            const resultsContainer = document.getElementById('resultsContainer');
            const predictionResult = document.getElementById('predictionResult');
            
            resultsContainer.innerHTML = '<div class="loading">🔍 Searching...</div>';
            predictionResult.innerHTML = '';
            
            try {
                const response = await fetch(`${API_URL}/search`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({query: query, top_k: 5})
                });
                
                if (!response.ok) throw new Error('Search failed');
                
                const data = await response.json();
                
                // Show prediction
                if (data.prediction) {
                    predictionResult.innerHTML = renderPrediction(data.prediction);
                }
                
                // Show results
                if (data.results && data.results.length > 0) {
                    resultsContainer.innerHTML = data.results.map((r, i) => renderIncident(r, i+1)).join('');
                } else {
                    resultsContainer.innerHTML = '<div class="empty">No similar incidents found. This might be a new issue! 🆕</div>';
                }
            } catch (error) {
                resultsContainer.innerHTML = `<div class="error">❌ Error: ${error.message}</div>`;
            }
        }
        
        function renderPrediction(pred) {
            const isLikely = pred.incident_likelihood === 'Likely';
            const className = isLikely ? 'likely' : 'unlikely';
            const emoji = isLikely ? '🚨' : '✅';
            
            return `
                <div class="prediction-card ${className}">
                    <div class="title">${emoji} ${pred.incident_likelihood} Incident</div>
                    <div class="confidence">Confidence: ${(pred.confidence * 100).toFixed(1)}%</div>
                </div>
            `;
        }
        
        function renderIncident(incident, index) {
            const statusClass = incident.status === 'Resolved' ? 'resolved' : 'open';
            const score = incident.score ? incident.score.toFixed(2) : 'N/A';
            const text = incident.text.substring(0, 150) + (incident.text.length > 150 ? '...' : '');
            
            let resolutionHtml = '';
            if (incident.status === 'Resolved' && incident.resolution_text) {
                const resText = incident.resolution_text.substring(0, 200) + (incident.resolution_text.length > 200 ? '...' : '');
                resolutionHtml = `
                    <div class="incident-resolution">
                        <div class="label">💡 Resolution:</div>
                        <div>${resText}</div>
                    </div>
                `;
            }
            
            return `
                <div class="incident-card">
                    <div class="incident-header">
                        <span class="incident-status ${statusClass}">${incident.status || 'Unknown'}</span>
                        <span class="incident-score">Similarity: ${score}</span>
                    </div>
                    <div class="incident-text">${text}</div>
                    ${resolutionHtml}
                    <div class="incident-meta">
                        <span>ID: ${incident.issue_id || 'N/A'}</span>
                        <span>${incident.timestamp ? new Date(incident.timestamp).toLocaleString() : 'Unknown time'}</span>
                    </div>
                </div>
            `;
        }
    </script>
</body>
</html>
"""

# ============================================================================
# API ENDPOINTS FOR JIRA
# ============================================================================

@app.route('/jira/panel', methods=['GET'])
def jira_panel():
    """
    Render the Jira panel HTML
    Can be embedded as iframe in Jira Connect App
    """
    # Get issue data from query params (passed by Jira)
    issue_key = request.args.get('issue_key', '')
    issue_summary = request.args.get('issue_summary', '')
    issue_description = request.args.get('issue_description', '')
    
    return render_template_string(
        JIRA_PANEL_HTML,
        api_url=FLASK_API_URL,
        issue_summary=issue_summary,
        issue_description=issue_description
    )


@app.route('/jira/webhook', methods=['POST'])
def jira_webhook():
    """
    Webhook endpoint for Jira automation
    Triggered when issues are created/updated
    Returns AI predictions and similar incidents
    """
    try:
        data = request.get_json()
        
        # Extract issue data from Jira webhook payload
        issue = data.get('issue', {})
        fields = issue.get('fields', {})
        
        issue_key = issue.get('key', '')
        summary = fields.get('summary', '')
        description = fields.get('description', '')
        
        query = f"{summary} {description}"
        
        # Get prediction
        pred_response = requests.post(
            f"{FLASK_API_URL}/predict_incident",
            json={"query": query},
            timeout=10
        )
        
        # Search similar incidents
        search_response = requests.post(
            f"{FLASK_API_URL}/search",
            json={"query": query, "top_k": 3},
            timeout=10
        )
        
        result = {
            "issue_key": issue_key,
            "prediction": pred_response.json() if pred_response.status_code == 200 else None,
            "similar_incidents": search_response.json().get('results', []) if search_response.status_code == 200 else []
        }
        
        logger.info(f"Processed Jira webhook for {issue_key}: {result['prediction']}")
        
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error processing Jira webhook: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/jira/comment', methods=['POST'])
def jira_add_comment():
    """
    Add AI analysis as comment to Jira issue
    Can be triggered by automation rule
    """
    try:
        data = request.get_json()
        issue_key = data.get('issue_key')
        query = data.get('query', '')
        
        if not issue_key or not query:
            return jsonify({"error": "Missing issue_key or query"}), 400
        
        # Get AI analysis
        pred_response = requests.post(
            f"{FLASK_API_URL}/predict_incident",
            json={"query": query},
            timeout=10
        )
        
        search_response = requests.post(
            f"{FLASK_API_URL}/search",
            json={"query": query, "top_k": 3},
            timeout=10
        )
        
        if pred_response.status_code != 200:
            return jsonify({"error": "Prediction failed"}), 500
        
        prediction = pred_response.json()
        results = search_response.json().get('results', []) if search_response.status_code == 200 else []
        
        # Format comment for Jira
        likelihood = prediction.get('incident_likelihood', 'Unknown')
        confidence = prediction.get('confidence', 0)
        
        comment_text = f"🤖 *Thala AI Analysis*\\n\\n"
        comment_text += f"*Incident Likelihood:* {likelihood} ({confidence*100:.1f}% confidence)\\n\\n"
        
        if results:
            comment_text += f"*Similar Past Incidents ({len(results)}):*\\n"
            for i, r in enumerate(results[:3], 1):
                status = r.get('status', 'Unknown')
                score = r.get('score', 0)
                text = r.get('text', '')[:100]
                comment_text += f"{i}. [{status}] (Similarity: {score:.2f}) - {text}...\\n"
                
                if r.get('resolution_text') and status == 'Resolved':
                    res_text = r.get('resolution_text', '')[:100]
                    comment_text += f"   💡 _Resolution: {res_text}..._\\n"
        else:
            comment_text += "_No similar incidents found. This might be a new type of issue._\\n"
        
        return jsonify({
            "issue_key": issue_key,
            "comment": comment_text,
            "prediction": prediction,
            "similar_count": len(results)
        })
    
    except Exception as e:
        logger.error(f"Error creating Jira comment: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "thala-jira-integration"})


# ============================================================================
# MAIN
# ============================================================================

def start_jira_integration(port=5001):
    """Start the Jira integration server"""
    logger.info(f"🚀 Starting Thala AI Jira Integration on port {port}...")
    logger.info(f"📡 Flask API: {FLASK_API_URL}")
    logger.info(f"📋 Jira URL: {JIRA_URL}")
    logger.info(f"🌐 Panel URL: http://localhost:{port}/jira/panel")
    app.run(host='0.0.0.0', port=port, debug=False)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    start_jira_integration()












