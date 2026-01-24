# Thala - ITSM Incident Prediction System

AI-powered incident management system that uses LLM and machine learning to classify, predict, and track IT incidents from Slack and Jira.

## 🎯 Features

### Core Capabilities
- **Semantic Incident Classification** - Gemini 2.0 Flash LLM classifies Slack messages (incident_report, resolution, discussion, unrelated)
- **Incident Prediction** - XGBoost ML model predicts incident likelihood with confidence scores
- **Semantic Search** - Find similar past incidents using vector embeddings (384-dim)
- **Auto-Labeling** - Automatic incident classification from Jira resolutions
- **Cross-Source Context** - Tracks issues across Slack ↔ Jira with semantic linking

### Advanced Features
- **Conversation Context Tracking** - Knows who discussed what issue when
- **Discussion Context Capture** - Captures technical fix details from discussions
- **Smart Resolution Linking** - Links vague resolutions ("fixed it") to correct issues
- **Smart Search Ranking** - Prioritizes resolved incidents with complete resolution info
- **Auto-Training** - Hourly model retraining with new labeled data

## 🚀 Quick Start

### Prerequisites
- Python 3.12+
- Elasticsearch 9.1.5+ running
- Kafka (optional - for real-time connectors)
- Gemini API key ([Get one here](https://aistudio.google.com/apikey))

### 1. Install Dependencies

```bash
# Activate virtual environment
call thala\Scripts\activate.bat  # Windows
# OR
source thala/bin/activate         # Linux/Mac

# Install packages
pip install -r requirements.txt
```

### 2. Configure Environment

Create `.env` file in project root:

```ini
# Gemini LLM (Required)
USE_GEMINI_LABELING=true
GEMINI_API_KEY=your_gemini_api_key_here

# Slack
SLACK_BOT_TOKEN=xoxb-your-slack-bot-token
SLACK_CHANNEL_ID=C01234567890

# Jira
JIRA_URL=https://your-instance.atlassian.net
JIRA_EMAIL=your-email@example.com
JIRA_API_TOKEN=your_jira_api_token

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_SLACK=thala-slack-events
KAFKA_TOPIC_JIRA=thala-jira-events

# Flask
FLASK_API_URL=http://localhost:5000
```

### 3. Start the System

```bash
# Start integrated system (includes Flask + Connectors)
python integrated_main.py

# OR start Flask API only
python new.py
```

System will be available at: **http://localhost:5000**

## 📡 API Endpoints

### Health Check
```bash
GET /health
```

### Predict Incident
```bash
POST /predict_incident
Content-Type: application/json

{
  "query": "API server is down"
}

# Response
{
  "query": "API server is down",
  "incident_likelihood": "Likely",
  "confidence": 0.95
}
```

### Search Similar Incidents
```bash
POST /search
Content-Type: application/json

{
  "query": "database connection timeout",
  "top_k": 10
}

# Response
{
  "prediction": {
    "incident_likelihood": "Likely",
    "confidence": 0.89
  },
  "results": [
    {
      "text": "Slack: Database connection timeout...",
      "status": "Resolved",
      "resolution_text": "Increased connection pool to 200 | timeout fixed",
      "resolved_by": "U08L203J5TK",
      "resolved_at": "2025-10-19T...",
      "incident_likelihood": "Likely",
      "score": 1.65
    }
  ]
}
```

### Index New Incident
```bash
POST /index
Content-Type: application/json

{
  "texts": ["Production API returning 500 errors"],
  "timestamp": "2025-10-19T10:00:00",
  "status": "Open",
  "source": "slack",
  "issue_id": "slack_1234567890",
  "incident_likelihood": "Likely"
}
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Conversation Context Queue              │
│  Tracks issues, user interactions, discussions          │
│  (Slack + Jira synced)                                  │
└────────────┬─────────────────────────┬──────────────────┘
             │                         │
    ┌────────▼─────────┐      ┌───────▼──────────┐
    │  Slack Messages  │      │   Jira Tickets   │
    │  (Gemini LLM)    │      │  (Gemini LLM)    │
    └────────┬─────────┘      └───────┬──────────┘
             │                        │
             └──────────┬─────────────┘
                        ▼
                  Kafka Topics
                        │
                        ▼
              ┌─────────────────┐
              │  Flask API      │
              │  + XGBoost ML   │
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │ Elasticsearch   │
              │ (Vector Search) │
              └─────────────────┘
```

**Data Flow:**
1. Slack/Jira → Gemini LLM → Classification
2. Messages → Kafka → Consumer → Flask
3. Flask → Elasticsearch (with embeddings)
4. XGBoost model trained on labeled data
5. Search returns prioritized results

## 🧠 How It Works

### Semantic Classification (LLM-Based)
- **No keyword matching** - Pure semantic understanding via Gemini
- Classifies: incident_report, resolution, discussion, unrelated
- Thread-aware and context-sensitive

### Conversation Context
- Tracks who discussed which issues
- Tracks when issues were last mentioned
- Boosts resolution matching for same user + recent issues

### Resolution Linking
```
User: "Payment gateway timeout"          → Incident created
User: "Increased timeout to 60 seconds"  → Discussion tracked
User: "payment working now"              → Resolution linked
```

**Resolution text captured:**
```
"Increased timeout to 60 seconds | payment working now"
```

### Smart Search Prioritization
1. **Resolved + Complete Info** (resolution_text + resolved_by + resolved_at)
2. **Everything else** (Open, incomplete resolved)
3. Within each group: sorted by semantic score

## 🔧 Configuration

### Similarity Threshold
Adjust in `team-thala/src/slack_connector_llm.py`:
```python
threshold=0.3  # Lower = more aggressive linking
```

### Context Window
Adjust in `slack_connector_llm.py`:
```python
self.max_age_hours = 72  # Track issues for 72 hours
```

### Discussion History
Adjust in `slack_connector_llm.py`:
```python
cutoff_time = resolution_time - timedelta(minutes=30)  # Look back 30 min
```

## 🧪 Testing

### Test Incident Flow
**In Slack:**
1. Post: `"Database connection failing"`
2. Post: `"Restarted database pool"`  (discussion)
3. Post: `"database working now"`     (resolution)

**Search:**
```bash
curl -s -X POST "http://localhost:5000/search" \
  -H "Content-Type: application/json" \
  -d '{"query": "database connection", "top_k": 5}'
```

**Expected:** Should show resolved incident with complete resolution text including the restart action.

### Test Cross-Source Sync
1. Create Jira ticket: "API 500 errors"
2. In Slack: "API fixed"
3. System links Slack resolution to Jira ticket

## 📊 Data Schema

### Elasticsearch Document
```json
{
  "text": "Slack: Payment timeout...",
  "embedding": [0.123, 0.456, ...],  // 384-dim
  "timestamp": "2025-10-19T10:00:00",
  "status": "Resolved",
  "incident_likelihood": "Likely",
  "source": "slack",
  "issue_id": "slack_1234567890",
  "resolution_text": "Increased timeout to 60s | working now",
  "resolved_by": "U08L203J5TK",
  "resolved_at": "2025-10-19T10:15:00"
}
```

## 🛠️ Development

### Project Structure
```
thala/
├── integrated_main.py          # Main entry point
├── new.py                      # Flask API + ML model
├── requirements.txt            # Python dependencies
├── initial_data.csv            # Training data
├── team-thala/src/
│   ├── slack_connector_llm.py  # LLM-powered Slack connector
│   ├── jira_connector.py       # Jira connector with LLM
│   ├── kafka_producer.py       # Kafka message producer
│   └── kafka_consumer_to_flask.py  # Kafka → Flask bridge
└── thala/                      # Virtual environment
```

### Adding New Features
1. **New connector** - Create in `team-thala/src/`, follow Slack/Jira pattern
2. **New classification** - Update Gemini prompt in `slack_connector_llm.py`
3. **New ML features** - Modify `new.py` training pipeline

## 🐛 Troubleshooting

### "Gemini required for LLM Slack connector"
- Set `GEMINI_API_KEY` in `.env`
- Install: `pip install google-genai`

### Resolution not linking
- Check logs for similarity scores (should be > 0.3)
- Verify user IDs match in Slack messages
- Check context queue has the issue

### Duplicates in search results
- System deduplicates by `issue_id`
- Keeps version with complete resolution info
- If still seeing duplicates, check Elasticsearch

### Search not prioritizing resolved incidents
- Verify resolved incidents have resolution_text, resolved_by, resolved_at (all non-null)
- Incomplete resolved incidents are excluded from results

## 📝 Requirements

```
flask
transformers
elasticsearch
numpy
xgboost
sentence-transformers
pandas
schedule
requests
python-dotenv
jira
slack-sdk
kafka-python-ng
google-genai
```

## 🔒 Security Notes

- **Production:** Add API authentication
- **Elasticsearch:** Use TLS certificates (already configured)
- **Kafka:** Enable SASL/SSL for production
- **API Keys:** Never commit `.env` to version control

## 📄 License

Internal use only.

## 🤝 Support

For issues or questions:
1. Check logs: `thala_integrated.log`, `thala_prediction.log`
2. Review API responses for error details
3. Verify all services (Elasticsearch, Kafka) are running

---

**Version:** 2.0  
**Last Updated:** October 2025  
**Status:** Production Ready

