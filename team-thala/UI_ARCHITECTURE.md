# 🏗️ Thala AI - UI Integration Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Thala AI System                             │
└─────────────────────────────────────────────────────────────────────┘

┌────────────────────┐        ┌────────────────────┐
│   Slack Workspace  │        │   Jira Cloud/      │
│                    │        │   Server           │
│  Users:            │        │                    │
│  • Dev Team        │        │  Project Managers  │
│  • SRE Team        │        │  Support Team      │
│  • DevOps          │        │                    │
└────────┬───────────┘        └─────────┬──────────┘
         │                              │
         │ /thala commands              │ REST API / Webhooks
         │ @mentions                    │ Automation Rules
         │                              │
         ▼                              ▼
┌─────────────────────┐        ┌──────────────────────┐
│  Slack Bot UI       │        │  Jira Integration    │
│  (Port: Socket)     │        │  (Port: 5001)        │
│                     │        │                      │
│  • Slash Commands   │        │  • Panel Iframe      │
│  • Interactive UI   │        │  • Webhook Endpoint  │
│  • Block Kit        │        │  • Comment API       │
└─────────┬───────────┘        └──────────┬───────────┘
          │                               │
          │ HTTP POST                     │ HTTP POST
          │                               │
          └──────────────┬────────────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │   Flask API          │
              │   (Port: 5000)       │
              │                      │
              │  Endpoints:          │
              │  • /search           │
              │  • /predict_incident │
              │  • /index            │
              │  • /update_status    │
              └──────────┬───────────┘
                         │
         ┌───────────────┼───────────────┐
         │               │               │
         ▼               ▼               ▼
┌─────────────┐  ┌──────────────┐  ┌──────────────┐
│Elasticsearch│  │  XGBoost     │  │  Sentence    │
│             │  │  Model       │  │  Transformer │
│ Indexed     │  │              │  │              │
│ Incidents   │  │ Predictions  │  │ Embeddings   │
└─────────────┘  └──────────────┘  └──────────────┘
```

---

## Component Flow

### 1. Slack Bot Flow

```
User in Slack: "/thala search payment issues"
         │
         ▼
┌─────────────────────────────────────────────┐
│  Slack Bot UI (slack_bot_ui.py)             │
│  1. Parse command                           │
│  2. Extract query: "payment issues"         │
└─────────────────┬───────────────────────────┘
                  │
                  │ POST /search
                  │ {"query": "payment issues", "top_k": 5}
                  ▼
┌─────────────────────────────────────────────┐
│  Flask API (new.py)                         │
│  1. Generate query embedding                │
│  2. Search Elasticsearch (cosine sim)       │
│  3. Get XGBoost prediction                  │
│  4. Return results + prediction             │
└─────────────────┬───────────────────────────┘
                  │
                  │ JSON Response:
                  │ {
                  │   "prediction": {
                  │     "incident_likelihood": "Likely",
                  │     "confidence": 0.95
                  │   },
                  │   "results": [...]
                  │ }
                  ▼
┌─────────────────────────────────────────────┐
│  Slack Bot UI                               │
│  1. Format results as Block Kit UI          │
│  2. Create rich cards with buttons          │
│  3. Show resolution history                 │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
         User sees rich UI in Slack:
         ┌──────────────────────────┐
         │ 🔍 Search Results        │
         ├──────────────────────────┤
         │ ✅ #1 - Similarity: 0.92 │
         │ Payment API timeout...   │
         │ 💡 Resolution:           │
         │ Increased timeout to 60s │
         │ [View Details]           │
         └──────────────────────────┘
```

### 2. Jira Panel Flow

```
User opens Jira issue: "Payment API Error"
         │
         ▼
┌─────────────────────────────────────────────┐
│  Jira UI                                    │
│  - Loads panel iframe OR                    │
│  - Triggers automation webhook              │
└─────────────────┬───────────────────────────┘
                  │
                  │ GET /jira/panel?issue_summary=...
                  │ OR
                  │ POST /jira/webhook
                  ▼
┌─────────────────────────────────────────────┐
│  Jira Integration (jira_panel_integration)  │
│  1. Extract issue summary + description     │
│  2. Build query                             │
└─────────────────┬───────────────────────────┘
                  │
                  │ POST /search + /predict_incident
                  ▼
┌─────────────────────────────────────────────┐
│  Flask API                                  │
│  - Semantic search                          │
│  - AI prediction                            │
└─────────────────┬───────────────────────────┘
                  │
                  │ JSON Response
                  ▼
┌─────────────────────────────────────────────┐
│  Jira Integration                           │
│  1. Render HTML panel with results          │
│  2. OR format comment for Jira              │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
         User sees in Jira:
         ┌──────────────────────────┐
         │ 🤖 Thala AI             │
         ├──────────────────────────┤
         │ 🚨 Likely Incident (95%) │
         │                          │
         │ Similar Past Incidents:  │
         │ 1. [Resolved] Payment... │
         │    💡 Restarted service  │
         │ 2. [Resolved] API 502... │
         └──────────────────────────┘
```

### 3. Cross-Platform Context Sync

```
Slack Message: "Payment API is down"
         │
         ▼
┌──────────────────────────────────────┐
│ Slack Connector (slack_connector_llm)│
│ 1. Gemini LLM classifies             │
│ 2. Adds to Context Queue             │
│ 3. Sends to Kafka                    │
└─────────────┬────────────────────────┘
              │
              ▼
       ┌──────────┐
       │  Kafka   │
       └──────┬───┘
              │
              ▼
┌──────────────────────────────────────┐
│ Kafka Consumer                       │
│ (kafka_consumer_to_flask.py)        │
└─────────────┬────────────────────────┘
              │
              │ POST /index
              ▼
       ┌──────────────┐
       │ Elasticsearch│ ← Now searchable via Slack Bot & Jira Panel!
       └──────────────┘


Jira Ticket Created: "KAN-100: Payment API Error"
         │
         ▼
┌──────────────────────────────────────┐
│ Jira Connector (jira_connector.py)   │
│ 1. Gemini LLM classifies             │
│ 2. Syncs to Slack Context Queue      │
│ 3. Sends to Kafka                    │
└─────────────┬────────────────────────┘
              │
              ▼
       Same flow → Elasticsearch


Resolution in Slack: "Fixed by restarting service"
         │
         ▼
┌──────────────────────────────────────┐
│ Slack Connector                      │
│ 1. Links to original issue           │
│ 2. Updates status                    │
│ 3. Sends update to Kafka             │
└─────────────┬────────────────────────┘
              │
              │ POST /update_status
              ▼
       ┌──────────────┐
       │ Elasticsearch│ ← Status updated to "Resolved"
       └──────────────┘
       
       Now both Slack Bot and Jira Panel show:
       - Status: Resolved
       - Resolution: "Fixed by restarting service"
       - Resolved by: @user
       - Resolved at: timestamp
```

---

## Data Flow

### Incident Creation → Search → Resolution

```
1. CREATION
   ┌──────┐      ┌──────┐
   │Slack │  OR  │ Jira │
   └───┬──┘      └───┬──┘
       │             │
       └──────┬──────┘
              ▼
        ┌──────────┐
        │  Kafka   │
        └─────┬────┘
              ▼
        ┌──────────────┐
        │ Flask API    │
        │ /index       │
        └─────┬────────┘
              ▼
        ┌──────────────────────┐
        │ Elasticsearch        │
        │ {                    │
        │   text: "...",       │
        │   status: "Open",    │
        │   embedding: [...],  │
        │   issue_id: "..."    │
        │ }                    │
        └──────────────────────┘

2. SEARCH (via UI)
   ┌──────────┐      ┌──────────┐
   │ Slack    │  OR  │ Jira     │
   │ /thala   │      │ Panel    │
   │ search   │      │          │
   └────┬─────┘      └────┬─────┘
        │                 │
        └────────┬────────┘
                 ▼
        ┌─────────────────┐
        │ Flask API       │
        │ /search         │
        │ - Semantic sim  │
        │ - ML prediction │
        └────────┬────────┘
                 ▼
        ┌──────────────────────┐
        │ Elasticsearch        │
        │ Cosine similarity    │
        │ score query          │
        └────────┬─────────────┘
                 │
                 ▼
            Results sorted by:
            1. Resolved + complete info
            2. Semantic similarity
            3. Recency

3. RESOLUTION
   ┌──────┐      ┌──────┐
   │Slack │  OR  │ Jira │
   │msg   │      │status│
   └───┬──┘      └───┬──┘
       │             │
       └──────┬──────┘
              ▼
        ┌──────────────────┐
        │ Connector        │
        │ - Links to issue │
        │ - Captures fix   │
        └─────┬────────────┘
              ▼
        ┌──────────┐
        │  Kafka   │
        │ "update" │
        └─────┬────┘
              ▼
        ┌──────────────────┐
        │ Flask API        │
        │ /update_status   │
        └─────┬────────────┘
              ▼
        ┌────────────────────────┐
        │ Elasticsearch          │
        │ UPDATE:                │
        │   status: "Resolved",  │
        │   resolution_text: "…",│
        │   resolved_by: "…",    │
        │   resolved_at: "…"     │
        └────────────────────────┘
        
        ↓
        
   Next search shows resolution! 🎉
```

---

## Technology Stack

### Slack Bot
- **Framework**: `slack-bolt` (Python)
- **Mode**: Socket Mode (no public URL needed)
- **UI**: Slack Block Kit (rich interactive messages)
- **Auth**: Bot Token + App-Level Token

### Jira Integration
- **Framework**: Flask (REST API)
- **Integration Types**:
  - Iframe panels (Forge/Connect)
  - Webhooks (Automation Rules)
  - REST API (Comments)
- **Auth**: Basic Auth via Jira API Token

### Backend (Existing)
- **API**: Flask REST API
- **Search**: Elasticsearch (semantic vectors)
- **ML**: XGBoost (incident prediction)
- **Embeddings**: SentenceTransformer
- **LLM**: Gemini 2.0 Flash (classification)
- **Messaging**: Kafka (async communication)

---

## Security Considerations

### Slack Bot
1. ✅ **Socket Mode**: No public webhooks needed
2. ✅ **Token Validation**: Slack SDK validates all requests
3. ✅ **Scopes**: Minimal bot scopes (read + write)
4. ⚠️ **Recommendation**: Store tokens in environment variables (not in code)

### Jira Integration
1. ✅ **CORS**: Enabled for Jira domain
2. ⚠️ **Authentication**: Add API key validation for webhooks
3. ⚠️ **Rate Limiting**: Consider adding for public endpoints
4. ⚠️ **HTTPS**: Use ngrok or proper SSL for webhooks

**Recommended additions** (for production):

```python
# Add to jira_panel_integration.py

from functools import wraps
import hmac
import hashlib

def verify_jira_webhook(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Verify Jira webhook signature
        signature = request.headers.get('X-Hub-Signature')
        secret = os.getenv('JIRA_WEBHOOK_SECRET')
        
        if signature and secret:
            body = request.get_data()
            expected = hmac.new(
                secret.encode(),
                body,
                hashlib.sha256
            ).hexdigest()
            
            if not hmac.compare_digest(signature, f"sha256={expected}"):
                return jsonify({"error": "Invalid signature"}), 403
        
        return f(*args, **kwargs)
    return decorated_function

@app.route('/jira/webhook', methods=['POST'])
@verify_jira_webhook
def jira_webhook():
    # ... existing code
```

---

## Scalability

### Current Setup (Single Server)
- Supports: 10-50 users
- Requests/sec: ~10
- Good for: Development, small teams

### Scale to 100+ Users
1. **Load Balancer**: Nginx → Multiple Flask instances
2. **Redis Cache**: Cache search results (5 min TTL)
3. **Async Workers**: Celery for long-running predictions
4. **Database**: PostgreSQL for metadata (offload from ES)

### Scale to 1000+ Users
1. **Kubernetes**: Deploy as microservices
2. **Elasticsearch Cluster**: 3+ nodes
3. **Kafka Cluster**: Multi-broker setup
4. **CDN**: Static assets for Jira panels
5. **Monitoring**: Prometheus + Grafana

---

## Monitoring & Debugging

### Logs to Watch

```bash
# Slack Bot
tail -f team-thala/src/logs/slack_bot.log

# Jira Integration
tail -f team-thala/src/logs/jira_integration.log

# Flask API
tail -f thala_integrated.log

# All together
tail -f team-thala/src/logs/*.log thala_integrated.log
```

### Health Checks

```bash
# Flask API
curl http://localhost:5000/health

# Jira Integration
curl http://localhost:5001/health

# Elasticsearch
curl http://localhost:9200/_cluster/health

# Kafka
kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Metrics to Track

1. **Search Performance**
   - Average response time
   - Search accuracy (click-through rate)
   - Cache hit rate

2. **Prediction Accuracy**
   - True positives / False positives
   - User feedback on predictions

3. **User Engagement**
   - Slack commands used per day
   - Jira panel views
   - Resolution link success rate

---

## Future Enhancements

### Phase 1 (Current) ✅
- [x] Slack slash commands
- [x] Basic search and predict
- [x] Jira iframe panel
- [x] Automation webhooks

### Phase 2 (Next Sprint)
- [ ] Slack interactive buttons (Create Jira, Mark Resolved)
- [ ] Jira modal dialogs
- [ ] Slack shortcuts (right-click actions)
- [ ] Real-time notifications

### Phase 3 (Future)
- [ ] Slack App Home tab (dashboard)
- [ ] Jira Forge app (native integration)
- [ ] MS Teams integration
- [ ] Mobile app (React Native)

### Phase 4 (Advanced)
- [ ] Slack Workflow Builder steps
- [ ] Jira automation templates
- [ ] Slack Connect (external workspaces)
- [ ] Public API for 3rd party integrations

---

**For detailed setup instructions**, see [UI_SETUP_GUIDE.md](UI_SETUP_GUIDE.md)







