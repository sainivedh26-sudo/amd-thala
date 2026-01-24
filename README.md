# team-thala
<img width="1024" height="1024" alt="image-Photoroom" src="https://github.com/user-attachments/assets/2b5bef99-dd5e-4744-997b-e9962d8e8c03" />
<img width="1024" height="1024" alt="image-Photoroom" src="Logo.png" />

![made-with-python](https://img.shields.io/badge/Made%20with-Python3-brightgreen)

<!-- LOGO -->
<!-- LOGO -->
<br />
<h1 align="center">
  <font size="7">THALA</font>
</h1>
  <p align="center">
    Intelligent incident management system for automated detection, classification, and resolution tracking.
    <br />
    </p>
</p>
<p align="center">
  <a href="#about-the-project">About The Project</a> •
  <a href="#architecture">Architecture</a> •
  <a href="#key-features">Key Features</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#usage">Usage</a> •
  <a href="#api-endpoints">API Endpoints</a>
</p>


## About The Project

Thala is an intelligent incident management system that automatically:
- Detects incidents from Slack messages, Jira tickets, and emails
- Classifies & predicts severity, category, and likelihood using agent
- Tracks resolutions and links them to original incidents
- Searches similar past incidents using semantic similarity
- Extracts text from image attachments using AWS Textract

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Slack     │     │    Jira     │     │    Email    │
│  Messages   │     │   Tickets   │     │  Messages   │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┴───────────────────┘
                           │
                    ┌───────▼────────┐
                    │     Kafka      │
                    │   (Message     │
                    │    Broker)     │
                    └───────┬────────┘
                            │
              ┌─────────────┴─────────────┐
              │                           │
       ┌──────▼────────┐         ┌───────▼──────────┐
       │ Slack/Jira    │         │   Flask API       │
       │ Connectors    │         │   (Backend)       │
       │ (Groq LLM)    │         │                   │
       └──────┬────────┘         └───────┬──────────┘
              │                           │
              │                           │
    ┌─────────▼──────────┐      ┌────────▼──────────┐
    │ AWS Textract       │      │  Elasticsearch     │
    │ (Image Extraction) │      │  (Vector Search)   │
    │ + S3 Storage       │      │  (Incident DB)     │
    └────────────────────┘      └───────────────────┘
                                         │
                                ┌────────▼──────────┐
                                │   Slack Bot UI     │
                                │   (/thala commands)│
                                └────────────────────┘
```

## Data Flow

1. **Ingestion**: Slack/Jira/Email → Connectors → Kafka
2. **Classification**: Groq LLM classifies messages (incident, resolution, discussion, unrelated)
3. **Prediction**: Groq agent predicts category & severity
4. **Attachment Processing**: Images → S3 → Textract → Extracted text → Context
5. **Storage**: Flask API → Elasticsearch (with embeddings for semantic search)
6. **Resolution Tracking**: Links resolution messages to original incidents
7. **UI**: Slack bot commands (/thala latest_issue, /thala search)

## Key Features

### 1. Intelligent Classification
- Uses Groq LLM (llama-3.3-70b) to classify messages semantically
- No keyword matching - pure agent understanding
- Types: incident_report, resolution, discussion, unrelated

### 2. Smart Resolution Linking
- Links vague resolutions ("auth issue fixed") to correct incidents
- Uses semantic similarity (embeddings) + conversational context
- Automatically marks incidents as "Resolved" in Elasticsearch

### 3. Attachment Processing (AWS Integration)
- Downloads images from Slack/Jira attachments
- Uploads to S3 bucket (thala-images)
- Extracts text using AWS Textract
- Adds extracted text to message context for classification

### 4. Incident Prediction
- **Category**: Database, API, Frontend, Infrastructure, Authentication, etc.
- **Severity**: Critical, High, Medium, Low
- **Likelihood**: Likely, Unlikely (for new queries)
- Uses Groq LLM with few-shot learning

### 5. Semantic Search
- Finds similar past incidents using vector embeddings
- Prioritizes resolved incidents with complete resolution info
- Returns similarity scores and resolution details

### 6. Slack Bot Commands
- `/thala latest_issue [page]` - View ongoing incidents (paginated)
- `/thala search <query>` - Search similar resolved incidents
- `/thala predict <description>` - Predict category/severity
- `/thala` - Show help

## Quick Start

### Prerequisites
- Python 3.12+
- Elasticsearch 9.1.5+ (running)
- Kafka (KRaft mode, optional for real-time)
- AWS Account (for S3 + Textract)

### Installation

Install dependencies:
```bash
pip install -r requirements.txt
pip install -r team-thala/src/ui_requirements.txt
```

### Environment Setup

Create `.env` file in the root directory:
```ini
# Groq (for classification & prediction)
GROQ_API_KEY=your-groq-api-key

# Slack (Bot Token, NOT App Token)
SLACK_BOT_TOKEN=xoxb-your-bot-token
SLACK_CHANNEL_ID=C01234567890

# Jira
JIRA_URL=https://your-instance.atlassian.net
JIRA_EMAIL=your-email@example.com
JIRA_API_TOKEN=your-jira-token

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_SLACK=thala-slack-events
KAFKA_TOPIC_JIRA=thala-jira-events

# Flask API
FLASK_API_URL=http://localhost:5000

# AWS (for attachment processing)
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-east-2
AWS_S3_BUCKET=thala-images

# Elasticsearch
ELASTICSEARCH_URL=http://localhost:9200
```

### Slack App Setup

1. Create Slack app at https://api.slack.com/apps
2. Add Bot Token Scopes:
   - `channels:history`, `channels:read`
   - `chat:write`, `commands`
   - `app_mentions:read`, `im:history`
   - `files:read` (REQUIRED for attachments)
3. Install app to workspace
4. Copy Bot Token (xoxb-...) to `.env`

**See**: `team-thala/SLACK_FILES_READ_SETUP.md` for detailed setup instructions.

## Usage

### Start All Services (Integrated)
```bash
python integrated_main.py
```

### Start Services Separately
```bash
# Terminal 1: Flask API
python new.py

# Terminal 2: Kafka Consumer
python team-thala/src/kafka_consumer_to_flask.py

# Terminal 3: Slack Connector
python team-thala/src/slack_connector_enhanced.py

# Terminal 4: Slack Bot UI
python team-thala/src/slack_bot_ui.py
```

### Slack Commands

```sh
/thala                          # Show help and available commands
/thala latest_issue [page]      # View ongoing incidents (paginated, 10 per page)
/thala search <query>           # Search similar resolved incidents
/thala predict <description>    # Predict incident category and severity
```

## How It Works

### Incident Creation Flow
```
Slack: "API server is down"
  → Groq LLM classifies as "incident_report"
  → Groq predicts: Category=API, Severity=High
  → Sent to Kafka → Flask → Elasticsearch
  → Tracked in Incident Tracker
  → Available in Slack: /thala latest_issue
```

### Resolution Flow
```
Slack: "API issue has been fixed"
  → Groq LLM classifies as "resolution"
  → Semantic search finds matching open incident
  → Updates status to "Resolved" in Elasticsearch
  → Logs resolution text, resolved_by, resolved_at
  → Removed from ongoing incidents list
```

### Attachment Flow
```
Slack: [Image attachment] "Check this error"
  → Download image from Slack (files_info API)
  → Upload to S3 bucket
  → Extract text using Textract
  → Add extracted text to message context
  → Classify with full context (image + text)
  → Create incident if classified as incident_report
```

### Search Flow
```
Slack: /thala search "database timeout"
  → Flask API performs semantic search in Elasticsearch
  → Returns similar resolved incidents
  → Prioritizes incidents with complete resolution info
  → Displays in Slack with rich formatting
```

## Key Components

### slack_connector_enhanced.py
- Monitors Slack channels for messages
- Classifies messages using Groq LLM
- Processes attachments (S3 + Textract)
- Detects resolutions and links to incidents
- Prevents resolution messages from creating new incidents
- Handles vague messages intelligently

### slack_bot_ui.py
- Slack bot with slash commands
- Paginated incident listing
- Semantic search interface
- Rich UI with Slack Block Kit

### gemini_predictor.py (uses Groq)
- Predicts category & severity
- Uses few-shot learning with training examples
- Caches predictions (24h TTL)

### aws_attachment_processor.py
- Downloads attachments from Slack/Jira
- Uploads to S3 bucket
- Extracts text using Textract
- Handles image format conversion (PNG → JPEG)

### new.py (Flask API)
- `/index` - Store incidents in Elasticsearch
- `/search` - Semantic similarity search
- `/predict_incident` - Predict likelihood
- `/update_status` - Mark incidents as resolved
- `/lookup_incident` - Find incident by ID

## API Endpoints

### POST /index
Store new incident in Elasticsearch
```json
{
  "texts": ["API server is down"],
  "timestamp": "2025-11-01T10:00:00",
  "status": "Open",
  "source": "slack",
  "category": "API",
  "severity": "High"
}
```

### POST /search
Semantic similarity search
```json
{
  "query": "database connection timeout",
  "top_k": 10
}
```

### POST /update_status
Mark incident as resolved
```json
{
  "issue_id": "slack_1234567890",
  "status": "Resolved",
  "resolution_text": "Fixed connection pool",
  "resolved_by": "U08L203J5TK",
  "resolved_at": "2025-11-01T10:15:00"
}
```

### POST /lookup_incident
Find incident by ID
```json
{
  "issue_id": "slack_1234567890"
}
```

## Important Notes

### Token Types
- **Bot Token** (xoxb-...): Required for Web API calls (files_info, channels, etc.)
- **App Token** (xapp-...): Only for Socket Mode (not used currently)
- **Use Bot Token** in SLACK_BOT_TOKEN environment variable

### LLM Providers
- **Classification**: Groq (llama-3.3-70b-versatile)
- **Prediction**: Groq (was Gemini, migrated)
- **Search**: Sentence Transformers (local embeddings)

### Attachment Requirements
- Slack app must have `files:read` scope
- AWS credentials must be configured
- S3 bucket must exist (thala-images)
- Textract must be enabled in AWS region

### Resolution Detection
- No keyword matching - pure semantic understanding
- Links resolutions even if ID not mentioned explicitly
- Uses conversational context (recent incidents)
- Fallback to most recent open incident if no match

## Common Issues

### "Missing scope: files:read"
- Add `files:read` scope in Slack app settings
- Reinstall app to workspace
- See: `team-thala/SLACK_FILES_READ_SETUP.md`

### "Cannot identify image file"
- Image format issue (converted to JPEG automatically)
- Slack bot may not have proper file download permissions
- Check S3 upload succeeded before Textract

### "Resolution not linking"
- Check logs for similarity scores (should be > 0.3)
- Verify incident exists in Elasticsearch
- Ensure incident is still "Open" status

### "Vague messages creating incidents"
- System now requires technical details OR successful image extraction
- Vague messages without image context are classified as "discussion"
- Check attachment extraction logs

## Credits

- Built with Groq LLM for intelligent classification
- AWS Textract for image text extraction
- Elasticsearch for semantic search
- Kafka for real-time event streaming
