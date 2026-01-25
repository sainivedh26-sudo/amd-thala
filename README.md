# Agent Thala
<img width="1024" height="1024" alt="image-Photoroom" src="Logo.png" />

![made-with-python](https://img.shields.io/badge/Made%20with-Python3-brightgreen)
![Flask](https://img.shields.io/badge/Flask-000?logo=flask&logoColor=fff) ![Python 3.12+](https://img.shields.io/badge/Python-3.12+-3776AB?logo=python&logoColor=fff)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?logo=apachekafka&logoColor=fff)
![Elasticsearch](https://img.shields.io/badge/Elasticsearch-0377CC?logo=elasticsearch&logoColor=fff)
![AWS Bedrock](https://img.shields.io/badge/AWS%20Bedrock-FF9900?logo=aws&logoColor=fff) ![Hugging Face](https://img.shields.io/badge/Hugging%20Face-FFD21E?logo=huggingface&logoColor=000)
![AWS S3](https://img.shields.io/badge/Amazon%20S3-FF9900?logo=amazons3&logoColor=fff) ![AWS Textract](https://img.shields.io/badge/AWS%20Textract-FF9900?logo=aws&logoColor=fff)
![Nginx](https://img.shields.io/badge/nginx-009639?logo=nginx&logoColor=fff)
![Slack](https://img.shields.io/badge/Slack-4A154B?logo=slack&logoColor=fff) ![Jira](https://img.shields.io/badge/Jira-0052CC?logo=jira&logoColor=fff)
![boto3](https://img.shields.io/badge/boto3-FF9900?logo=aws&logoColor=fff)
![beautifulsoup4](https://img.shields.io/badge/beautifulsoup4-000?logo=python&logoColor=fff)

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


<br>
<br>

<h2>Ready to check the demo ?</h2>
<br>

[![Agnent Thala Prototype Launch](https://github.com/user-attachments/assets/3efb52f1-96f9-4f73-8e20-4643aafeb749)](https://www.youtube.com/watch?v=pK1OFuGYdEE)



## About The Project

Thala is an intelligent incident management system that automatically:
- Detects incidents from Slack messages, Jira tickets, and emails
- Classifies & predicts severity, category, and likelihood using agent
- Tracks resolutions and links them to original incidents
- Searches similar past incidents using semantic similarity
- Extracts text from image attachments using AWS Textract

## Architecture
<img width="1912" height="1076" alt="architecture-thala" src="https://github.com/user-attachments/assets/b393e411-f913-4ba2-8bba-13848aca0f16" />


## Data Flow

1. **Ingestion**: Slack/Jira/Email → Connectors → Kafka
2. **Classification**: Groq LLM classifies messages (incident, resolution, discussion, unrelated)
3. **Prediction**: AWS Bedrock (llama-3.3-70b) agent predicts category & severity
4. **Attachment Processing**: Images → S3 → Textract → Extracted text → Context
5. **Storage**: Flask API → Elasticsearch (with embeddings for semantic search)
6. **Resolution Tracking**: Links resolution messages to original incidents
7. **UI**: Slack bot commands (/thala latest_issue, /thala search)

## Key Features

![clip7](https://github.com/user-attachments/assets/7aadff99-fc76-40ae-a629-e375f6ea6dfa)
![clipp6](https://github.com/user-attachments/assets/d544533f-2d71-4035-83f8-859038369e0d)
![clip4](https://github.com/user-attachments/assets/940ed5ef-5323-4604-bc83-c8cbcc14ef10)
![clip3](https://github.com/user-attachments/assets/2b53d1a8-a589-41d6-b235-e2b139874a1e)
![clipp2](https://github.com/user-attachments/assets/94c2489b-4c73-432d-8b4a-63298c82c171)
![clip1](https://github.com/user-attachments/assets/890c69c5-8cf1-4727-aad1-c45382df2dd1)





### 1. Intelligent Classification
- Uses LLM from AWS Bedrock (llama-3.3-70b) to classify messages semantically
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
- Uses Llama model with few-shot learning

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
```

## How It Works

### Incident Creation Flow
```
Slack: "API server is down"
  → LLM from AWS Bedrock (llama-3.3-70b) classifies as "incident_report"
  → It predicts: Category=API, Severity=High
  → Sent to Kafka → Flask → Elasticsearch
  → Tracked in Incident Tracker
  → Available in Slack: /thala latest_issue
```

### Resolution Flow
```
Slack: "API issue has been fixed"
  → LLM from AWS Bedrock (llama-3.3-70b) classifies as "resolution"
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
- Classifies messages using LLM from AWS Bedrock (llama-3.3-70b)
- Processes attachments (S3 + Textract)
- Detects resolutions and links to incidents
- Prevents resolution messages from creating new incidents
- Handles vague messages intelligently

### slack_bot_ui.py
- Slack bot with slash commands
- Paginated incident listing
- Semantic search interface
- Rich UI with Slack Block Kit

### gemini_predictor.py (uses Llama model from AWS Bedrock now)
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

## Developers

[Sai Nivedh](https://www.github.com/SaiNivedh26)

[Kishore Balaji](https://www.github.com/Kishore-1803)

<br>

### If you like this, then
[!["Buy Me A Coffee"](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://www.buymeacoffee.com/sai_nivedh_26)

