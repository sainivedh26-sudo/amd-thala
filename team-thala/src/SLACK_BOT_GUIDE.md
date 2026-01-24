# Thala Slack Bot - User Guide

## Overview

The Thala Slack Bot provides AI-powered incident classification and tracking directly in Slack using slash commands.

## Features

### 1. `/thala predict <description>`
Predicts the **category** and **severity** of an incident based on its description.

**Example:**
```
/thala predict database connection timeout on production server
```

**Response includes:**
- 📁 **Category**: Database, API, Frontend, Infrastructure, etc.
- 🔴 **Severity**: Critical, High, Medium, or Low
- 📊 **Confidence**: How confident the AI is in its prediction
- 💡 **Reasoning**: Why the AI classified it this way

### 2. `/thala latest_issue`
Shows the latest ongoing incident with a summary of all discussions.

**Example:**
```
/thala latest_issue
```

**Response includes:**
- 🆔 Incident ID
- 📍 Source (Slack, Jira, Email)
- 📁 Category and 🔴 Severity (if available)
- 🕐 When it started
- 📝 AI-generated summary of all discussions
- 💬 Number of discussion messages

## Setup

### Prerequisites
1. Slack workspace with admin access
2. Gemini API key
3. `another.csv` file with training examples

### Installation

1. **Install dependencies:**
```bash
cd team-thala/src
pip install -r ui_requirements.txt
```

2. **Configure environment variables in `.env`:**
```env
# Slack Configuration
SLACK_BOT_TOKEN=xoxb-your-bot-token
SLACK_APP_TOKEN=xapp-your-app-token

# Gemini API
GEMINI_API_KEY=your-gemini-api-key

# Optional: Enable/disable prediction
ENABLE_CATEGORY_PREDICTION=true
```

3. **Create Slack App:**
   - Go to https://api.slack.com/apps
   - Create new app "Thala ITSM Assistant"
   - Enable Socket Mode
   - Add Bot Token Scopes:
     - `commands` - For slash commands
     - `chat:write` - To send messages
     - `app_mentions:read` - To respond to mentions
   - Create slash command `/thala`
   - Install app to workspace

4. **Start the bot:**
```bash
python slack_bot_ui.py
```

## How It Works

### Prediction System
- Uses **Gemini AI** with few-shot learning
- Trained on examples from `another.csv`
- **Cached predictions** to minimize API calls (1 hour cache)
- Provides category, severity, confidence, and reasoning

### Incident Tracking
- Automatically tracks incidents from Kafka messages
- Maintains discussion history for each incident
- AI-generated summaries using Gemini
- Thread-safe for concurrent access

### Optimization
- **Classification caching**: Identical messages use cached results (1 hour TTL)
- **Prediction caching**: Same descriptions use cached predictions (24 hour TTL)
- **Smart API usage**: Only calls Gemini for new/unique content

## Architecture

```
Kafka Messages → Incident Tracker → Slack Bot UI
                       ↓
                 Gemini Predictor
                       ↓
                 Category/Severity
```

### Components

1. **`gemini_predictor.py`**: Handles category/severity prediction
   - Loads training examples from `another.csv`
   - Uses few-shot learning with Gemini
   - Caches predictions for 24 hours

2. **`incident_tracker.py`**: Tracks ongoing incidents
   - Maintains rolling window of recent incidents
   - Stores discussion history
   - Thread-safe operations

3. **`slack_bot_ui.py`**: Slack bot interface
   - Handles `/thala` commands
   - Formats rich UI responses
   - Integrates with predictor and tracker

4. **`kafka_consumer_to_flask.py`**: Updated to:
   - Predict category/severity for new incidents
   - Track incidents and discussions
   - Update incident status on resolution

5. **`slack_connector_llm.py`**: Optimized with:
   - Classification caching (1 hour)
   - Reduced redundant Gemini calls
   - Smart message deduplication

## Usage Examples

### Predict Incident Classification
```
/thala predict API gateway returning 502 errors
```
Response:
```
🤖 Incident Classification
━━━━━━━━━━━━━━━━━━━━━━━━
Description:
API gateway returning 502 errors

📁 Category: API
🟠 Severity: High
📊 Confidence: ██████████ 95%
💡 Reasoning: API gateway errors indicate upstream service failures affecting multiple users

🔮 Powered by Gemini AI | Based on historical incident patterns
```

### Check Latest Incident
```
/thala latest_issue
```
Response:
```
🚨 Latest Ongoing Incident
━━━━━━━━━━━━━━━━━━━━━━━━
Incident Description:
Database connection pool exhausted on postgres-prod-01

🆔 ID: slack_1234567890.123
📍 Source: SLACK
📁 Category: Database
🔴 Severity: Critical
🕐 Started: 2025-11-01 14:30:00

📝 Discussion Summary:
• Investigating connection pool settings
• Increased max_connections from 100 to 200
• Monitoring for improvements

💬 5 discussion message(s) | 🔄 Status: Open
```

## Troubleshooting

### Bot not responding
- Check `SLACK_BOT_TOKEN` and `SLACK_APP_TOKEN` in `.env`
- Verify Socket Mode is enabled in Slack app settings
- Check bot logs for errors

### Predictions not working
- Verify `GEMINI_API_KEY` is set correctly
- Check if `another.csv` exists in project root
- Look for "Gemini predictor initialized" in logs

### Latest issue shows nothing
- Ensure Kafka consumer is running
- Check if incidents are being tracked (look for "[TRACKER]" in logs)
- Verify incident status is "Open"

## API Usage Optimization

The system minimizes Gemini API calls through:

1. **Classification Cache** (1 hour):
   - Caches Slack message classifications
   - Prevents re-classifying identical messages

2. **Prediction Cache** (24 hours):
   - Caches category/severity predictions
   - Reuses predictions for similar descriptions

3. **Smart Triggers**:
   - Only classifies new messages
   - Only predicts for new incidents (status=Open)
   - Skips context-only updates

### Expected API Usage
- **New Slack message**: 1 API call (classification)
- **New incident**: 1 API call (category/severity prediction)
- **Latest issue summary**: 1 API call (discussion summary)
- **Duplicate/cached**: 0 API calls

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SLACK_BOT_TOKEN` | Yes | - | Bot user OAuth token |
| `SLACK_APP_TOKEN` | Yes | - | App-level token for Socket Mode |
| `GEMINI_API_KEY` | Yes | - | Google Gemini API key |
| `ENABLE_CATEGORY_PREDICTION` | No | `true` | Enable/disable predictions |
| `LOG_LEVEL` | No | `INFO` | Logging level |

## Support

For issues or questions:
1. Check logs in console output
2. Verify all environment variables are set
3. Ensure `another.csv` has proper format (Category, Severity, Description columns)
4. Check Gemini API quota and limits
