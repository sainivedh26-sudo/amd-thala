# 🚀 Thala AI - UI Integration Setup Guide

This guide shows you how to set up **Slack Bot** and **Jira Plugin** integrations for Thala AI based on your current configuration.

---

## 📋 Table of Contents

1. [Slack Bot Setup](#slack-bot-setup)
2. [Jira Plugin Setup](#jira-plugin-setup)
3. [Configuration](#configuration)
4. [Usage Examples](#usage-examples)
5. [Troubleshooting](#troubleshooting)

---

## 🤖 Slack Bot Setup

### Prerequisites
- Slack Workspace admin access
- Your Thala AI system running (`integrated_main.py`)
- Internet connection (for Socket Mode)

### Step 1: Create Slack App

1. **Go to**: https://api.slack.com/apps
2. **Click**: "Create New App" → "From scratch"
3. **Enter**:
   - App Name: `Thala AI`
   - Workspace: Select your workspace
4. **Click**: "Create App"

### Step 2: Configure Bot Token Scopes

1. Go to **OAuth & Permissions**
2. Under **Bot Token Scopes**, add:
   ```
   app_mentions:read      # Listen to @mentions
   chat:write             # Send messages
   commands               # Handle slash commands
   im:history             # Read DM history
   channels:history       # Read channel messages
   groups:history         # Read private channel messages
   ```
3. Click **Save Changes**

### Step 3: Enable Socket Mode

1. Go to **Socket Mode** in left sidebar
2. Toggle **Enable Socket Mode** → ON
3. Enter token name: `thala-socket`
4. **Copy the token** (starts with `xapp-...`)
5. Save as `SLACK_APP_TOKEN` in your `.env`

### Step 4: Install App to Workspace

1. Go to **OAuth & Permissions**
2. Click **Install to Workspace**
3. Review permissions → Click **Allow**
4. **Copy the Bot Token** (starts with `xoxb-...`)
5. Save as `SLACK_BOT_TOKEN` in your `.env`

### Step 5: Create Slash Command

1. Go to **Slash Commands**
2. Click **Create New Command**
3. Configure:
   ```
   Command: /thala
   Request URL: https://your-server.com/slack/commands (not needed for Socket Mode)
   Short Description: Search incidents and get AI predictions
   Usage Hint: search <query> | predict <text> | stats | help
   ```
4. Click **Save**

### Step 6: Enable Event Subscriptions (Optional)

1. Go to **Event Subscriptions**
2. Toggle **Enable Events** → ON
3. Under **Subscribe to bot events**, add:
   ```
   app_mention    # When bot is @mentioned
   message.im     # Direct messages to bot
   ```
4. Click **Save Changes**

### Step 7: Update Environment Variables

Add to your `.env` file (same directory as `integrated_main.py`):

```bash
# Slack Bot Configuration
SLACK_BOT_TOKEN=xoxb-your-bot-token-here
SLACK_APP_TOKEN=xapp-your-app-level-token-here
FLASK_API_URL=http://localhost:5000
```

### Step 8: Install Python Dependencies

```bash
cd team-thala/src
pip install -r ui_requirements.txt
```

### Step 9: Start the Slack Bot

**Option A: Standalone Mode**
```bash
cd team-thala/src
python slack_bot_ui.py
```

**Option B: Integrated Mode (Recommended)**
Add to your `integrated_main.py`:

```python
# At the top with other imports
from slack_bot_ui import start_slack_bot
import threading

# In IntegratedThalaSystem.__init__() method
if os.getenv('SLACK_BOT_TOKEN') and os.getenv('SLACK_APP_TOKEN'):
    bot_thread = threading.Thread(target=start_slack_bot, daemon=True)
    bot_thread.start()
    self.logger.info("Slack Bot UI started")
```

### Step 10: Test in Slack

1. **In any channel**, type:
   ```
   /thala help
   ```

2. **Search for incidents**:
   ```
   /thala search login API timeout
   ```

3. **Get AI prediction**:
   ```
   /thala predict database connection slow
   ```

4. **View stats**:
   ```
   /thala stats
   ```

5. **Mention the bot**:
   ```
   @Thala AI what incidents are similar to payment gateway errors?
   ```

---

## 📋 Jira Plugin Setup

### Architecture Options

**Option 1: Atlassian Forge App** (Recommended for production)
- Native Jira Cloud integration
- Appears in Jira sidebar
- Requires Atlassian Forge CLI

**Option 2: Iframe Integration** (Quick setup)
- Embed Thala AI panel via iframe
- Works with Jira Cloud/Server
- No Forge CLI needed

**Option 3: Automation Rules** (Easiest)
- Use Jira's built-in automation
- Webhook to Thala AI
- Add AI comments automatically

We'll cover **Option 2** (iframe) and **Option 3** (automation) as they work with your current setup.

---

### Option 2A: Iframe Panel Integration

#### Step 1: Start Jira Integration Server

```bash
cd team-thala/src
python jira_panel_integration.py
```

This starts a server on port 5001.

#### Step 2: Expose the Server

**For local testing**, use ngrok:
```bash
ngrok http 5001
```

Copy the HTTPS URL (e.g., `https://abc123.ngrok.io`)

#### Step 3: Add Panel URL to Jira

##### For Jira Cloud (Using Forge):

1. Install Forge CLI:
   ```bash
   npm install -g @forge/cli
   ```

2. Create a Forge app:
   ```bash
   forge create
   ```

3. In `manifest.yml`, add:
   ```yaml
   modules:
     jira:issuePanel:
       - key: thala-ai-panel
         title: Thala AI Insights
         icon: https://your-icon-url.com/icon.png
         render: native
         resource: panel-resource
   
   resources:
     - key: panel-resource
       url: https://your-ngrok-url.ngrok.io/jira/panel?issue_key={{issue.key}}&issue_summary={{issue.summary}}&issue_description={{issue.description}}
   ```

4. Deploy:
   ```bash
   forge deploy
   forge install
   ```

##### For Jira Server/Data Center:

1. Create a **Web Panel** plugin using Atlassian Plugin SDK
2. Or use **"Add gadget"** in dashboard and add iframe gadget
3. Point to: `https://your-server.com/jira/panel?issue_key=...`

#### Step 4: View in Jira

1. Open any Jira issue
2. Look for **"Thala AI Insights"** panel in right sidebar
3. It will automatically show similar past incidents!

---

### Option 2B: Browser Extension (Alternative)

For a quick workaround without Forge:

1. Create a browser bookmark with this JavaScript:
   ```javascript
   javascript:(function(){
     var summary = document.querySelector('[data-testid="issue.views.field.rich-text.summary"]')?.innerText || '';
     var desc = document.querySelector('[data-testid="issue.views.field.rich-text.description"]')?.innerText || '';
     window.open('http://localhost:5001/jira/panel?issue_summary=' + encodeURIComponent(summary) + '&issue_description=' + encodeURIComponent(desc), '_blank', 'width=600,height=800');
   })();
   ```

2. Click the bookmark when viewing any Jira issue
3. Thala AI panel opens in a new window!

---

### Option 3: Jira Automation Rules (Recommended for quick setup)

This automatically adds AI analysis as a comment when issues are created.

#### Step 1: Create Automation Rule

1. In Jira, go to **Settings** → **System** → **Automation**
2. Click **Create rule**
3. Configure:

   **Trigger**: Issue Created
   
   **Condition** (optional): Issue type = Bug OR Incident
   
   **Action**: Send web request
   ```
   URL: http://your-server:5001/jira/comment
   Method: POST
   Headers: Content-Type: application/json
   Body:
   {
     "issue_key": "{{issue.key}}",
     "query": "{{issue.summary}} {{issue.description}}"
   }
   ```
   
   **Action 2**: Add comment
   ```
   Comment: {{webhookResponse.body.comment}}
   ```

4. **Name**: "Thala AI - Auto-analyze incidents"
5. Click **Turn on**

#### Step 2: Test

1. Create a new Jira issue:
   ```
   Summary: Payment API returning 502 errors
   Description: Users unable to complete checkout. Started 10 minutes ago.
   ```

2. Within seconds, Thala AI will add a comment:
   ```
   🤖 Thala AI Analysis
   
   Incident Likelihood: Likely (95% confidence)
   
   Similar Past Incidents (3):
   1. [Resolved] (Similarity: 0.85) - Payment gateway timeout...
      💡 Resolution: Increased timeout to 60 seconds...
   2. [Resolved] (Similarity: 0.78) - API 502 errors...
      💡 Resolution: Restarted payment service...
   ```

---

### Option 4: Jira Issue Macro (For Confluence Integration)

If you use Confluence for documentation:

1. Create a **User Macro** in Confluence
2. Name: `thala-similar-incidents`
3. Macro Body:
   ```html
   <iframe src="http://your-server:5001/jira/panel?issue_summary=$paramQuery" 
           width="100%" height="600px" frameborder="0">
   </iframe>
   ```

4. Use in Confluence pages:
   ```
   {thala-similar-incidents:query=payment API errors}
   ```

---

## ⚙️ Configuration

### Environment Variables

Add to your `.env` file:

```bash
# ============== Slack Bot ==============
SLACK_BOT_TOKEN=xoxb-your-bot-token
SLACK_APP_TOKEN=xapp-your-app-token

# ============== Jira Integration ==============
JIRA_URL=https://your-company.atlassian.net
JIRA_EMAIL=your-email@company.com
JIRA_API_TOKEN=your-jira-api-token

# ============== Thala AI Backend ==============
FLASK_API_URL=http://localhost:5000

# ============== Optional ==============
USE_GEMINI_LABELING=true
GEMINI_API_KEY=your-gemini-key
```

### Port Configuration

- **Main Flask API**: Port 5000 (default)
- **Jira Integration**: Port 5001 (default)
- **Slack Bot**: Uses Socket Mode (no port needed)

---

## 💡 Usage Examples

### Slack Bot Examples

#### 1. Search Similar Incidents
```
/thala search login page not loading
```
**Response**: 
- Shows top 5 similar incidents
- Displays resolutions if available
- Shows AI prediction (Likely/Not Likely)

#### 2. Predict Incident Likelihood
```
/thala predict API gateway returning 504 timeout
```
**Response**:
- Incident likelihood: Likely (95% confidence)
- Suggests creating Jira ticket if high likelihood

#### 3. View System Stats
```
/thala stats
```
**Response**:
- Total incidents tracked
- Resolved vs Open count
- Model status

#### 4. Natural Conversation
```
@Thala AI we're seeing database connection issues
```
**Response**:
- Automatically searches similar incidents
- Shows past resolutions
- Links to related Jira tickets

### Jira Examples

#### Scenario 1: Creating a New Bug

**You create**:
```
Summary: User authentication failing
Description: Login endpoint returning 401 for valid credentials
```

**Thala AI automatically comments**:
```
🤖 Thala AI Analysis

Incident Likelihood: Likely (87% confidence)

Similar Past Incidents (3):
1. [Resolved] (Similarity: 0.92) - Auth service returning 401
   💡 Resolution: Session timeout increased to 60 seconds
   
2. [Resolved] (Similarity: 0.85) - Login API authentication fail
   💡 Resolution: Restarted Redis cache
```

#### Scenario 2: Manual Query

Open the Thala AI panel and search:
```
payment gateway timeout
```

**Results show**:
- Past incidents with same issue
- Resolutions that worked
- Links to related Slack discussions

---

## 🔍 Testing the Integration

### End-to-End Test Flow

1. **Create incident in Slack**:
   ```
   Payment API is down!
   ```

2. **Bot classifies it** → Stores in Elasticsearch

3. **Search in Slack**:
   ```
   /thala search payment API
   ```
   Shows the incident you just created

4. **Create Jira ticket** → Thala AI comments with similar incidents

5. **Resolve in Jira** → Updates Elasticsearch

6. **Resolve in Slack**:
   ```
   Fixed by restarting payment service
   ```

7. **Search again**:
   ```
   /thala search payment API
   ```
   Now shows resolution!

---

## 🐛 Troubleshooting

### Slack Bot Issues

**Problem**: "Thala AI" not responding to `/thala` commands

**Solutions**:
1. Check bot is running: `ps aux | grep slack_bot_ui`
2. Verify tokens in `.env`:
   ```bash
   echo $SLACK_BOT_TOKEN  # Should start with xoxb-
   echo $SLACK_APP_TOKEN  # Should start with xapp-
   ```
3. Check Socket Mode is enabled in Slack App settings
4. Verify slash command is created
5. Check logs: Look for "Starting Thala AI Slack Bot..."

---

**Problem**: Commands work but no search results

**Solutions**:
1. Check Flask API is running: `curl http://localhost:5000/health`
2. Verify `FLASK_API_URL` in `.env`
3. Check Elasticsearch is running: `curl http://localhost:9200`
4. Look at Flask logs for errors

---

**Problem**: Socket Mode connection fails

**Solutions**:
1. Check firewall allows websocket connections
2. Verify SLACK_APP_TOKEN is correct (from Socket Mode settings)
3. Reinstall app to workspace
4. Check you're using `slack-bolt>=1.18.0`

---

### Jira Integration Issues

**Problem**: Panel not showing in Jira

**Solutions**:
1. Check Jira integration server is running on port 5001
2. If using ngrok, verify HTTPS URL is correct
3. Check CORS is enabled (already in code)
4. For Forge apps, run `forge deploy` again
5. Clear browser cache

---

**Problem**: Automation rule not adding comments

**Solutions**:
1. Verify webhook URL is reachable from Jira Cloud
2. Check automation rule is enabled
3. View rule execution logs in Jira
4. Test webhook manually:
   ```bash
   curl -X POST http://localhost:5001/jira/comment \
     -H "Content-Type: application/json" \
     -d '{"issue_key":"TEST-1","query":"test incident"}'
   ```
5. Check Flask API is accessible from Jira's servers (not localhost)

---

**Problem**: Panel shows "No results"

**Solutions**:
1. Check Flask API URL is correct
2. Verify Elasticsearch has data: `curl http://localhost:9200/thala_knowledge/_count`
3. Check browser console for CORS errors
4. Verify query parameters are passed correctly

---

### General Issues

**Problem**: "Connection refused" errors

**Solutions**:
1. Check all services are running:
   ```bash
   # Kafka
   ps aux | grep kafka
   
   # Elasticsearch
   curl http://localhost:9200
   
   # Flask API
   curl http://localhost:5000/health
   
   # Slack Bot
   ps aux | grep slack_bot_ui
   
   # Jira Integration
   ps aux | grep jira_panel_integration
   ```

2. Check ports are not blocked:
   ```bash
   netstat -tuln | grep -E "5000|5001|9200|9092"
   ```

---

## 🎯 Next Steps

### Enhance Slack Bot

1. **Add interactive buttons** for:
   - "Create Jira Ticket" from Slack incident
   - "Mark as Resolved" directly in Slack
   - "See more details" with modal

2. **Add shortcuts**:
   - Right-click message → "Search similar"
   - Message action → "Report as incident"

3. **Add scheduled reports**:
   - Daily summary of new incidents
   - Weekly resolved incidents report

### Enhance Jira Panel

1. **Add filters**:
   - Filter by date range
   - Filter by status (Open/Resolved)
   - Filter by source (Slack/Jira)

2. **Add actions**:
   - "Link this issue" button
   - "Copy resolution" button
   - "View in Slack" link

3. **Add visualizations**:
   - Timeline of similar incidents
   - Resolution success rate
   - Average time to resolve

---

## 📚 Additional Resources

- **Slack API Docs**: https://api.slack.com/
- **Slack Bolt Python**: https://slack.dev/bolt-python/
- **Atlassian Forge**: https://developer.atlassian.com/platform/forge/
- **Jira Automation**: https://www.atlassian.com/software/jira/guides/automation
- **Your Thala AI Docs**: `README.md`

---

## 🆘 Support

If you encounter issues:

1. **Check logs**:
   ```bash
   tail -f thala_integrated.log
   tail -f team-thala/src/logs/thala_ingestion.log
   ```

2. **Enable debug mode**:
   ```python
   # In slack_bot_ui.py or jira_panel_integration.py
   logging.basicConfig(level=logging.DEBUG)
   ```

3. **Test API manually**:
   ```bash
   # Search
   curl -X POST http://localhost:5000/search \
     -H "Content-Type: application/json" \
     -d '{"query":"test","top_k":5}'
   
   # Predict
   curl -X POST http://localhost:5000/predict_incident \
     -H "Content-Type: application/json" \
     -d '{"query":"API error"}'
   ```

---

**Happy Integrating! 🚀**







