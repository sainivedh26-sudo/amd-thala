# How to Add `files:read` Scope to Your Slack App

## Problem
Your Slack app is missing the `files:read` scope, which is required to download image attachments from Slack messages.

## Solution

### Step 1: Go to Slack API App Settings
1. Visit https://api.slack.com/apps
2. Select your Thala app from the list

### Step 2: Navigate to OAuth & Permissions
1. In the left sidebar, click **"OAuth & Permissions"** (under "Features")
2. Scroll down to **"Scopes"** section

### Step 3: Add Bot Token Scopes
In the **"Bot Token Scopes"** section, click **"Add an OAuth Scope"** and add:
- `files:read` - View files shared in channels and conversations that the app has access to

### Step 4: Reinstall App to Workspace
1. Scroll up to the top of the page
2. Click **"Reinstall to Workspace"** button (or "Install to Workspace" if not installed)
3. Authorize the new permissions
4. Copy the new **Bot User OAuth Token** (starts with `xoxb-`)
5. Update your `.env` file with the new token:
   ```
   SLACK_BOT_TOKEN=xoxb-your-new-token-here
   ```

### Step 5: Restart Your Application
After updating the token, restart your Slack connector:
```bash
# Stop the current process (Ctrl+C)
# Then restart
python team-thala/src/main.py
```

## Current Required Scopes
Your app should have these scopes:
- ✅ `channels:history` - View messages in public channels
- ✅ `channels:read` - View basic information about public channels
- ✅ `chat:write` - Send messages as the bot
- ✅ `commands` - Use slash commands
- ✅ `app_mentions:read` - View app mentions in channels
- ✅ `im:history` - View direct messages
- ⚠️ **`files:read`** - **ADD THIS** - View files shared in channels

## Verification
After adding the scope and restarting, try sending an image attachment in Slack. You should see:
```
[ATTACHMENT] Processing image.png (type: image/png)
[S3] Uploaded file to: https://thala-images.s3.us-east-2.amazonaws.com/...
[TEXTRACT] Extracted X lines from image
```

Instead of:
```
[ATTACHMENT] files_info failed: missing_scope
[ATTACHMENT] Received HTML error page instead of image
```




