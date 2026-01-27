"""
Slack Bot UI with slash commands
Provides /thala predict, /thala latest_issue, and /thala search commands
"""
import os
import json
import logging
import requests
from datetime import datetime
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from dotenv import load_dotenv
try:
    from bedrock_predictor import get_predictor
except Exception:
    from gemini_predictor import get_predictor
from incident_tracker import get_tracker
from search_client import get_search_client

load_dotenv()

BEDROCK_ENABLED = True

# Connect to search backend (Elasticsearch or OpenSearch)
es = get_search_client()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

if not BEDROCK_ENABLED:
    logger.warning("Bedrock not enabled - Quick Fix LLM analysis will not work")

# Initialize Slack app
app = App(token=os.getenv("SLACK_BOT_TOKEN"))

# Flask API URL for search endpoint
FLASK_API_URL = os.getenv('FLASK_API_URL', 'http://localhost:5000')

# Get predictor and tracker instances
predictor = get_predictor()
tracker = get_tracker()

bedrock_client = None
if BEDROCK_ENABLED:
    try:
        import boto3
        from botocore.config import Config
        region = os.getenv('AWS_REGION', 'us-east-2')
        bedrock_client = boto3.client('bedrock-runtime', region_name=region, config=Config(retries={"max_attempts":3,"mode":"standard"}))
        logger.info("Bedrock client initialized for Quick Fix analysis")
    except Exception as e:
        logger.error(f"Failed to initialize Bedrock client: {e}")

# AWS Lambda URL for web search
LAMBDA_URL = os.getenv('AWS_LAMBDA_URL', 'https://oj4j6xjjvv7xlgg5sxzzf7essq0ahhox.lambda-url.us-east-2.on.aws/')

@app.command("/thala")
def handle_thala_command(ack, command, respond):
    """
    Main /thala command handler
    Supports: /thala predict <description> and /thala latest_issue
    """
    ack()
    
    text = command.get('text', '').strip()
    
    if not text:
        respond({
            "response_type": "ephemeral",
            "text": "🤖 *Thala ITSM Assistant*",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "🤖 *Thala ITSM Assistant*\n\nAvailable commands:"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "• `/thala predict <description>` - Predict category and severity for an incident\n• `/thala latest_issue [page]` - Show ongoing incidents with pagination (default: page 1)\n• `/thala issues [page]` - Same as latest_issue\n• `/thala search <query>` - Search similar resolved incidents from history"
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": "💡 Examples:\n• `/thala predict database connection timeout`\n• `/thala search database connection pool`"
                        }
                    ]
                }
            ]
        })
        return
    
    # Parse subcommand
    parts = text.split(maxsplit=1)
    subcommand = parts[0].lower()
    
    if subcommand == "predict":
        handle_predict(respond, parts)
    elif subcommand == "latest_issue" or subcommand == "issues":
        # Support both /thala latest_issue and /thala issues
        # Check if page number is provided
        page = 1
        if len(parts) > 1:
            try:
                page = int(parts[1])
            except:
                page = 1
        handle_latest_issue(respond, page=page)
    elif subcommand == "search":
        handle_search(respond, parts)
    else:
        respond({
            "response_type": "ephemeral",
            "text": f"❌ Unknown command: `{subcommand}`\n\nUse `/thala` to see available commands."
        })

def handle_predict(respond, parts):
    """Handle /thala predict <description>"""
    if len(parts) < 2:
        respond({
            "response_type": "ephemeral",
            "text": "❌ Please provide a description.\n\nUsage: `/thala predict <description>`\n\nExample: `/thala predict database connection timeout`"
        })
        return
    
    description = parts[1].strip()
    
    if len(description) < 5:
        respond({
            "response_type": "ephemeral",
            "text": "❌ Description too short. Please provide more details."
        })
        return
    
    try:
        # Get prediction from Gemini
        logger.info(f"[PREDICT] User requested prediction for: {description[:50]}...")
        result = predictor.predict(description)
        
        # Determine severity emoji
        severity_emoji = {
            'Critical': '🔴',
            'High': '🟠',
            'Medium': '🟡',
            'Low': '🟢'
        }.get(result['severity'], '⚪')
        
        # Build confidence bar
        confidence_pct = int(result['confidence'] * 100)
        confidence_bar = '█' * (confidence_pct // 10) + '░' * (10 - confidence_pct // 10)
        
        # Send response
        respond({
            "response_type": "in_channel",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🤖 Incident Classification",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Description:*\n```{description}```"
                    }
                },
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*📁 Category*\n`{result['category']}`"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*{severity_emoji} Severity*\n`{result['severity']}`"
                        }
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*📊 Confidence*\n{confidence_bar} `{confidence_pct}%`"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*💡 Reasoning*\n_{result['reasoning']}_"
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": "🔮 Powered by Gemini AI | Based on historical incident patterns"
                        }
                    ]
                }
            ]
        })
        
    except Exception as e:
        logger.error(f"Error in predict command: {e}")
        respond({
            "response_type": "ephemeral",
            "text": f"❌ Error processing prediction: {str(e)}"
        })

def handle_latest_issue(respond, page=1, per_page=5):
    """Handle /thala latest_issue - Query from Elasticsearch with pagination"""
    try:
        # Query Elasticsearch for open incidents with pagination
        from_index = (page - 1) * per_page
        
        # Build query - use filter with term for keyword fields (more efficient and AOSS-compatible)
        # REMOVED timestamp filter temporarily to debug - will re-add once we confirm status matching works
        search_body = {
                "query": {
                "bool": {
                    "filter": [
                        {"term": {"status": "Open"}}  # Use term in filter for keyword field
                        # Temporarily removed timestamp filter to debug
                        # {"range": {"timestamp": {"gte": "now-7d"}}}
                    ],
                    "must_not": [
                        {"term": {"source": "test"}}
                    ]
                }
                },
                "sort": [
                    {"timestamp": {"order": "desc"}}
                ],
            "size": per_page,
            "from": from_index
        }
        
        logger.info(f"[DEBUG] Querying for open incidents: {json.dumps(search_body, indent=2)}")
        
        result = es.search(
            index="thala_knowledge",
            body=search_body
        )
        
        total_hits = result['hits']['total']['value']
        logger.info(f"[DEBUG] Query returned {total_hits} total hits")
        
        # Debug: Query ALL documents (no filters) to see what's actually indexed
        debug_body = {
            "query": {
                "bool": {
                    "must_not": [
                        {"term": {"source": "test"}}
                    ]
                }
            },
            "size": 20,
            "sort": [{"timestamp": {"order": "desc"}}]
        }
        debug_result = es.search(index="thala_knowledge", body=debug_body)
        logger.info(f"[DEBUG] ALL documents (last 7d): {debug_result['hits']['total']['value']} total")
        for hit in debug_result['hits']['hits'][:5]:
            doc = hit['_source']
            logger.info(f"[DEBUG] Document: _id={hit['_id']}, issue_id={doc.get('issue_id')}, status={doc.get('status')}, source={doc.get('source')}, timestamp={doc.get('timestamp')}, text={doc.get('text', '')[:40]}...")
        
        # Also try to find the document by issue_id if we have a specific ID from the query
        # This helps debug if the document exists but isn't matching the status query
        if total_hits == 0 and debug_result['hits']['total']['value'] > 0:
            logger.warning(f"[DEBUG] Found {debug_result['hits']['total']['value']} total documents but 0 with status=Open. Checking status values...")
            status_values = set()
            open_count = 0
            for hit in debug_result['hits']['hits']:
                doc = hit['_source']
                status_val = doc.get('status', 'MISSING')
                status_values.add(status_val)
                issue_id = doc.get('issue_id', 'N/A')
                logger.info(f"[DEBUG] Document {issue_id} has status: '{status_val}' (repr: {repr(status_val)})")
                if status_val and status_val.strip().lower() == 'open':
                    open_count += 1
                    logger.info(f"[DEBUG] ✅ Document {issue_id} IS open (but term query didn't match!)")
            logger.info(f"[DEBUG] All unique status values found: {status_values}")
            logger.info(f"[DEBUG] Documents that should match 'Open': {open_count}")
            
            # If we found open documents but term query didn't match, try a different query
            if open_count > 0:
                logger.warning(f"[DEBUG] Trying alternative query with match instead of term...")
                alt_query = {
                    "query": {
                        "bool": {
                            "filter": [
                                {"match": {"status": "Open"}}  # Try match instead of term
                            ],
                            "must_not": [
                                {"term": {"source": "test"}}
                            ]
                        }
                    },
                    "size": per_page,
                    "sort": [{"timestamp": {"order": "desc"}}]
                }
                alt_result = es.search(index="thala_knowledge", body=alt_query)
                logger.info(f"[DEBUG] Alternative query (match) returned {alt_result['hits']['total']['value']} hits")
                if alt_result['hits']['total']['value'] > 0:
                    # Use the alternative result
                    result = alt_result
                    total_hits = alt_result['hits']['total']['value']
                    logger.info(f"[DEBUG] Using alternative query result!")
        
        total_incidents = result['hits']['total']['value']
        
        if not result['hits']['hits']:
            logger.info(f"[DEBUG] No open incidents in Elasticsearch")
            respond({
                "response_type": "ephemeral",
                "text": "✅ No ongoing incidents at the moment! 🎉"
            })
            return
        
        # Debug: Log what we found
        logger.info(f"[DEBUG] Processing {len(result['hits']['hits'])} hits from search")
        for hit in result['hits']['hits']:
            doc = hit['_source']
            logger.info(f"[DEBUG] Found incident: {hit['_id']} - {doc.get('text', '')[:60]}... status={doc.get('status')} source={doc.get('source')} timestamp={doc.get('timestamp')}")
        
        # Get all incidents from this page
        incidents_data = []
        from datetime import datetime
        for hit in result['hits']['hits']:
            doc = hit['_source']
            incident_id = hit['_id']

            # Convert to incident format
        timestamp = doc.get('timestamp')
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except:
                timestamp = datetime.now()
        
            # Use stored category/severity or predict if not available
            category = doc.get('category')
            severity = doc.get('severity')
            if not category or not severity:
                try:
                    prediction = predictor.predict(doc.get('text', ''))
                    category = prediction.get('category', category or 'Unknown')
                    severity = prediction.get('severity', severity or 'Unknown')
                except Exception as e:
                    logger.error(f"Error predicting for {incident_id}: {e}")
                    category = category or 'Unknown'
                    severity = severity or 'Unknown'

            incidents_data.append({
            'id': incident_id,
            'text': doc.get('text', ''),
            'source': doc.get('source', 'unknown').upper(),
            'timestamp': timestamp,
            'status': doc.get('status', 'Open'),
                'category': category,
                'severity': severity,
                'discussions': doc.get('discussions', [])
            })
        
        logger.info(f"[DEBUG] Found {len(incidents_data)} incidents (page {page}, total: {total_incidents})")
        
        # Build response blocks
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🚨 Ongoing Incidents (Page {page}) - {total_incidents} total",
                    "emoji": True
                }
            },
            {
                "type": "divider"
            }
        ]
        
        # Add each incident (use page-relative numbering, not global index)
        for idx, incident in enumerate(incidents_data, start=1):
        # Format timestamp
            timestamp_str = incident['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        
        # Determine severity emoji
        severity_emoji = '⚪'
        if incident.get('severity'):
            severity_emoji = {
                'Critical': '🔴',
                'High': '🟠',
                'Medium': '🟡',
                'Low': '🟢'
            }.get(incident['severity'], '⚪')
        
            # Get discussion count
            discussion_count = len(incident.get('discussions', []))

            # Clean and truncate text for display
            import re
            raw_text = incident.get('text', '') or ''
            cleaned = raw_text
            source_prefixes = ['Slack:', 'Jira:', 'Email:', 'SLACK:', 'JIRA:', 'EMAIL:']
            for p in source_prefixes:
                if cleaned.startswith(p):
                    cleaned = cleaned[len(p):].strip()
                    break
            for p in source_prefixes:
                cleaned = cleaned.replace(f"{p} ", "").replace(f"{p}", "")
            cleaned = re.sub(r"\[Attachment:.*?\]", "", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\[File:.*?\]", "", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\[User:.*?\]", "", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\[.*?Time:.*?\]", "", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\s+", " ", cleaned).strip()
            incident_text = f"{incident['source'].title()}: {cleaned}" if cleaned else incident['source'].title()
            if len(incident_text) > 300:
                incident_text = incident_text[:300] + "..."

            # Incident header
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*#{idx}. {incident_text}*"
                }
            })

            # Incident details
            fields = [
                {"type": "mrkdwn", "text": f"*🆔 ID*\n`{incident['id']}`"},
                {"type": "mrkdwn", "text": f"*📍 Source*\n`{incident['source']}`"},
                {"type": "mrkdwn", "text": f"*📁 Category*\n`{incident.get('category', 'Unknown')}`"},
                {"type": "mrkdwn", "text": f"*{severity_emoji} Severity*\n`{incident.get('severity', 'Unknown')}`"}
            ]

            blocks.append({"type": "section", "fields": fields})

            blocks.append({
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"🕐 Started: `{timestamp_str}` | 💬 {discussion_count} discussion(s) | 🔄 Status: *{incident['status']}*"}
                ]
            })

            # Add Quick Fix button for each incident
            blocks.append({
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "🔧 Quick Fix"},
                        "style": "primary",
                        "value": f"quick_fix_{incident['id']}",
                        "action_id": "show_quick_fix_options"
                    }
                ]
            })

            # Add divider between incidents (except last)
            if idx < from_index + len(incidents_data):
                blocks.append({"type": "divider"})
        
        # Add pagination buttons
        total_pages = (total_incidents + per_page - 1) // per_page if total_incidents > 0 else 1
        
        action_elements = []
        if page > 1:
            action_elements.append({
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "◀ Previous"
                },
                "value": f"page_{page - 1}",
                "action_id": "prev_page"
            })
        
        if page < total_pages:
            action_elements.append({
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "Next ▶"
                },
                "value": f"page_{page + 1}",
                "action_id": "next_page"
            })
        
        if action_elements:
            blocks.append({
                "type": "actions",
                "elements": action_elements
            })
        
        blocks.append({
            "type": "context",
            "elements": [
            {
                "type": "mrkdwn",
                    "text": f"Showing {from_index + 1}-{min(from_index + per_page, total_incidents)} of {total_incidents} ongoing incidents"
                }
            ]
        })
        
        respond({
            "response_type": "in_channel",
            "blocks": blocks
        })
        
    except Exception as e:
        logger.error(f"Error in latest_issue command: {e}")
        respond({
            "response_type": "ephemeral",
            "text": f"❌ Error retrieving latest issue: {str(e)}"
        })

def handle_search(respond, parts):
    """Handle /thala search <query> - Search for similar resolved incidents"""
    if len(parts) < 2:
        respond({
            "response_type": "ephemeral",
            "text": "❌ Please provide a search query.\n\nUsage: `/thala search <query>`\n\nExample: `/thala search database connection pool`"
        })
        return
    
    query = parts[1].strip()
    
    if len(query) < 3:
        respond({
            "response_type": "ephemeral",
            "text": "❌ Search query too short. Please provide at least 3 characters."
        })
        return
    
    try:
        logger.info(f"[SEARCH] User searching for: {query[:50]}...")
        
        # Call Flask API search endpoint
        search_url = f"{FLASK_API_URL}/search"
        search_payload = {
            "query": query,
            "top_k": 10  # Get top 10 similar incidents
        }
        
        response = requests.post(search_url, json=search_payload, timeout=10)
        
        if response.status_code != 200:
            error_msg = "Unknown error"
            try:
                error_data = response.json()
                error_msg = error_data.get('error', 'Unknown error')
            except:
                error_msg = f"HTTP {response.status_code}"
            
            respond({
                "response_type": "ephemeral",
                "text": f"❌ Error searching incidents: {error_msg}"
            })
            return
        
        result = response.json()
        incidents = result.get('results', [])
        prediction = result.get('prediction')
        
        if not incidents:
            respond({
                "response_type": "ephemeral",
                "text": f"🔍 No similar incidents found for: `{query}`\n\nTry a different search term or check if any incidents have been resolved."
            })
            return
        
        # Format response blocks
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🔍 Similar Incidents: {query[:50]}",
                    "emoji": True
                }
            },
            {
                "type": "divider"
            }
        ]
        
        # Add prediction if available
        if prediction:
            likelihood = prediction.get('incident_likelihood', 'Unknown')
            confidence = int(prediction.get('confidence', 0) * 100)
            likelihood_emoji = "🟢" if likelihood == "Not Likely" else "🔴"
            
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*📊 Incident Likelihood:* {likelihood_emoji} `{likelihood}` (Confidence: {confidence}%)"
                }
            })
            blocks.append({"type": "divider"})
        
        # Group by status: Resolved first, then Open
        resolved_incidents = [inc for inc in incidents if inc.get('status') == 'Resolved']
        open_incidents = [inc for inc in incidents if inc.get('status') == 'Open']

        # Show resolved incidents first (these are prioritized by Flask endpoint)
        if resolved_incidents:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*✅ Resolved Incidents ({len(resolved_incidents)} found)*"}
            })

            for i, incident in enumerate(resolved_incidents[:5], 1):
                text = incident.get('text', 'N/A')
                score = incident.get('score') or 0.0
                timestamp = incident.get('timestamp', '')
                resolution_text = incident.get('resolution_text', '')
                resolved_by = incident.get('resolved_by', 'Unknown')
                resolved_at = incident.get('resolved_at', '')
                issue_id = incident.get('issue_id', 'N/A')

                # Format timestamps
                time_str = ""
                if timestamp:
                    try:
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        time_str = dt.strftime("%Y-%m-%d %H:%M")
                    except:
                        time_str = timestamp[:16] if len(timestamp) > 16 else timestamp

                resolved_time_str = ""
                if resolved_at:
                    try:
                        dt = datetime.fromisoformat(resolved_at.replace('Z', '+00:00'))
                        resolved_time_str = dt.strftime("%Y-%m-%d %H:%M")
                    except:
                        resolved_time_str = resolved_at[:16] if len(resolved_at) > 16 else resolved_at

                incident_text = f"*#{i}* {text[:100]}..." if len(text) > 100 else f"*#{i}* {text}"

                fields = [
                    {"type": "mrkdwn", "text": f"*Incident:*\n{incident_text}"}
                ]
                if resolution_text:
                    resolution_display = resolution_text[:150] + "..." if len(resolution_text) > 150 else resolution_text
                    fields.append({"type": "mrkdwn", "text": f"*Resolution:*\n{resolution_display}"})
                if resolved_by and resolved_by != 'Unknown':
                    fields.append({"type": "mrkdwn", "text": f"*Resolved by:* {resolved_by}"})
                if resolved_time_str:
                    fields.append({"type": "mrkdwn", "text": f"*Resolved at:* {resolved_time_str}"})

                blocks.append({"type": "section", "fields": fields})

                context_elements = [{"type": "mrkdwn", "text": f"ID: `{issue_id}` | Similarity: `{float(score):.2f}`"}]
                if time_str:
                    context_elements.append({"type": "mrkdwn", "text": f"Original: {time_str}"})
                blocks.append({"type": "context", "elements": context_elements})

                if i < len(resolved_incidents[:5]):
                    blocks.append({"type": "divider"})

        # Show open incidents if any
        if open_incidents:
            blocks.append({"type": "divider"})
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*🔄 Open Incidents ({len(open_incidents)} found)*"}
            })

            for i, incident in enumerate(open_incidents[:3], 1):
                text = incident.get('text', 'N/A')
                score = incident.get('score') or 0.0
                timestamp = incident.get('timestamp', '')
                issue_id = incident.get('issue_id', 'N/A')

                time_str = ""
                if timestamp:
                    try:
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        time_str = dt.strftime("%Y-%m-%d %H:%M")
                    except:
                        time_str = timestamp[:16] if len(timestamp) > 16 else timestamp

                incident_text = f"{text[:100]}..." if len(text) > 100 else text

                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*#{i}* {incident_text}\nID: `{issue_id}` | Similarity: `{float(score):.2f}` | Started: {time_str}"}
                })

                if i < len(open_incidents[:3]):
                    blocks.append({"type": "divider"})

        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"🔍 Found {len(incidents)} total similar incidents | Powered by semantic similarity search"}]
        })
        
        respond({
            "response_type": "in_channel",
            "blocks": blocks
        })
        
    except requests.exceptions.ConnectionError:
        respond({
            "response_type": "ephemeral",
            "text": f"❌ Cannot connect to Flask API at {FLASK_API_URL}. Please ensure the Flask API is running."
        })
    except requests.exceptions.Timeout:
        respond({
            "response_type": "ephemeral",
            "text": "❌ Search request timed out. Please try again."
        })
    except Exception as e:
        logger.error(f"Error in search command: {e}")
        respond({
            "response_type": "ephemeral",
            "text": f"❌ Error searching incidents: {str(e)}"
        })

def summarize_discussion(discussion_texts):
    """Use Gemini to summarize discussion messages"""
    if not discussion_texts:
        return "_No discussion yet_"
    
    try:
        from google.genai import types
        
        discussion_combined = "\n".join([f"- {text}" for text in discussion_texts])
        
        prompt = f"""Summarize the following incident discussion in 2-3 concise bullet points.
Focus on:
1. What actions have been taken so far
2. Current status/findings
3. Next steps if mentioned

Discussion:
{discussion_combined}

Provide a brief summary in bullet points (max 3 points, each under 100 characters).
"""
        
        response = predictor.gemini_client.models.generate_content(
            model="gemini-2.0-flash-exp",
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.3,
                max_output_tokens=200
            )
        )
        
        summary = response.text.strip()
        
        # Format as markdown bullet points if not already
        if not summary.startswith('-') and not summary.startswith('•'):
            lines = summary.split('\n')
            summary = '\n'.join([f"• {line.strip()}" for line in lines if line.strip()])
        
        return summary
        
    except Exception as e:
        logger.error(f"Error summarizing discussion: {e}")
        # Return simple concatenation as fallback
        return "• " + "\n• ".join(discussion_texts[-3:])

@app.event("app_mention")
def handle_mention(event, say):
    """Handle @thala mentions"""
    text = event.get('text', '')
    
    # Remove the mention from text
    text = text.split('>', 1)[-1].strip() if '>' in text else text
    
    if not text:
        say("👋 Hi! Use `/thala` to see available commands.")
        return
    
    # Check if it's a predict request
    if any(word in text.lower() for word in ['predict', 'classify', 'category', 'severity']):
        result = predictor.predict(text)
        say(f"📊 *Prediction:* Category: `{result['category']}`, Severity: `{result['severity']}` (Confidence: {int(result['confidence']*100)}%)")
    else:
        say("🤖 I can help you predict incident categories and severities! Use `/thala predict <description>`")

@app.action("prev_page")
def handle_prev_page(ack, action, respond):
    """Handle Previous page button click"""
    ack()
    try:
        page = int(action['value'].replace('page_', ''))
        handle_latest_issue(respond, page=page)
    except Exception as e:
        logger.error(f"Error handling prev_page: {e}")
        respond({
            "response_type": "ephemeral",
            "text": f"❌ Error: {str(e)}"
        })

@app.action("next_page")
def handle_next_page(ack, action, respond):
    """Handle Next page button click"""
    ack()
    try:
        page = int(action['value'].replace('page_', ''))
        handle_latest_issue(respond, page=page)
    except Exception as e:
        logger.error(f"Error handling next_page: {e}")
        respond({
            "response_type": "ephemeral",
            "text": f"❌ Error: {str(e)}"
        })

@app.action("show_quick_fix_options")
def handle_show_quick_fix_options(ack, action, respond):
    """Show Quick Fix options menu"""
    ack()
    try:
        incident_id = action['value'].replace('quick_fix_', '')
        
        # Get incident details from Elasticsearch
        try:
            doc = es.get(index="thala_knowledge", id=incident_id)
            incident = doc['_source']
            incident_text = incident.get('text', 'N/A')
        except:
            respond({
                "response_type": "ephemeral",
                "text": "❌ Could not find incident details"
            })
            return
        
        respond({
            "response_type": "ephemeral",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🔧 Quick Fix Options",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Incident:*\n{incident_text[:200]}{'...' if len(incident_text) > 200 else ''}"
                    }
                },
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "Choose a Quick Fix method:"
                    }
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "📚 Past Incidents"
                            },
                            "style": "primary",
                            "value": f"past_{incident_id}",
                            "action_id": "quick_fix_past_incidents"
                        },
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "🌐 Web Search"
                            },
                            "style": "primary",
                            "value": f"web_{incident_id}",
                            "action_id": "quick_fix_web_search"
                        },
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "🔀 Combine Both"
                            },
                            "style": "primary",
                            "value": f"combine_{incident_id}",
                            "action_id": "quick_fix_combine"
                        }
                    ]
                }
            ]
        })
    except Exception as e:
        logger.error(f"Error showing quick fix options: {e}")
        respond({
            "response_type": "ephemeral",
            "text": f"❌ Error: {str(e)}"
        })

def extract_technical_query(incident_text):
    """
    Extract the actual technical issue description from incident text
    Removes source prefixes (Slack:, Jira:, etc.) and attachment metadata
    Focuses on the technical content for web search
    
    Args:
        incident_text: Raw incident text that may contain source prefixes and metadata
    
    Returns:
        str: Cleaned technical query optimized for web search (e.g., "user authentication service connection refused")
    """
    import re
    
    # Remove source prefixes
    cleaned = incident_text
    source_prefixes = ['Slack:', 'Jira:', 'Email:', 'SLACK:', 'JIRA:', 'EMAIL:']
    for prefix in source_prefixes:
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):].strip()
            break
    
    # Remove attachment metadata patterns
    cleaned = re.sub(r'\[Attachment:.*?\]', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\[File:.*?\]', '', cleaned, flags=re.IGNORECASE)
    
    # Remove "Slack: ", "Jira: " from anywhere in text
    for prefix in source_prefixes:
        cleaned = cleaned.replace(f"{prefix} ", "").replace(f"{prefix}:", "")
    
    # Extract key technical information from structured data
    lines = cleaned.split('\n')
    technical_keywords = []
    service_name = None
    error_message = None
    status_info = None
    
    for line in lines:
        line = line.strip()
        if not line or line in ['X', '-', '=', '|']:
            continue
        
        # Extract Service name
        if 'service:' in line.lower():
            match = re.search(r'service:\s*(.+)', line, re.IGNORECASE)
            if match:
                service_name = match.group(1).strip()
        
        # Extract Status
        if 'status:' in line.lower():
            match = re.search(r'status:\s*(.+)', line, re.IGNORECASE)
            if match:
                status_info = match.group(1).strip()
        
        # Extract Error messages
        if 'error:' in line.lower() or 'last error:' in line.lower():
            match = re.search(r'(?:last\s+)?error:\s*(.+)', line, re.IGNORECASE)
            if match:
                error_message = match.group(1).strip()
        
        # Extract technical keywords (avoid generic dashboard words)
        if any(keyword in line.lower() for keyword in ['connection', 'refused', 'timeout', 'failed', 'down', 'authentication', 'database', 'api', 'service']):
            # Skip dashboard headers
            if not any(dash_word in line.lower() for dash_word in ['dashboard', 'production status', 'uptime:', 'response time:']):
                technical_keywords.append(line)
    
    # Build optimized query for web search
    query_parts = []
    
    # Priority 1: Service name + error (most specific)
    if service_name and error_message:
        query_parts.append(f"{service_name} {error_message}")
    # Priority 2: Service name + status
    elif service_name and status_info:
        if status_info.lower() not in ['down', 'up']:
            query_parts.append(f"{service_name} {status_info}")
        else:
            query_parts.append(f"{service_name} {status_info}")
    # Priority 3: Service name only
    elif service_name:
        query_parts.append(service_name)
    
    # Add error message if available and not already included
    if error_message and error_message not in ' '.join(query_parts):
        query_parts.append(error_message)
    
    # Add other technical keywords (limit to most relevant)
    for keyword_line in technical_keywords[:3]:  # Top 3 relevant lines
        # Extract meaningful words (avoid common words)
        words = [w for w in keyword_line.split() if len(w) > 3 and w.lower() not in ['the', 'this', 'that', 'with', 'from', 'production', 'status', 'dashboard']]
        if words:
            query_parts.append(' '.join(words[:5]))  # Max 5 words per line
    
    # Combine into query
    if query_parts:
        query = ' '.join(query_parts)
    else:
        # Fallback: use cleaned text but filter out metadata
        query = ' '.join([line for line in cleaned.split('\n') 
                         if line.strip() 
                         and len(line.strip()) > 5 
                         and not line.strip().startswith('[')
                         and 'dashboard' not in line.lower()[:20]])[:200]
    
    # Clean up extra whitespace and limit length
    query = ' '.join(query.split())
    
    # Limit to reasonable length for search (150 chars is optimal for web search)
    if len(query) > 150:
        # Keep first part (usually most important) and truncate intelligently
        query = query[:147] + '...'
    
    logger.info(f"[EXTRACT QUERY] Extracted technical query: '{query}' from '{incident_text[:80]}...'")
    return query.strip()

def analyze_with_llm(incident_text, context_data, toolkit_name):
    """
    Use Groq LLM to analyze incident and context data to provide fix suggestions
    
    Args:
        incident_text: The incident description
        context_data: Dictionary with context (past_incidents or web_results)
        toolkit_name: Name of toolkit used (for response)
    
    Returns:
        str: LLM-generated fix suggestions
    """
    if not bedrock_client:
        return "❌ LLM analysis not available (Bedrock not configured)"
    
    try:
        # Clean incident text - remove source prefixes like "Slack: ", "Jira: ", "Email: "
        # These are just metadata, not part of the actual technical issue
        cleaned_incident_text = incident_text
        source_prefixes = ['Slack:', 'Jira:', 'Email:', 'SLACK:', 'JIRA:', 'EMAIL:']
        for prefix in source_prefixes:
            if cleaned_incident_text.startswith(prefix):
                cleaned_incident_text = cleaned_incident_text[len(prefix):].strip()
                break
        
        # Also remove from middle of text if it appears
        for prefix in source_prefixes:
            cleaned_incident_text = cleaned_incident_text.replace(f"{prefix} ", "").replace(f"{prefix}:", "")
        
        # Use cleaned text for analysis
        incident_text = cleaned_incident_text.strip()
        # Build prompt based on toolkit
        if toolkit_name == "Past Incidents":
            # Analyze past resolved incidents
            past_incidents = context_data.get('past_incidents', [])
            
            if not past_incidents:
                return "⚠️ No similar past incidents found in the knowledge base."
            
            prompt = f"""You are an expert IT incident resolver. Analyze the current incident and similar past resolved incidents to provide actionable fix suggestions.

**CRITICAL: Focus ONLY on the technical issue described, NOT on the communication channel (Slack/Jira/Email).**
- If the incident mentions "Slack: ..." or "Jira: ..." - this is just the source, ignore it
- Focus on the actual technical problem (e.g., "authentication issue", "database connection", "API timeout")
- DO NOT mention Slack API, Jira, or communication platforms as root causes
- Analyze the real infrastructure/service/application issue

**Current Incident:**
{incident_text}

**Similar Past Resolved Incidents ({len(past_incidents)} found):**
"""
            # Filter out incidents with "Issue deleted from Jira" as they're not useful
            # Handle None values properly
            useful_incidents = []
            for inc in past_incidents[:10]:  # Check more to find useful ones
                resolution_text = inc.get('resolution_text') or ''
                # Convert to string and check if it's useful
                resolution_lower = str(resolution_text).lower().strip()
                if resolution_lower not in ['issue deleted from jira', 'issue deleted from jira.', '', 'none', 'n/a']:
                    useful_incidents.append(inc)
            
            # If no useful incidents, use the first few anyway
            if not useful_incidents:
                useful_incidents = past_incidents[:3]
            
            for i, incident in enumerate(useful_incidents[:5], 1):  # Use top 5 useful ones
                resolution_text = incident.get('resolution_text') or 'N/A'
                # Handle None and check if resolution is just "Issue deleted from Jira"
                resolution_str = str(resolution_text).lower().strip()
                if resolution_str in ['issue deleted from jira', 'issue deleted from jira.', '', 'none', 'n/a']:
                    resolution_text = 'Issue was deleted/cancelled (no resolution details available)'
                
                prompt += f"""
{i}. **Incident:** {incident.get('text', 'N/A')[:200]}
   **Resolution:** {resolution_text[:300]}
   **Resolved by:** {incident.get('resolved_by', 'Unknown')}
   **Resolved at:** {incident.get('resolved_at', 'N/A')}
"""
            
            prompt += """
Based on the current incident and how similar issues were resolved in the past, provide:
1. **Root Cause Analysis** (what likely caused this issue)
2. **Immediate Fix Steps** (step-by-step actions to resolve)
3. **Prevention Measures** (how to prevent this in future)

Format your response in clear sections with numbered steps. Be specific and actionable.
"""
        elif toolkit_name == "Web Search":
            # Analyze web search results
            web_results = context_data.get('web_results', [])
            
            if not web_results:
                return "⚠️ No relevant web resources found."
            
            prompt = f"""You are an expert IT incident resolver. Analyze the current incident and relevant web resources to provide actionable fix suggestions.

**CRITICAL: Focus ONLY on the technical issue described, NOT on the communication channel (Slack/Jira/Email).**
- If the incident mentions "Slack: ..." or "Jira: ..." - this is just the source, ignore it
- Focus on the actual technical problem (e.g., "authentication issue", "database connection", "API timeout")
- DO NOT mention Slack API, Jira, or communication platforms as root causes
- Analyze the real infrastructure/service/application issue

**Current Incident:**
{incident_text}

**Relevant Web Resources ({len(web_results)} found):**
"""
            for i, result in enumerate(web_results[:5], 1):  # Use top 5
                url = result.get('url', 'N/A')
                title = result.get('title', 'N/A')
                content = result.get('main_content', result.get('excerpt', 'N/A'))[:400]
                
                prompt += f"""
{i}. **{title}**
   Source: {url}
   Content: {content}...
"""
            
            prompt += """
Based on the current incident and relevant web resources, provide:
1. **Root Cause Analysis** (what likely caused this issue)
2. **Immediate Fix Steps** (step-by-step actions to resolve)
3. **Best Practices** (industry-standard approaches)

Format your response in clear sections with numbered steps. Be specific and actionable. Reference the web sources when relevant.
"""
        else:  # Combine Both
            past_incidents = context_data.get('past_incidents', [])
            web_results = context_data.get('web_results', [])
            
            prompt = f"""You are an expert IT incident resolver. Analyze the current incident using both past incident history and web resources to provide comprehensive fix suggestions.

**CRITICAL: Focus ONLY on the technical issue described, NOT on the communication channel (Slack/Jira/Email).**
- If the incident mentions "Slack: ..." or "Jira: ..." - this is just the source, ignore it
- Focus on the actual technical problem (e.g., "authentication issue", "database connection", "API timeout")
- DO NOT mention Slack API, Jira, or communication platforms as root causes
- Analyze the real infrastructure/service/application issue from the extracted text/description

**Current Incident:**
{incident_text}

**Past Resolved Incidents ({len(past_incidents)} found):**
"""
            for i, incident in enumerate(past_incidents[:3], 1):
                prompt += f"""
{i}. {incident.get('text', 'N/A')[:150]}...
   Resolution: {incident.get('resolution_text', 'N/A')[:200]}
"""
            
            prompt += f"""
**Relevant Web Resources ({len(web_results)} found):**
"""
            for i, result in enumerate(web_results[:3], 1):
                prompt += f"""
{i}. {result.get('title', 'N/A')}: {result.get('main_content', result.get('excerpt', 'N/A'))[:200]}...
"""
            
            prompt += """
Based on the current incident, past resolutions, and web resources, provide:
1. **Root Cause Analysis** (combine insights from both sources)
2. **Immediate Fix Steps** (prioritize solutions that worked in past incidents)
3. **Comprehensive Solution** (combine best practices from web with proven fixes)
4. **Prevention & Monitoring** (long-term measures)

Format your response in clear sections with numbered steps. Be specific and actionable.
"""
        
        # Call Bedrock API
        bedrock_model = os.getenv('BEDROCK_LLAMA_MODEL_ID', 'us.meta.llama3-3-70b-instruct-v1:0')  # Use inference profile ID with 'us.' prefix
        resp = bedrock_client.converse(
            modelId=bedrock_model,
            system=[{"text": "You are an expert IT incident resolver. Provide clear, actionable fix suggestions based on incident data and context. IMPORTANT: Focus on the actual technical issue (infrastructure, services, applications, databases, APIs), NOT on communication platforms (Slack, Jira, Email) which are just message sources."}],
            messages=[
                {"role": "user", "content": [{"text": prompt}]}
            ],
            inferenceConfig={"temperature": 0.3, "maxTokens": 1500, "topP": 0.9}
        )
        content = resp['output']['message']['content']
        parts = [c.get('text') for c in content if 'text' in c]
        analysis = ("\n".join([p for p in parts if p]) or '').strip()
        if not analysis:
            raise ValueError("Bedrock returned empty content")
        return analysis
        
    except Exception as e:
        logger.error(f"Error in LLM analysis: {e}", exc_info=True)
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return f"❌ Error during LLM analysis: {str(e)}"

@app.action("quick_fix_past_incidents")
def handle_quick_fix_past_incidents(ack, action, client, respond, body):
    """Handle Quick Fix using Past Incidents"""
    ack()
    
    try:
        incident_id = action['value'].replace('past_', '')
        
        # Get channel_id from different possible locations
        channel_id = None
        if 'channel' in action:
            channel_id = action['channel'].get('id') if isinstance(action['channel'], dict) else action['channel']
        elif 'container' in action:
            channel_id = action['container'].get('channel_id')
        elif 'channel_id' in action:
            channel_id = action['channel_id']
        elif body and 'channel' in body:
            channel_id = body['channel'].get('id') if isinstance(body['channel'], dict) else body['channel']
        
        if not channel_id:
            logger.error(f"[QUICK FIX PAST] Could not find channel_id in action: {action.keys()}")
            respond({
                "response_type": "ephemeral",
                "text": "❌ Could not determine channel. Please try the command again."
            })
            return
        
        # Get incident details
        try:
            doc = es.get(index="thala_knowledge", id=incident_id)
            incident = doc['_source']
            incident_text = incident.get('text', 'N/A')
        except Exception as e:
            respond({
                "response_type": "ephemeral",
                "text": f"❌ Could not find incident: {str(e)}"
            })
            return
        
        # Show loading message
        respond({
            "response_type": "ephemeral",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "🔍 *Using toolkit: Past Incidents*\n⏳ Searching past resolved incidents and analyzing..."
                    }
                }
            ]
        })
        
        # Search for similar past incidents
        search_url = f"{FLASK_API_URL}/search"
        search_payload = {
            "query": incident_text,
            "top_k": 10
        }
        
        try:
            response = requests.post(search_url, json=search_payload, timeout=15)
            
            if response.status_code != 200:
                respond({
                    "response_type": "ephemeral",
                    "text": f"❌ Error searching past incidents: HTTP {response.status_code}"
                })
                return
            
            search_results = response.json()
            past_incidents = [inc for inc in search_results.get('results', []) if inc.get('status') == 'Resolved']
            
            logger.info(f"[QUICK FIX PAST] Found {len(past_incidents)} resolved incidents")
            
            if not past_incidents:
                respond({
                    "response_type": "ephemeral",
                    "text": "⚠️ No similar resolved incidents found in the knowledge base."
                })
                return
            
            # Analyze with LLM
            context_data = {'past_incidents': past_incidents}
            logger.info(f"[QUICK FIX PAST] Calling LLM with {len(past_incidents)} incidents...")
            fix_suggestions = analyze_with_llm(incident_text, context_data, "Past Incidents")
            logger.info(f"[QUICK FIX PAST] LLM returned {len(fix_suggestions)} chars")
            
            # Format response
            blocks = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🔧 Quick Fix: Past Incidents",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Current Incident:*\n{incident_text[:300]}{'...' if len(incident_text) > 300 else ''}"
                    }
                },
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*📚 Found {len(past_incidents)} similar resolved incidents*"
                    }
                },
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*Fix Suggestions:*"
                    }
                }
            ]
            
            # Split fix_suggestions into multiple blocks (max 2900 chars per block)
            fix_suggestions_blocks = []
            current_block = ""
            current_length = 0
            max_length = 2900
            
            for line in fix_suggestions.split('\n'):
                line_length = len(line) + 1
                if current_length + line_length > max_length and current_block:
                    fix_suggestions_blocks.append({
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": current_block.strip()
                        }
                    })
                    current_block = line + "\n"
                    current_length = line_length
                else:
                    current_block += line + "\n"
                    current_length += line_length
            
            if current_block.strip():
                fix_suggestions_blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": current_block.strip()
                    }
                })
            
            blocks.extend(fix_suggestions_blocks)
            
            # Add top 3 past incidents for reference
            if past_incidents:
                blocks.append({"type": "divider"})
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*📋 Reference: Top Resolved Incidents*"
                    }
                })
                
                for i, inc in enumerate(past_incidents[:3], 1):
                    resolution = inc.get('resolution_text', 'N/A')[:150]
                    resolved_by = inc.get('resolved_by', 'Unknown')
                    blocks.append({
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*#{i} Resolution:*\n{resolution}..."
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*Resolved by:*\n{resolved_by}"
                            }
                        ]
                    })
            
            # Post result as new message
            logger.info(f"[QUICK FIX PAST] Posting result message to channel {channel_id}")
            try:
                client.chat_postMessage(
                    channel=channel_id,
                    blocks=blocks,
                    text="🔧 Quick Fix: Past Incidents results"
                )
                logger.info(f"[QUICK FIX PAST] ✅ Successfully posted result message")
            except Exception as post_error:
                logger.error(f"[QUICK FIX PAST] Error posting message: {post_error}")
                respond({
                    "response_type": "ephemeral",
                    "text": f"✅ Analysis complete, but error posting to channel: {str(post_error)}\n\n*Fix Suggestions:*\n\n{fix_suggestions[:1500]}"
                })
            
        except Exception as e:
            logger.error(f"Error in quick_fix_past_incidents: {e}")
            respond({
                "response_type": "ephemeral",
                "text": f"❌ Error: {str(e)}"
            })
            
    except Exception as e:
        logger.error(f"Error handling quick_fix_past_incidents: {e}")
        respond({
            "response_type": "ephemeral",
            "text": f"❌ Error: {str(e)}"
        })

@app.action("quick_fix_web_search")
def handle_quick_fix_web_search(ack, action, client, respond, body):
    """Handle Quick Fix using Web Search"""
    ack()
    
    try:
        incident_id = action['value'].replace('web_', '')
        
        # Get channel_id from different possible locations in action payload
        channel_id = None
        if 'channel' in action:
            channel_id = action['channel'].get('id') if isinstance(action['channel'], dict) else action['channel']
        elif 'container' in action:
            channel_id = action['container'].get('channel_id')
        elif 'channel_id' in action:
            channel_id = action['channel_id']
        elif body and 'channel' in body:
            channel_id = body['channel'].get('id') if isinstance(body['channel'], dict) else body['channel']
        
        if not channel_id:
            logger.error(f"[QUICK FIX WEB] Could not find channel_id in action: {action.keys()}")
            respond({
                "response_type": "ephemeral",
                "text": "❌ Could not determine channel. Please try the command again."
            })
            return
        
        user_id = action.get('user', {}).get('id') if isinstance(action.get('user'), dict) else action.get('user')
        
        # Get incident details
        try:
            doc = es.get(index="thala_knowledge", id=incident_id)
            incident = doc['_source']
            incident_text = incident.get('text', 'N/A')
        except Exception as e:
            respond({
                "response_type": "ephemeral",
                "text": f"❌ Could not find incident: {str(e)}"
            })
            return
        
        # Show loading message (ephemeral - only visible to user)
        loading_response = respond({
            "response_type": "ephemeral",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "🔍 *Using toolkit: Web Search*\n⏳ Searching web resources and analyzing..."
                    }
                }
            ]
        })
        
        # Store message timestamp for potential updates
        loading_ts = None
        if isinstance(loading_response, dict) and 'ts' in loading_response:
            loading_ts = loading_response['ts']
        
        # Call AWS Lambda for web search
        try:
            # Extract technical query (remove Slack: prefix, attachment metadata, etc.)
            technical_query = extract_technical_query(incident_text)
            logger.info(f"[QUICK FIX WEB] Original text: {incident_text[:100]}...")
            logger.info(f"[QUICK FIX WEB] Technical query: {technical_query[:100]}...")
            logger.info(f"[QUICK FIX WEB] Calling Lambda URL: {LAMBDA_URL}")
            
            # Lambda Function URLs expect body as JSON string, not direct JSON
            lambda_response = requests.post(
                LAMBDA_URL,
                json={'query': technical_query},  # Use cleaned technical query, not raw incident text
                timeout=60,  # Increased timeout - Lambda might take time for Kendra + crawling
                headers={'Content-Type': 'application/json'}
            )
            
            logger.info(f"[QUICK FIX WEB] Lambda response status: {lambda_response.status_code}")
            
            if lambda_response.status_code != 200:
                error_text = lambda_response.text[:500]
                logger.error(f"[QUICK FIX WEB] Lambda error: {lambda_response.status_code} - {error_text}")
                respond({
                    "response_type": "ephemeral",
                    "text": f"❌ Web search failed: HTTP {lambda_response.status_code}\n\nError: {error_text}"
                })
                return
            
            lambda_data = lambda_response.json()
            logger.info(f"[QUICK FIX WEB] Lambda returned {len(lambda_data.get('data', []))} results")
            web_results = lambda_data.get('data', [])
            
            # Analyze with LLM
            logger.info(f"[QUICK FIX WEB] Analyzing with LLM...")
            context_data = {'web_results': web_results}
            fix_suggestions = analyze_with_llm(incident_text, context_data, "Web Search")
            logger.info(f"[QUICK FIX WEB] LLM analysis complete, length: {len(fix_suggestions)}")
            
            # Format response
            blocks = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🔧 Quick Fix: Web Search",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Current Incident:*\n{incident_text[:300]}{'...' if len(incident_text) > 300 else ''}"
                    }
                },
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*🌐 Found {len(web_results)} relevant web resources*"
                    }
                },
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*Fix Suggestions:*"
                    }
                }
            ]
            
            # Split fix_suggestions into multiple blocks (max 2900 chars per block)
            fix_suggestions_blocks = []
            current_block = ""
            current_length = 0
            max_length = 2900
            
            for line in fix_suggestions.split('\n'):
                line_length = len(line) + 1
                if current_length + line_length > max_length and current_block:
                    fix_suggestions_blocks.append({
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": current_block.strip()
                        }
                    })
                    current_block = line + "\n"
                    current_length = line_length
                else:
                    current_block += line + "\n"
                    current_length += line_length
            
            if current_block.strip():
                fix_suggestions_blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": current_block.strip()
                    }
                })
            
            blocks.extend(fix_suggestions_blocks)
            
            # Add top 3 web resources for reference
            if web_results:
                blocks.append({"type": "divider"})
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*📋 Reference: Top Web Resources*"
                    }
                })
                
                for i, result in enumerate(web_results[:3], 1):
                    title = result.get('title', 'N/A')
                    url = result.get('url', 'N/A')
                    excerpt = result.get('excerpt', result.get('main_content', 'N/A')[:100])
                    blocks.append({
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*#{i} {title}*\n<{url}|{url}>\n{excerpt}..."
                        }
                    })
            
            # Post result as new message (can't use respond() twice)
            logger.info(f"[QUICK FIX WEB] Posting result message to channel {channel_id}")
            try:
                client.chat_postMessage(
                    channel=channel_id,
                    blocks=blocks,
                    text="🔧 Quick Fix: Web Search results"
                )
                logger.info(f"[QUICK FIX WEB] ✅ Successfully posted result message")
                
                # Update loading message to show completion (if we have the timestamp)
                # Note: Updating ephemeral messages requires a response_url, which we don't have here
                # So we'll just let the loading message stay - it's ephemeral anyway
            except Exception as post_error:
                logger.error(f"[QUICK FIX WEB] Error posting message: {post_error}")
                # Fallback: try to respond with error
                try:
                    respond({
                        "response_type": "ephemeral",
                        "text": f"✅ Analysis complete, but error posting to channel: {str(post_error)}\n\n*Fix Suggestions:*\n\n{fix_suggestions[:1500]}"
                    })
                except:
                    pass
            
        except requests.exceptions.Timeout:
            logger.error(f"[QUICK FIX WEB] Request timeout after 60 seconds")
            respond({
                "response_type": "ephemeral",
                "text": "❌ Web search timed out (Lambda took too long). This might happen if:\n• Kendra search is slow\n• Website crawling is taking time\n• Network connectivity issues\n\nTry again or use 'Past Incidents' option instead."
            })
        except requests.exceptions.ConnectionError:
            logger.error(f"[QUICK FIX WEB] Connection error to Lambda URL")
            respond({
                "response_type": "ephemeral",
                "text": f"❌ Cannot connect to Lambda function at {LAMBDA_URL}\n\nPlease check:\n• Lambda Function URL is correct\n• Network connectivity\n• Lambda function is deployed and active"
            })
        except Exception as e:
            logger.error(f"Error in quick_fix_web_search: {e}")
            import traceback
            logger.error(f"[QUICK FIX WEB] Traceback: {traceback.format_exc()}")
            respond({
                "response_type": "ephemeral",
                "text": f"❌ Error: {str(e)}\n\nCheck logs for details."
            })
            
    except Exception as e:
        logger.error(f"Error handling quick_fix_web_search: {e}")
        respond({
            "response_type": "ephemeral",
            "text": f"❌ Error: {str(e)}"
        })

@app.action("quick_fix_combine")
def handle_quick_fix_combine(ack, action, client, respond, body):
    """Handle Quick Fix combining both Past Incidents and Web Search"""
    ack()
    
    try:
        incident_id = action['value'].replace('combine_', '')
        
        # Get channel_id from different possible locations
        channel_id = None
        if 'channel' in action:
            channel_id = action['channel'].get('id') if isinstance(action['channel'], dict) else action['channel']
        elif 'container' in action:
            channel_id = action['container'].get('channel_id')
        elif 'channel_id' in action:
            channel_id = action['channel_id']
        elif body and 'channel' in body:
            channel_id = body['channel'].get('id') if isinstance(body['channel'], dict) else body['channel']
        
        if not channel_id:
            logger.error(f"[QUICK FIX COMBINE] Could not find channel_id in action: {action.keys()}")
            respond({
                "response_type": "ephemeral",
                "text": "❌ Could not determine channel. Please try the command again."
            })
            return
        
        # Get incident details
        try:
            doc = es.get(index="thala_knowledge", id=incident_id)
            incident = doc['_source']
            incident_text = incident.get('text', 'N/A')
        except Exception as e:
            respond({
                "response_type": "ephemeral",
                "text": f"❌ Could not find incident: {str(e)}"
            })
            return
        
        # Show loading message
        respond({
            "response_type": "ephemeral",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "🔍 *Using toolkit: Combine Both*\n⏳ Searching past incidents + web resources and analyzing..."
                    }
                }
            ]
        })
        
        # Search past incidents
        past_incidents = []
        try:
            search_url = f"{FLASK_API_URL}/search"
            search_payload = {"query": incident_text, "top_k": 10}
            search_response = requests.post(search_url, json=search_payload, timeout=15)
            if search_response.status_code == 200:
                search_results = search_response.json()
                past_incidents = [inc for inc in search_results.get('results', []) if inc.get('status') == 'Resolved']
        except Exception as e:
            logger.warning(f"Error searching past incidents: {e}")
        
        # Search web
        web_results = []
        try:
            # Extract technical query (remove Slack: prefix, attachment metadata, etc.)
            technical_query = extract_technical_query(incident_text)
            logger.info(f"[QUICK FIX COMBINE] Original text: {incident_text[:100]}...")
            logger.info(f"[QUICK FIX COMBINE] Technical query for Lambda: {technical_query[:100]}...")
            logger.info(f"[QUICK FIX COMBINE] Calling Lambda URL for web search")
            lambda_response = requests.post(
                LAMBDA_URL, 
                json={'query': technical_query},  # Use cleaned technical query
                timeout=60,
                headers={'Content-Type': 'application/json'}
            )
            if lambda_response.status_code == 200:
                lambda_data = lambda_response.json()
                web_results = lambda_data.get('data', [])
                logger.info(f"[QUICK FIX COMBINE] Found {len(web_results)} web results")
            else:
                logger.warning(f"[QUICK FIX COMBINE] Lambda returned {lambda_response.status_code}")
        except Exception as e:
            logger.warning(f"Error searching web: {e}")
        
        # Analyze with LLM (combining both)
        context_data = {
            'past_incidents': past_incidents,
            'web_results': web_results
        }
        fix_suggestions = analyze_with_llm(incident_text, context_data, "Combine Both")
        
        # Format response
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🔧 Quick Fix: Combined Analysis",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Current Incident:*\n{incident_text[:300]}{'...' if len(incident_text) > 300 else ''}"
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*📊 Combined Analysis*\n📚 Past Incidents: {len(past_incidents)} found\n🌐 Web Resources: {len(web_results)} found"
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Comprehensive Fix Suggestions:*"
                }
            }
        ]
        
        # Split fix_suggestions into multiple blocks (max 2900 chars per block to stay under 3001 limit)
        fix_suggestions_blocks = []
        current_block = ""
        current_length = 0
        max_length = 2900
        
        for line in fix_suggestions.split('\n'):
            line_length = len(line) + 1  # +1 for newline
            if current_length + line_length > max_length and current_block:
                # Save current block and start new one
                fix_suggestions_blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": current_block.strip()
                    }
                })
                current_block = line + "\n"
                current_length = line_length
            else:
                current_block += line + "\n"
                current_length += line_length
        
        # Add remaining content
        if current_block.strip():
            fix_suggestions_blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": current_block.strip()
                }
            })
        
        blocks.extend(fix_suggestions_blocks)
        
        # Post result as new message
        logger.info(f"[QUICK FIX COMBINE] Posting result message to channel {channel_id}")
        try:
            client.chat_postMessage(
                channel=channel_id,
                blocks=blocks,
                text="🔧 Quick Fix: Combined Analysis results"
            )
            logger.info(f"[QUICK FIX COMBINE] ✅ Successfully posted result message")
        except Exception as post_error:
            logger.error(f"[QUICK FIX COMBINE] Error posting message: {post_error}")
            respond({
                "response_type": "ephemeral",
                "text": f"✅ Analysis complete, but error posting to channel: {str(post_error)}\n\n*Fix Suggestions:*\n\n{fix_suggestions[:1500]}"
            })
        
    except Exception as e:
        logger.error(f"Error handling quick_fix_combine: {e}")
        respond({
            "response_type": "ephemeral",
            "text": f"❌ Error: {str(e)}"
        })

def start_bot():
    """Start the Slack bot"""
    logger.info("=" * 60)
    logger.info("Starting Thala Slack Bot UI")
    logger.info("Commands: /thala predict, /thala latest_issue [page], /thala issues [page], /thala search <query>")
    logger.info(f"Flask API URL: {FLASK_API_URL}")
    logger.info("=" * 60)
    
    # Use Socket Mode for development
    socket_token = os.getenv("SLACK_APP_TOKEN")
    
    if not socket_token:
        logger.error("SLACK_APP_TOKEN not found. Cannot start Socket Mode.")
        logger.info("Please set SLACK_APP_TOKEN in .env file")
        return
    
    handler = SocketModeHandler(app, socket_token)
    handler.start()

if __name__ == "__main__":
    start_bot()
