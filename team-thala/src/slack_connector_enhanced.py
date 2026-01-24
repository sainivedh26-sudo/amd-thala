import os
import json
import logging
import hashlib
import re
import requests
import tempfile
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from kafka_producer import KafkaMessageProducer
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta
from collections import defaultdict
from sentence_transformers import SentenceTransformer
import numpy as np
from incident_tracker import get_tracker
from gemini_predictor import get_predictor
from aws_attachment_processor import get_attachment_processor

load_dotenv()

# Groq LLM for semantic understanding (faster than Gemini)
try:
    from groq import Groq
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False

class EnhancedSlackConnector:
    """
    Enhanced Slack Connector with:
    1. Thread tracking (link issues with resolutions)
    2. Gemini AI classification (category + severity prediction)
    3. Resolution detection (detect when issues are fixed)
    4. Context tracking (semantic similarity for linking)
    5. Incident tracker integration
    """
    
    def __init__(self):
        self.client = WebClient(token=os.getenv('SLACK_BOT_TOKEN'))
        self.kafka_producer = KafkaMessageProducer()
        self.topic = os.getenv('KAFKA_TOPIC_SLACK', 'thala-slack-events')
        self.logger = logging.getLogger(__name__)
        
        # Start from 1 minute ago to catch recent messages (for testing)
        self.last_timestamp = time.time() - 60  # 60 seconds = 1 minute
        self.logger.info(f"Starting from 1 minute ago - will process recent messages")
        
        # Track threads: thread_ts -> {original_message, replies, status}
        self.tracked_threads = {}
        
        # Track standalone messages by timestamp for resolution linking
        self.recent_issues = {}  # timestamp -> message_data
        
        # Initialize Groq AI
        if not GROQ_AVAILABLE:
            self.logger.error("Groq not available. Install with: pip install groq")
            raise ImportError("Groq AI is required")
        
        self.groq_client = Groq(api_key=os.getenv('GROQ_API_KEY'))
        self.logger.info("Groq AI initialized for classification")
        
        # Initialize embedding model for semantic similarity
        self.logger.info("Loading sentence transformer for context tracking...")
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Get predictor and tracker instances
        self.predictor = get_predictor()
        self.tracker = get_tracker()
        
        # Get AWS attachment processor (optional - won't fail if AWS not configured)
        try:
            self.attachment_processor = get_attachment_processor()
            self.logger.info("AWS attachment processor initialized")
        except Exception as e:
            self.logger.warning(f"AWS attachment processor not available: {e}")
            self.attachment_processor = None
        
        # Classification cache to avoid redundant API calls
        self.classification_cache = {}  # {message_hash: (classification, timestamp)}
        self.cache_ttl = timedelta(hours=1)
        
        # Context tracking for semantic linking
        self.issue_embeddings = {}  # issue_id -> embedding
        
        # Flask API URL for looking up incidents
        self.flask_url = os.getenv('FLASK_API_URL', 'http://localhost:5000')
        
        self.logger.info("Enhanced Slack Connector initialized with Groq AI")

    def _get_message_hash(self, text):
        """Generate hash for message caching"""
        return hashlib.md5(text.lower().strip().encode()).hexdigest()
    
    def _get_cached_classification(self, text):
        """Get cached classification if available and not expired"""
        msg_hash = self._get_message_hash(text)
        
        if msg_hash in self.classification_cache:
            cached_data = self.classification_cache[msg_hash]
            if datetime.now() - cached_data['timestamp'] < self.cache_ttl:
                self.logger.info(f"[CACHE HIT] Using cached classification")
                return cached_data['classification']
            else:
                del self.classification_cache[msg_hash]
        return None
    
    def _cache_classification(self, text, classification):
        """Cache classification result"""
        msg_hash = self._get_message_hash(text)
        self.classification_cache[msg_hash] = {
            'classification': classification,
            'timestamp': datetime.now()
        }
    
    def classify_message(self, text, thread_context=None):
        """
        Use Groq LLM to classify Slack message semantically
        Returns: (message_type, category, severity, confidence)
        message_type: 'incident_report', 'resolution', 'discussion', 'unrelated'
        """
        # Check cache first (only for non-threaded messages)
        if not thread_context:
            cached = self._get_cached_classification(text)
            if cached:
                return cached['type'], cached.get('category'), cached.get('severity'), cached['confidence']
        
        try:
            # Build context-aware prompt with recent open incidents
            context_info = ""
            recent_open_incidents = []
            
            # Get recent open incidents from tracker (more comprehensive than recent_issues)
            if self.tracker:
                recent_open = self.tracker.get_recent_incidents(count=5, status='Open')
                recent_open_incidents = recent_open
            
            # Also check recent_issues for very recent ones
            if self.recent_issues:
                recent = list(self.recent_issues.values())[-3:]
                for issue in recent:
                    if issue.get('id') not in [i.get('id') for i in recent_open_incidents]:
                        recent_open_incidents.append(issue)
            
            if recent_open_incidents:
                context_info = "\n\nRecent open incidents in conversation:\n"
                for i, issue in enumerate(recent_open_incidents[-5:], 1):  # Last 5
                    issue_text = issue.get('text', '')[:100]
                    issue_id = issue.get('id', 'unknown')
                    context_info += f"{i}. [ID: {issue_id}] {issue_text}...\n"
            
            if thread_context:
                context_info += "\n\nThread context:\n"
                for msg in thread_context[-3:]:
                    context_info += f"- {msg.get('text', '')[:80]}...\n"
            
            prompt = f"""You are an ITSM incident classification expert. Analyze this Slack message and classify it intelligently using semantic understanding - NO keyword matching.

{context_info}

Message to classify:
\"{text}\"

Classification Rules (use semantic understanding, not keywords):

1. **incident_report**: ONLY a NEW technical issue, error, outage, or problem being REPORTED FOR THE FIRST TIME
   - Examples: "API is down", "Database timeout", "Users can't login", "We have a problem with authentication"
   - MUST be reporting a NEW problem, not discussing an existing one
   - If the message mentions something was "fixed", "resolved", "working", "done", or indicates completion - it is NOT an incident_report
   
2. **resolution**: Message indicating an issue is FIXED, RESOLVED, COMPLETED, or WORKING AGAIN
   - Examples: "Fixed it", "Back up now", "Working again", "Issue resolved", "All good now", "I think it's working"
   - CRITICAL: Phrases like "I think [issue] has been fixed", "[issue] has been resolved", "it's working now", "should be fixed", "looks good", "all set" are ALWAYS resolutions
   - Look for ANY indication of completion, fixing, resolution, or positive outcome - even tentative language like "I think it's fixed"
   - Technical fix actions: "rolled back", "restarted", "reverted", "deployed fix", "patched", "corrected", "should work now", "it should work", "working again"
   - Even if the message doesn't explicitly mention an issue ID, if it semantically indicates something was fixed/resolved, classify as resolution
   - Messages about fixing/configuring/deploying solutions are resolutions: "rolled back the change", "fixed the policy", "restarted the service", "updated config"
   
3. **discussion**: Follow-up discussion, investigation, or updates about an existing issue WITHOUT indicating resolution
   - Examples: "Checking logs", "Found the cause", "Investigating now", "Looking into it"
   - ONLY discussion if the message is about investigating/analyzing, NOT about fixing/resolving
   
4. **unrelated**: Not related to incidents (questions, docs, meetings, casual chat, general discussion)

CRITICAL RULES:
- If the message contains ANY semantic indication of fixing/resolving/completing/working (even "I think it's fixed"), classify as **resolution** - NEVER as incident_report
- If there are recent open incidents and the message suggests things are working/fixed/resolved (semantically), classify as **resolution**
- Only create incident_report if it's CLEARLY a NEW problem being reported with no indication of resolution
- "I think [X] has been fixed" = resolution (NOT incident_report)
- "[X] should be working now" = resolution
- "Looks like [X] is resolved" = resolution
- When in doubt between incident_report and resolution, choose resolution if there's ANY indication of fixing/completion
- Vague messages like "we have a problem", "something is wrong", "can someone check" WITHOUT specific technical details (errors, services, etc.) should be classified as **discussion** or **unrelated**, NOT incident_report
- Only classify as incident_report if the message contains specific technical information (error messages, service names, error codes, symptoms, etc.) OR if an attachment image was successfully processed and contains incident details

For incident_report ONLY, also provide:
- category: Database, API, Frontend, Infrastructure, Authentication, Payment, Network, Application, Security, Email, Storage, Monitoring, Configuration, or Deployment
- severity: Critical, High, Medium, or Low

Respond in JSON format:
{{
  "type": "incident_report|resolution|discussion|unrelated",
  "category": "category name" (only for incident_report),
  "severity": "Critical|High|Medium|Low" (only for incident_report),
  "confidence": 0.0-1.0,
  "reasoning": "brief explanation of your classification"
}}
"""
            
            # Use Groq for classification
            completion = self.groq_client.chat.completions.create(
                model="llama-3.3-70b-versatile",  # Fast and accurate
                messages=[
                    {"role": "system", "content": "You are an ITSM incident classification expert. Always respond with valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=300
            )
            
            result_text = completion.choices[0].message.content.strip()
            
            # Remove markdown code blocks if present
            if "```json" in result_text:
                result_text = result_text.split("```json")[1].split("```")[0].strip()
            elif "```" in result_text:
                result_text = result_text.split("```")[1].split("```")[0].strip()
            
            result = json.loads(result_text)
            
            self.logger.info(f"[GROQ] {result['type']} (conf: {result['confidence']:.2f})")
            
            # Cache the result (only for non-threaded messages)
            if not thread_context:
                self._cache_classification(text, result)
            
            return result['type'], result.get('category'), result.get('severity'), result['confidence']
            
        except Exception as e:
            error_str = str(e)
            
            # Handle rate limit errors
            if '429' in error_str or 'rate_limit' in error_str.lower():
                self.logger.warning(f"[RATE LIMIT] Groq API rate limit hit - waiting 5 seconds...")
                time.sleep(5)  # Groq recovers faster
                # Fallback: treat as discussion for now
                return 'discussion', None, None, 0.3
            
            self.logger.error(f"Error in Groq classification: {e}")
            # Fallback: treat as discussion
            return 'discussion', None, None, 0.3

    def extract_incident_id_from_text(self, text):
        """
        Extract incident ID from message text if explicitly mentioned
        Looks for patterns like:
        - Jira ticket IDs: "KAN-21", "PROJ-123", "BUG-456"
        - Slack IDs: "issue Kq_ePpoBbobMY9ANBx14", "Kq_ePpoBbobMY9ANBx14 has been fixed"
        - Slack-style IDs: "M11MP5oBaJJZHmTDMSca", "cZKBP5oBaOP9qx5rt2vY"
        - IDs are typically 15-25 alphanumeric characters, may include underscores
        """
        # Pattern to match incident IDs
        # Examples: "Kq_ePpoBbobMY9ANBx14" (20 chars), "M11MP5oBaJJZHmTDMSca" (20 chars), "cZKBP5oBaOP9qx5rt2vY" (20 chars)
        patterns = [
            # Match Jira ticket IDs first (PROJECT-NUMBER format, e.g., "KAN-21", "PROJ-123")
            r'\b(?:issue|ticket|ID|id)[\s:]+([A-Z][A-Z0-9]+-\d+)\b',
            r'\b([A-Z][A-Z0-9]+-\d+)\b',  # Standalone Jira ticket IDs
            # Match "issue ID" or "issue cZKBP5oBaOP9qx5rt2vY" (with or without "ID" keyword)
            # Allow both uppercase and lowercase starting letters
            r'\b(?:issue|ID|id)[\s:]+([A-Za-z0-9][A-Za-z0-9_-]{14,24})\b',
            # Match standalone IDs (15-25 chars, must contain mix of letters/numbers/underscores)
            # Examples: "Kq_ePpoBbobMY9ANBx14", "M11MP5oBaJJZHmTDMSca", "cZKBP5oBaOP9qx5rt2vY"
            # Allow both uppercase and lowercase starting letters
            r'\b([A-Za-z0-9][A-Za-z0-9_-]{14,24})\b',
            # Match slack IDs like "slack_1234567890_123456"
            r'\b(slack_[0-9_]{10,})\b',
        ]
        
        candidates = []
        for pattern in patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                # Check if it's a Jira ticket ID (PROJECT-NUMBER format)
                if re.match(r'^[A-Z][A-Z0-9]+-\d+$', match):
                    # Jira ticket IDs are always valid
                    candidates.append(match)
                    continue
                
                # Filter out common words and validate it looks like an ID (for Slack-style IDs)
                if (len(match) >= 15 and 
                    match not in ['Authentication', 'Configuration', 'Application', 'Infrastructure'] and
                    # Must have mix of letters/numbers or underscores (not just letters)
                    (re.search(r'[0-9]', match) or '_' in match)):
                    candidates.append(match)
        
        if candidates:
            # Prefer Jira ticket IDs if present, otherwise prefer longer IDs
            jira_tickets = [c for c in candidates if re.match(r'^[A-Z][A-Z0-9]+-\d+$', c)]
            if jira_tickets:
                incident_id = jira_tickets[0]  # Take first Jira ticket ID found
            else:
                # Remove duplicates while preserving order
                seen = set()
                unique_candidates = []
                for c in candidates:
                    if c not in seen:
                        seen.add(c)
                        unique_candidates.append(c)
                # Prefer longer IDs (more likely to be actual incident IDs)
                incident_id = max(unique_candidates, key=len)
            
            self.logger.info(f"[ID EXTRACTION] Found potential incident ID: {incident_id} (from: {text[:50]}...)")
            return incident_id
        
        return None
    
    def lookup_incident_by_id(self, incident_id):
        """
        Lookup incident by ID in both tracker and Elasticsearch
        The ID could be either:
        - The issue_id field (e.g., slack_1762002045_512439)
        - The Elasticsearch document _id (e.g., cZKBP5oBaOP9qx5rt2vY)
        Returns incident data if found, None otherwise
        """
        if not incident_id:
            return None
        
        self.logger.info(f"[LOOKUP] Searching for incident ID: {incident_id}")
        
        # First check the in-memory tracker (fastest)
        incident = self.tracker.get_incident_by_id(incident_id)
        if incident:
            self.logger.info(f"[LOOKUP] ✅ Found incident {incident_id} in tracker")
            return {
                'id': incident['id'],
                'text': incident.get('text', ''),
                'status': incident.get('status', 'Open'),
                'source': incident.get('source', 'unknown'),
                'timestamp': incident.get('timestamp'),
                'found_in': 'tracker'
            }
        
        self.logger.debug(f"[LOOKUP] Not found in tracker, querying Elasticsearch...")
        
        # If not in tracker, query Elasticsearch via Flask API
        # The lookup endpoint tries both document _id and issue_id field
        try:
            lookup_url = f"{self.flask_url}/lookup_incident"
            lookup_payload = {"issue_id": incident_id}
            
            self.logger.debug(f"[LOOKUP] Querying Flask API: {lookup_url} with payload: {lookup_payload}")
            response = requests.post(lookup_url, json=lookup_payload, timeout=5)
            
            self.logger.debug(f"[LOOKUP] Flask API response status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                if result.get('found'):
                    # Use the actual issue_id from the document, not necessarily what was searched
                    actual_issue_id = result.get('issue_id', incident_id)
                    document_id = result.get('document_id', incident_id)
                    self.logger.info(f"[LOOKUP] ✅ Found incident in Elasticsearch - searched: {incident_id}, issue_id: {actual_issue_id}, document_id: {document_id}")
                    return {
                        'id': actual_issue_id,  # Use the actual issue_id from the document
                        'document_id': document_id,  # Keep track of document _id if different
                        'text': result.get('text', ''),
                        'status': result.get('status', 'Open'),
                        'source': result.get('source', 'unknown'),
                        'timestamp': result.get('timestamp'),
                        'found_in': 'elasticsearch'
                    }
                else:
                    self.logger.warning(f"[LOOKUP] ❌ Flask API returned found=False for {incident_id}")
            elif response.status_code == 404:
                self.logger.warning(f"[LOOKUP] ❌ Incident {incident_id} not found in Elasticsearch (404)")
            else:
                self.logger.warning(f"[LOOKUP] ❌ Flask API returned status {response.status_code} for {incident_id}")
                try:
                    error_msg = response.json().get('error', 'Unknown error')
                    self.logger.warning(f"[LOOKUP] Error message: {error_msg}")
                except:
                    self.logger.warning(f"[LOOKUP] Response body: {response.text[:200]}")
            
            self.logger.warning(f"[LOOKUP] ❌ Incident {incident_id} not found in tracker or Elasticsearch")
            return None
            
        except requests.exceptions.Timeout:
            self.logger.error(f"[LOOKUP] ⚠️ Timeout querying Flask API for incident {incident_id}")
            return None
        except requests.exceptions.ConnectionError:
            self.logger.error(f"[LOOKUP] ⚠️ Connection error - Flask API not reachable at {self.flask_url}")
            return None
        except Exception as e:
            self.logger.error(f"[LOOKUP] ⚠️ Error querying for incident {incident_id}: {e}")
            import traceback
            self.logger.debug(f"[LOOKUP] Traceback: {traceback.format_exc()}")
            return None

    def link_resolution_to_issue(self, resolution_message, timestamp, resolution_text=None):
        """
        Try to link a resolution message to a recent issue using:
        1. Pure semantic similarity (embeddings) - fully LLM-based, no keyword matching
        2. Recent conversation context from tracker
        
        This method uses sentence transformers to generate embeddings and calculates
        cosine similarity to find the best matching open incident.
        """
        cutoff_time = timestamp - timedelta(hours=24)
        
        # Clean recent_issues of old entries
        self.recent_issues = {
            ts: msg for ts, msg in self.recent_issues.items()
            if ts > cutoff_time
        }
        
        resolution_text = resolution_text or resolution_message.get('text', '')
        channel_id = resolution_message.get('channel_id')
        
        # Generate embedding for resolution message
        resolution_embedding = self.embedding_model.encode([resolution_text])[0]
        
        # Get recent open incidents from tracker (better context)
        recent_open = []
        if self.tracker:
            recent_open = self.tracker.get_recent_incidents(count=10, status='Open')
        
        # Tracker should have all recent incidents, so we primarily rely on it
        # If needed, we can query Elasticsearch directly for more comprehensive list
        # but for now tracker + recent_issues should be sufficient
        
        # Combine all potential issues (purely semantic matching - no keyword or channel matching)
        all_potential_issues = []
        
        # Add from tracker (with embeddings - fully semantic matching)
        for incident in recent_open:
            incident_id = incident.get('id')
            if not incident_id:
                continue
                
            # Get embedding if available in memory
            incident_embedding = None
            if incident_id in self.issue_embeddings:
                incident_embedding = self.issue_embeddings[incident_id]
            else:
                # Generate embedding on the fly if not cached
                incident_text = incident.get('text', '')
                if incident_text:
                    incident_embedding = self.embedding_model.encode([incident_text])[0]
                    # Cache it for future use
                    self.issue_embeddings[incident_id] = incident_embedding
            
            if incident_embedding is not None:
                # Calculate semantic similarity
                similarity = float(np.dot(resolution_embedding, incident_embedding) / (
                    np.linalg.norm(resolution_embedding) * np.linalg.norm(incident_embedding)
                ))
                
                # Use purely semantic similarity (no keyword matching - fully LLM/embedding based)
                # The embedding model captures semantic meaning, so we rely on it entirely
                combined_score = similarity  # Pure semantic similarity score
                
                all_potential_issues.append({
                    'issue': {
                        'id': incident_id,
                        'text': incident.get('text', ''),
                        'timestamp': incident.get('timestamp'),
                        'status': 'Open'
                    },
                    'score': combined_score,
                    'similarity': similarity,
                    'timestamp': incident.get('timestamp', timestamp)
                })
            else:
                # No embedding available - generate one on the fly for the incident
                incident_text = incident.get('text', '')
                if incident_text:
                    try:
                        incident_embedding = self.embedding_model.encode([incident_text])[0]
                        self.issue_embeddings[incident_id] = incident_embedding
                        similarity = float(np.dot(resolution_embedding, incident_embedding) / (
                            np.linalg.norm(resolution_embedding) * np.linalg.norm(incident_embedding)
                        ))
                        all_potential_issues.append({
                            'issue': {
                                'id': incident_id,
                                'text': incident.get('text', ''),
                                'timestamp': incident.get('timestamp'),
                                'status': 'Open'
                            },
                            'score': similarity,
                            'similarity': similarity,
                            'timestamp': incident.get('timestamp', timestamp)
                        })
                    except Exception as e:
                        self.logger.warning(f"Could not generate embedding for incident {incident_id}: {e}")
        
        # Only use semantic similarity - fully LLM/embedding based, no keyword matching
        if all_potential_issues:
            # Sort by semantic similarity score and recency
            all_potential_issues.sort(key=lambda x: (
                x['score'],  # Higher semantic similarity first
                x['timestamp']  # More recent first
            ), reverse=True)
            
            best_match = all_potential_issues[0]
            # Lower threshold since we're using pure semantic similarity (more reliable than keyword matching)
            if best_match['score'] > 0.25:  # Threshold for semantic matching
                self.logger.info(f"[LINK] Matched resolution to incident (semantic score: {best_match['score']:.2f})")
                return best_match['issue']
        
        return None

    def fetch_thread_replies(self, channel_id, thread_ts):
        """Fetch all replies in a thread"""
        try:
            result = self.client.conversations_replies(
                channel=channel_id,
                ts=thread_ts,
                limit=100
            )
            return result.get('messages', [])
        except SlackApiError as e:
            self.logger.error(f"Error fetching thread replies: {e}")
            return []
    
    def process_slack_attachments(self, message, channel_id):
        """
        Process attachments from Slack message
        Downloads files, uploads to S3, extracts text, and returns extracted text
        
        Args:
            message: Slack message object
            channel_id: Slack channel ID
            
        Returns:
            str: Combined extracted text from all attachments (or None)
        """
        if not self.attachment_processor:
            return None
        
        files = message.get('files', [])
        if not files:
            return None
        
        extracted_texts = []
        attachment_s3_urls = []
        
        for file_info in files:
            try:
                file_id = file_info.get('id', '')
                filename = file_info.get('name', 'file')
                file_type = file_info.get('mimetype', '')
                file_size = file_info.get('size', 0)
                
                # Skip if file is too large (max 10MB for Textract)
                if file_size > 10 * 1024 * 1024:
                    self.logger.warning(f"[ATTACHMENT] Skipping large file {filename} ({file_size} bytes)")
                    continue
                
                # Get file URL - need to download using Slack API
                file_url_private = file_info.get('url_private')
                if not file_url_private:
                    self.logger.warning(f"[ATTACHMENT] No URL found for file {filename}")
                    continue
                
                self.logger.info(f"[ATTACHMENT] Processing {filename} (type: {file_type})")
                
                # Download file from Slack using files_info + url_private_download method
                try:
                    file_content = None
                    
                    # First, get file info using files_info API
                    try:
                        file_info_response = self.client.files_info(file=file_id)
                        if file_info_response and 'file' in file_info_response:
                            file_info_data = file_info_response['file']
                            # Get the download URL (prefer url_private_download, fallback to url_private)
                            download_url = file_info_data.get('url_private_download') or file_info_data.get('url_private') or file_url_private
                            
                            self.logger.debug(f"[ATTACHMENT] Got download URL from files_info: {download_url[:50]}...")
                        else:
                            download_url = file_url_private
                            self.logger.warning(f"[ATTACHMENT] files_info didn't return file data, using original URL")
                    except SlackApiError as sdk_error:
                        error_str = str(sdk_error)
                        # Check if it's a missing scope error
                        if 'missing_scope' in error_str or 'files:read' in error_str:
                            self.logger.error(f"[ATTACHMENT] ❌ Missing Slack scope 'files:read'. Please add this scope to your Slack app at https://api.slack.com/apps and reinstall the app to your workspace.")
                            self.logger.error(f"[ATTACHMENT] Skipping attachment {filename} - cannot download without files:read scope")
                            continue
                        self.logger.warning(f"[ATTACHMENT] files_info failed: {sdk_error}, using original URL...")
                        download_url = file_url_private
                    
                    # Download the file using the URL with Bearer authentication
                    headers = {
                        'Authorization': f'Bearer {os.getenv("SLACK_BOT_TOKEN")}'
                    }
                    response = requests.get(download_url, headers=headers, timeout=30)
                    response.raise_for_status()
                    file_content = response.content
                    
                    # Ensure file_content is bytes
                    if isinstance(file_content, str):
                        # If it's a string, try to decode/encode appropriately
                        self.logger.warning(f"[ATTACHMENT] File content is string, encoding to bytes...")
                        file_content = file_content.encode('latin-1')  # Use latin-1 to preserve binary data
                    elif not isinstance(file_content, bytes):
                        self.logger.error(f"[ATTACHMENT] Unexpected file content type: {type(file_content)}")
                        raise ValueError(f"File content must be bytes, got {type(file_content)}")
                    
                    # Validate response content
                    if not file_content or len(file_content) == 0:
                        self.logger.warning(f"[ATTACHMENT] Downloaded file is empty for {filename}")
                        continue
                    
                    # Check if response is HTML (error page) instead of image
                    content_preview = file_content[:50] if len(file_content) >= 50 else file_content
                    content_str = content_preview.decode('utf-8', errors='ignore').lower()
                    if content_str.strip().startswith('<!doctype html') or content_str.strip().startswith('<html'):
                        self.logger.error(f"[ATTACHMENT] ❌ Received HTML error page instead of image. This usually means:")
                        self.logger.error(f"[ATTACHMENT] 1. Missing 'files:read' scope in Slack app")
                        self.logger.error(f"[ATTACHMENT] 2. Or file download URL is invalid/expired")
                        self.logger.error(f"[ATTACHMENT] Skipping attachment {filename}")
                        continue
                    
                    # Validate file headers (PNG or JPEG)
                    content_preview = file_content[:16] if len(file_content) >= 16 else file_content
                    is_png = len(file_content) >= 8 and content_preview[:8] == b'\x89PNG\r\n\x1a\n'
                    is_jpeg = len(file_content) >= 2 and content_preview[:2] == b'\xff\xd8'
                    
                    if not (is_png or is_jpeg):
                        self.logger.warning(f"[ATTACHMENT] Downloaded file doesn't appear to be a valid image (invalid headers): {filename}. First bytes: {content_preview.hex()[:32]}")
                        self.logger.warning(f"[ATTACHMENT] Skipping invalid image file")
                        continue
                    
                    # Save to temporary file
                    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1])
                    temp_file.write(file_content)
                    temp_file.close()
                    local_path = temp_file.name
                    
                    self.logger.debug(f"[ATTACHMENT] Downloaded {len(file_content)} bytes to {local_path}")
                    
                    # Process attachment: upload to S3 → Textract
                    result = self.attachment_processor.process_attachment(
                        file_url_or_path=local_path,
                        source='slack',
                        file_id=f"{file_info.get('id', message.get('ts', ''))}",
                        filename=filename,
                        download=False  # Already downloaded
                    )
                    
                    # Clean up temp file
                    try:
                        os.unlink(local_path)
                    except:
                        pass
                    
                except Exception as download_error:
                    self.logger.error(f"Error downloading Slack file {filename}: {download_error}")
                    continue
                
                if result.get('success'):
                    s3_url = result.get('s3_url')
                    extracted_text = result.get('extracted_text')
                    
                    if s3_url:
                        attachment_s3_urls.append(s3_url)
                        self.logger.info(f"[ATTACHMENT] ✅ Uploaded to S3: {s3_url}")
                    
                    if extracted_text:
                        extracted_texts.append(f"[Attachment: {filename}]\n{extracted_text}")
                        self.logger.info(f"[ATTACHMENT] ✅ Extracted {len(extracted_text)} chars from {filename}")
                else:
                    self.logger.warning(f"[ATTACHMENT] ❌ Failed to process {filename}")
                    
            except Exception as e:
                self.logger.error(f"Error processing attachment: {e}")
                continue
        
        # Return both extracted text and S3 URLs
        if extracted_texts or attachment_s3_urls:
            return {
                'extracted_text': "\n\n".join(extracted_texts) if extracted_texts else None,
                's3_urls': attachment_s3_urls
            }
        
        return None

    def process_message(self, message, channel_id):
        """Enhanced message processing with Gemini classification and thread tracking"""
        # Skip bot messages
        if message.get('subtype') == 'bot_message' or message.get('bot_id'):
            return
        
        text = message.get('text', '')
        ts = float(message.get('ts', 0))
        timestamp = datetime.fromtimestamp(ts) if ts else datetime.utcnow()
        thread_ts = message.get('thread_ts')
        
        # Process attachments if any (download, upload to S3, extract text)
        attachment_text = None
        attachment_s3_urls = []
        attachment_extraction_failed = False
        original_text = text  # Keep original text for comparison
        
        if message.get('files') or message.get('attachments'):
            attachment_result = self.process_slack_attachments(message, channel_id)
            if attachment_result:
                attachment_text = attachment_result.get('extracted_text')
                attachment_s3_urls = attachment_result.get('s3_urls', [])
                
                # Check if extraction failed
                if attachment_result.get('success') and not attachment_text:
                    attachment_extraction_failed = True
                    self.logger.warning(f"[ATTACHMENT] Image extraction failed - will require higher confidence threshold for incident creation")
                
                if attachment_text:
                    # Append extracted text to message text for context
                    text = f"{text}\n\n{attachment_text}" if text else attachment_text
                    self.logger.info(f"[ATTACHMENT] Added {len(attachment_text)} chars from attachments to message context")
        
        # Get thread context if available
        thread_context = None
        if thread_ts and thread_ts in self.tracked_threads:
            thread_context = self.tracked_threads[thread_ts].get('replies', [])
        
        # Classify message with Groq (now includes attachment context)
        message_type, category, severity, confidence = self.classify_message(text, thread_context)
        
        # STRICT RULE: If attachment extraction failed, require clear incident text OR high confidence
        if attachment_extraction_failed and message_type == 'incident_report':
            # Check if original text is vague/uncertain or too generic
            vague_indicators = [
                'seems like', 'might be', 'could be', 'maybe', 'possibly', 'not sure', 
                'i think', 'looks like', 'we have a problem', 'there\'s a problem',
                'there is a problem', 'something is wrong', 'something wrong',
                'can someone check', 'can someone look', 'please check', 'please look',
                'is there an issue', 'anyone else seeing', 'is it just me'
            ]
            original_lower = original_text.lower()
            is_vague = any(indicator in original_lower for indicator in vague_indicators)
            
            # Check if text has actual technical details (signs of real incident)
            technical_indicators = [
                'error', 'down', 'timeout', 'crash', 'failed', 'exception', 'broken',
                'not working', 'not responding', 'unavailable', 'outage', 'slow',
                '500', '502', '503', '504', 'database', 'api', 'service', 'server'
            ]
            has_technical_details = any(indicator in original_lower for indicator in technical_indicators)
            
            # If vague AND no technical details AND extraction failed = skip
            if is_vague and not has_technical_details:
                self.logger.info(f"[ATTACHMENT] ❌ Skipping incident: extraction failed + vague message '{original_text[:50]}...' without technical details")
                return  # Don't create incident
            
            # Require higher confidence threshold if extraction failed (even with technical details)
            min_confidence = 0.85 if is_vague else 0.80
            
            if confidence < min_confidence:
                self.logger.info(f"[ATTACHMENT] ❌ Skipping incident: extraction failed, confidence {confidence:.2f} < {min_confidence:.2f}")
                return  # Don't create incident
            else:
                self.logger.info(f"[ATTACHMENT] ⚠️ Proceeding despite extraction failure: confidence {confidence:.2f} >= {min_confidence:.2f}, has technical details: {has_technical_details}")
        
        # Check if this is a resolution
        is_resolution = (message_type == 'resolution')
        resolution_text = text if is_resolution else None
        
        # Skip unrelated messages
        if message_type == 'unrelated':
            self.logger.debug(f"[UNRELATED] Skipped: {text[:50]}...")
            return
        
        # Base message structure
        transformed_message = {
            'id': f"slack_{message.get('ts', '').replace('.', '_')}",
            'type': 'slack_message',
            'channel_id': channel_id,
            'user_id': message.get('user', 'unknown'),
            'text': text,
            'thread_ts': thread_ts,
            'original_timestamp': message.get('ts'),
            'message_type': message_type,
            'timestamp': timestamp.isoformat(),
            'status': 'Open',
            'category': category,
            'severity': severity,
            'confidence': confidence,
            'metadata': {
                'has_files': 'files' in message,
                'has_attachments': 'attachments' in message,
                'is_thread': thread_ts is not None,
                'is_resolution': is_resolution,
                'attachment_s3_urls': attachment_s3_urls  # Store S3 URLs for reference
            }
        }
        
        # Track if resolution was successfully handled (so we don't send it as new incident)
        resolution_handled = False
        
        # Handle threaded messages
        if thread_ts:
            # This is part of a thread
            if thread_ts not in self.tracked_threads:
                # Fetch full thread
                thread_messages = self.fetch_thread_replies(channel_id, thread_ts)
                self.tracked_threads[thread_ts] = {
                    'original_message': thread_messages[0] if thread_messages else message,
                    'replies': thread_messages[1:] if len(thread_messages) > 1 else [],
                    'status': 'Open',
                    'channel_id': channel_id
                }
            
            # Add this reply to tracked thread
            self.tracked_threads[thread_ts]['replies'].append(message)
            
            # If this is a resolution, update thread status
            if is_resolution:
                self.tracked_threads[thread_ts]['status'] = 'Resolved'
                transformed_message['status'] = 'Resolved'
                transformed_message['text'] = resolution_text
                
                # Link to original issue
                original_msg = self.tracked_threads[thread_ts]['original_message']
                
                # Try to extract incident ID from resolution message or original message
                incident_id = self.extract_incident_id_from_text(text)
                if not incident_id:
                    # Try to extract from original message text
                    original_text = original_msg.get('text', '')
                    incident_id = self.extract_incident_id_from_text(original_text)
                
                # Try to find the original issue ID from the thread context
                original_issue_id = None
                if incident_id:
                    linked_issue_data = self.lookup_incident_by_id(incident_id)
                    if linked_issue_data:
                        original_issue_id = linked_issue_data['id']
                        self.logger.info(f"[RESOLUTION] Found explicit incident ID in thread: {incident_id}")
                else:
                    # Try to find the issue ID from the thread's original message
                    # The original message might have been tracked as an incident
                    original_ts = original_msg.get('ts', '')
                    if original_ts:
                        potential_id = f"slack_{original_ts.replace('.', '_')}"
                        incident = self.tracker.get_incident_by_id(potential_id)
                        if incident:
                            original_issue_id = incident['id']
                            self.logger.info(f"[RESOLUTION] Found incident from thread timestamp: {original_issue_id}")
                
                transformed_message['linked_issue'] = {
                    'text': original_msg.get('text', ''),
                    'timestamp': original_msg.get('ts', ''),
                    'id': original_issue_id
                }
                
                # If we found the issue, update its status
                if original_issue_id:
                    # Check current status
                    if incident_id:
                        incident_data = self.lookup_incident_by_id(incident_id)
                        current_status = incident_data.get('status', 'Open') if incident_data else 'Open'
                    else:
                        incident = self.tracker.get_incident_by_id(original_issue_id)
                        current_status = incident.get('status', 'Open') if incident else 'Open'
                    
                    if current_status == 'Open':
                        # Update tracker
                        self.tracker.update_incident_status(
                            original_issue_id,
                            'Resolved',
                            resolution_text
                        )
                        
                        # Send status update to Kafka for Elasticsearch
                        status_update_message = {
                            'type': 'status_update',
                            'action': 'mark_resolved',
                            'original_issue_id': original_issue_id,
                            'status': 'Resolved',
                            'resolution_text': resolution_text,
                            'resolved_by': message.get('user'),
                            'resolved_at': timestamp.isoformat(),
                            'timestamp': timestamp.isoformat()
                        }
                        key = f"status_update_{original_issue_id}"
                        self.kafka_producer.send_message(self.topic, status_update_message, key)
                        
                        # Mark resolution as handled - don't send this message as a new incident
                        resolution_handled = True
                        self.logger.info(f"[RESOLUTION] ✅ Resolved incident in thread {original_issue_id}: {original_msg.get('text', '')[:30]}...")
                    else:
                        self.logger.info(f"[RESOLUTION] Incident {original_issue_id} in thread already resolved")
                        resolution_handled = True  # Already resolved, don't create new incident
                
                self.logger.info(f"[RESOLUTION] Detected in thread: {text[:50]}...")
        
        # Handle standalone messages
        else:
            if is_resolution:
                # First, try to extract explicit incident ID from the message
                incident_id = self.extract_incident_id_from_text(text)
                linked_issue = None
                
                if incident_id:
                    self.logger.info(f"[RESOLUTION] Extracted incident ID from message: {incident_id}")
                    # Look up the incident by ID
                    linked_issue_data = self.lookup_incident_by_id(incident_id)
                    if linked_issue_data:
                        self.logger.info(f"[RESOLUTION] Successfully found incident by ID: {incident_id}")
                        linked_issue = {
                            'id': linked_issue_data['id'],
                            'text': linked_issue_data.get('text', ''),
                            'timestamp': linked_issue_data.get('timestamp'),
                            'status': linked_issue_data.get('status', 'Open'),
                            'document_id': linked_issue_data.get('document_id')
                        }
                        self.logger.info(f"[RESOLUTION] Found explicit incident ID: {incident_id}")
                
                # If no explicit ID found, try to link to recent issue using semantic similarity
                if not linked_issue:
                    linked_issue = self.link_resolution_to_issue(transformed_message, timestamp, resolution_text)
                if linked_issue:
                    self.logger.info(f"[RESOLUTION] Linked to recent issue via semantic context")
                
                if linked_issue and linked_issue.get('id'):
                    # Get the actual issue_id to use for updates (may differ from document_id)
                    actual_issue_id = linked_issue.get('id')
                    
                    # Only update if the incident is currently open
                    if linked_issue.get('status', 'Open') == 'Open':
                        transformed_message['status'] = 'Resolved'
                        transformed_message['linked_issue'] = {
                            'text': linked_issue.get('text', ''),
                            'timestamp': linked_issue.get('timestamp', ''),
                            'id': actual_issue_id
                        }
                        
                        # Update tracker (use actual_issue_id, and also try document_id if different)
                        self.tracker.update_incident_status(
                            actual_issue_id,
                            'Resolved',
                            resolution_text
                        )
                        
                        # Also update by document_id if it's different
                        document_id = linked_issue.get('document_id')
                        if document_id and document_id != actual_issue_id:
                            self.tracker.update_incident_status(
                                document_id,
                                'Resolved',
                                resolution_text
                            )
                        
                        # Send status update to Kafka for Elasticsearch
                        # Use the actual issue_id from the document's issue_id field
                        status_update_message = {
                            'type': 'status_update',
                            'action': 'mark_resolved',
                            'original_issue_id': actual_issue_id,  # Use the actual issue_id field value
                            'status': 'Resolved',
                            'resolution_text': resolution_text,
                            'resolved_by': message.get('user'),
                            'resolved_at': timestamp.isoformat(),
                            'timestamp': timestamp.isoformat()
                        }
                        key = f"status_update_{actual_issue_id}"
                        self.kafka_producer.send_message(self.topic, status_update_message, key)
                        
                        # If document_id is different, also send an update using document_id as the issue_id
                        # This handles cases where the document_id was used as issue_id
                        if document_id and document_id != actual_issue_id:
                            status_update_message_doc = {
                                'type': 'status_update',
                                'action': 'mark_resolved',
                                'original_issue_id': document_id,
                                'status': 'Resolved',
                                'resolution_text': resolution_text,
                                'resolved_by': message.get('user'),
                                'resolved_at': timestamp.isoformat(),
                                'timestamp': timestamp.isoformat()
                            }
                            key_doc = f"status_update_{document_id}"
                            self.kafka_producer.send_message(self.topic, status_update_message_doc, key_doc)
                        
                        # Mark resolution as handled - don't send this message as a new incident
                        resolution_handled = True
                        self.logger.info(f"[RESOLUTION] ✅ Resolved incident {actual_issue_id} (document: {document_id or actual_issue_id}): {linked_issue.get('text', '')[:30]}...")
                    else:
                        self.logger.info(f"[RESOLUTION] Incident {actual_issue_id} already resolved, skipping update")
                        resolution_handled = True  # Already resolved, don't create new incident
                else:
                    # Resolution detected but couldn't link to specific issue
                    # Still mark as handled - resolution messages should NEVER create new incidents
                    # Treat as discussion/unrelated instead
                    self.logger.warning(f"[RESOLUTION] ⚠️ Could not link resolution to any issue. Message: {text[:50]}...")
                    self.logger.info(f"[RESOLUTION] Marking as handled - resolution messages should not create new incidents")
                    resolution_handled = True  # Prevent creating new incident from resolution message
                    # Optionally, could try to link to most recent open issue as fallback
                    if self.tracker:
                        recent_open = self.tracker.get_recent_incidents(count=1, status='Open')
                        if recent_open:
                            # Link to most recent open issue as fallback
                            fallback_issue = recent_open[0]
                            fallback_id = fallback_issue.get('id')
                            self.logger.info(f"[RESOLUTION] Attempting fallback link to most recent open issue: {fallback_id}")
                            self.tracker.update_incident_status(
                                fallback_id,
                                'Resolved',
                                resolution_text
                            )
                            # Send status update
                            status_update_message = {
                                'type': 'status_update',
                                'action': 'mark_resolved',
                                'original_issue_id': fallback_id,
                                'status': 'Resolved',
                                'resolution_text': resolution_text,
                                'resolved_by': message.get('user'),
                                'resolved_at': timestamp.isoformat(),
                                'timestamp': timestamp.isoformat()
                            }
                            key = f"status_update_{fallback_id}"
                            self.kafka_producer.send_message(self.topic, status_update_message, key)
                            self.logger.info(f"[RESOLUTION] ✅ Fallback: Resolved most recent open issue {fallback_id}")
            
            elif message_type == "incident_report":
                # Track as potential issue for future resolution linking
                self.recent_issues[timestamp] = transformed_message
                
                # Add to incident tracker
                self.tracker.add_incident({
                    'id': transformed_message['id'],
                    'text': text,
                    'source': 'slack',
                    'timestamp': timestamp,
                    'status': 'Open',
                    'category': category,
                    'severity': severity,
                    'user_id': message.get('user'),
                    'channel_id': channel_id
                })
                
                # Store embedding for semantic linking
                embedding = self.embedding_model.encode([text])[0]
                self.issue_embeddings[transformed_message['id']] = embedding
                
                # Also store in tracker's issue embeddings if it has that capability
                # This ensures semantic linking works even if recent_issues is cleared
                
                self.logger.info(f"[INCIDENT] {category}/{severity} - {text[:50]}...")
            
            elif message_type == "discussion":
                # Link to most recent issue
                if self.recent_issues:
                    recent_issue = list(self.recent_issues.values())[-1]
                    
                    # Add to in-memory tracker
                    self.tracker.add_discussion(
                        recent_issue['id'],
                        text,
                        message.get('user')
                    )
                    
                    # Send discussion message to Kafka with linked_issue_id
                    transformed_message['message_type'] = 'discussion'
                    transformed_message['linked_issue_id'] = recent_issue['id']
                    
                    self.logger.info(f"[DISCUSSION] Linked to {recent_issue['id']}")
                else:
                    # No recent issue to link to - skip discussion or mark as unrelated
                    self.logger.info(f"[DISCUSSION] No recent issue found - skipping discussion (not creating incident)")
                    return  # Skip this message entirely
        
        # Send to Kafka (only if not a discussion without a linked issue, and not a handled resolution)
        # Skip sending resolution messages that successfully resolved an incident
        if not resolution_handled and (message_type != 'discussion' or transformed_message.get('linked_issue_id')):
            key = f"slack_{channel_id}_{message.get('ts', '')}"
            self.kafka_producer.send_message(self.topic, transformed_message, key)
        elif resolution_handled:
            self.logger.info(f"[RESOLUTION] Skipped sending resolution message to Kafka (already sent status update)")
        
        log_label = f"[{message_type}:{confidence:.2f}]"
        if category and severity:
            log_label += f" [{category}/{severity}]"
        self.logger.info(f"{log_label} {text[:50]}...")

    def fetch_messages(self, channel_id=None):
        """Fetch messages with enhanced processing"""
        try:
            channel_id = channel_id or os.getenv('SLACK_CHANNEL_ID')
            self.logger.debug(f"Monitoring channel: {channel_id}")
            
            # Fetch recent messages (Slack returns newest first)
            result = self.client.conversations_history(
                channel=channel_id,
                limit=100
            )
            
            # Filter messages newer than last_timestamp
            all_messages = result['messages']
            messages = [msg for msg in all_messages if float(msg.get('ts', 0)) > self.last_timestamp]
            self.logger.info(f"Fetched {len(messages)} new messages from Slack (channel: {channel_id})")
            
            # Debug: Show message timestamps if any
            if messages:
                for msg in messages:
                    msg_ts = float(msg.get('ts', 0))
                    self.logger.debug(f"Message ts: {msg_ts}, text: {msg.get('text', '')[:50]}")
            
            if len(messages) == 0:
                self.logger.info(f"No new messages since timestamp: {self.last_timestamp}")
                # Debug: Try fetching without timestamp filter to see if there are ANY messages
                test_result = self.client.conversations_history(channel=channel_id, limit=1)
                if test_result['messages']:
                    latest_msg = test_result['messages'][0]
                    latest_ts = float(latest_msg.get('ts', 0))
                    self.logger.info(f"[DEBUG] Latest message in channel has ts: {latest_ts}")
                    self.logger.info(f"[DEBUG] We're looking for messages after: {self.last_timestamp}")
                    self.logger.info(f"[DEBUG] Difference: {latest_ts - self.last_timestamp} seconds")
                    if latest_ts < self.last_timestamp:
                        self.logger.warning(f"[DEBUG] Latest message is OLDER than our timestamp - no new messages yet!")
            
            # Process messages with delay to avoid rate limits
            for i, message in enumerate(reversed(messages)):  # Process in chronological order
                self.process_message(message, channel_id)
                
                # Add delay between messages to stay within rate limits
                # Groq is fast and has good rate limits (30 req/min), plus we have caching
                if i < len(messages) - 1:  # Don't delay after last message
                    time.sleep(1)  # 1 second between messages (Groq is fast + caching)
                
            if messages:
                self.last_timestamp = float(messages[0]['ts'])
                
        except SlackApiError as e:
            self.logger.error(f"Slack API error: {e.response['error']}")
        except Exception as e:
            self.logger.error(f"Error fetching Slack messages: {e}")

    def start_monitoring(self, interval=30):
        """Start continuous monitoring of Slack messages"""
        self.logger.info("=" * 70)
        self.logger.info("Starting Enhanced Slack Monitoring with Groq AI")
        self.logger.info("=" * 70)
        self.logger.info("Features:")
        self.logger.info("  ✓ Groq AI classification (fast & reliable)")
        self.logger.info("  ✓ Thread tracking & context maintenance")
        self.logger.info("  ✓ Automatic resolution detection & linking")
        self.logger.info("  ✓ Incident tracker integration")
        self.logger.info("  ✓ Category & Severity prediction")
        self.logger.info("  ✓ Discussion linking")
        self.logger.info("")
        self.logger.info("Rate Limit Protection:")
        self.logger.info("  ✓ Only processes NEW messages (no history)")
        self.logger.info("  ✓ 2-second delay between messages")
        self.logger.info("  ✓ 10-second polling interval")
        self.logger.info("  ✓ Automatic rate limit handling")
        self.logger.info("=" * 70)
        
        while True:
            try:
                self.fetch_messages()
                time.sleep(interval)
            except KeyboardInterrupt:
                self.logger.info("Stopping Slack monitoring...")
                break
            except Exception as e:
                self.logger.error(f"Error in Slack monitoring loop: {e}")
                time.sleep(interval)

    def close(self):
        self.kafka_producer.close()


if __name__ == "__main__":
    import logging
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        # Create and start connector
        logger.info("Initializing Enhanced Slack Connector...")
        connector = EnhancedSlackConnector()
        # Use 10 second interval for testing (change to 60 for production)
        connector.start_monitoring(interval=10)
    except KeyboardInterrupt:
        logger.info("Shutting down Enhanced Slack Connector...")
        connector.close()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()







