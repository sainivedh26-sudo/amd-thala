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
from bedrock_predictor import get_predictor
from aws_attachment_processor import get_attachment_processor
import boto3
from botocore.config import Config

load_dotenv()

class EnhancedSlackConnector:
    """
    Enhanced Slack Connector with:
    1. Thread tracking (link issues with resolutions)
    2. Bedrock AI classification (category + severity prediction)
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
        
        # Initialize Bedrock AI
        region = os.getenv('AWS_REGION', 'us-east-2')
        self.bedrock_client = boto3.client(
            'bedrock-runtime',
            region_name=region,
            config=Config(retries={"max_attempts": 3, "mode": "standard"})
        )
        self.model_id = os.getenv('BEDROCK_LLAMA_MODEL_ID', 'us.meta.llama3-3-70b-instruct-v1:0')  # Use inference profile ID with 'us.' prefix
        self.logger.info(f"Bedrock AI initialized for classification (model: {self.model_id})")
        
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
        
        self.logger.info("Enhanced Slack Connector initialized with Bedrock AI")

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
        Use Bedrock LLM to classify Slack message semantically
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
            
            # System prompt for Bedrock
            system_prompt = "You are an ITSM incident classification expert. Always respond with valid JSON."
            
            # User prompt with full classification rules
            user_prompt = f"""Analyze this Slack message and classify it intelligently using semantic understanding - NO keyword matching.

{context_info}

Message to classify:
"{text}"

Classification Rules (use semantic understanding, not keywords):

1. **incident_report**: ONLY a NEW technical issue, error, outage, or problem being REPORTED FOR THE FIRST TIME
   - Examples: "API is down", "Database timeout", "Users can't login", "We have a problem with authentication", "there seems to be an issue in deployment"
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
   - Examples: "Checking logs", "Found the cause", "Investigating now", "Looking into it", "I'll check now"
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
- Messages like "we have a problem", "seems to be an issue" WITH specific context (deployment, database, auth, etc.) should be classified as **incident_report** if they're reporting a new problem
- Messages that say "I'll check now" or "investigating" without a clear fix/resolution are **discussion**
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
            
            # Use Bedrock Converse API for classification
            resp = self.bedrock_client.converse(
                modelId=self.model_id,
                messages=[
                    {"role": "user", "content": [{"text": user_prompt}]}
                ],
                system=[{"text": system_prompt}],
                inferenceConfig={
                    'maxTokens': 300,
                    'temperature': 0.2,
                    'topP': 0.9
                }
            )
            
            content = resp['output']['message']['content']
            parts = [c.get('text') for c in content if 'text' in c]
            result_text = ("\n".join([p for p in parts if p]) or '').strip()
            
            if not result_text:
                raise ValueError("Bedrock returned empty content")
            
            # Remove markdown code blocks if present
            if "```json" in result_text:
                result_text = result_text.split("```json")[1].split("```")[0].strip()
            elif "```" in result_text:
                result_text = result_text.split("```")[1].split("```")[0].strip()
            
            result = json.loads(result_text)
            
            self.logger.info(f"[BEDROCK] {result['type']} (conf: {result['confidence']:.2f})")
            
            # Cache the result (only for non-threaded messages)
            if not thread_context:
                self._cache_classification(text, result)
            
            return result['type'], result.get('category'), result.get('severity'), result['confidence']
            
        except Exception as e:
            error_str = str(e)
            
            # Handle throttling errors
            if 'ThrottlingException' in error_str or 'rate' in error_str.lower():
                self.logger.warning(f"[RATE LIMIT] Bedrock API throttled - waiting 5 seconds...")
                time.sleep(5)
                # Fallback: treat as discussion for now
                return 'discussion', None, None, 0.3
            
            self.logger.error(f"Error in Bedrock classification: {e}")
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
        patterns = [
            # Match Jira ticket IDs first (PROJECT-NUMBER format, e.g., "KAN-21", "PROJ-123")
            r'\b(?:issue|ticket|ID|id)[\s:]+([A-Z][A-Z0-9]+-\d+)\b',
            r'\b([A-Z][A-Z0-9]+-\d+)\b',  # Standalone Jira ticket IDs
            # Match "issue ID" or "issue cZKBP5oBaOP9qx5rt2vY" (with or without "ID" keyword)
            r'\b(?:issue|ID|id)[\s:]+([A-Za-z0-9][A-Za-z0-9_-]{14,24})\b',
            # Match standalone IDs (15-25 chars, must contain mix of letters/numbers/underscores)
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
        try:
            lookup_url = f"{self.flask_url}/lookup_incident"
            lookup_payload = {"issue_id": incident_id}
            
            response = requests.post(lookup_url, json=lookup_payload, timeout=5)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('found'):
                    actual_issue_id = result.get('issue_id', incident_id)
                    document_id = result.get('document_id', incident_id)
                    self.logger.info(f"[LOOKUP] ✅ Found incident in Elasticsearch")
                    return {
                        'id': actual_issue_id,
                        'document_id': document_id,
                        'text': result.get('text', ''),
                        'status': result.get('status', 'Open'),
                        'source': result.get('source', 'unknown'),
                        'timestamp': result.get('timestamp'),
                        'found_in': 'elasticsearch'
                    }
            
            self.logger.warning(f"[LOOKUP] ❌ Incident {incident_id} not found")
            return None
            
        except Exception as e:
            self.logger.error(f"[LOOKUP] ⚠️ Error querying for incident {incident_id}: {e}")
            return None

    def link_resolution_to_issue(self, resolution_message, timestamp, resolution_text=None):
        """
        Try to link a resolution message to a recent issue using semantic similarity + LLM matching
        """
        cutoff_time = timestamp - timedelta(hours=48)  # Increased from 24h to 48h
        
        # Clean recent_issues of old entries
        self.recent_issues = {
            ts: msg for ts, msg in self.recent_issues.items()
            if ts > cutoff_time
        }
        
        resolution_text = resolution_text or resolution_message.get('text', '')
        
        # Generate embedding for resolution message
        resolution_embedding = self.embedding_model.encode([resolution_text])[0]
        
        # Get recent open incidents from tracker (increased count)
        recent_open = []
        if self.tracker:
            recent_open = self.tracker.get_recent_incidents(count=20, status='Open')  # Increased from 10 to 20
        
        # Combine all potential issues (purely semantic matching)
        all_potential_issues = []
        
        # Add from tracker (with embeddings)
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
        
        # Sort by semantic similarity score and recency
        if all_potential_issues:
            all_potential_issues.sort(key=lambda x: (
                x['score'],  # Higher semantic similarity first
                x['timestamp']  # More recent first
            ), reverse=True)
            
            # First try: Use semantic similarity with lower threshold
            best_match = all_potential_issues[0]
            if best_match['score'] > 0.20:  # Lowered threshold from 0.25 to 0.20
                self.logger.info(f"[LINK] Matched resolution to incident via semantic similarity (score: {best_match['score']:.2f})")
                return best_match['issue']
            
            # Second try: If semantic similarity is low, use LLM to determine if resolution matches
            # Take top 3 candidates and ask LLM which one matches
            top_candidates = all_potential_issues[:3]
            if top_candidates and best_match['score'] > 0.10:  # Only use LLM if there's at least some similarity
                self.logger.info(f"[LINK] Semantic similarity low ({best_match['score']:.2f}), using LLM to match resolution...")
                
                llm_match = self._llm_match_resolution_to_issue(resolution_text, top_candidates)
                if llm_match:
                    self.logger.info(f"[LINK] ✅ LLM matched resolution to incident {llm_match['id']}")
                    return llm_match
                else:
                    self.logger.info(f"[LINK] LLM did not find a match")
            
            # Final fallback: If best match has any similarity (>0.10), use it (prefer linking over not linking)
            if best_match['score'] > 0.10:
                self.logger.info(f"[LINK] Fallback: Using best match with low similarity (score: {best_match['score']:.2f})")
                return best_match['issue']
        
        return None
    
    def _llm_match_resolution_to_issue(self, resolution_text, candidates):
        """
        Use LLM to determine if a resolution message matches any of the candidate issues
        Returns the matched issue if found, None otherwise
        """
        try:
            # Build context with candidate issues
            candidates_text = ""
            for i, candidate in enumerate(candidates, 1):
                issue_text = candidate['issue']['text'][:200]  # Limit text length
                candidates_text += f"{i}. Issue ID: {candidate['issue']['id']}\n   Description: {issue_text}\n\n"
            
            system_prompt = "You are an ITSM expert. Determine if a resolution message matches one of the provided incidents. Respond with ONLY the issue number (1, 2, or 3) if there's a match, or 'NONE' if no match."
            
            user_prompt = f"""A resolution message was posted: "{resolution_text}"

Here are the recent open incidents:
{candidates_text}

Does this resolution message refer to any of these incidents? Consider:
- Technical keywords (deployment, policy, VPC, database, etc.)
- Problem descriptions that match
- Solutions mentioned (rolled back, fixed, etc.)

Respond with ONLY the number (1, 2, or 3) of the matching incident, or 'NONE' if it doesn't match any."""
            
            resp = self.bedrock_client.converse(
                modelId=self.model_id,
                system=[{"text": system_prompt}],
                messages=[
                    {"role": "user", "content": [{"text": user_prompt}]}
                ],
                inferenceConfig={
                    'maxTokens': 10,  # Very short response - just a number
                    'temperature': 0.1,  # Low temperature for deterministic matching
                    'topP': 0.9
                }
            )
            
            content = resp['output']['message']['content']
            parts = [c.get('text') for c in content if 'text' in c]
            result_text = ("\n".join([p for p in parts if p]) or '').strip()
            
            # Parse result
            result_text = result_text.lower().strip()
            if result_text.startswith('none') or 'none' in result_text:
                return None
            
            # Try to extract number
            numbers = re.findall(r'\d+', result_text)
            if numbers:
                match_index = int(numbers[0]) - 1  # Convert to 0-based index
                if 0 <= match_index < len(candidates):
                    return candidates[match_index]['issue']
            
            return None
            
        except Exception as e:
            self.logger.error(f"[LINK] Error in LLM matching: {e}")
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
                
                # Get file URL
                file_url_private = file_info.get('url_private')
                if not file_url_private:
                    self.logger.warning(f"[ATTACHMENT] No URL found for file {filename}")
                    continue
                
                self.logger.info(f"[ATTACHMENT] Processing {filename} (type: {file_type})")
                
                # Download file from Slack using files_info + url_private_download method
                try:
                    file_content = None
                    
                    # Get file info using files_info API
                    try:
                        file_info_response = self.client.files_info(file=file_id)
                        if file_info_response and 'file' in file_info_response:
                            file_info_data = file_info_response['file']
                            download_url = file_info_data.get('url_private_download') or file_info_data.get('url_private') or file_url_private
                        else:
                            download_url = file_url_private
                    except SlackApiError as sdk_error:
                        if 'missing_scope' in str(sdk_error) or 'files:read' in str(sdk_error):
                            self.logger.error(f"[ATTACHMENT] ❌ Missing Slack scope 'files:read'")
                            continue
                        download_url = file_url_private
                    
                    # Download the file using Bearer authentication
                    headers = {
                        'Authorization': f'Bearer {os.getenv("SLACK_BOT_TOKEN")}'
                    }
                    response = requests.get(download_url, headers=headers, timeout=30)
                    response.raise_for_status()
                    file_content = response.content
                    
                    if not file_content or len(file_content) == 0:
                        self.logger.warning(f"[ATTACHMENT] Downloaded file is empty for {filename}")
                        continue
                    
                    # Save to temporary file
                    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1])
                    temp_file.write(file_content)
                    temp_file.close()
                    local_path = temp_file.name
                    
                    # Process attachment
                    result = self.attachment_processor.process_attachment(
                        file_url_or_path=local_path,
                        source='slack',
                        file_id=f"{file_info.get('id', message.get('ts', ''))}",
                        filename=filename,
                        download=False
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
                    
                    if extracted_text:
                        extracted_texts.append(f"[Attachment: {filename}]\n{extracted_text}")
                        self.logger.info(f"[ATTACHMENT] ✅ Extracted {len(extracted_text)} chars from {filename}")
                    
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
        """Enhanced message processing with Bedrock classification and thread tracking"""
        # Skip bot messages
        if message.get('subtype') == 'bot_message' or message.get('bot_id'):
            return
        
        text = message.get('text', '')
        ts = float(message.get('ts', 0))
        timestamp = datetime.fromtimestamp(ts) if ts else datetime.utcnow()
        thread_ts = message.get('thread_ts')
        
        # Process attachments if any
        attachment_text = None
        attachment_s3_urls = []
        attachment_extraction_failed = False
        original_text = text
        
        if message.get('files') or message.get('attachments'):
            attachment_result = self.process_slack_attachments(message, channel_id)
            if attachment_result:
                attachment_text = attachment_result.get('extracted_text')
                attachment_s3_urls = attachment_result.get('s3_urls', [])
                
                if attachment_result.get('success') and not attachment_text:
                    attachment_extraction_failed = True
                
                if attachment_text:
                    text = f"{text}\n\n{attachment_text}" if text else attachment_text
        
        # Get thread context if available
        thread_context = None
        if thread_ts and thread_ts in self.tracked_threads:
            thread_context = self.tracked_threads[thread_ts].get('replies', [])
        
        # Classify message with Bedrock
        message_type, category, severity, confidence = self.classify_message(text, thread_context)
        
        # STRICT RULE: If attachment extraction failed, require clear incident text OR high confidence
        if attachment_extraction_failed and message_type == 'incident_report':
            vague_indicators = [
                'seems like', 'might be', 'could be', 'maybe', 'possibly', 'not sure', 
                'i think', 'looks like', 'we have a problem', 'there\'s a problem',
                'there is a problem', 'something is wrong', 'something wrong',
                'can someone check', 'can someone look', 'please check', 'please look',
                'is there an issue', 'anyone else seeing', 'is it just me'
            ]
            original_lower = original_text.lower()
            is_vague = any(indicator in original_lower for indicator in vague_indicators)
            
            # Check if text has actual technical details
            technical_indicators = [
                'error', 'down', 'timeout', 'crash', 'failed', 'exception', 'broken',
                'not working', 'not responding', 'unavailable', 'outage', 'slow',
                '500', '502', '503', '504', 'database', 'api', 'service', 'server',
                'deployment', 'deploy', 'auth', 'authentication', 'vpc', 'network', 'policy'
            ]
            has_technical_details = any(indicator in original_lower for indicator in technical_indicators)
            
            # If vague AND no technical details = skip
            if is_vague and not has_technical_details:
                self.logger.info(f"[FILTER] ❌ Skipping: vague message without technical details")
                return
            
            min_confidence = 0.85 if is_vague else 0.80
            
            if confidence < min_confidence:
                self.logger.info(f"[FILTER] ❌ Skipping: confidence {confidence:.2f} < {min_confidence:.2f}")
                return
        
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
                'attachment_s3_urls': attachment_s3_urls
            }
        }
        
        # Track if resolution was successfully handled
        resolution_handled = False
        
        # Handle threaded messages
        if thread_ts:
            if thread_ts not in self.tracked_threads:
                thread_messages = self.fetch_thread_replies(channel_id, thread_ts)
                self.tracked_threads[thread_ts] = {
                    'original_message': thread_messages[0] if thread_messages else message,
                    'replies': thread_messages[1:] if len(thread_messages) > 1 else [],
                    'status': 'Open',
                    'channel_id': channel_id
                }
            
            self.tracked_threads[thread_ts]['replies'].append(message)
            
            if is_resolution:
                self.tracked_threads[thread_ts]['status'] = 'Resolved'
                transformed_message['status'] = 'Resolved'
                transformed_message['text'] = resolution_text
                
                original_msg = self.tracked_threads[thread_ts]['original_message']
                incident_id = self.extract_incident_id_from_text(text)
                if not incident_id:
                    original_text = original_msg.get('text', '')
                    incident_id = self.extract_incident_id_from_text(original_text)
                
                original_issue_id = None
                if incident_id:
                    linked_issue_data = self.lookup_incident_by_id(incident_id)
                    if linked_issue_data:
                        original_issue_id = linked_issue_data['id']
                else:
                    original_ts = original_msg.get('ts', '')
                    if original_ts:
                        potential_id = f"slack_{original_ts.replace('.', '_')}"
                        incident = self.tracker.get_incident_by_id(potential_id)
                        if incident:
                            original_issue_id = incident['id']
                
                transformed_message['linked_issue'] = {
                    'text': original_msg.get('text', ''),
                    'timestamp': original_msg.get('ts', ''),
                    'id': original_issue_id
                }
                
                if original_issue_id:
                    incident = self.tracker.get_incident_by_id(original_issue_id)
                    current_status = incident.get('status', 'Open') if incident else 'Open'
                    
                    if current_status == 'Open':
                        self.tracker.update_incident_status(
                            original_issue_id,
                            'Resolved',
                            resolution_text
                        )
                        
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
                        
                        resolution_handled = True
                        self.logger.info(f"[RESOLUTION] ✅ Resolved incident {original_issue_id}")
                    else:
                        resolution_handled = True
        
        # Handle standalone messages
        else:
            if is_resolution:
                incident_id = self.extract_incident_id_from_text(text)
                linked_issue = None
                
                if incident_id:
                    linked_issue_data = self.lookup_incident_by_id(incident_id)
                    if linked_issue_data:
                        linked_issue = {
                            'id': linked_issue_data['id'],
                            'text': linked_issue_data.get('text', ''),
                            'timestamp': linked_issue_data.get('timestamp'),
                            'status': linked_issue_data.get('status', 'Open'),
                            'document_id': linked_issue_data.get('document_id')
                        }
                
                if not linked_issue:
                    linked_issue = self.link_resolution_to_issue(transformed_message, timestamp, resolution_text)
                
                if linked_issue and linked_issue.get('id'):
                    actual_issue_id = linked_issue.get('id')
                    
                    if linked_issue.get('status', 'Open') == 'Open':
                        transformed_message['status'] = 'Resolved'
                        transformed_message['linked_issue'] = {
                            'text': linked_issue.get('text', ''),
                            'timestamp': linked_issue.get('timestamp', ''),
                            'id': actual_issue_id
                        }
                        
                        self.tracker.update_incident_status(
                            actual_issue_id,
                            'Resolved',
                            resolution_text
                        )
                        
                        status_update_message = {
                            'type': 'status_update',
                            'action': 'mark_resolved',
                            'original_issue_id': actual_issue_id,
                            'status': 'Resolved',
                            'resolution_text': resolution_text,
                            'resolved_by': message.get('user'),
                            'resolved_at': timestamp.isoformat(),
                            'timestamp': timestamp.isoformat()
                        }
                        key = f"status_update_{actual_issue_id}"
                        self.kafka_producer.send_message(self.topic, status_update_message, key)
                        
                        resolution_handled = True
                        self.logger.info(f"[RESOLUTION] ✅ Resolved incident {actual_issue_id}")
                    else:
                        resolution_handled = True
                else:
                    # Fallback: Try to link to most recent open issue
                    fallback_id = None
                    
                    # First try tracker
                    if self.tracker:
                        recent_open_fallback = self.tracker.get_recent_incidents(count=1, status='Open')
                        if recent_open_fallback:
                            fallback_issue = recent_open_fallback[0]
                            fallback_id = fallback_issue.get('id')
                    
                    # If tracker is empty, query AOSS directly via Flask API
                    if not fallback_id:
                        self.logger.info(f"[RESOLUTION] Tracker empty, querying AOSS for open issues...")
                        try:
                            # Query Flask search endpoint to get recent open issues
                            search_url = f"{self.flask_url}/search"
                            # Extract technical keywords from resolution for better matching
                            technical_keywords = resolution_text.lower()
                            # Remove common resolution words to get the actual issue keywords
                            resolution_words = ['fixed', 'resolved', 'working', 'done', 'rolled back', 'restarted', 'deployed', 'patched', 'have fixed', 'issue', 'this']
                            for word in resolution_words:
                                technical_keywords = technical_keywords.replace(word, ' ')
                            
                            # Use keywords from resolution to search (e.g., "deployment", "policy", "vpc", "database")
                            search_query = technical_keywords.strip()[:100] if technical_keywords.strip() else "deployment"
                            
                            search_payload = {
                                "query": search_query,
                                "top_k": 10  # Get top 10 to find open ones
                            }
                            
                            response = requests.post(search_url, json=search_payload, timeout=5)
                            if response.status_code == 200:
                                result = response.json()
                                open_issues = [
                                    inc for inc in result.get('results', [])
                                    if inc.get('status') == 'Open'
                                ]
                                if open_issues:
                                    # Use the first open issue (most relevant from search)
                                    fallback_issue = open_issues[0]
                                    fallback_id = fallback_issue.get('issue_id') or fallback_issue.get('id')
                                    self.logger.info(f"[RESOLUTION] Found {len(open_issues)} open issues from AOSS, using: {fallback_id}")
                        except Exception as e:
                            self.logger.warning(f"[RESOLUTION] Error querying AOSS for fallback: {e}")
                    
                    # If we found a fallback issue, update it
                    if fallback_id:
                        self.logger.warning(f"[RESOLUTION] ⚠️ Could not link resolution via semantic matching, using fallback to issue: {fallback_id}")
                        
                        # Update tracker if available
                        if self.tracker:
                            self.tracker.update_incident_status(
                                fallback_id,
                                'Resolved',
                                resolution_text
                            )
                        
                        # Send status update to Kafka/Redis
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
                        
                        resolution_handled = True
                        self.logger.info(f"[RESOLUTION] ✅ Fallback: Resolved issue {fallback_id}")
                    else:
                        self.logger.warning(f"[RESOLUTION] ⚠️ Could not link resolution and no open issues found in tracker or AOSS")
                        resolution_handled = True
            
            elif message_type == "incident_report":
                # Track as potential issue
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
                
                self.logger.info(f"[INCIDENT] {category}/{severity} - {text[:50]}...")
            
            elif message_type == "discussion":
                # Link to most recent issue
                if self.recent_issues:
                    recent_issue = list(self.recent_issues.values())[-1]
                    
                    self.tracker.add_discussion(
                        recent_issue['id'],
                        text,
                        message.get('user')
                    )
                    
                    transformed_message['message_type'] = 'discussion'
                    transformed_message['linked_issue_id'] = recent_issue['id']
                    
                    self.logger.info(f"[DISCUSSION] Linked to {recent_issue['id']}")
                else:
                    self.logger.info(f"[DISCUSSION] No recent issue found - skipping")
                    return
        
        # Send to Kafka (skip handled resolutions)
        if not resolution_handled and (message_type != 'discussion' or transformed_message.get('linked_issue_id')):
            key = f"slack_{channel_id}_{message.get('ts', '')}"
            self.kafka_producer.send_message(self.topic, transformed_message, key)
        
        log_label = f"[{message_type}:{confidence:.2f}]"
        if category and severity:
            log_label += f" [{category}/{severity}]"
        self.logger.info(f"{log_label} {text[:50]}...")

    def fetch_messages(self, channel_id=None):
        """Fetch messages with enhanced processing"""
        try:
            channel_id = channel_id or os.getenv('SLACK_CHANNEL_ID')
            
            # Fetch recent messages
            result = self.client.conversations_history(
                channel=channel_id,
                limit=100
            )
            
            # Filter messages newer than last_timestamp
            all_messages = result['messages']
            messages = [msg for msg in all_messages if float(msg.get('ts', 0)) > self.last_timestamp]
            self.logger.info(f"Fetched {len(messages)} new messages from Slack")
            
            # Process messages
            for i, message in enumerate(reversed(messages)):
                self.process_message(message, channel_id)
                
                if i < len(messages) - 1:
                    time.sleep(1)
                
            if messages:
                self.last_timestamp = float(messages[0]['ts'])
                
        except SlackApiError as e:
            self.logger.error(f"Slack API error: {e.response['error']}")
        except Exception as e:
            self.logger.error(f"Error fetching Slack messages: {e}")

    def start_monitoring(self, interval=30):
        """Start continuous monitoring of Slack messages"""
        self.logger.info("=" * 70)
        self.logger.info("Starting Enhanced Slack Monitoring with Bedrock AI")
        self.logger.info("=" * 70)
        self.logger.info("Features:")
        self.logger.info("  ✓ Bedrock AI classification (Llama 3.3 70B)")
        self.logger.info("  ✓ Thread tracking & context maintenance")
        self.logger.info("  ✓ Automatic resolution detection & linking")
        self.logger.info("  ✓ Incident tracker integration")
        self.logger.info("  ✓ Category & Severity prediction")
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
        logger.info("Initializing Enhanced Slack Connector...")
        connector = EnhancedSlackConnector()
        connector.start_monitoring(interval=10)
    except KeyboardInterrupt:
        logger.info("Shutting down Enhanced Slack Connector...")
        connector.close()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
