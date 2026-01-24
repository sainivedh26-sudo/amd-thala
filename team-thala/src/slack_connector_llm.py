import os
import json
import logging
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from kafka_producer import KafkaMessageProducer
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta
from sentence_transformers import SentenceTransformer
import numpy as np

load_dotenv()

# Gemini LLM for semantic understanding
try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

class ContextQueue:
    """
    Maintains semantic context of issues across Slack and Jira
    Uses embeddings for semantic matching + conversation flow tracking
    """
    def __init__(self, model):
        self.model = model
        self.issues = []  # List of {text, embedding, source, timestamp, id, status}
        self.max_age_hours = 72  # Track issues for 72 hours
        
        # Conversation context tracking
        self.user_interactions = {}  # {user_id: [(issue_id, timestamp), ...]}
        self.hot_issues = {}  # {issue_id: last_interaction_timestamp}
        
        # Discussion tracking for each issue
        self.issue_discussions = {}  # {issue_id: [{user_id, text, timestamp, type}, ...]}
        
    def add_issue(self, text, source, issue_id, timestamp, metadata=None):
        """Add an issue to the context queue"""
        embedding = self.model.encode([text])[0]
        
        self.issues.append({
            'id': issue_id,
            'text': text,
            'embedding': embedding,
            'source': source,  # 'slack' or 'jira'
            'timestamp': timestamp,
            'status': 'open',
            'metadata': metadata or {}
        })
        
        # Mark as hot issue (recently discussed)
        self.hot_issues[issue_id] = timestamp
        
        # Track user interaction if available
        if metadata and 'user_id' in metadata:
            user_id = metadata['user_id']
            if user_id not in self.user_interactions:
                self.user_interactions[user_id] = []
            self.user_interactions[user_id].append((issue_id, timestamp))
            # Keep only last 10 interactions per user
            self.user_interactions[user_id] = self.user_interactions[user_id][-10:]
        
        # Clean old issues
        self._clean_old_issues()
    
    def update_issue_status(self, issue_id, status):
        """Update status of an issue"""
        for issue in self.issues:
            if issue['id'] == issue_id:
                issue['status'] = status
                return True
        return False
    
    def add_discussion(self, issue_id, user_id, text, timestamp, message_type='discussion'):
        """Add a discussion message to an issue's context"""
        if issue_id not in self.issue_discussions:
            self.issue_discussions[issue_id] = []
        
        self.issue_discussions[issue_id].append({
            'user_id': user_id,
            'text': text,
            'timestamp': timestamp,
            'type': message_type
        })
        
        # Keep only last 20 discussion messages per issue
        self.issue_discussions[issue_id] = self.issue_discussions[issue_id][-20:]
    
    def get_resolution_context(self, issue_id, resolver_user_id, resolution_time):
        """
        Get discussion context for resolution text
        Extracts relevant discussion messages (especially technical details)
        """
        if issue_id not in self.issue_discussions:
            return None
        
        discussions = self.issue_discussions[issue_id]
        
        # Get recent discussions (last 30 minutes before resolution)
        cutoff_time = resolution_time - timedelta(minutes=30)
        recent_discussions = [
            d for d in discussions 
            if d['timestamp'] > cutoff_time
        ]
        
        if not recent_discussions:
            return None
        
        # Prioritize discussions from the resolver
        resolver_discussions = [
            d for d in recent_discussions 
            if d['user_id'] == resolver_user_id
        ]
        
        # Build context string from recent technical discussions
        context_parts = []
        
        # Use resolver's messages if available, otherwise use all recent
        relevant_discussions = resolver_discussions if resolver_discussions else recent_discussions[-3:]
        
        for disc in relevant_discussions[-3:]:  # Last 3 relevant messages
            # Filter out very short messages (likely just acknowledgments)
            if len(disc['text']) > 15:
                context_parts.append(disc['text'])
        
        return " | ".join(context_parts) if context_parts else None
    
    def find_related_issue(self, text, threshold=0.6, sources=None, user_id=None, current_time=None):
        """
        Find related issue using conversation context + semantic similarity
        Args:
            text: Text to match against
            threshold: Similarity threshold (0-1)
            sources: List of sources to search ('slack', 'jira'), or None for all
            user_id: User posting the message (for conversation context)
            current_time: Current timestamp for recency calculation
        Returns:
            Most similar issue or None
        """
        if not self.issues:
            return None
        
        query_embedding = self.model.encode([text])[0]
        current_time = current_time or datetime.now()
        
        best_match = None
        best_score = threshold
        
        for issue in self.issues:
            # Skip closed issues
            if issue['status'] != 'open':
                continue
            
            # Filter by source if specified
            if sources and issue['source'] not in sources:
                continue
            
            # Calculate semantic similarity
            similarity = np.dot(query_embedding, issue['embedding']) / (
                np.linalg.norm(query_embedding) * np.linalg.norm(issue['embedding'])
            )
            
            # Calculate conversation context score
            context_score = 0.0
            
            # 1. Recency boost: Issues discussed recently get higher priority
            if issue['id'] in self.hot_issues:
                time_diff = (current_time - self.hot_issues[issue['id']]).total_seconds()
                if time_diff < 600:  # Last 10 minutes
                    recency_boost = 0.3 * (1 - time_diff / 600)  # 0.3 to 0.0
                    context_score += recency_boost
                elif time_diff < 1800:  # Last 30 minutes
                    recency_boost = 0.2 * (1 - (time_diff - 600) / 1200)
                    context_score += recency_boost
            
            # 2. User interaction boost: Issues this user discussed
            if user_id and user_id in self.user_interactions:
                user_issues = [iid for iid, _ in self.user_interactions[user_id]]
                if issue['id'] in user_issues:
                    # Find when user last interacted with this issue
                    for iid, ts in reversed(self.user_interactions[user_id]):
                        if iid == issue['id']:
                            time_diff = (current_time - ts).total_seconds()
                            if time_diff < 300:  # Last 5 minutes
                                context_score += 0.4  # Strong boost
                            elif time_diff < 900:  # Last 15 minutes
                                context_score += 0.25
                            break
            
            # Combined score: semantic similarity (60%) + conversation context (40%)
            combined_score = (similarity * 0.6) + (context_score * 0.4) + similarity
            
            if combined_score > best_score:
                best_score = combined_score
                best_match = {
                    **issue, 
                    'similarity': similarity,
                    'context_score': context_score,
                    'combined_score': combined_score
                }
        
        return best_match
    
    def get_open_issues(self, source=None):
        """Get all open issues, optionally filtered by source"""
        issues = [i for i in self.issues if i['status'] == 'open']
        if source:
            issues = [i for i in issues if i['source'] == source]
        return issues
    
    def _clean_old_issues(self):
        """Remove issues older than max_age_hours"""
        cutoff = datetime.now() - timedelta(hours=self.max_age_hours)
        self.issues = [
            issue for issue in self.issues 
            if issue['timestamp'] > cutoff
        ]


class LLMSlackConnector:
    """
    LLM-powered Slack connector with semantic understanding
    - Uses Gemini for classification (no keywords)
    - Maintains context queue synced with Jira
    - Semantically matches resolutions to issues
    """
    
    def __init__(self):
        self.client = WebClient(token=os.getenv('SLACK_BOT_TOKEN'))
        self.kafka_producer = KafkaMessageProducer()
        self.topic = os.getenv('KAFKA_TOPIC_SLACK', 'thala-slack-events')
        self.logger = logging.getLogger(__name__)
        self.last_timestamp = 0
        
        # Initialize semantic model for embeddings
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Context queue for tracking issues across Slack and Jira
        self.context_queue = ContextQueue(self.embedding_model)
        
        # Initialize Gemini LLM
        self.use_gemini = os.getenv('USE_GEMINI_LABELING', 'true').lower() == 'true'
        self.gemini_client = None
        
        if self.use_gemini and GEMINI_AVAILABLE:
            gemini_api_key = os.getenv('GEMINI_API_KEY')
            if gemini_api_key:
                try:
                    self.gemini_client = genai.Client(api_key=gemini_api_key)
                    self.logger.info("Gemini LLM integration enabled for Slack semantic understanding")
                except Exception as e:
                    self.logger.error(f"Failed to initialize Gemini: {e}")
                    self.use_gemini = False
            else:
                self.logger.error("GEMINI_API_KEY not found. LLM-based Slack connector requires Gemini!")
                raise ValueError("GEMINI_API_KEY required for LLM Slack connector")
        else:
            self.logger.error("Gemini not available. Install with: pip install google-genai")
            raise ImportError("Gemini required for LLM Slack connector")
    
    def sync_jira_issue(self, issue_data):
        """
        Sync Jira issue to context queue
        Called by main system when Jira issues are created/updated
        """
        text = f"{issue_data.get('summary', '')} {issue_data.get('description', '')}"
        self.context_queue.add_issue(
            text=text,
            source='jira',
            issue_id=issue_data.get('id'),
            timestamp=datetime.now(),
            metadata={
                'status': issue_data.get('status'),
                'priority': issue_data.get('priority'),
                'reporter': issue_data.get('reporter')
            }
        )
        self.logger.info(f"Synced Jira issue to context: {issue_data.get('id')}")
    
    def sync_jira_resolution(self, issue_id):
        """Mark Jira issue as resolved in context queue"""
        self.context_queue.update_issue_status(issue_id, 'resolved')
        self.logger.info(f"Marked Jira issue {issue_id} as resolved in context")
    
    def classify_with_gemini(self, text, thread_context=None):
        """
        Use Gemini to classify Slack message semantically
        Returns: (message_type, details, confidence)
        message_type: 'incident_report', 'resolution', 'discussion', 'unrelated'
        """
        try:
            # Build context-aware prompt
            context_info = ""
            open_issues = self.context_queue.get_open_issues()
            if open_issues:
                recent_issues = open_issues[-5:]  # Last 5 open issues
                context_info = "\n\nRecent open issues in context:\n"
                for i, issue in enumerate(recent_issues, 1):
                    context_info += f"{i}. [{issue['source'].upper()}] {issue['text'][:100]}...\n"
            
            thread_info = ""
            if thread_context:
                thread_info = f"\n\nThis message is part of a thread. Previous messages:\n{thread_context}\n"
            
            prompt = f"""You are an ITSM assistant analyzing Slack messages for incident management.

Analyze this Slack message and classify it into ONE of these categories:

1. **incident_report**: User is reporting a new problem/issue/outage/error
   - Examples: "Server is down", "Getting 500 errors", "App is slow", "Can't login"

2. **resolution**: User is indicating an issue has been fixed/resolved
   - Examples: "Fixed it", "It's working now", "Issue resolved", "Back online"
   - IMPORTANT: Even vague statements like "all good now" or "sorted" count as resolutions

3. **discussion**: Technical discussion about an ongoing issue (troubleshooting, investigating)
   - Examples: "Let me check the logs", "Restarting the service", "I see the problem"

4. **unrelated**: Not related to incidents (questions, meetings, general chat, feature requests)
   - Examples: "How do I deploy?", "Meeting at 3pm", "Feature idea", "Thanks!"
{context_info}{thread_info}
Message to analyze:
"{text}"

Respond in JSON format:
{{
  "type": "incident_report|resolution|discussion|unrelated",
  "confidence": 0.0-1.0,
  "reasoning": "brief explanation",
  "refers_to_existing_issue": true/false,
  "related_issue_reference": "brief description if refers to existing issue"
}}

Be especially careful with resolutions - they often reference issues abstractly without repeating the full problem.
"""
            
            contents = [
                types.Content(
                    role="user",
                    parts=[types.Part.from_text(text=prompt)]
                )
            ]
            
            response = self.gemini_client.models.generate_content(
                model="gemini-2.0-flash-exp",
                contents=contents
            )
            
            result_text = response.text.strip()
            
            # Parse JSON response
            # Remove markdown code blocks if present
            if "```json" in result_text:
                result_text = result_text.split("```json")[1].split("```")[0].strip()
            elif "```" in result_text:
                result_text = result_text.split("```")[1].split("```")[0].strip()
            
            result = json.loads(result_text)
            
            self.logger.info(f"Gemini classification: {result['type']} (conf: {result['confidence']:.2f})")
            
            return result['type'], result, result['confidence']
            
        except Exception as e:
            self.logger.error(f"Error in Gemini classification: {e}")
            # Fallback: treat as discussion
            return 'discussion', {'reasoning': 'Classification error'}, 0.3
    
    def handle_incident_report(self, message, channel_id, classification):
        """Handle a new incident report"""
        text = message.get('text', '')
        ts = float(message.get('ts', 0))
        timestamp = datetime.fromtimestamp(ts) if ts else datetime.now()
        
        # Add to context queue
        issue_id = f"slack_{message.get('ts', '')}"
        self.context_queue.add_issue(
            text=text,
            source='slack',
            issue_id=issue_id,
            timestamp=timestamp,
            metadata={
                'channel_id': channel_id,
                'user_id': message.get('user', 'unknown'),
                'thread_ts': message.get('thread_ts')
            }
        )
        
        # Create message for Kafka
        transformed_message = {
            'id': issue_id,
            'type': 'slack_message',
            'channel_id': channel_id,
            'user_id': message.get('user', 'unknown'),
            'text': text,
            'thread_ts': message.get('thread_ts'),
            'original_timestamp': message.get('ts'),
            'message_type': 'incident_report',
            'timestamp': timestamp.isoformat(),
            'status': 'Open',
            'incident_likelihood': 'Likely',  # Incident reports are likely incidents
            'confidence': classification.get('confidence', 0.8),
            'classification': classification,
            'metadata': {
                'has_files': 'files' in message,
                'has_attachments': 'attachments' in message,
                'is_thread': 'thread_ts' in message,
                'gemini_reasoning': classification.get('reasoning', '')
            }
        }
        
        # Send to Kafka
        key = f"slack_{channel_id}_{message.get('ts', '')}"
        self.kafka_producer.send_message(self.topic, transformed_message, key)
        
        self.logger.info(f"[INCIDENT REPORT] {text[:60]}... (confidence: {classification.get('confidence', 0):.2f})")
    
    def handle_resolution(self, message, channel_id, classification):
        """Handle a resolution message - link to related issue"""
        text = message.get('text', '')
        ts = float(message.get('ts', 0))
        timestamp = datetime.fromtimestamp(ts) if ts else datetime.now()
        
        # Find related issue using conversation context + semantic similarity
        related_issue = self.context_queue.find_related_issue(
            text=text,
            threshold=0.3,  # Lower threshold since we have conversation context now
            sources=None,  # Search both Slack and Jira
            user_id=message.get('user'),  # Pass user for conversation tracking
            current_time=timestamp
        )
        
        linked_issue_info = None
        if related_issue:
            # Mark issue as resolved in context
            self.context_queue.update_issue_status(related_issue['id'], 'resolved')
            
            # Convert numpy float to Python float for JSON serialization
            similarity_score = float(related_issue['similarity'])
            
            linked_issue_info = {
                'id': related_issue['id'],
                'text': related_issue['text'],
                'source': related_issue['source'],
                'timestamp': related_issue['timestamp'].isoformat(),
                'similarity': similarity_score
            }
            
            # Get discussion context for comprehensive resolution text
            discussion_context = self.context_queue.get_resolution_context(
                related_issue['id'],
                message.get('user', 'unknown'),
                timestamp
            )
            
            # Build comprehensive resolution text
            if discussion_context:
                comprehensive_resolution = f"{discussion_context} | {text}"
            else:
                comprehensive_resolution = text
            
            # Send update to mark original issue as resolved
            self._update_original_issue_status(
                related_issue['id'], 
                comprehensive_resolution,  # Use comprehensive text
                resolved_by=message.get('user', 'unknown')
            )
            
            context_info = f" [context: {related_issue.get('context_score', 0):.2f}]" if 'context_score' in related_issue else ""
            self.logger.info(
                f"[RESOLUTION LINKED] {text[:40]}... → {related_issue['source'].upper()} "
                f"issue: {related_issue['text'][:50]}... (similarity: {similarity_score:.2f}{context_info})"
            )
        else:
            self.logger.warning(f"[RESOLUTION UNLINKED] Could not find related issue for: {text[:50]}...")
        
        # Create resolution message
        transformed_message = {
            'id': f"slack_{message.get('ts', '')}",
            'type': 'slack_message',
            'channel_id': channel_id,
            'user_id': message.get('user', 'unknown'),
            'text': text,
            'thread_ts': message.get('thread_ts'),
            'original_timestamp': message.get('ts'),
            'message_type': 'resolution',
            'timestamp': timestamp.isoformat(),
            'status': 'Resolved',
            'incident_likelihood': 'Likely' if linked_issue_info else 'Not Likely',
            'confidence': classification.get('confidence', 0.7),
            'classification': classification,
            'linked_issue': linked_issue_info,
            'metadata': {
                'has_files': 'files' in message,
                'has_attachments': 'attachments' in message,
                'is_thread': 'thread_ts' in message,
                'gemini_reasoning': classification.get('reasoning', ''),
                'cross_source_link': linked_issue_info['source'] == 'jira' if linked_issue_info else False
            }
        }
        
        # Send to Kafka
        key = f"slack_{channel_id}_{message.get('ts', '')}"
        self.kafka_producer.send_message(self.topic, transformed_message, key)
    
    def handle_discussion(self, message, channel_id, classification):
        """Handle ongoing discussion - may need context but not a new incident"""
        text = message.get('text', '')
        ts = float(message.get('ts', 0))
        timestamp = datetime.fromtimestamp(ts) if ts else datetime.now()
        
        # Try to find related issue for context
        related_issue = self.context_queue.find_related_issue(
            text, 
            threshold=0.5,
            user_id=message.get('user'),
            current_time=timestamp
        )
        
        # Update hot issues tracker (this user is discussing this)
        if related_issue:
            self.context_queue.hot_issues[related_issue['id']] = timestamp
            
            # Add this discussion to the issue's context
            self.context_queue.add_discussion(
                related_issue['id'],
                message.get('user', 'unknown'),
                text,
                timestamp,
                message_type='discussion'
            )
        
        transformed_message = {
            'id': f"slack_{message.get('ts', '')}",
            'type': 'slack_message',
            'channel_id': channel_id,
            'user_id': message.get('user', 'unknown'),
            'text': text,
            'thread_ts': message.get('thread_ts'),
            'original_timestamp': message.get('ts'),
            'message_type': 'discussion',
            'timestamp': timestamp.isoformat(),
            'status': 'Open',
            'incident_likelihood': 'Likely',  # Discussion about incidents is still relevant
            'confidence': classification.get('confidence', 0.6),
            'classification': classification,
            'linked_issue': {
                'id': related_issue['id'],
                'text': related_issue['text'],
                'source': related_issue['source'],
                'similarity': float(related_issue['similarity'])  # Convert numpy float
            } if related_issue else None,
            'metadata': {
                'has_files': 'files' in message,
                'has_attachments': 'attachments' in message,
                'is_thread': 'thread_ts' in message,
                'gemini_reasoning': classification.get('reasoning', '')
            }
        }
        
        key = f"slack_{channel_id}_{message.get('ts', '')}"
        self.kafka_producer.send_message(self.topic, transformed_message, key)
        
        self.logger.info(f"[DISCUSSION] {text[:60]}...")
    
    def _update_original_issue_status(self, issue_id, resolution_text, resolved_by=None):
        """Send update to Kafka to mark original issue as resolved"""
        try:
            now = datetime.now()
            update_message = {
                'id': f"{issue_id}_status_update",
                'original_issue_id': issue_id,
                'type': 'status_update',
                'status': 'Resolved',
                'resolution_text': resolution_text,
                'resolved_by': resolved_by or 'unknown',
                'resolved_at': now.isoformat(),
                'timestamp': now.isoformat(),
                'action': 'mark_resolved'
            }
            
            key = f"update_{issue_id}"
            self.kafka_producer.send_message(self.topic, update_message, key)
            self.logger.info(f"[STATUS UPDATE] Sent resolution update for {issue_id} (by: {resolved_by})")
            
        except Exception as e:
            self.logger.error(f"Error sending status update: {e}")
    
    def fetch_thread_context(self, channel_id, thread_ts):
        """Fetch thread context for better understanding"""
        try:
            result = self.client.conversations_replies(
                channel=channel_id,
                ts=thread_ts,
                limit=10
            )
            messages = result.get('messages', [])
            context = "\n".join([
                f"- {msg.get('text', '')}" 
                for msg in messages[:-1]  # Exclude current message
            ])
            return context
        except Exception as e:
            self.logger.error(f"Error fetching thread context: {e}")
            return None
    
    def process_message(self, message, channel_id):
        """Process Slack message with LLM-based semantic understanding"""
        # Skip bot messages
        if message.get('subtype') == 'bot_message' or message.get('bot_id'):
            return
        
        text = message.get('text', '')
        
        # Get thread context if available
        thread_context = None
        if message.get('thread_ts'):
            thread_context = self.fetch_thread_context(channel_id, message.get('thread_ts'))
        
        # Classify with Gemini
        message_type, classification, confidence = self.classify_with_gemini(text, thread_context)
        
        # Handle based on classification
        if message_type == 'incident_report':
            self.handle_incident_report(message, channel_id, classification)
        elif message_type == 'resolution':
            self.handle_resolution(message, channel_id, classification)
        elif message_type == 'discussion':
            self.handle_discussion(message, channel_id, classification)
        else:
            # Unrelated - skip
            self.logger.debug(f"[UNRELATED] Skipped: {text[:50]}...")
    
    def fetch_messages(self, channel_id=None):
        """Fetch messages from Slack"""
        try:
            channel_id = channel_id or os.getenv('SLACK_CHANNEL_ID')
            
            result = self.client.conversations_history(
                channel=channel_id,
                oldest=str(self.last_timestamp),
                limit=100
            )
            
            messages = result['messages']
            self.logger.info(f"Fetched {len(messages)} new messages from Slack")
            
            for message in reversed(messages):  # Process chronologically
                self.process_message(message, channel_id)
                
            if messages:
                self.last_timestamp = float(messages[0]['ts'])
                
        except SlackApiError as e:
            self.logger.error(f"Slack API error: {e.response['error']}")
        except Exception as e:
            self.logger.error(f"Error fetching Slack messages: {e}")
    
    def start_monitoring(self, interval=30):
        """Start continuous monitoring"""
        self.logger.info("=" * 70)
        self.logger.info("Starting LLM-Powered Slack Monitoring")
        self.logger.info("Features:")
        self.logger.info("  - Gemini LLM for semantic classification (no keywords)")
        self.logger.info("  - Cross-source context tracking (Slack + Jira)")
        self.logger.info("  - Semantic resolution linking")
        self.logger.info("=" * 70)
        
        while True:
            try:
                self.fetch_messages()
                time.sleep(interval)
            except KeyboardInterrupt:
                self.logger.info("Stopping Slack monitoring...")
                break
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(interval)
    
    def close(self):
        self.kafka_producer.close()

