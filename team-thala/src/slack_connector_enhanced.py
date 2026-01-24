import os
import json
import logging
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from kafka_producer import KafkaMessageProducer
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta
from collections import defaultdict

load_dotenv()

class EnhancedSlackConnector:
    """
    Enhanced Slack Connector with:
    1. Thread tracking (link issues with resolutions)
    2. Auto-labeling (classify as Likely/Not Likely)
    3. Resolution detection (detect when issues are fixed)
    """
    
    def __init__(self):
        self.client = WebClient(token=os.getenv('SLACK_BOT_TOKEN'))
        self.kafka_producer = KafkaMessageProducer()
        self.topic = os.getenv('KAFKA_TOPIC_SLACK', 'thala-slack-events')
        self.logger = logging.getLogger(__name__)
        self.last_timestamp = 0
        
        # Track threads: thread_ts -> {original_message, replies, status}
        self.tracked_threads = {}
        
        # Track standalone messages by timestamp for resolution linking
        self.recent_issues = {}  # timestamp -> message_data
        
        # Keywords for classification
        self.incident_keywords = {
            'critical': ['down', 'outage', 'crash', 'crashed', 'unavailable', 'not working',
                        'broken', 'failed', 'failure', 'cannot', 'unable', 'error 500', 
                        '502', '503', 'timeout', 'dead', 'killed', 'panic'],
            'high': ['slow', 'degraded', 'high latency', 'performance', 'hanging',
                    'stuck', 'frozen', 'unresponsive', 'delay', 'lagging'],
            'medium': ['issue', 'problem', 'bug', 'glitch', 'weird', 'strange',
                      'not expected', 'incorrect'],
            'resolution': ['fixed', 'resolved', 'working now', 'back up', 'restored',
                          'solved', 'done', 'completed', 'restarted', 'cleared',
                          'patched', 'updated', 'working again', 'all good', 'sorted']
        }
        
        self.non_incident_keywords = ['documentation', 'docs', 'how do i', 'how to',
                                     'question', 'meeting', 'planning', 'update',
                                     'feature request', 'enhancement', 'could we',
                                     'would be nice', 'suggestion', 'testing', 'test']

    def classify_message(self, text):
        """
        Classify message as Likely/Not Likely incident and assign confidence
        Returns: (incident_likelihood, confidence, severity)
        """
        text_lower = text.lower()
        
        # Check for non-incident first
        if any(keyword in text_lower for keyword in self.non_incident_keywords):
            return "Not Likely", 0.85, None
        
        # Check for resolution keywords (these are labeled incidents too)
        if any(keyword in text_lower for keyword in self.incident_keywords['resolution']):
            # Resolution message - check if it mentions an incident
            if any(keyword in text_lower for keyword in self.incident_keywords['critical']):
                return "Likely", 0.75, "resolution"
            return "Not Likely", 0.70, "resolution"
        
        # Check for incident keywords
        critical_match = any(keyword in text_lower for keyword in self.incident_keywords['critical'])
        high_match = any(keyword in text_lower for keyword in self.incident_keywords['high'])
        medium_match = any(keyword in text_lower for keyword in self.incident_keywords['medium'])
        
        if critical_match:
            return "Likely", 0.90, "critical"
        elif high_match:
            return "Likely", 0.75, "high"
        elif medium_match:
            return "Likely", 0.60, "medium"
        
        # Default: if it has IT keywords but not classified, assume medium
        return "Not Likely", 0.55, None

    def detect_resolution(self, text, thread_messages=None):
        """
        Detect if a message is resolving a previous issue
        Returns: (is_resolution, resolution_text)
        """
        text_lower = text.lower()
        
        # Check for resolution keywords
        has_resolution_keyword = any(
            keyword in text_lower 
            for keyword in self.incident_keywords['resolution']
        )
        
        if not has_resolution_keyword:
            return False, None
        
        # Extract resolution details
        resolution_text = text
        
        # If part of thread, combine with thread context
        if thread_messages:
            context = " | ".join([msg.get('text', '') for msg in thread_messages[-3:]])
            resolution_text = f"{context} | Resolution: {text}"
        
        return True, resolution_text

    def link_resolution_to_issue(self, resolution_message, timestamp):
        """
        Try to link a resolution message to a recent issue
        Looks back 24 hours for related issues
        """
        cutoff_time = timestamp - timedelta(hours=24)
        
        # Clean recent_issues of old entries
        self.recent_issues = {
            ts: msg for ts, msg in self.recent_issues.items()
            if ts > cutoff_time
        }
        
        # Find most recent issue in same channel
        channel_id = resolution_message.get('channel_id')
        potential_issues = [
            (ts, msg) for ts, msg in self.recent_issues.items()
            if msg.get('channel_id') == channel_id
        ]
        
        if potential_issues:
            # Get most recent issue
            most_recent_ts, issue_message = max(potential_issues, key=lambda x: x[0])
            return issue_message
        
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

    def process_message(self, message, channel_id):
        """Enhanced message processing with auto-labeling and thread tracking"""
        # Skip bot messages
        if message.get('subtype') == 'bot_message' or message.get('bot_id'):
            return
        
        text = message.get('text', '')
        ts = float(message.get('ts', 0))
        timestamp = datetime.fromtimestamp(ts) if ts else datetime.utcnow()
        thread_ts = message.get('thread_ts')
        
        # Classify message
        incident_likelihood, confidence, severity = self.classify_message(text)
        
        # Check if this is a resolution
        is_resolution, resolution_text = self.detect_resolution(text)
        
        # Base message structure
        transformed_message = {
            'id': f"slack_{message.get('ts', '')}",
            'type': 'slack_message',
            'channel_id': channel_id,
            'user_id': message.get('user', 'unknown'),
            'text': text,
            'thread_ts': thread_ts,
            'original_timestamp': message.get('ts'),
            'message_type': message.get('subtype', 'message'),
            'timestamp': timestamp.isoformat(),
            'status': 'Open',
            'incident_likelihood': incident_likelihood,
            'confidence': confidence,
            'severity': severity,
            'metadata': {
                'has_files': 'files' in message,
                'has_attachments': 'attachments' in message,
                'is_thread': thread_ts is not None,
                'is_resolution': is_resolution
            }
        }
        
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
                transformed_message['linked_issue'] = {
                    'text': original_msg.get('text', ''),
                    'timestamp': original_msg.get('ts', '')
                }
                
                self.logger.info(f"[RESOLUTION] Detected in thread: {text[:50]}...")
        
        # Handle standalone messages
        else:
            if is_resolution:
                # Try to link to recent issue
                linked_issue = self.link_resolution_to_issue(transformed_message, timestamp)
                
                if linked_issue:
                    transformed_message['status'] = 'Resolved'
                    transformed_message['text'] = resolution_text
                    transformed_message['linked_issue'] = {
                        'text': linked_issue.get('text', ''),
                        'timestamp': linked_issue.get('timestamp', '')
                    }
                    self.logger.info(f"[RESOLUTION] Linked to issue: {linked_issue.get('text', '')[:30]}...")
                else:
                    self.logger.info(f"[RESOLUTION] Could not link to previous issue")
            
            elif incident_likelihood == "Likely":
                # Track as potential issue for future resolution linking
                self.recent_issues[timestamp] = transformed_message
                self.logger.info(f"[INCIDENT] Tracked for resolution: {text[:50]}...")
        
        # Send to Kafka
        key = f"slack_{channel_id}_{message.get('ts', '')}"
        self.kafka_producer.send_message(self.topic, transformed_message, key)
        
        log_label = f"[{incident_likelihood}:{confidence:.2f}]"
        self.logger.info(f"{log_label} Processed: {text[:50]}...")

    def fetch_messages(self, channel_id=None):
        """Fetch messages with enhanced processing"""
        try:
            channel_id = channel_id or os.getenv('SLACK_CHANNEL_ID')
            
            # Fetch messages newer than last timestamp
            result = self.client.conversations_history(
                channel=channel_id,
                oldest=str(self.last_timestamp),
                limit=100
            )
            
            messages = result['messages']
            self.logger.info(f"Fetched {len(messages)} new messages from Slack")
            
            for message in reversed(messages):  # Process in chronological order
                self.process_message(message, channel_id)
                
            if messages:
                self.last_timestamp = float(messages[0]['ts'])
                
        except SlackApiError as e:
            self.logger.error(f"Slack API error: {e.response['error']}")
        except Exception as e:
            self.logger.error(f"Error fetching Slack messages: {e}")

    def start_monitoring(self, interval=30):
        """Start continuous monitoring of Slack messages"""
        self.logger.info("Starting Enhanced Slack monitoring...")
        self.logger.info("Features: Thread tracking, Auto-labeling, Resolution detection")
        
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

