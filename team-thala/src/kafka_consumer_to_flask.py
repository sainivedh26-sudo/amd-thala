"""
Kafka Consumer that reads messages from Kafka topics and sends them to Flask API
This bridges the Kafka queue with the ML prediction system
"""
import sys
import os
# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
import logging
import requests
from kafka import KafkaConsumer
from dotenv import load_dotenv
import time
from datetime import datetime
from incident_tracker import get_tracker
# Prefer Bedrock predictor (LLM on AWS). Fallback to Groq if unavailable.
try:
    from bedrock_predictor import get_predictor
except Exception:
    from gemini_predictor import get_predictor

load_dotenv()

# Get tracker and predictor instances
tracker = get_tracker()
predictor = None  # Will be initialized only if needed

class KafkaToFlaskBridge:
    """Consumes messages from Kafka and sends them to Flask API"""
    
    def __init__(self):
        self.flask_url = os.getenv('FLASK_API_URL', 'http://localhost:5000')
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
        self.topics = [
            os.getenv('KAFKA_TOPIC_SLACK', 'thala-slack-events'),
            os.getenv('KAFKA_TOPIC_JIRA', 'thala-jira-events'),
        ]
        self.logger = logging.getLogger(__name__)
        self.consumer = None
        self.running = True
        self.enable_prediction = os.getenv('ENABLE_CATEGORY_PREDICTION', 'true').lower() == 'true'
        
        # Initialize predictor if enabled
        if self.enable_prediction:
            try:
                global predictor
                predictor = get_predictor()
                self.logger.info("Category/Severity prediction enabled")
            except Exception as e:
                self.logger.warning(f"Could not initialize predictor: {e}. Predictions disabled.")
                self.enable_prediction = False
        
    def connect(self):
        """Connect to Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.kafka_servers,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',  # Start from latest messages
                enable_auto_commit=True,
                group_id='thala-flask-consumer',
                consumer_timeout_ms=1000  # Timeout for polling
            )
            self.logger.info(f"Connected to Kafka: {self.kafka_servers}")
            self.logger.info(f"Subscribed to topics: {self.topics}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def format_message_for_flask(self, message, topic):
        """Format Kafka message for Flask API"""
        try:
            # Check if this is a status update message
            if message.get('type') == 'status_update' and message.get('action') == 'mark_resolved':
                return self._format_status_update(message)
            
            # Handle discussion messages - add to tracker AND send to Flask
            if message.get('type') == 'context_update' or message.get('message_type') == 'discussion':
                linked_issue_id = message.get('linked_issue_id')
                if linked_issue_id:
                    # Add to in-memory tracker
                    tracker.add_discussion(
                        linked_issue_id,
                        message.get('text', ''),
                        message.get('user_id')
                    )
                    self.logger.info(f"[DISCUSSION] Added to tracker: {linked_issue_id}")
                    
                    # Also send to Flask to store in Elasticsearch
                    # Format as a discussion update
                    return {
                        'type': 'discussion',
                        'linked_issue_id': linked_issue_id,
                        'text': message.get('text', ''),
                        'user_id': message.get('user_id'),
                        'timestamp': message.get('timestamp', datetime.utcnow().isoformat())
                    }
                return None  # No linked issue, skip
            
            # Debug: Log received message
            if message.get('incident_likelihood'):
                self.logger.info(f"Received auto-labeled message: {message.get('id', 'unknown')} -> {message.get('incident_likelihood')}")
            
            # Determine source from topic
            source = 'unknown'
            if 'slack' in topic.lower():
                source = 'slack'
            elif 'jira' in topic.lower():
                source = 'jira'
            elif 'email' in topic.lower():
                source = 'email'
            
            # Format text based on source
            text = self._format_text(message, source)
            
            # Get timestamp
            timestamp = message.get('timestamp')
            if not timestamp:
                timestamp = datetime.utcnow().isoformat()
            
            # Get status and normalize for Jira
            raw_status = message.get('status', 'Open')
            status = self._normalize_status(raw_status, source)
            
            # Get incident_likelihood (for auto-labeled resolutions)
            incident_likelihood = message.get('incident_likelihood')
            
            # Get category and severity (either from message or predict)
            category = message.get('category')
            severity = message.get('severity')
            
            # If not provided, predict using Gemini
            if not category or not severity:
                if self.enable_prediction and status == 'Open' and predictor:
                    try:
                        prediction = predictor.predict(text)
                        category = prediction['category']
                        severity = prediction['severity']
                        self.logger.info(f"[PREDICT] {message.get('id')}: {category}/{severity}")
                    except Exception as e:
                        self.logger.error(f"Prediction error: {e}")
            else:
                self.logger.info(f"[ALREADY CLASSIFIED] {message.get('id')}: {category}/{severity}")
            
            # Add to incident tracker
            if status == 'Open':
                tracker.add_incident({
                    'id': message.get('id'),
                    'text': text,
                    'source': source,
                    'timestamp': timestamp,
                    'status': status,
                    'category': category,
                    'severity': severity,
                    'user_id': message.get('user_id'),
                    'channel_id': message.get('channel_id')
                })
                
                # Debug: Verify it was added
                stats = tracker.get_stats()
                self.logger.info(f"[TRACKER DEBUG] Stats after add: {stats}")
            
            # Prepare payload for Flask
            payload = {
                "texts": [text],
                "timestamp": timestamp,
                "status": status,
                "source": source,
                "issue_id": message.get('id')  # Include issue_id for tracking
            }
            
            # Add incident_likelihood if present
            if incident_likelihood:
                payload["incident_likelihood"] = incident_likelihood
            
            # Add category and severity if predicted
            if category:
                payload["category"] = category
            if severity:
                payload["severity"] = severity
            
            return payload
            
        except Exception as e:
            self.logger.error(f"Error formatting message: {e}")
            return None
    
    def _format_status_update(self, message):
        """Format status update message"""
        # Update tracker
        issue_id = message.get('original_issue_id')
        if issue_id:
            tracker.update_incident_status(
                issue_id,
                message.get('status'),
                message.get('resolution_text')
            )
        
        return {
            "action": "update_status",
            "original_issue_id": message.get('original_issue_id'),
            "status": message.get('status'),
            "resolution_text": message.get('resolution_text'),
            "resolved_by": message.get('resolved_by'),
            "resolved_at": message.get('resolved_at'),
            "timestamp": message.get('timestamp')
        }
    
    def _normalize_status(self, raw_status, source):
        """Normalize status from different sources to Open/Resolved"""
        if not raw_status:
            return 'Open'
        
        status_lower = raw_status.lower().strip()
        
        # Jira statuses
        if source == 'jira':
            # Resolved statuses
            resolved_statuses = ['done', 'resolved', 'closed', 'cancelled']
            if any(resolved in status_lower for resolved in resolved_statuses):
                return 'Resolved'
            # All other statuses (To Do, In Progress, In Review, etc.) are Open
            return 'Open'
        
        # Slack/Email: use as-is or default to Open
        if status_lower in ['resolved', 'closed', 'done']:
            return 'Resolved'
        
        return 'Open'
    
    def _format_text(self, message, source):
        """Format message text based on source"""
        if source == 'slack':
            user = message.get('user_id', 'unknown')
            text = message.get('text', '')
            ts = message.get('original_timestamp', '')
            return f"Slack: {text} [User: {user}, Time: {ts}]"
        
        elif source == 'jira':
            summary = message.get('summary', '')
            description = message.get('description', '')
            issue_id = message.get('id', '')
            created = message.get('created', '')
            reporter = message.get('reporter', 'Unknown')
            return f"Jira [{issue_id}]: {summary} at {created}. Reporter: {reporter}. Description: {description[:200]}"
        
        elif source == 'email':
            subject = message.get('subject', '')
            sender = message.get('sender', '')
            body = message.get('body', '')[:200]
            return f"Email from {sender}: {subject}. {body}"
        
        else:
            return json.dumps(message)
    
    def send_to_flask(self, payload):
        """Send formatted payload to Flask API"""
        try:
            # Check if this is a discussion update
            if payload.get('type') == 'discussion':
                endpoint = f"{self.flask_url}/add_discussion"
                response = requests.post(endpoint, json=payload, timeout=30)
                
                if response.status_code == 200:
                    self.logger.info(f"[OK] Discussion added to {payload.get('linked_issue_id')}")
                    return True
                else:
                    self.logger.error(f"Flask API error on discussion: {response.status_code} - {response.text}")
                    return False
            
            # Check if this is a status update
            if payload.get('action') == 'update_status':
                endpoint = f"{self.flask_url}/update_status"
                response = requests.post(endpoint, json=payload, timeout=30)
                
                if response.status_code == 200:
                    self.logger.info(f"[OK] Status updated for {payload.get('original_issue_id')}: {payload.get('status')}")
                    return True
                else:
                    self.logger.error(f"Flask API error on update: {response.status_code} - {response.text}")
                    return False
            
            # Debug: Log if incident_likelihood is being sent
            if payload.get('incident_likelihood'):
                self.logger.info(f"[AUTO-LABEL] Sending to Flask with incident_likelihood='{payload['incident_likelihood']}'")
            
            response = requests.post(
                f"{self.flask_url}/index",
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                label_info = f" [LABELED: {payload.get('incident_likelihood')}]" if payload.get('incident_likelihood') else ""
                self.logger.info(f"[OK] Sent to Flask: {payload['texts'][0][:50]}...{label_info}")
                return True
            else:
                self.logger.error(f"Flask API error: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.ConnectionError:
            self.logger.error(f"Cannot connect to Flask API at {self.flask_url}")
            return False
        except Exception as e:
            self.logger.error(f"Error sending to Flask: {e}")
            return False
    
    def start_consuming(self):
        """Start consuming messages from Kafka"""
        if not self.connect():
            self.logger.error("Failed to connect to Kafka, cannot start consuming")
            return
        
        self.logger.info("=" * 60)
        self.logger.info("Kafka -> Flask Bridge Started")
        self.logger.info(f"Consuming from: {', '.join(self.topics)}")
        self.logger.info(f"Forwarding to: {self.flask_url}/index")
        self.logger.info("=" * 60)
        
        message_count = 0
        error_count = 0
        
        try:
            while self.running:
                try:
                    # Poll for messages
                    messages = self.consumer.poll(timeout_ms=1000)
                    
                    for topic_partition, records in messages.items():
                        topic = topic_partition.topic
                        
                        for record in records:
                            message_count += 1
                            
                            try:
                                # Format message for Flask
                                payload = self.format_message_for_flask(record.value, topic)
                                
                                if payload:
                                    # Send to Flask API
                                    success = self.send_to_flask(payload)
                                    
                                    if not success:
                                        error_count += 1
                                        
                                    # Log progress every 10 messages
                                    if message_count % 10 == 0:
                                        self.logger.info(f"Processed {message_count} messages (errors: {error_count})")
                                        
                            except Exception as msg_error:
                                error_count += 1
                                self.logger.error(f"Error processing message: {msg_error}")
                    
                    # Brief pause to avoid busy waiting
                    time.sleep(0.1)
                    
                except Exception as poll_error:
                    self.logger.error(f"Error during polling: {poll_error}")
                    time.sleep(5)
                    
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal, stopping...")
        finally:
            self.close()
            self.logger.info(f"Kafka consumer stopped. Total messages: {message_count}, Errors: {error_count}")
    
    def close(self):
        """Close Kafka consumer"""
        self.running = False
        if self.consumer:
            self.consumer.close()
            self.logger.info("Kafka consumer closed")

def main():
    """Main entry point for standalone execution"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('kafka_consumer.log'),
            logging.StreamHandler()
        ]
    )
    
    bridge = KafkaToFlaskBridge()
    bridge.start_consuming()

if __name__ == "__main__":
    main()

