"""
Kafka Consumer that reads messages from Kafka topics and sends them to Flask API
This bridges the Kafka queue with the ML prediction system
"""
import json
import logging
import os
import requests
from kafka import KafkaConsumer
from dotenv import load_dotenv
import time
from datetime import datetime

load_dotenv()

class KafkaToFlaskBridge:
    """Consumes messages from Kafka and sends them to Flask API"""
    
    def __init__(self):
        self.flask_url = os.getenv('FLASK_API_URL', 'http://localhost:5000')
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
        self.topics = [
            os.getenv('KAFKA_TOPIC_SLACK', 'thala-slack-events'),
            os.getenv('KAFKA_TOPIC_JIRA', 'thala-jira-events'),
            os.getenv('KAFKA_TOPIC_EMAIL', 'thala-email-events')
        ]
        self.logger = logging.getLogger(__name__)
        self.consumer = None
        self.running = True
        
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
            
            # Get status
            status = message.get('status', 'Open')
            
            # Get incident_likelihood (for auto-labeled resolutions)
            incident_likelihood = message.get('incident_likelihood')
            
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
            
            return payload
            
        except Exception as e:
            self.logger.error(f"Error formatting message: {e}")
            return None
    
    def _format_status_update(self, message):
        """Format status update message"""
        return {
            "action": "update_status",
            "original_issue_id": message.get('original_issue_id'),
            "status": message.get('status'),
            "resolution_text": message.get('resolution_text'),
            "resolved_by": message.get('resolved_by'),
            "resolved_at": message.get('resolved_at'),
            "timestamp": message.get('timestamp')
        }
    
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
            # Check if this is a status update
            if payload.get('action') == 'update_status':
                endpoint = f"{self.flask_url}/update_status"
                response = requests.post(endpoint, json=payload, timeout=10)
                
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
                timeout=10
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

