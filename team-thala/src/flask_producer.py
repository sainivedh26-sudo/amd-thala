import json
import logging
import requests
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

class FlaskAPIProducer:
    """Sends data to Flask API instead of Kafka"""
    
    def __init__(self):
        self.flask_url = os.getenv('FLASK_API_URL', 'http://localhost:5000')
        self.flask_user = os.getenv('FLASK_USER', 'elastic')
        self.flask_password = os.getenv('FLASK_PASSWORD', 'NHyNwkjjOmO1GwUBU54_')
        self.logger = logging.getLogger(__name__)

    def send_message(self, topic, message, key=None):
        """Send message to Flask /index endpoint"""
        try:
            # Format message for Flask API
            timestamp = message.get('timestamp', datetime.utcnow().isoformat())
            
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
            
            # Prepare payload for Flask
            payload = {
                "texts": [text],
                "timestamp": timestamp,
                "status": message.get('status', 'Open'),
                "source": source
            }
            
            # Send to Flask /index endpoint
            response = requests.post(
                f"{self.flask_url}/index",
                json=payload,
                auth=(self.flask_user, self.flask_password),
                timeout=10
            )
            
            if response.status_code == 200:
                self.logger.info(f"Message sent to Flask API: {message.get('id', 'unknown')}")
                return True
            else:
                self.logger.error(f"Failed to send to Flask API: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error sending message to Flask API: {e}")
            return False
    
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
            return f"Jira [{issue_id}]: {summary} at {created}. Description: {description}"
        
        elif source == 'email':
            subject = message.get('subject', '')
            sender = message.get('sender', '')
            body = message.get('body', '')[:200]  # Limit body length
            return f"Email from {sender}: {subject}. {body}"
        
        else:
            return json.dumps(message)
    
    def close(self):
        """Close connection (no-op for HTTP client)"""
        self.logger.info("Flask API producer closed")




