import json
import logging
from kafka import KafkaProducer
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

class KafkaMessageProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )
        self.logger = logging.getLogger(__name__)

    def send_message(self, topic, message, key=None):
        try:
            # Add timestamp to message
            message['timestamp'] = datetime.utcnow().isoformat()
            message['source_system'] = 'thala-ingestion'
            
            future = self.producer.send(topic, value=message, key=key)
            self.producer.flush()
            self.logger.info(f"Message sent to topic {topic}: {message.get('id', 'unknown')}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to send message to Kafka: {e}")
            return False

    def close(self):
        self.producer.close()