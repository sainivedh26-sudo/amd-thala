import json
import logging
from datetime import datetime
import os
from dotenv import load_dotenv
from typing import Optional

import redis
from kafka import KafkaProducer

load_dotenv()

class KafkaMessageProducer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # Optional: allow disabling Kafka and using Redis only
        self.use_kafka = os.getenv('USE_KAFKA', 'true').lower() == 'true'
        self.producer: Optional[KafkaProducer] = None

        # Redis fallback config
        self.redis_fallback = os.getenv('REDIS_FALLBACK_ENABLED', 'true').lower() == 'true'
        self.redis_prefix = os.getenv('REDIS_LIST_PREFIX', 'thala:queue:')
        self.redis_client: Optional[redis.Redis] = None

        # Initialize Redis fallback
        if self.redis_fallback:
            try:
                self.redis_client = redis.Redis(
                    host=os.getenv('REDIS_HOST', '127.0.0.1'),
                    port=int(os.getenv('REDIS_PORT', '6379')),
                    password=os.getenv('REDIS_PASSWORD'),
                    decode_responses=True
                )
                # simple ping test
                self.redis_client.ping()
                self.logger.info("[REDIS] Fallback enabled and connected")
            except Exception as e:
                self.logger.error(f"[REDIS] Fallback initialization failed: {e}")
                self.redis_client = None
                self.redis_fallback = False

        # Initialize Kafka producer if enabled
        if self.use_kafka:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
                    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                    key_serializer=lambda x: x.encode('utf-8') if x else None
                )
                self.logger.info("[KAFKA] Producer initialized")
            except Exception as e:
                self.logger.error(f"[KAFKA] Initialization failed: {e}")
                self.producer = None
                if not self.redis_fallback:
                    self.logger.warning("[KAFKA] No Redis fallback configured; messages will be dropped")

    def _redis_push(self, topic, message):
        try:
            if not self.redis_client:
                return False
            list_key = f"{self.redis_prefix}{topic}"
            self.redis_client.rpush(list_key, json.dumps(message))
            self.logger.info(f"[REDIS] Queued to {list_key}: {message.get('id','unknown')}")
            return True
        except Exception as e:
            self.logger.error(f"[REDIS] Failed to push: {e}")
            return False

    def send_message(self, topic, message, key=None):
        try:
            # Add timestamp to message
            message['timestamp'] = datetime.utcnow().isoformat()
            message['source_system'] = 'thala-ingestion'
            # Try Kafka first if available
            if self.producer is not None:
                future = self.producer.send(topic, value=message, key=key)
                self.producer.flush()
                self.logger.info(f"[KAFKA] Message sent to {topic}: {message.get('id', 'unknown')}")
                return True
            # Kafka not available; use Redis fallback
            if self.redis_fallback:
                return self._redis_push(topic, message)
            self.logger.warning("No producer available and Redis fallback disabled; dropping message")
            return False
        except Exception as e:
            self.logger.error(f"[KAFKA] Send failed: {e}")
            # attempt Redis fallback
            if self.redis_fallback:
                return self._redis_push(topic, message)
            return False

    def close(self):
        try:
            if self.producer is not None:
                self.producer.close()
        except Exception:
            pass