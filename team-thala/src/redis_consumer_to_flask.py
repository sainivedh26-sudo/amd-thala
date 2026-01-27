"""
Redis Consumer that reads messages from Redis lists and sends to Flask API.
Pairs with Java MSK→Redis bridge.
"""
import os
import json
import logging
import time
from datetime import datetime
from dotenv import load_dotenv
import redis

# Reuse formatter and sender from Kafka bridge
from kafka_consumer_to_flask import KafkaToFlaskBridge

load_dotenv()

class RedisToFlaskBridge(KafkaToFlaskBridge):
    def __init__(self):
        super().__init__()
        self.redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
        self.redis_port = int(os.getenv('REDIS_PORT', '6379'))
        self.redis_password = os.getenv('REDIS_PASSWORD')
        # Comma-separated list of Redis list keys to read from, default mirrors topics
        topics = [
            os.getenv('KAFKA_TOPIC_SLACK', 'thala-slack-events'),
            os.getenv('KAFKA_TOPIC_JIRA', 'thala-jira-events'),
            os.getenv('KAFKA_TOPIC_EMAIL', 'thala-email-events')
        ]
        prefix = os.getenv('REDIS_LIST_PREFIX', 'thala:queue:')
        self.redis_lists = [f"{prefix}{t}" for t in topics]
        self.logger = logging.getLogger(__name__)
        self.enable_prediction = os.getenv('ENABLE_CATEGORY_PREDICTION', 'true').lower() == 'true'
        try:
            pool = redis.ConnectionPool(host=self.redis_host, port=self.redis_port, password=self.redis_password, decode_responses=True)
            self.client = redis.Redis(connection_pool=pool)
            self.logger.info(f"Connected to Redis at {self.redis_host}:{self.redis_port}")
        except Exception as e:
            raise RuntimeError(f"Failed to connect to Redis: {e}")

    def start_consuming(self):
        self.logger.info("=" * 60)
        self.logger.info("Redis -> Flask Bridge Started")
        self.logger.info(f"Lists: {', '.join(self.redis_lists)}")
        self.logger.info(f"Forwarding to: {self.flask_url}/index")
        self.logger.info("=" * 60)

        message_count = 0
        error_count = 0

        while True:
            try:
                # BLPOP supports multiple keys; returns (key, value) or None
                result = self.client.blpop(self.redis_lists, timeout=2)
                if not result:
                    continue
                list_key, value = result
                message_count += 1

                try:
                    data = json.loads(value)
                except Exception:
                    self.logger.error(f"Invalid JSON from {list_key}: {value[:120]}")
                    continue

                # Infer topic from list key suffix
                topic = list_key.split(':', 2)[-1]
                payload = self.format_message_for_flask(data, topic)
                if payload:
                    ok = self.send_to_flask(payload)
                    if not ok:
                        error_count += 1
                    if message_count % 10 == 0:
                        self.logger.info(f"Processed {message_count} messages (errors: {error_count})")
            except KeyboardInterrupt:
                break
            except Exception as e:
                error_count += 1
                self.logger.error(f"Error loop: {e}")
                time.sleep(1)

        self.logger.info("Stopping Redis bridge...")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    bridge = RedisToFlaskBridge()
    bridge.start_consuming()


if __name__ == '__main__':
    main()




