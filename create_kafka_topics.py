"""
Manually create Kafka topics if they don't exist
"""
import os
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_TOPICS = [
    'thala-slack-events',
    'thala-jira-events',
    'thala-email-events'
]

def create_topics():
    """Create Kafka topics"""
    try:
        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
        
        admin_client = KafkaAdminClient(
            bootstrap_servers=kafka_servers,
            client_id='thala-create-topics',
            request_timeout_ms=15000
        )
        
        # Check existing topics
        existing = admin_client.list_topics()
        missing = [t for t in KAFKA_TOPICS if t not in existing]
        
        if not missing:
            logger.info(f"✅ All topics already exist: {KAFKA_TOPICS}")
            admin_client.close()
            return True
        
        logger.info(f"Creating missing topics: {missing}")
        new_topics = [
            NewTopic(name=topic, num_partitions=3, replication_factor=1)
            for topic in missing
        ]
        
        admin_client.create_topics(new_topics=new_topics, timeout_ms=15000)
        logger.info(f"✅ Successfully created topics: {missing}")
        
        admin_client.close()
        return True
        
    except TopicAlreadyExistsError:
        logger.info("✅ Topics already exist")
        return True
    except Exception as e:
        logger.error(f"❌ Error creating topics: {e}")
        return False

if __name__ == "__main__":
    print("Creating Kafka topics...")
    if create_topics():
        print("Done!")
    else:
        print("Failed. Check logs above.")

