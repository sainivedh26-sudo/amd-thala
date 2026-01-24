"""
Clear all data from Elasticsearch and Kafka to start fresh
"""
import os
import sys
from elasticsearch import Elasticsearch
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Elasticsearch connection
es = Elasticsearch(
    hosts=["https://localhost:9200"],
    ca_certs="D:\\elasticsearch-9.1.5-windows-x86_64\\elasticsearch-9.1.5\\config\\certs\\http_ca.crt",
    verify_certs=False,
    basic_auth=("elastic", "NHyNwkjjOmO1GwUBU54_")
)

# Kafka topics to clear
KAFKA_TOPICS = [
    'thala-slack-events',
    'thala-jira-events',
    'thala-email-events'
]

def clear_elasticsearch():
    """Delete and recreate the Elasticsearch index"""
    index_name = "thala_knowledge"
    
    try:
        # Check if index exists
        if es.indices.exists(index=index_name):
            logger.info(f"Deleting Elasticsearch index: {index_name}")
            es.indices.delete(index=index_name)
            logger.info(f"✅ Successfully deleted index: {index_name}")
        else:
            logger.info(f"Index {index_name} does not exist, nothing to delete")
        
        # Recreate the index with mapping
        logger.info(f"Creating fresh Elasticsearch index: {index_name}")
        mapping = {
            "mappings": {
                "properties": {
                    "text": {"type": "text"},
                    "embedding": {"type": "dense_vector", "dims": 384},
                    "timestamp": {"type": "date"},
                    "status": {"type": "keyword"},
                    "incident_likelihood": {"type": "keyword"},
                    "source": {"type": "keyword"},
                    "resolution_text": {"type": "text"},
                    "resolved_by": {"type": "keyword"},
                    "resolved_at": {"type": "date"},
                    "issue_id": {"type": "keyword"},
                    "discussions": {"type": "nested", "properties": {
                        "text": {"type": "text"},
                        "user_id": {"type": "keyword"},
                        "timestamp": {"type": "date"}
                    }}
                }
            }
        }
        
        es.indices.create(index=index_name, body=mapping)
        logger.info(f"✅ Successfully created fresh index: {index_name}")
        
    except Exception as e:
        logger.error(f"❌ Error clearing Elasticsearch: {e}")
        return False
    
    return True

def clear_kafka_topics():
    """Delete and recreate Kafka topics"""
    try:
        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
        from kafka import KafkaConsumer, KafkaProducer
        
        # First, try to delete and recreate topics
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=kafka_servers,
                client_id='thala-clear-script',
                request_timeout_ms=10000
            )
            
            # Delete existing topics
            existing_topics = admin_client.list_topics()
            topics_to_delete = [topic for topic in KAFKA_TOPICS if topic in existing_topics]
            
            if topics_to_delete:
                logger.info(f"Deleting Kafka topics: {topics_to_delete}")
                admin_client.delete_topics(topics=topics_to_delete, timeout_ms=10000)
                logger.info(f"✅ Successfully deleted topics: {topics_to_delete}")
                # Wait longer for deletion to fully complete
                import time
                logger.info("Waiting for Kafka to process topic deletion...")
                time.sleep(5)
            else:
                logger.info("No existing Kafka topics to delete")
            
            # Close admin client and create a new one to avoid connection issues
            admin_client.close()
            time.sleep(1)
            
            # Recreate topics with retry
            logger.info(f"Creating fresh Kafka topics: {KAFKA_TOPICS}")
            new_topics = [
                NewTopic(name=topic, num_partitions=3, replication_factor=1)
                for topic in KAFKA_TOPICS
            ]
            
            # Retry creating topics up to 3 times
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    admin_client = KafkaAdminClient(
                        bootstrap_servers=kafka_servers,
                        client_id='thala-clear-script-retry',
                        request_timeout_ms=15000
                    )
                    admin_client.create_topics(new_topics=new_topics, timeout_ms=15000)
                    admin_client.close()
                    logger.info(f"✅ Successfully created fresh topics: {KAFKA_TOPICS}")
                    return True
                except Exception as create_error:
                    if attempt < max_retries - 1:
                        logger.warning(f"Attempt {attempt + 1} failed: {create_error}. Retrying in 2 seconds...")
                        time.sleep(2)
                        if admin_client:
                            try:
                                admin_client.close()
                            except:
                                pass
                    else:
                        raise create_error
            
            return True
            
        except TopicAlreadyExistsError:
            logger.warning("⚠️  Some topics already exist. They may have just been recreated.")
            logger.info("Verifying topics exist...")
            try:
                admin_client = KafkaAdminClient(
                    bootstrap_servers=kafka_servers,
                    client_id='thala-verify',
                    request_timeout_ms=5000
                )
                existing = admin_client.list_topics()
                missing = [t for t in KAFKA_TOPICS if t not in existing]
                if missing:
                    logger.warning(f"Missing topics: {missing}. Try running the script again.")
                else:
                    logger.info(f"✅ All topics exist: {KAFKA_TOPICS}")
                admin_client.close()
            except:
                pass
            return True
        except Exception as admin_error:
            logger.warning(f"⚠️  Could not delete/recreate topics via admin API: {admin_error}")
            logger.info("Kafka topics were deleted but recreation failed.")
            logger.info("To manually create topics, use:")
            logger.info("  kafka-topics.sh --bootstrap-server localhost:9092 --create --topic <topic-name> --partitions 3 --replication-factor 1")
            for topic in KAFKA_TOPICS:
                logger.info(f"  kafka-topics.sh --bootstrap-server localhost:9092 --create --topic {topic} --partitions 3 --replication-factor 1")
            return False
        
    except Exception as e:
        logger.error(f"❌ Error with Kafka: {e}")
        logger.error("Make sure Kafka is running and accessible")
        return False

def main():
    """Main function to clear all data"""
    print("=" * 70)
    print("🧹 Clearing All Thala Data - Elasticsearch & Kafka")
    print("=" * 70)
    print()
    
    # Test Elasticsearch connection
    try:
        if not es.ping():
            logger.error("❌ Cannot connect to Elasticsearch. Make sure it's running.")
            return
        logger.info("✅ Connected to Elasticsearch")
    except Exception as e:
        logger.error(f"❌ Cannot connect to Elasticsearch: {e}")
        logger.error("Make sure Elasticsearch is running at localhost:9200")
        return
    
    # Test Kafka connection
    try:
        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
        admin_client = KafkaAdminClient(
            bootstrap_servers=kafka_servers,
            client_id='thala-clear-test'
        )
        admin_client.list_topics()  # Test connection
        admin_client.close()
        logger.info("✅ Connected to Kafka")
    except Exception as e:
        logger.error(f"❌ Cannot connect to Kafka: {e}")
        logger.error("Make sure Kafka is running at localhost:9092")
        return
    
    print()
    confirm = input("⚠️  This will DELETE ALL data from Elasticsearch and Kafka. Continue? (yes/no): ")
    if confirm.lower() != 'yes':
        print("Cancelled.")
        return
    
    print()
    print("Starting cleanup...")
    print()
    
    # Clear Elasticsearch
    logger.info("Step 1: Clearing Elasticsearch...")
    es_success = clear_elasticsearch()
    
    print()
    
    # Clear Kafka
    logger.info("Step 2: Clearing Kafka topics...")
    kafka_success = clear_kafka_topics()
    
    print()
    print("=" * 70)
    if es_success and kafka_success:
        print("✅ Successfully cleared all data! System is ready for fresh start.")
    else:
        print("⚠️  Some operations may have failed. Check logs above.")
    print("=" * 70)

if __name__ == "__main__":
    main()

