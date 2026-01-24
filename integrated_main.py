"""
Integrated ITSM Incident Prediction System
Runs Flask API and data connectors in a single process
"""
import logging
import threading
import signal
import sys
import os
import time
from pathlib import Path

# Add team-thala/src to path to import connectors
sys.path.insert(0, str(Path(__file__).parent / "team-thala" / "src"))

# Import Flask app components from new.py
from new import app, initialize_app

# Import connectors
try:
    # Use LLM-powered Slack connector with semantic understanding
    from slack_connector_llm import LLMSlackConnector as SlackConnector
    from jira_connector import JiraConnector
    from kafka_consumer_to_flask import KafkaToFlaskBridge
    CONNECTORS_AVAILABLE = True
except ImportError as e:
    logging.warning(f"Could not import connectors: {e}")
    CONNECTORS_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('thala_integrated.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class IntegratedThalaSystem:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.running = True
        self.flask_thread = None
        self.connector_threads = []
        
        if CONNECTORS_AVAILABLE:
            try:
                # Initialize Slack connector first (it has the context queue)
                self.slack_connector = SlackConnector()
                # Initialize Jira connector with reference to Slack for context sync
                self.jira_connector = JiraConnector(slack_connector=self.slack_connector)
                self.kafka_consumer = KafkaToFlaskBridge()
                self.connectors_enabled = True
                self.logger.info("Connectors initialized with Jira-Slack context sync")
            except Exception as e:
                self.logger.error(f"Failed to initialize connectors: {e}")
                self.connectors_enabled = False
        else:
            self.connectors_enabled = False

    def start_flask_app(self):
        """Start Flask app in current thread"""
        self.logger.info("Starting Flask API server on http://0.0.0.0:5000")
        initialize_app()
        app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

    def start_connectors(self):
        """Start data connectors and Kafka consumer in separate threads"""
        if not self.connectors_enabled:
            self.logger.warning("Connectors not available, running Flask API only")
            return
        
        self.logger.info("Starting data connectors and Kafka consumer...")
        
        # Start Kafka Consumer (bridges Kafka → Flask)
        kafka_thread = threading.Thread(
            target=self.kafka_consumer.start_consuming,
            daemon=True,
            name="KafkaConsumer"
        )
        self.connector_threads.append(kafka_thread)
        
        # Start Slack monitoring (sends to Kafka)
        slack_thread = threading.Thread(
            target=self.slack_connector.start_monitoring,
            args=(15,),
            daemon=True,
            name="SlackConnector"
        )
        self.connector_threads.append(slack_thread)
        
        # Start Jira monitoring (sends to Kafka)
        jira_thread = threading.Thread(
            target=self.jira_connector.start_monitoring,
            args=(15,),
            daemon=True,
            name="JiraConnector"
        )
        self.connector_threads.append(jira_thread)
        
        # Start all connector threads
        for thread in self.connector_threads:
            thread.start()
            self.logger.info(f"Started {thread.name}")

    def start(self):
        """Start the integrated system"""
        self.logger.info("=" * 60)
        self.logger.info("Starting Thala ITSM Incident Prediction System")
        self.logger.info("=" * 60)
        
        # Start connectors first (they run in background)
        self.start_connectors()
        
        # Give connectors a moment to initialize
        time.sleep(2)
        
        self.logger.info("All connectors started successfully!")
        self.logger.info("")
        self.logger.info("Architecture:")
        self.logger.info("  Jira/Slack -> Kafka Queue -> Kafka Consumer -> Flask API -> Elasticsearch + ML")
        self.logger.info("  Jira <-> Slack: Context sync via shared semantic queue")
        self.logger.info("")
        self.logger.info("Semantic Features:")
        self.logger.info("  - Gemini LLM for Slack classification (no keywords)")
        self.logger.info("  - Cross-source issue tracking (Slack ↔ Jira)")
        self.logger.info("  - Semantic resolution linking using embeddings")
        self.logger.info("")
        self.logger.info("Flask API will be available at: http://localhost:5000 (or http://127.0.0.1:5000)")
        self.logger.info("")
        self.logger.info("Available endpoints:")
        self.logger.info("  POST /index - Index new incident data")
        self.logger.info("  POST /predict_incident - Predict incident likelihood")
        self.logger.info("  POST /search - Search similar incidents")
        self.logger.info("  GET  /health - Health check")
        self.logger.info("")
        
        # Start Flask app in main thread
        try:
            self.start_flask_app()
        except KeyboardInterrupt:
            self.logger.info("Received shutdown signal...")
        finally:
            self.shutdown()

    def shutdown(self):
        """Shutdown the system gracefully"""
        self.logger.info("Shutting down Thala ITSM System...")
        self.running = False
        
        if self.connectors_enabled:
            try:
                self.kafka_consumer.close()
                self.slack_connector.close()
                self.jira_connector.close()
            except Exception as e:
                self.logger.error(f"Error closing connectors: {e}")
        
        self.logger.info("Shutdown complete.")

def signal_handler(sig, frame):
    """Handle shutdown signals"""
    print("\nShutdown signal received...")
    sys.exit(0)

if __name__ == "__main__":
    # Handle graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start the integrated system
    system = IntegratedThalaSystem()
    system.start()

