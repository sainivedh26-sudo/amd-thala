import logging
import threading
import signal
import sys
import os
from slack_connector import SlackConnector
from email_connector import EmailConnector
from jira_connector import JiraConnector
from dotenv import load_dotenv

load_dotenv()

# Configure logging
log_level = os.getenv('LOG_LEVEL', 'INFO')
log_file = os.getenv('LOG_FILE', 'logs/thala_ingestion.log')

# Create logs directory if it doesn't exist
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    level=getattr(logging, log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

class ThalaIngestionPipeline:
    def __init__(self):
        self.slack_connector = SlackConnector()
        #self.email_connector = EmailConnector()
        self.jira_connector = JiraConnector()
        self.running = True
        self.logger = logging.getLogger(__name__)

    def start(self):
        self.logger.info("Starting Thala Data Ingestion Pipeline...")
        
        # Start Slack monitoring in separate thread
        slack_thread = threading.Thread(
            target=self.slack_connector.start_monitoring,
            args=(15,),  # Check every 15 seconds
            daemon=True
        )
        
        # Start Email monitoring in separate thread
        #email_thread = threading.Thread(
        #    target=self.email_connector.start_monitoring,
        #    args=(60,),  # Check every 60 seconds
        #    daemon=True
        #)

        # Start Jira monitoring in separate thread
        jira_thread = threading.Thread(
            target=self.jira_connector.start_monitoring,
            args=(15,),  # Check every 15 seconds
            daemon=True
        )

        # Start threads
        slack_thread.start()
        #email_thread.start()
        jira_thread.start()

        self.logger.info("All connectors started successfully!")
        
        # Keep main thread alive
        try:
            while self.running:
                slack_thread.join(timeout=1)
               # email_thread.join(timeout=1)
                jira_thread.join(timeout=1)
        except KeyboardInterrupt:
            self.logger.info("Received shutdown signal...")
        finally:
            self.shutdown()

    def shutdown(self):
        self.logger.info("Shutting down Thala Ingestion Pipeline...")
        self.running = False
        self.slack_connector.close()
        #self.email_connector.close()
        self.jira_connector.close()
        self.logger.info("Shutdown complete.")

if __name__ == "__main__":
    pipeline = ThalaIngestionPipeline()
    
    # Handle graceful shutdown
    def signal_handler(sig, frame):
        pipeline.shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    pipeline.start()
