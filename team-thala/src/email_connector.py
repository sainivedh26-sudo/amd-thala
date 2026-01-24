import imaplib
import email
import logging
import os
import time
from email.header import decode_header
from kafka_producer import KafkaMessageProducer
from dotenv import load_dotenv
import hashlib

load_dotenv()

class EmailConnector:
    def __init__(self):
        self.host = os.getenv('EMAIL_HOST', 'imap.gmail.com')
        self.port = int(os.getenv('EMAIL_PORT', '993'))
        self.username = os.getenv('EMAIL_USER')
        self.password = os.getenv('EMAIL_PASSWORD')
        self.kafka_producer = KafkaMessageProducer()
        self.topic = os.getenv('KAFKA_TOPIC_EMAIL', 'thala-email-events')
        self.logger = logging.getLogger(__name__)
        self.processed_emails = set()

    def connect(self):
        try:
            self.mail = imaplib.IMAP4_SSL(self.host, self.port)
            self.mail.login(self.username, self.password)
            self.mail.select('INBOX')
            self.logger.info("Connected to email server")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to email server: {e}")
            return False

    def fetch_new_emails(self):
        try:
            self.mail.select('INBOX')  # <-- Add this line
            status, messages = self.mail.search('UTF-8', 'UNSEEN')
            self.logger.info(f"Raw IMAP search result: {messages}")
            if status == 'OK':
                email_ids = messages[0].split()
                self.logger.info(f"Found {len(email_ids)} new emails")
                for email_id in email_ids:
                    self.process_email(email_id)
        except Exception as e:
            self.logger.error(f"Error fetching emails: {e}")

    def process_email(self, email_id):
        try:
            # Fetch email data
            status, msg_data = self.mail.fetch(email_id, '(RFC822)')
            
            if status == 'OK':
                email_body = msg_data[0][1]
                email_message = email.message_from_bytes(email_body)
                
                # Generate unique ID
                msg_id = email_message.get('Message-ID', '')
                unique_id = hashlib.md5(f"{msg_id}{email_id}".encode()).hexdigest()
                
                # Skip if already processed
                if unique_id in self.processed_emails:
                    return
                
                # Extract email details
                subject = self.decode_header_value(email_message.get('Subject', ''))
                sender = email_message.get('From', '')
                recipient = email_message.get('To', '')
                date = email_message.get('Date', '')
                
                # Get email body
                body = self.extract_body(email_message)
                
                # Transform to standardized format
                transformed_message = {
                    'id': f"email_{unique_id}",
                    'type': 'email',
                    'message_id': msg_id,
                    'subject': subject,
                    'sender': sender,
                    'recipient': recipient,
                    'date': date,
                    'body': body,
                    'metadata': {
                        'has_attachments': self.has_attachments(email_message),
                        'is_multipart': email_message.is_multipart(),
                        'content_type': email_message.get_content_type()
                    }
                }
                
                # Send to Kafka
                key = f"email_{unique_id}"
                self.kafka_producer.send_message(self.topic, transformed_message, key)
                
                # Mark as processed
                self.processed_emails.add(unique_id)
                
        except Exception as e:
            self.logger.error(f"Error processing email {email_id}: {e}")

    def decode_header_value(self, header_value):
        if header_value:
            decoded_header = decode_header(header_value)
            return ''.join([
                text.decode(encoding or 'utf-8') if isinstance(text, bytes) else text
                for text, encoding in decoded_header
            ])
        return ''

    def extract_body(self, email_message):
        body = ""
        if email_message.is_multipart():
            for part in email_message.walk():
                if part.get_content_type() == "text/plain":
                    charset = part.get_content_charset() or 'utf-8'
                    body = part.get_payload(decode=True).decode(charset, errors='ignore')
                    break
        else:
            charset = email_message.get_content_charset() or 'utf-8'
            body = email_message.get_payload(decode=True).decode(charset, errors='ignore')
        return body

    def has_attachments(self, email_message):
        for part in email_message.walk():
            if part.get_content_disposition() == 'attachment':
                return True
        return False

    def start_monitoring(self, interval=60):
        """Start continuous monitoring of emails"""
        self.logger.info("Starting email monitoring...")
        
        if not self.connect():
            return
        
        while True:
            try:
                self.fetch_new_emails()
                time.sleep(interval)
            except KeyboardInterrupt:
                self.logger.info("Stopping email monitoring...")
                break
            except Exception as e:
                self.logger.error(f"Error in email monitoring loop: {e}")
                # Reconnect if connection lost
                if not self.connect():
                    time.sleep(interval)

    def close(self):
        if hasattr(self, 'mail'):
            self.mail.close()
            self.mail.logout()
        self.kafka_producer.close()