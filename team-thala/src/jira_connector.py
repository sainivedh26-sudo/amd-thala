import logging
from jira import JIRA
import os
import tempfile
from kafka_producer import KafkaMessageProducer
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta

load_dotenv()

# Optional: AWS attachment processor
try:
    from aws_attachment_processor import get_attachment_processor
    ATTACHMENT_PROCESSOR_AVAILABLE = True
except ImportError:
    ATTACHMENT_PROCESSOR_AVAILABLE = False

# Optional: Gemini LLM for intelligent classification
try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

class JiraConnector:
    def __init__(self, slack_connector=None):
        self.server = os.getenv('JIRA_URL')
        self.email = os.getenv('JIRA_EMAIL')
        self.api_token = os.getenv('JIRA_API_TOKEN')
        self.kafka_producer = KafkaMessageProducer()
        self.topic = os.getenv('KAFKA_TOPIC_JIRA', 'thala-jira-events')
        self.jira = JIRA(
            server=self.server,
            basic_auth=(self.email, self.api_token)
        )
        self.logger = logging.getLogger(__name__)
        self.processed_issues = set()  # Track NEW issues
        self.monitored_issues = {}  # Track status of monitored issues {issue_key: {'status': ..., 'created': ...}}
        
        # Reference to Slack connector for context sync
        self.slack_connector = slack_connector
        
        # Optional Gemini LLM setup
        self.use_gemini = os.getenv('USE_GEMINI_LABELING', 'false').lower() == 'true'
        self.gemini_client = None
        if self.use_gemini and GEMINI_AVAILABLE:
            gemini_api_key = os.getenv('GEMINI_API_KEY')
            if gemini_api_key:
                try:
                    self.gemini_client = genai.Client(api_key=gemini_api_key)
                    self.logger.info("Gemini LLM integration enabled for intelligent labeling")
                except Exception as e:
                    self.logger.warning(f"Failed to initialize Gemini: {e}")
                    self.use_gemini = False
            else:
                self.logger.warning("GEMINI_API_KEY not found, using rule-based labeling only")
                self.use_gemini = False
        
        # Initialize AWS attachment processor (optional)
        self.attachment_processor = None
        if ATTACHMENT_PROCESSOR_AVAILABLE:
            try:
                self.attachment_processor = get_attachment_processor()
                self.logger.info("AWS attachment processor initialized for Jira")
            except Exception as e:
                self.logger.warning(f"AWS attachment processor not available: {e}")

    def process_jira_attachments(self, issue):
        """
        Process attachments from Jira issue
        Downloads files, uploads to S3, extracts text, and returns extracted text
        
        Args:
            issue: Jira issue object
            
        Returns:
            str: Combined extracted text from all attachments (or None)
        """
        if not self.attachment_processor:
            return None
        
        try:
            attachments = issue.fields.attachment if hasattr(issue.fields, 'attachment') and issue.fields.attachment else []
            if not attachments:
                return None
            
            extracted_texts = []
            attachment_urls = []
            
            for attachment in attachments:
                try:
                    filename = attachment.filename
                    file_size = attachment.size
                    file_id = attachment.id
                    
                    # Skip if file is too large (max 10MB for Textract)
                    if file_size > 10 * 1024 * 1024:
                        self.logger.warning(f"[ATTACHMENT] Skipping large file {filename} ({file_size} bytes)")
                        continue
                    
                    self.logger.info(f"[ATTACHMENT] Processing Jira attachment {filename} (type: {attachment.mimeType})")
                    
                    # Download attachment from Jira to temp file
                    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1]) as temp_file:
                        attachment_path = temp_file.name
                        attachment.get(attachment_path)
                    
                    # Process attachment: upload to S3 → Textract
                    result = self.attachment_processor.process_attachment(
                        file_url_or_path=attachment_path,
                        source='jira',
                        file_id=file_id,
                        filename=filename,
                        download=False  # Already have local file
                    )
                    
                    # Clean up temp file
                    try:
                        os.unlink(attachment_path)
                    except:
                        pass
                    
                    if result.get('success'):
                        s3_url = result.get('s3_url')
                        extracted_text = result.get('extracted_text')
                        
                        if s3_url:
                            attachment_urls.append(s3_url)
                            self.logger.info(f"[ATTACHMENT] ✅ Uploaded to S3: {s3_url}")
                        
                        if extracted_text:
                            extracted_texts.append(f"[Attachment: {filename}]\n{extracted_text}")
                            self.logger.info(f"[ATTACHMENT] ✅ Extracted {len(extracted_text)} chars from {filename}")
                    else:
                        self.logger.warning(f"[ATTACHMENT] ❌ Failed to process {filename}")
                        
                except Exception as e:
                    self.logger.error(f"Error processing Jira attachment: {e}")
                    continue
            
            # Combine all extracted text
            if extracted_texts:
                combined_text = "\n\n".join(extracted_texts)
                return combined_text
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error processing Jira attachments: {e}")
            return None
    
    def fetch_new_issues(self, jql='project = KAN ORDER BY created DESC', max_results=10):
        """Fetch NEW issues that haven't been processed yet"""
        issues = self.jira.search_issues(jql, maxResults=max_results)
        for issue in issues:
            # Skip if already processed
            if issue.key in self.processed_issues:
                continue
            
            # Process attachments if any
            description = issue.fields.description or ""
            attachment_text = self.process_jira_attachments(issue)
            if attachment_text:
                # Append extracted text to description
                description = f"{description}\n\n{attachment_text}" if description else attachment_text
                self.logger.info(f"[ATTACHMENT] Added {len(attachment_text)} chars from attachments to issue {issue.key}")
            
            data = {
                "id": issue.key,
                "summary": issue.fields.summary,
                "description": description,  # Now includes extracted text from attachments
                "status": issue.fields.status.name,
                "created": issue.fields.created,
                "reporter": issue.fields.reporter.displayName if issue.fields.reporter else "Unknown",
                "timestamp": issue.fields.created,
                "priority": issue.fields.priority.name if issue.fields.priority else "Medium",
                "issue_type": issue.fields.issuetype.name if issue.fields.issuetype else "Task",
                "labels": issue.fields.labels if issue.fields.labels else []
            }
            self.logger.info(f"Fetched NEW Jira issue: {issue.key} - Status: {data['status']}")
            self.kafka_producer.send_message(self.topic, data, key=issue.key)
            self.processed_issues.add(issue.key)
            
            # Add to monitored issues for status tracking
            self.monitored_issues[issue.key] = {
                'status': issue.fields.status.name,
                'created': issue.fields.created,
                'priority': data['priority'],
                'issue_type': data['issue_type'],
                'labels': data['labels']
            }
            
            # Sync with Slack context queue
            if self.slack_connector:
                try:
                    self.slack_connector.sync_jira_issue(data)
                except Exception as e:
                    self.logger.warning(f"Failed to sync Jira issue to Slack context: {e}")
    
    def monitor_issue_updates(self):
        """Monitor existing issues for status changes, resolutions, and deletions"""
        if not self.monitored_issues:
            return
        
        try:
            # Build JQL for monitored issues
            issue_keys = list(self.monitored_issues.keys())
            if len(issue_keys) > 50:
                issue_keys = issue_keys[-50:]  # Limit to last 50 issues
            
            jql = f"key in ({','.join(issue_keys)}) ORDER BY updated DESC"
            
            try:
                issues = self.jira.search_issues(jql, maxResults=50)
                found_keys = {issue.key for issue in issues}
            except Exception as search_error:
                self.logger.warning(f"Error searching issues: {search_error}")
                # If search fails, try checking individual issues
                found_keys = set()
                for key in issue_keys[:10]:  # Limit to 10 for performance
                    try:
                        issue = self.jira.issue(key)
                        found_keys.add(key)
                    except Exception as e:
                        # Issue not found (likely deleted)
                        error_str = str(e).lower()
                        if 'not found' in error_str or 'does not exist' in error_str or '404' in error_str:
                            self.logger.info(f"🔍 Issue {key} not found in Jira (deleted)")
                        found_keys.discard(key)
            
            # Check for deleted issues (in monitored_issues but not found in search)
            deleted_keys = set(issue_keys) - found_keys
            for deleted_key in deleted_keys:
                if deleted_key in self.monitored_issues:
                    self.logger.info(f"🔍 Issue {deleted_key} not found in Jira - likely deleted")
                    self.handle_deletion(deleted_key)
                    # Remove from monitored issues
                    del self.monitored_issues[deleted_key]
            
            # Process found issues
            if 'issues' in locals():
                for issue in issues:
                    current_status = issue.fields.status.name
                    previous_info = self.monitored_issues.get(issue.key)
                    
                    if not previous_info:
                        continue
                    
                    previous_status = previous_info['status']
                    
                    # Check if status changed to resolved/done
                    if previous_status != current_status and current_status in ['Done', 'Resolved', 'Closed']:
                        self.logger.info(f"Issue {issue.key} status changed: {previous_status} → {current_status}")
                        self.handle_resolution(issue, previous_info)
                        
                        # Sync resolution to Slack context queue
                        if self.slack_connector:
                            try:
                                self.slack_connector.sync_jira_resolution(issue.key)
                            except Exception as e:
                                self.logger.warning(f"Failed to sync Jira resolution to Slack context: {e}")
                        
                    # Update monitored status
                    self.monitored_issues[issue.key]['status'] = current_status
                
        except Exception as e:
            self.logger.error(f"Error monitoring issue updates: {e}")
    
    def handle_deletion(self, issue_key):
        """Handle when an issue is deleted from Jira - mark as Resolved in Elasticsearch"""
        try:
            self.logger.info(f"[DELETION] Marking {issue_key} as Resolved (deleted from Jira)")
            
            # Send status update message to mark as resolved
            status_update = {
                "id": f"{issue_key}_deleted",
                "type": "status_update",
                "action": "mark_resolved",
                "original_issue_id": issue_key,
                "status": "Resolved",
                "resolution_text": "Issue deleted from Jira",
                "resolved_by": "System (Jira Deletion)",
                "resolved_at": datetime.utcnow().isoformat(),
                "timestamp": datetime.utcnow().isoformat()
            }
            
            self.kafka_producer.send_message(self.topic, status_update, key=f"delete_{issue_key}")
            self.logger.info(f"[DELETION] ✅ Sent deletion update for {issue_key} to Kafka")
            
        except Exception as e:
            self.logger.error(f"Error handling deletion for {issue_key}: {e}")
    
    def handle_resolution(self, issue, previous_info):
        """Handle when an issue is resolved - send status update instead of creating new entry"""
        try:
            # Extract resolution comments
            resolution_text = self.extract_resolution_comments(issue)
            
            # Calculate resolution time
            created_time = datetime.fromisoformat(previous_info['created'].replace('Z', '+00:00'))
            resolved_time = datetime.fromisoformat(issue.fields.updated.replace('Z', '+00:00'))
            resolution_duration = (resolved_time - created_time).total_seconds() / 3600  # Hours
            
            # Auto-determine incident likelihood
            incident_likelihood = self.determine_incident_likelihood(
                issue, 
                previous_info, 
                resolution_duration,
                resolution_text
            )
            
            # Send status update message instead of creating new entry
            status_update = {
                "id": f"{issue.key}_status_update",
                "type": "status_update",
                "action": "mark_resolved",
                "original_issue_id": issue.key,  # This is the key - use original issue ID
                "status": issue.fields.status.name,
                "resolution_text": resolution_text,
                "resolved_by": issue.fields.assignee.displayName if issue.fields.assignee else "Unknown",
                "resolved_at": issue.fields.updated,
                "incident_likelihood": incident_likelihood,
                "resolution_time_hours": round(resolution_duration, 2),
                "timestamp": issue.fields.updated
            }
            
            self.logger.info(f"[RESOLUTION] {issue.key} auto-labeled as '{incident_likelihood}' (resolved in {resolution_duration:.1f}h)")
            self.kafka_producer.send_message(self.topic, status_update, key=f"update_{issue.key}")
            
        except Exception as e:
            self.logger.error(f"Error handling resolution for {issue.key}: {e}")
    
    def extract_resolution_comments(self, issue):
        """Extract resolution description from comments (including attachment text)"""
        try:
            comments = self.jira.comments(issue.key)
            comment_texts = []
            
            # Process each comment for attachments
            for comment in comments:
                comment_body = comment.body
                
                # Check if comment has attachments and process them
                if hasattr(comment, 'attachments') and comment.attachments:
                    attachment_text = self.process_jira_comment_attachments(comment)
                    if attachment_text:
                        comment_body = f"{comment_body}\n\n{attachment_text}" if comment_body else attachment_text
                
                comment_texts.append(comment_body)
            
            if not comment_texts:
                return issue.fields.description or "No resolution comments"
            
            # Get the last few comments (usually contain resolution info)
            recent_comments = comment_texts[-3:] if len(comment_texts) >= 3 else comment_texts
            resolution_text = " | ".join(recent_comments)
            
            return resolution_text[:500]  # Limit length
            
        except Exception as e:
            self.logger.warning(f"Could not extract comments for {issue.key}: {e}")
            return issue.fields.description or "Resolution details unavailable"
    
    def process_jira_comment_attachments(self, comment):
        """
        Process attachments from Jira comment
        
        Args:
            comment: Jira comment object
            
        Returns:
            str: Combined extracted text from comment attachments (or None)
        """
        if not self.attachment_processor or not hasattr(comment, 'attachments'):
            return None
        
        try:
            attachments = comment.attachments
            if not attachments:
                return None
            
            extracted_texts = []
            
            for attachment in attachments:
                try:
                    filename = attachment.filename
                    file_size = attachment.size
                    file_id = attachment.id
                    
                    if file_size > 10 * 1024 * 1024:
                        continue
                    
                    # Download attachment to temp file
                    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1]) as temp_file:
                        attachment_path = temp_file.name
                        attachment.get(attachment_path)
                    
                    # Process attachment
                    result = self.attachment_processor.process_attachment(
                        file_url_or_path=attachment_path,
                        source='jira',
                        file_id=file_id,
                        filename=filename,
                        download=False
                    )
                    
                    # Clean up
                    try:
                        os.unlink(attachment_path)
                    except:
                        pass
                    
                    if result.get('success') and result.get('extracted_text'):
                        extracted_texts.append(f"[Comment Attachment: {filename}]\n{result['extracted_text']}")
                        
                except Exception as e:
                    self.logger.error(f"Error processing comment attachment: {e}")
                    continue
            
            if extracted_texts:
                return "\n\n".join(extracted_texts)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error processing Jira comment attachments: {e}")
            return None
    
    def determine_incident_likelihood(self, issue, previous_info, resolution_time_hours, resolution_text):
        """Auto-determine if this was a real incident using rules + optional LLM"""
        
        # If Gemini is enabled, use LLM for intelligent classification
        if self.use_gemini and self.gemini_client:
            return self.determine_likelihood_with_gemini(issue, previous_info, resolution_time_hours, resolution_text)
        
        # Otherwise, use rule-based classification
        return self.determine_likelihood_with_rules(previous_info, resolution_time_hours)
    
    def determine_likelihood_with_rules(self, previous_info, resolution_time_hours):
        """Rule-based incident likelihood determination"""
        priority = previous_info['priority']
        issue_type = previous_info['issue_type']
        labels = [label.lower() for label in previous_info['labels']]
        
        # Rule 1: High/Critical priority → Likely
        if priority in ['Highest', 'Critical', 'High']:
            return "Likely"
        
        # Rule 2: Incident-related labels → Likely
        incident_labels = ['incident', 'outage', 'downtime', 'critical', 'production', 'urgent', 'emergency']
        if any(label in incident_labels for label in labels):
            return "Likely"
        
        # Rule 3: Issue type indicates incident → Likely
        incident_types = ['incident', 'bug', 'production bug', 'critical bug']
        if issue_type.lower() in incident_types:
            return "Likely"
        
        # Rule 4: Resolution time > 1 hour → Likely (significant issue)
        if resolution_time_hours > 1.0:
            return "Likely"
        
        # Rule 5: Quick resolution < 30 minutes → Not Likely (minor issue or question)
        if resolution_time_hours < 0.5:
            return "Not Likely"
        
        # Default: Medium priority, moderate time → Not Likely
        return "Not Likely"
    
    def determine_likelihood_with_gemini(self, issue, previous_info, resolution_time_hours, resolution_text):
        """Use Gemini LLM for intelligent incident classification"""
        try:
            prompt = f"""You are an ITSM incident classifier. Analyze this Jira ticket and determine if it was a real incident.

Ticket Details:
- Issue Key: {issue.key}
- Summary: {issue.fields.summary}
- Description: {issue.fields.description or 'N/A'}
- Priority: {previous_info['priority']}
- Type: {previous_info['issue_type']}
- Labels: {', '.join(previous_info['labels']) if previous_info['labels'] else 'None'}
- Resolution Time: {resolution_time_hours:.1f} hours
- Resolution Comments: {resolution_text}

Classification Criteria:
- "Likely" = Real incident affecting users/systems (outages, crashes, errors, performance issues)
- "Not Likely" = Questions, feature requests, minor cosmetic issues, scheduled maintenance

Respond with ONLY one word: "Likely" or "Not Likely"
"""
            
            contents = [
                types.Content(
                    role="user",
                    parts=[types.Part.from_text(text=prompt)]
                )
            ]
            
            response = self.gemini_client.models.generate_content(
                model="gemini-2.0-flash-exp",
                contents=contents
            )
            
            result = response.text.strip()
            
            # Validate response
            if "Likely" in result and "Not Likely" not in result:
                self.logger.info(f"Gemini classified {issue.key} as: Likely")
                return "Likely"
            elif "Not Likely" in result:
                self.logger.info(f"Gemini classified {issue.key} as: Not Likely")
                return "Not Likely"
            else:
                # Fallback to rules if LLM response unclear
                self.logger.warning(f"Gemini gave unclear response: {result}, using rules")
                return self.determine_likelihood_with_rules(previous_info, resolution_time_hours)
                
        except Exception as e:
            self.logger.error(f"Error using Gemini for classification: {e}")
            # Fallback to rule-based
            return self.determine_likelihood_with_rules(previous_info, resolution_time_hours)
    
    def start_monitoring(self, interval=15):
        """Enhanced: Monitor both NEW issues AND status updates/resolutions"""
        self.logger.info("Starting Jira monitoring (new issues + status updates)...")
        while True:
            try:
                self.fetch_new_issues()  # Check for new issues
                self.monitor_issue_updates()  # Check for status changes/resolutions
                time.sleep(interval)
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.logger.error(f"Error in Jira monitoring: {e}")
    
    def close(self):
        """Close Jira connector"""
        self.kafka_producer.close()


if __name__ == "__main__":
    import logging
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        # Create and start Jira connector
        logger.info("=" * 60)
        logger.info("Starting Jira Connector")
        logger.info("=" * 60)
        logger.info("Features:")
        logger.info("  ✓ Monitor new Jira issues")
        logger.info("  ✓ Track issue status changes")
        logger.info("  ✓ Automatic resolution detection")
        logger.info("  ✓ Attachment processing (S3 + Textract)")
        logger.info("  ✓ Image text extraction for incident context")
        logger.info("=" * 60)
        
        connector = JiraConnector()
        
        # Use 15 second interval (checks for new issues every 15 seconds)
        # Change to 60 for production
        logger.info("Starting Jira monitoring (interval: 15 seconds)...")
        logger.info("Monitoring for:")
        logger.info("  - New Jira issues (automatically creates incidents)")
        logger.info("  - Issue status changes (tracks resolutions)")
        logger.info("  - Attachments (uploads to S3, extracts text)")
        logger.info("")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 60)
        
        connector.start_monitoring(interval=15)
    except KeyboardInterrupt:
        logger.info("Shutting down Jira Connector...")
        connector.close()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
