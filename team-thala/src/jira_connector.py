import logging
from jira import JIRA
import os
from kafka_producer import KafkaMessageProducer
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta

load_dotenv()

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

    def fetch_new_issues(self, jql='project = KAN ORDER BY created DESC', max_results=10):
        """Fetch NEW issues that haven't been processed yet"""
        issues = self.jira.search_issues(jql, maxResults=max_results)
        for issue in issues:
            # Skip if already processed
            if issue.key in self.processed_issues:
                continue
            
            data = {
                "id": issue.key,
                "summary": issue.fields.summary,
                "description": issue.fields.description or "",
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
        """Monitor existing issues for status changes and resolutions"""
        if not self.monitored_issues:
            return
        
        try:
            # Build JQL for monitored issues
            issue_keys = list(self.monitored_issues.keys())
            if len(issue_keys) > 50:
                issue_keys = issue_keys[-50:]  # Limit to last 50 issues
            
            jql = f"key in ({','.join(issue_keys)}) ORDER BY updated DESC"
            issues = self.jira.search_issues(jql, maxResults=50)
            
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
    
    def handle_resolution(self, issue, previous_info):
        """Handle when an issue is resolved - extract resolution and auto-label"""
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
            
            # Format resolution message
            resolution_data = {
                "id": f"{issue.key}_resolution",
                "original_issue_key": issue.key,
                "summary": issue.fields.summary,
                "description": issue.fields.description or "",
                "resolution_comments": resolution_text,
                "status": issue.fields.status.name,
                "created": previous_info['created'],
                "resolved": issue.fields.updated,
                "resolution_time_hours": round(resolution_duration, 2),
                "reporter": issue.fields.reporter.displayName if issue.fields.reporter else "Unknown",
                "assignee": issue.fields.assignee.displayName if issue.fields.assignee else "Unknown",
                "priority": previous_info['priority'],
                "issue_type": previous_info['issue_type'],
                "labels": previous_info['labels'],
                "timestamp": issue.fields.updated,
                "incident_likelihood": incident_likelihood  # Auto-labeled!
            }
            
            self.logger.info(f"[RESOLUTION] {issue.key} auto-labeled as '{incident_likelihood}' (resolved in {resolution_duration:.1f}h)")
            self.kafka_producer.send_message(self.topic, resolution_data, key=f"{issue.key}_resolution")
            
        except Exception as e:
            self.logger.error(f"Error handling resolution for {issue.key}: {e}")
    
    def extract_resolution_comments(self, issue):
        """Extract resolution description from comments"""
        try:
            comments = self.jira.comments(issue.key)
            if not comments:
                return issue.fields.description or "No resolution comments"
            
            # Get the last few comments (usually contain resolution info)
            recent_comments = comments[-3:] if len(comments) >= 3 else comments
            resolution_text = " | ".join([comment.body for comment in recent_comments])
            
            return resolution_text[:500]  # Limit length
            
        except Exception as e:
            self.logger.warning(f"Could not extract comments for {issue.key}: {e}")
            return issue.fields.description or "Resolution details unavailable"
    
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
