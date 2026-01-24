"""
Incident Tracker - Tracks latest incidents from Kafka
Maintains a rolling window of recent incidents for /thala latest_issue command
"""
import logging
from datetime import datetime, timedelta
from collections import deque
import threading

class IncidentTracker:
    """
    Tracks recent incidents from Kafka messages
    Thread-safe for concurrent access
    """
    
    def __init__(self, max_incidents=100):
        self.logger = logging.getLogger(__name__)
        self.max_incidents = max_incidents
        self.incidents = deque(maxlen=max_incidents)  # Rolling window
        self.lock = threading.Lock()
        self.current_incident = None  # Latest ongoing incident
        
    def add_incident(self, incident_data):
        """
        Add a new incident to the tracker
        
        Args:
            incident_data: dict with keys: id, text, source, timestamp, status, etc.
        """
        with self.lock:
            # Parse timestamp
            if isinstance(incident_data.get('timestamp'), str):
                try:
                    timestamp = datetime.fromisoformat(incident_data['timestamp'].replace('Z', '+00:00'))
                except:
                    timestamp = datetime.now()
            else:
                timestamp = incident_data.get('timestamp', datetime.now())
            
            incident = {
                'id': incident_data.get('id'),
                'text': incident_data.get('text', ''),
                'source': incident_data.get('source', 'unknown'),
                'timestamp': timestamp,
                'status': incident_data.get('status', 'Open'),
                'category': incident_data.get('category'),
                'severity': incident_data.get('severity'),
                'user_id': incident_data.get('user_id'),
                'channel_id': incident_data.get('channel_id'),
                'discussion': []  # Will store discussion messages
            }
            
            self.incidents.append(incident)
            
            # Update current incident if this is open
            if incident['status'] == 'Open':
                self.current_incident = incident
                self.logger.info(f"[TRACKER] New incident tracked: {incident['id']} - {incident['text'][:50]}...")
    
    def add_discussion(self, incident_id, discussion_text, user_id=None):
        """Add a discussion message to an incident"""
        with self.lock:
            for incident in reversed(self.incidents):
                if incident['id'] == incident_id:
                    incident['discussion'].append({
                        'text': discussion_text,
                        'user_id': user_id,
                        'timestamp': datetime.now()
                    })
                    self.logger.debug(f"[TRACKER] Added discussion to {incident_id}")
                    return True
            return False
    
    def update_incident_status(self, incident_id, status, resolution_text=None):
        """Update incident status"""
        with self.lock:
            for incident in reversed(self.incidents):
                if incident['id'] == incident_id:
                    incident['status'] = status
                    if resolution_text:
                        incident['resolution'] = resolution_text
                    
                    # Clear current incident if it's resolved
                    if self.current_incident and self.current_incident['id'] == incident_id:
                        if status == 'Resolved':
                            self.current_incident = None
                    
                    self.logger.info(f"[TRACKER] Updated {incident_id} status to {status}")
                    return True
            return False
    
    def get_latest_incident(self):
        """Get the latest ongoing incident with all discussion context"""
        with self.lock:
            if not self.current_incident:
                # Try to find the most recent open incident
                for incident in reversed(self.incidents):
                    if incident['status'] == 'Open':
                        self.current_incident = incident
                        break
            
            return self.current_incident
    
    def get_recent_incidents(self, count=10, status=None):
        """Get recent incidents, optionally filtered by status"""
        with self.lock:
            incidents_list = list(self.incidents)
            
            if status:
                incidents_list = [i for i in incidents_list if i['status'] == status]
            
            # Return most recent first
            return list(reversed(incidents_list))[:count]
    
    def get_incident_by_id(self, incident_id):
        """Get a specific incident by ID"""
        with self.lock:
            for incident in reversed(self.incidents):
                if incident['id'] == incident_id:
                    return incident
            return None
    
    def clear_old_incidents(self, hours=72):
        """Clear incidents older than specified hours"""
        with self.lock:
            cutoff = datetime.now() - timedelta(hours=hours)
            # Convert deque to list, filter, convert back
            filtered = [i for i in self.incidents if i['timestamp'] > cutoff]
            self.incidents.clear()
            self.incidents.extend(filtered)
            self.logger.info(f"[TRACKER] Cleared old incidents, {len(self.incidents)} remaining")
    
    def get_stats(self):
        """Get tracker statistics"""
        with self.lock:
            open_count = sum(1 for i in self.incidents if i['status'] == 'Open')
            resolved_count = sum(1 for i in self.incidents if i['status'] == 'Resolved')
            
            return {
                'total': len(self.incidents),
                'open': open_count,
                'resolved': resolved_count,
                'current_incident': self.current_incident is not None
            }


# Singleton instance
_tracker_instance = None

def get_tracker():
    """Get or create singleton tracker instance"""
    global _tracker_instance
    if _tracker_instance is None:
        _tracker_instance = IncidentTracker()
    return _tracker_instance
