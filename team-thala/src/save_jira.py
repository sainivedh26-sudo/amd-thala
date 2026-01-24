import os
import json
from kafka import KafkaConsumer

output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'jira_messages.jsonl'))

# Load all existing issues into a dict: {id: issue_data}
existing_issues = {}
if os.path.exists(output_path):
    with open(output_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                data = json.loads(line)
                if "id" in data:
                    existing_issues[data["id"]] = data
            except Exception:
                continue

consumer = KafkaConsumer(
    'thala-jira-events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='thala-jira-save-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def save_all_issues(issues_dict, path):
    with open(path, 'w', encoding='utf-8') as f:
        for issue in issues_dict.values():
            json.dump(issue, f)
            f.write('\n')

for message in consumer:
    issue_id = message.value.get("id")
    if issue_id:
        structured = {
            "id": issue_id,
            "summary": message.value.get("summary"),
            "description": message.value.get("description"),
            "status": message.value.get("status"),
            "created": message.value.get("created"),
            "reporter": message.value.get("reporter"),
            "source_system": message.value.get("source_system"),
        }
        # Update or add the issue
        existing_issues[issue_id] = structured
        save_all_issues(existing_issues, output_path)
        print("Saved/Updated structured Jira message:", structured)