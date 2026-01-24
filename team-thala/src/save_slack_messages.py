from kafka import KafkaConsumer
import json
import os

consumer = KafkaConsumer(
    'thala-slack-events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='thala-slack-save-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'slack_messages.jsonl'))

# Load existing Slack message IDs to avoid duplicates
existing_ids = set()
if os.path.exists(output_path):
    with open(output_path, 'r', encoding='utf-8') as f_existing:
        for line in f_existing:
            try:
                data = json.loads(line)
                if "id" in data:
                    existing_ids.add(data["id"])
            except Exception:
                continue

with open(output_path, 'a', encoding='utf-8') as f:
    for message in consumer:
        msg_id = message.value.get("id")
        if msg_id and msg_id not in existing_ids:
            json.dump(message.value, f)
            f.write('\n')
            f.flush()
            existing_ids.add(msg_id)
            print("Saved Slack message:", message.value)