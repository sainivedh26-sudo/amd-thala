from kafka import KafkaConsumer
import json
import os

consumer = KafkaConsumer(
    'thala-email-events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='thala-email-save-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'email_messages.jsonl'))
with open(output_path, 'a', encoding='utf-8') as f:
    for message in consumer:
        # Keep only relevant fields
        structured = {
            "id": message.value.get("id"),
            "timestamp": message.value.get("timestamp"),
            "subject": message.value.get("subject"),
            "sender": message.value.get("sender"),
            "recipient": message.value.get("recipient"),
            "body": message.value.get("body"),
            "source_system": message.value.get("source_system"),
        }
        json.dump(structured, f)
        f.write('\n')
        f.flush()
        print("Saved structured Email message:", structured)