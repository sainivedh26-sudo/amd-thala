@echo off
start cmd /k "cd thala-ingestion\src && python main.py"
start cmd /k "cd thala-ingestion\src && python save_slack_messages.py"
start cmd /k "cd thala-ingestion\src && python save_email_messages.py"
start cmd /k "cd thala-ingestion\src && python save_jira.py"