@echo off
start cmd /k "python main.py"
start cmd /k "python save_slack_messages.py"
start cmd /k "python save_jira.py"
