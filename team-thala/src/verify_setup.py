"""
Verification script to check if all components are properly configured
Run this before starting the Slack bot
"""
import os
import sys
from pathlib import Path

def check_env_var(var_name, required=True):
    """Check if environment variable is set"""
    value = os.getenv(var_name)
    if value:
        print(f"✅ {var_name}: Set")
        return True
    else:
        if required:
            print(f"❌ {var_name}: NOT SET (Required)")
        else:
            print(f"⚠️  {var_name}: NOT SET (Optional)")
        return not required

def check_file(file_path, description):
    """Check if file exists"""
    if os.path.exists(file_path):
        print(f"✅ {description}: Found at {file_path}")
        return True
    else:
        print(f"❌ {description}: NOT FOUND at {file_path}")
        return False

def check_import(module_name, package_name=None):
    """Check if Python package can be imported"""
    try:
        __import__(module_name)
        print(f"✅ {package_name or module_name}: Installed")
        return True
    except ImportError:
        print(f"❌ {package_name or module_name}: NOT INSTALLED")
        return False

def main():
    print("=" * 60)
    print("Thala ITSM - Setup Verification")
    print("=" * 60)
    print()
    
    all_checks_passed = True
    
    # Check environment variables
    print("📋 Checking Environment Variables...")
    print("-" * 60)
    all_checks_passed &= check_env_var("GEMINI_API_KEY", required=True)
    all_checks_passed &= check_env_var("SLACK_BOT_TOKEN", required=True)
    all_checks_passed &= check_env_var("SLACK_APP_TOKEN", required=True)
    all_checks_passed &= check_env_var("KAFKA_BOOTSTRAP_SERVERS", required=False)
    all_checks_passed &= check_env_var("FLASK_API_URL", required=False)
    print()
    
    # Check files
    print("📁 Checking Required Files...")
    print("-" * 60)
    
    # Find project root (go up from src)
    current_dir = Path(__file__).parent
    project_root = current_dir.parent.parent
    
    all_checks_passed &= check_file(
        project_root / "another.csv",
        "Training data (another.csv)"
    )
    all_checks_passed &= check_file(
        current_dir / "gemini_predictor.py",
        "Gemini Predictor"
    )
    all_checks_passed &= check_file(
        current_dir / "incident_tracker.py",
        "Incident Tracker"
    )
    all_checks_passed &= check_file(
        current_dir / "slack_bot_ui.py",
        "Slack Bot UI"
    )
    print()
    
    # Check Python packages
    print("📦 Checking Python Packages...")
    print("-" * 60)
    all_checks_passed &= check_import("slack_bolt", "slack-bolt")
    all_checks_passed &= check_import("slack_sdk", "slack-sdk")
    all_checks_passed &= check_import("google.genai", "google-genai")
    all_checks_passed &= check_import("pandas", "pandas")
    all_checks_passed &= check_import("kafka", "kafka-python")
    all_checks_passed &= check_import("dotenv", "python-dotenv")
    print()
    
    # Verify another.csv structure
    print("🔍 Verifying another.csv Structure...")
    print("-" * 60)
    another_csv_path = project_root / "another.csv"
    if os.path.exists(another_csv_path):
        try:
            import pandas as pd
            df = pd.read_csv(another_csv_path)
            required_cols = ['Category', 'Severity', 'Description']
            
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                print(f"❌ Missing columns in another.csv: {missing_cols}")
                all_checks_passed = False
            else:
                print(f"✅ All required columns present: {required_cols}")
                print(f"   Total training examples: {len(df)}")
                print(f"   Categories: {df['Category'].nunique()}")
                print(f"   Severity levels: {df['Severity'].nunique()}")
        except Exception as e:
            print(f"❌ Error reading another.csv: {e}")
            all_checks_passed = False
    print()
    
    # Test Gemini API
    print("🔮 Testing Gemini API Connection...")
    print("-" * 60)
    gemini_key = os.getenv("GEMINI_API_KEY")
    if gemini_key:
        try:
            from google import genai
            client = genai.Client(api_key=gemini_key)
            # Try a simple test
            response = client.models.generate_content(
                model="gemini-2.0-flash-exp",
                contents="Say 'OK' if you can read this"
            )
            if response and response.text:
                print("✅ Gemini API: Connected successfully")
            else:
                print("⚠️  Gemini API: Connected but response unclear")
        except Exception as e:
            print(f"❌ Gemini API: Connection failed - {e}")
            all_checks_passed = False
    else:
        print("⏭️  Gemini API: Skipped (no API key)")
    print()
    
    # Summary
    print("=" * 60)
    if all_checks_passed:
        print("✅ ALL CHECKS PASSED!")
        print()
        print("You can now start the services:")
        print("  1. python kafka_consumer_to_flask.py")
        print("  2. python slack_bot_ui.py")
        print()
        print("Then test in Slack:")
        print("  /thala predict database connection timeout")
        print("  /thala latest_issue")
    else:
        print("❌ SOME CHECKS FAILED")
        print()
        print("Please fix the issues above before starting the services.")
        print()
        print("Common fixes:")
        print("  - Set missing environment variables in .env file")
        print("  - Install missing packages: pip install -r ui_requirements.txt")
        print("  - Ensure another.csv is in the project root")
        print("  - Verify Gemini API key is valid")
    print("=" * 60)
    
    return 0 if all_checks_passed else 1

if __name__ == "__main__":
    sys.exit(main())
