"""
Fix Kafka storage issues by cleaning log directories
"""
import os
import shutil
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka data directories to clean
KAFKA_DATA_DIRS = [
    r"D:\kafka\kafka_2.13-4.1.0\kafka_2.13-4.1.0\kafka-data",
    r"D:\kafka\kafka_2.13-4.1.0\kafka_2.13-4.1.0\sfafka",
    r"C:\kafka\kafka-logs",
]

def clean_kafka_directories():
    """Clean Kafka log directories"""
    print("=" * 70)
    print("Cleaning Kafka Storage Directories")
    print("=" * 70)
    print()
    
    import stat
    import subprocess
    
    def remove_readonly(func, path, exc):
        """Handle readonly files"""
        try:
            os.chmod(path, stat.S_IWRITE)
            func(path)
        except Exception as e:
            logger.warning(f"Could not remove {path}: {e}")
    
    cleaned = []
    failed = []
    
    for dir_path in KAFKA_DATA_DIRS:
        path = Path(dir_path)
        if path.exists():
            try:
                logger.info(f"Deleting: {dir_path}")
                # First try with readonly handler
                shutil.rmtree(dir_path, onerror=remove_readonly)
                logger.info(f"✅ Successfully deleted: {dir_path}")
                cleaned.append(dir_path)
            except PermissionError as pe:
                logger.error(f"❌ Access denied deleting {dir_path}: {pe}")
                logger.warning("Kafka may still be running or files are locked.")
                logger.warning("Trying Windows RMDIR command...")
                try:
                    # Try using Windows rmdir command
                    result = subprocess.run(
                        ["rmdir", "/s", "/q", dir_path],
                        shell=True,
                        capture_output=True,
                        text=True
                    )
                    if result.returncode == 0 or not path.exists():
                        logger.info(f"✅ Successfully deleted using rmdir: {dir_path}")
                        cleaned.append(dir_path)
                    else:
                        logger.error(f"❌ RMDIR also failed: {result.stderr}")
                        failed.append(dir_path)
                except Exception as e:
                    logger.error(f"❌ Error with rmdir: {e}")
                    failed.append(dir_path)
            except Exception as e:
                logger.error(f"❌ Error deleting {dir_path}: {e}")
                failed.append(dir_path)
        else:
            logger.info(f"Directory doesn't exist (skipping): {dir_path}")
    
    # Recreate directories
    for dir_path in KAFKA_DATA_DIRS:
        path = Path(dir_path)
        if not path.exists():
            try:
                logger.info(f"Creating: {dir_path}")
                path.mkdir(parents=True, exist_ok=True)
                logger.info(f"✅ Successfully created: {dir_path}")
            except Exception as e:
                logger.error(f"❌ Error creating {dir_path}: {e}")
    
    print()
    print("=" * 70)
    if cleaned:
        print(f"✅ Cleaned {len(cleaned)} Kafka data directory(ies)")
    if failed:
        print(f"⚠️  {len(failed)} directory(ies) could not be deleted:")
        for f in failed:
            print(f"   - {f}")
        print()
        print("Manual steps:")
        print("1. Make sure Kafka is COMPLETELY stopped")
        print("2. Open Command Prompt as Administrator")
        print("3. Run:")
        for f in failed:
            print(f"   rmdir /s /q \"{f}\"")
        print()
        print("OR use the batch script: fix_kafka_complete.bat")
        print("   (Make sure to run it as Administrator)")
    print()
    if cleaned or not failed:
        print("Next steps:")
        print("1. If Kafka was running, restart it")
        print("2. Run: python create_kafka_topics.py")
    print("=" * 70)

if __name__ == "__main__":
    confirm = input("⚠️  This will DELETE all Kafka data. Make sure Kafka is STOPPED. Continue? (yes/no): ")
    if confirm.lower() == 'yes':
        clean_kafka_directories()
    else:
        print("Cancelled.")

