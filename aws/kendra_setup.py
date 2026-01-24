# setup_kendra.py
import boto3
import time
import os
from dotenv import load_dotenv

load_dotenv()

kendra = boto3.client('kendra', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-2'))
account_id = boto3.client('sts').get_caller_identity()['Account']
IAM = ""
# Role ARN from the previous step
KENDRA_ROLE_ARN = f"arn:aws:iam::IAM:role/KendraIndexRole"



def create_kendra_index():
    """Create Kendra index"""
    print("Creating Kendra Index...")
    
    index_response = kendra.create_index(
        Name='website-search-index',
        Edition='DEVELOPER_EDITION',  # Use DEVELOPER_EDITION for testing
        RoleArn=KENDRA_ROLE_ARN,
        Description='Index for technical website content extraction'
    )
    
    index_id = index_response['Id']
    print(f"✓ Index created with ID: {index_id}")
    
    # Wait for index to be active
    print("Waiting for index to be ACTIVE...")
    while True:
        index_desc = kendra.describe_index(Id=index_id)
        status = index_desc['Status']
        print(f"  Status: {status}")
        
        if status == 'ACTIVE':
            print("✓ Index is now ACTIVE")
            break
        elif status == 'FAILED':
            print("✗ Index creation failed")
            return None
        
        time.sleep(30)
    
    return index_id


def create_s3_data_source(index_id, bucket_name):
    """Create S3 data source for Kendra"""
    print(f"\nCreating S3 Data Source pointing to: {bucket_name}")
    
    data_source_response = kendra.create_data_source(
        Name='website-documents-source',
        Type='S3',
        Configuration={
            'S3Configuration': {
                'BucketName': bucket_name
            }
        },
        IndexId=index_id,
        RoleArn=KENDRA_ROLE_ARN,
        Description='S3 bucket containing website documents'
    )
    
    data_source_id = data_source_response['Id']
    print(f"✓ Data source created with ID: {data_source_id}")
    
    # Wait for data source to be active
    print("Waiting for data source to be ACTIVE...")
    while True:
        ds_desc = kendra.describe_data_source(
            Id=data_source_id,
            IndexId=index_id
        )
        status = ds_desc['Status']
        print(f"  Status: {status}")
        
        if status == 'ACTIVE':
            print("✓ Data source is now ACTIVE")
            break
        elif status == 'FAILED':
            print("✗ Data source creation failed")
            return None
        
        time.sleep(30)
    
    return data_source_id


def sync_data_source(index_id, data_source_id):
    """Sync the data source with the index"""
    print("\nSyncing data source with index...")
    
    sync_response = kendra.start_data_source_sync_job(
        Id=data_source_id,
        IndexId=index_id
    )
    
    execution_id = sync_response['ExecutionId']
    print(f"✓ Sync job started with execution ID: {execution_id}")
    
    # Monitor sync progress
    print("Monitoring sync progress...")
    while True:
        jobs = kendra.list_data_source_sync_jobs(
            Id=data_source_id,
            IndexId=index_id
        )
        
        if jobs['History']:
            status = jobs['History'][0]['Status']
            print(f"  Status: {status}")
            
            if status in ['SUCCEEDED', 'FAILED', 'ABORTED']:
                print(f"✓ Sync completed with status: {status}")
                break
        
        time.sleep(60)


if __name__ == "__main__":
    # Step 1: Create index
    index_id = create_kendra_index()
    
    if index_id:
        # Step 2: Create S3 data source (you need to have documents in S3)
        # For now, we'll skip this since you'll provide documents later
        print("\n" + "="*50)
        print(f"KENDRA INDEX ID: {index_id}")
        print("="*50)
        print("\nSave this Index ID for your Lambda function!")
        print("You can now upload documents to S3 and create a data source later.")
