import json
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(source_bucket, folder_prefix, destination_bucket):
    # Create an S3 client
    s3 = boto3.client('s3')

    # List all objects under the source prefix
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=folder_prefix)

    if 'Contents' not in response:
        print(f"No objects found in {source_bucket}/{folder_prefix}")
        return

    for obj in response['Contents']:
        # Get the source object key
        source_key = obj['Key']

        # Construct the destination key by maintaining the relative path
        # For example: if folder_prefix is "folder/subfolder/", folder_prefix could be "newfolder/subfolder/"
        destination_key = source_key

        # Define the copy source
        copy_source = {
            'Bucket': source_bucket,
            'Key': source_key
        }

        # Perform the copy operation
        print(f"Copying {source_key} to {destination_bucket}/{destination_key}")
        s3.copy(copy_source, destination_bucket, destination_key)

    print("Recursive copy completed.")

def main():
    # Example Usage
    source_bucket = 'source-bucket-name'
    folder_prefix = 'folder/subfolder/'  # Prefix to copy from
    destination_bucket = 'destination-bucket-name'

    lambda_handler(source_bucket, folder_prefix, destination_bucket)

if __name__ == "__main__":
    main()
