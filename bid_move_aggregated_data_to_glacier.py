import boto3
from datetime import datetime, timedelta
import pytz
import os

# AWS configurations
source_bucket = "your-source-bucket"
destination_bucket = "your-destination-bucket"
source_base_path = "s3://your-source-bucket/data/"
destination_base_path = "s3://your-destination-bucket/data/"

# Initialize boto3 client for interacting with S3
s3_client = boto3.client('s3')


# Function to get files older than 7 days from a specific S3 path
def get_files_older_than_days(bucket, prefix, days=7):
    """List files older than `days` from S3."""
    current_time = datetime.now(pytz.utc)
    cutoff_time = current_time - timedelta(days=days)

    # List files in the source S3 location
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files_to_move = []

    if 'Contents' in response:
        for file in response['Contents']:
            last_modified = file['LastModified']
            if last_modified < cutoff_time:
                files_to_move.append(file['Key'])

    return files_to_move


# Function to move files older than `days` from source to destination
def move_files_older_than_7_days(source_bucket, destination_bucket, source_base_path, destination_base_path, days=7):
    """Move all files older than `days` from source to destination."""

    # Get all partitioned files older than 7 days
    files_to_move = get_files_older_than_days(source_bucket, source_base_path, days)

    if len(files_to_move) == 0:
        print(f"No files older than {days} days.")
        return

    for file_key in files_to_move:
        # Compute destination key by replacing the source path with destination path
        destination_key = file_key.replace(source_base_path, destination_base_path)

        try:
            # Copy the file from source to destination
            print(f"Copying {file_key} to {destination_key}")
            s3_client.copy_object(
                CopySource={'Bucket': source_bucket, 'Key': file_key},
                Bucket=destination_bucket,
                Key=destination_key
            )

            # Delete the original file from the source
            print(f"Deleting {file_key} from source bucket")
            s3_client.delete_object(Bucket=source_bucket, Key=file_key)

        except Exception as e:
            print(f"Error processing {file_key}: {e}")


# Main function to execute the job
def main():
    # Call the function to move files older than 7 days
    move_files_older_than_7_days(source_bucket, destination_bucket, source_base_path, destination_base_path)


# Run the job
main()
