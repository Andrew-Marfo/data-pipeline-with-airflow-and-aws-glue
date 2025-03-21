from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Function to archive streams data from the source bucket
def archive_streams_data(source_bucket, archive_bucket):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_client = s3_hook.get_conn()  # Directly access the S3 client for more control

    # List all files in the streams folder of the source bucket
    files_to_archive = s3_hook.list_keys(bucket_name=source_bucket, prefix='streams/')

    if not files_to_archive:
        print("No files to archive in the streams folder of the source bucket.")
        return

    # Copy each file to the archive bucket with Glacier storage class
    for file_key in files_to_archive:
        # Copy file to archive bucket with Glacier storage class
        s3_client.copy_object(
            CopySource={'Bucket': source_bucket, 'Key': file_key},
            Bucket=archive_bucket,
            Key=file_key,
            StorageClass='DEEP_ARCHIVE'  # Specify Glacier storage class
        )
        print(f"File {file_key} copied to archive bucket (Glacier): s3://{archive_bucket}/{file_key}")

        # Delete file from source bucket
        s3_hook.delete_objects(bucket=source_bucket, keys=[file_key])
        print(f"File {file_key} deleted from source bucket.")
