from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Function to delete files from the intermediate bucket
def delete_files_from_intermediate_bucket(intermediate_bucket):
    s3_hook = S3Hook(aws_conn_id='aws_default')

    # List all files in the songs, streams, and users folders
    folders_to_clear = ['songs/', 'streams/', 'users/']
    for folder in folders_to_clear:
        files_to_delete = s3_hook.list_keys(bucket_name=intermediate_bucket, prefix=folder)
        if files_to_delete:
            # Delete all files in the folder
            s3_hook.delete_objects(bucket=intermediate_bucket, keys=files_to_delete)
            print(f"✅Deleted {len(files_to_delete)} files from {folder} in the intermediate bucket.")
        else:
            print(f"No files to delete in {folder} in the intermediate bucket.")