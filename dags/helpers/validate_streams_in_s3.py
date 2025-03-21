from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Function to validate if files exist in the streaming folder
def validate_streams_in_s3(bucket_name):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    objects = s3_hook.list_keys(bucket_name=bucket_name, prefix='streams/')

    if objects:
        return 'extract_and_combine_streams'
    else:
        return 'end_dag_if_no_streams_exists_task'