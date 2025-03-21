from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def move_files_to_intermediate_bucket(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    ti = kwargs['ti']
    intermediate_bucket = kwargs['intermediate_bucket']

    # Retrieve file paths from XCom
    streams_csv_path = ti.xcom_pull(task_ids='extract_and_combine_streams', key='streams_csv_path')
    songs_csv_path = ti.xcom_pull(task_ids='extract_and_combine_streams', key='songs_csv_path')
    users_csv_path = ti.xcom_pull(task_ids='extract_and_combine_streams', key='users_csv_path')

    # Define the destination bucket folders
    streams_dest_key = 'streams/combined_streams.csv'
    songs_dest_key = 'songs/songs.csv'
    users_dest_key = 'users/users.csv'

    # Upload files to the intermediate bucket
    s3_hook.load_file(
        filename=streams_csv_path,
        key=streams_dest_key,
        bucket_name=intermediate_bucket,
        replace=True
    )
    print(f"✅Streams CSV uploaded to s3://{intermediate_bucket}/{streams_dest_key}")

    s3_hook.load_file(
        filename=songs_csv_path,
        key=songs_dest_key,
        bucket_name=intermediate_bucket,
        replace=True
    )
    print(f"✅Songs CSV uploaded to s3://{intermediate_bucket}/{songs_dest_key}")

    s3_hook.load_file(
        filename=users_csv_path,
        key=users_dest_key,
        bucket_name=intermediate_bucket,
        replace=True
    )
    print(f"✅Users CSV uploaded to s3://{intermediate_bucket}/{users_dest_key}")