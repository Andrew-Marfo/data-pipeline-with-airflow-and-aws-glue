from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import os
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from helpers.combine_csv_files import combine_csv_files

# Function to validate if files exist in the streaming folder
def validate_streams_in_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'music-streaming-services'
    objects = s3_hook.list_keys(bucket_name=bucket_name, prefix='streams/')

    if objects:
        return 'extract_and_combine_streams'
    else:
        return 'end_dag_if_no_streams_exists_task'
    
# Function to extract and combine streams
def extract_and_combine_streams(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    ti = kwargs['ti']

    # Process streams folder (combine multiple CSV files)
    streams_temp_path = combine_csv_files(s3_hook,"music-streaming-services","streams/", "combined_streams.csv")
    ti.xcom_push(key='streams_csv_path', value=streams_temp_path)
    print(f"Combined streams CSV saved to: {streams_temp_path}")

    # Process songs folder (single CSV file)
    songs_temp_path = combine_csv_files(s3_hook,"music-streaming-services","songs/", "songs.csv")
    ti.xcom_push(key='songs_csv_path', value=songs_temp_path)
    print(f"Songs CSV saved to: {songs_temp_path}")

    # Process users folder (single CSV file)
    users_temp_path = combine_csv_files(s3_hook,"music-streaming-services","users/", "users.csv")
    ti.xcom_push(key='users_csv_path', value=users_temp_path)
    print(f"Users CSV saved to: {users_temp_path}")

# Move extracted files to an intermediate bucket
def move_files_to_intermediate_bucket(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    ti = kwargs['ti']

    # Retrieve file paths from XCom
    streams_csv_path = ti.xcom_pull(task_ids='extract_and_combine_streams', key='streams_csv_path')
    songs_csv_path = ti.xcom_pull(task_ids='extract_and_combine_streams', key='songs_csv_path')
    users_csv_path = ti.xcom_pull(task_ids='extract_and_combine_streams', key='users_csv_path')

    # Define the destination bucket and folders
    intermediate_bucket = 'music-streaming-intermediate'
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

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'music_streaming_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# Branching Operator
check_streaming_data = BranchPythonOperator(
    task_id='check_streaming_data',
    python_callable=validate_streams_in_s3,
    provide_context=True,
    dag=dag
)

# Tasks for each branch
extract_and_combine_streams = PythonOperator(
    task_id='extract_and_combine_streams',
    python_callable=extract_and_combine_streams,
    provide_context=True,
    dag=dag
)

# Task to end the DAG if no streams exist
end_dag_if_no_streams_exists_task = EmptyOperator(
    task_id='end_dag_if_no_streams_exists_task',
    dag=dag
)

# Task to move files to an intermediate bucket
move_files_to_intermediate_bucket = PythonOperator(
    task_id='move_files_to_intermediate_bucket',
    python_callable=move_files_to_intermediate_bucket,
    provide_context=True,
    dag=dag
)

# Define dependencies
check_streaming_data >> [extract_and_combine_streams, end_dag_if_no_streams_exists_task]
extract_and_combine_streams >> move_files_to_intermediate_bucket
