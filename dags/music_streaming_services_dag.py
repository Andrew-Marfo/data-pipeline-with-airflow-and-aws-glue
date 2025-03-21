from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from dotenv import load_dotenv
import os
from helpers.validate_streams_in_s3 import validate_streams_in_s3
from helpers.move_files_to_intermediate_bucket import move_files_to_intermediate_bucket
from helpers.combine_csv_files import combine_csv_files
from helpers.validate_csv_files import validate_csv_files
from helpers.delete_intermediate_bucket_files import delete_files_from_intermediate_bucket
from helpers.archive_streams_data import archive_streams_data

# Loading environment variables from .env file
load_dotenv()
SOURCE_BUCKET = os.getenv("SOURCE_BUCKET_NAME")
INTERMEDIATE_BUCKET = os.getenv("INTERMEDIATE_BUCKET_NAME")
ARCHIVE_BUCKET = os.getenv("ARCHIVE_BUCKET_NAME")

# Function to extract and combine streams
def extract_and_combine_streams(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    ti = kwargs['ti']

    # Process streams folder (combine multiple CSV files)
    streams_temp_path = combine_csv_files(s3_hook,SOURCE_BUCKET,"streams/", "combined_streams.csv")
    ti.xcom_push(key='streams_csv_path', value=streams_temp_path)
    print(f"Combined streams CSV saved to: {streams_temp_path}")

    # Process songs folder (single CSV file)
    songs_temp_path = combine_csv_files(s3_hook,SOURCE_BUCKET,"songs/", "songs.csv")
    ti.xcom_push(key='songs_csv_path', value=songs_temp_path)
    print(f"Songs CSV saved to: {songs_temp_path}")

    # Process users folder (single CSV file)
    users_temp_path = combine_csv_files(s3_hook,SOURCE_BUCKET,"users/", "users.csv")
    ti.xcom_push(key='users_csv_path', value=users_temp_path)
    print(f"Users CSV saved to: {users_temp_path}")

# Function to validate CSV files and decide the next task
def validate_csv_files_and_decide(**kwargs):
    try:
        validate_csv_files(**kwargs)
        return 'move_files_to_intermediate_bucket'  # Proceed to the next task
    except ValueError as e:
        print(f"Validation failed: {e}")
        return 'end_dag_if_validation_fails_task'


# Move extracted files to an intermediate bucket
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
    python_callable=validate_streams_in_s3,  # Pass the function itself
    op_kwargs={'bucket_name': SOURCE_BUCKET},  # Pass arguments here
    provide_context=True,
    dag=dag
)

# Tasks for each branch
extract_and_combine_streams_task = PythonOperator(
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
move_files_to_intermediate_bucket_task = PythonOperator(
    task_id='move_files_to_intermediate_bucket',
    python_callable=move_files_to_intermediate_bucket,  # Pass the function itself
    op_kwargs={'intermediate_bucket': INTERMEDIATE_BUCKET},  # Pass arguments here
    provide_context=True,
    dag=dag
)

# Task to validate CSV files and decide the next task
validate_csv_files_task = BranchPythonOperator(
    task_id='validate_csv_files_and_decide',
    python_callable=validate_csv_files_and_decide,
    provide_context=True,
    dag=dag
)

# Task to end the DAG if validation fails
end_dag_if_validation_fails_task = EmptyOperator(
    task_id='end_dag_if_validation_fails_task',
    dag=dag
)

# Task to trigger the Glue job
trigger_glue_job = GlueJobOperator(
    task_id='trigger_glue_job',
    job_name="Transform and compute KPI's",
    aws_conn_id='aws_default',
    script_args={
        "--S3_SOURCE_PATH": "s3://music-streaming-intermediate/",
        "--DYNAMODB_TABLE": "music_kpis_table",
    },
    dag=dag
)

# Task to delete files from the intermediate bucket
delete_files_from_intermediate_bucket_task = PythonOperator(
    task_id='delete_files_from_intermediate_bucket',
    python_callable=delete_files_from_intermediate_bucket,  # Pass the function itself
    op_kwargs={'intermediate_bucket': INTERMEDIATE_BUCKET},  # Pass arguments here
    provide_context=True,
    dag=dag
)

# Task to archive streams data from the source bucket
archive_streams_data_task = PythonOperator(
    task_id='archive_streams_data',
    python_callable=archive_streams_data,  # Pass the function itself
    op_kwargs={'source_bucket': SOURCE_BUCKET, 'archive_bucket': ARCHIVE_BUCKET},  # Pass arguments here
    provide_context=True,
    dag=dag
)


# Define dependencies
check_streaming_data >> [extract_and_combine_streams_task, end_dag_if_no_streams_exists_task]
extract_and_combine_streams_task >> validate_csv_files_task
validate_csv_files_task >> [move_files_to_intermediate_bucket_task, end_dag_if_validation_fails_task]
move_files_to_intermediate_bucket_task >> trigger_glue_job
trigger_glue_job >> [delete_files_from_intermediate_bucket_task, archive_streams_data_task]