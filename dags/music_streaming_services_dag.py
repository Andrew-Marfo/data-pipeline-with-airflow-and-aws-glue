from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from helpers.extract_and_combine_streams import extract_and_combine_streams
from helpers.validate_streams_in_s3 import validate_streams_in_s3
from helpers.move_files_to_intermediate_bucket import move_files_to_intermediate_bucket
from helpers.validate_csv_files import validate_csv_files
from helpers.delete_intermediate_bucket_files import delete_files_from_intermediate_bucket
from helpers.archive_streams_data import archive_streams_data


# Loading environment variables from .env file
load_dotenv()
SOURCE_BUCKET = os.getenv("SOURCE_BUCKET_NAME")
INTERMEDIATE_BUCKET = os.getenv("INTERMEDIATE_BUCKET_NAME")
ARCHIVE_BUCKET = os.getenv("ARCHIVE_BUCKET_NAME")

# Function to validate CSV files and decide the next task
def validate_csv_files_and_decide(**kwargs):
    try:
        validate_csv_files(**kwargs)
        return 'move_files_to_intermediate_bucket'
    except ValueError as e:
        print(f"❌Validation failed: {e}")
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

#Starting the dag task
start_dag_task = EmptyOperator(
    task_id='start_dag_task',
    dag=dag
)

# S3 Sensor to check for new files in the streams folder
sense_streaming_data = S3KeySensor(
    task_id='sense_streaming_data',
    bucket_key=f's3://{SOURCE_BUCKET}/streams/',
    aws_conn_id='aws_default',
    timeout=18 * 60 * 60,  # 18 hours timeout
    poke_interval=60 * 5,  # Check every 5 minutes
    mode='poke',
    dag=dag
)

# Tasks for each branch
extract_and_combine_streams_task = PythonOperator(
    task_id='extract_and_combine_streams',
    python_callable=extract_and_combine_streams,
    op_kwargs={'bucket_name': SOURCE_BUCKET}, 
    provide_context=True,
    dag=dag
)

# Task to move files to an intermediate bucket
move_files_to_intermediate_bucket_task = PythonOperator(
    task_id='move_files_to_intermediate_bucket',
    python_callable=move_files_to_intermediate_bucket, 
    op_kwargs={'intermediate_bucket': INTERMEDIATE_BUCKET}, 
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
        "--S3_SOURCE_PATH": f"s3://{INTERMEDIATE_BUCKET}/",
        "--DYNAMODB_TABLE": "music_kpis_table",
    },
    dag=dag
)

# Task to delete files from the intermediate bucket
delete_files_from_intermediate_bucket_task = PythonOperator(
    task_id='delete_files_from_intermediate_bucket',
    python_callable=delete_files_from_intermediate_bucket,
    op_kwargs={'intermediate_bucket': INTERMEDIATE_BUCKET}, 
    provide_context=True,
    dag=dag
)

# Task to archive streams data from the source bucket
archive_streams_data_task = PythonOperator(
    task_id='archive_streams_data',
    python_callable=archive_streams_data,
    op_kwargs={'source_bucket': SOURCE_BUCKET, 'archive_bucket': ARCHIVE_BUCKET},
    provide_context=True,
    dag=dag
)

# End dag task
end_dag_task = EmptyOperator(
    task_id='end_dag_task',
    dag=dag
)

# Define dependencies
start_dag_task >> sense_streaming_data  >> extract_and_combine_streams_task
extract_and_combine_streams_task >> validate_csv_files_task
validate_csv_files_task >> [move_files_to_intermediate_bucket_task, end_dag_if_validation_fails_task]
move_files_to_intermediate_bucket_task >> trigger_glue_job
trigger_glue_job >> [delete_files_from_intermediate_bucket_task, archive_streams_data_task] >> end_dag_task