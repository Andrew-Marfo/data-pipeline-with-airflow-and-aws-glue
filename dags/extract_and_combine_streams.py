from airflow.providers.amazon.aws.hooks.s3 import S3Hook


# Function to extract and combine streams
from helpers.combine_csv_files import combine_csv_files


def extract_and_combine_streams(bucket_name,ti="ti"):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # Process streams folder (combine multiple CSV files)
    streams_temp_path = combine_csv_files(s3_hook,bucket_name,"streams/", "combined_streams.csv")
    ti.xcom_push(key='streams_csv_path', value=streams_temp_path)
    print(f"✅Combined streams CSV saved to: {streams_temp_path}")

    # Process songs folder (single CSV file)
    songs_temp_path = combine_csv_files(s3_hook,bucket_name,"songs/", "songs.csv")
    ti.xcom_push(key='songs_csv_path', value=songs_temp_path)
    print(f"✅Songs CSV saved to: {songs_temp_path}")

    # Process users folder (single CSV file)
    users_temp_path = combine_csv_files(s3_hook,bucket_name,"users/", "users.csv")
    ti.xcom_push(key='users_csv_path', value=users_temp_path)
    print(f"✅Users CSV saved to: {users_temp_path}")