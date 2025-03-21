import pandas as pd

def validate_csv_files(**kwargs):
    ti = kwargs['ti']

    # Retrieve file paths from XCom
    streams_csv_path = ti.xcom_pull(task_ids='extract_and_combine_streams', key='streams_csv_path')
    songs_csv_path = ti.xcom_pull(task_ids='extract_and_combine_streams', key='songs_csv_path')
    users_csv_path = ti.xcom_pull(task_ids='extract_and_combine_streams', key='users_csv_path')

    # Function to validate a CSV file
    def validate_csv(file_path, required_columns):
        df = pd.read_csv(file_path)
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"❌Missing columns in {file_path}: {missing_columns}")
        print(f"✅Validation successful for {file_path}")

    # Define required columns for each file
    streams_required_columns = ['user_id', 'track_id', 'listen_time']
    songs_required_columns = ['track_id', 'track_name', 'artists', 'track_genre', 'duration_ms', 'popularity']
    users_required_columns = ['user_id', 'user_name', 'user_country']

    # Validate each file
    validate_csv(streams_csv_path, streams_required_columns)
    validate_csv(songs_csv_path, songs_required_columns)
    validate_csv(users_csv_path, users_required_columns)

    print("✅All CSV files validated successfully.")