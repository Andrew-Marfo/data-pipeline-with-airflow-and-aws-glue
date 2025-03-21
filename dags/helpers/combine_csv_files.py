import pandas as pd

def combine_csv_files(s3_hook,bucket_name,folder_prefix, output_file_name):

        # List all files in the folder
        file_keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=folder_prefix)
        if not file_keys:
            raise ValueError(f"No files found in {folder_prefix}")
        
        # Read and combine all CSV files
        combined_df = pd.DataFrame()
        for file_key in file_keys:
            if file_key.endswith(".csv"):
                file_obj = s3_hook.get_key(file_key, bucket_name)
                df = pd.read_csv(file_obj.get()["Body"])
                combined_df = pd.concat([combined_df, df], ignore_index=True)
        
        # Save the combined DataFrame to a temporary CSV file
        temp_file_path = f"/tmp/{output_file_name}"
        combined_df.to_csv(temp_file_path, index=False)
        return temp_file_path   