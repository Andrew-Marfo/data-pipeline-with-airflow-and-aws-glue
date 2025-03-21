# Music Streaming Data Pipeline

A robust Apache Airflow-based ETL pipeline that processes and analyzes music streaming data using AWS services.

## Overview

This project implements an automated data pipeline that:
1. Monitors an S3 bucket for new streaming data
2. Validates and combines CSV files
3. Processes the data using AWS Glue
4. Computes music streaming KPIs
5. Stores results in DynamoDB
6. Archives processed data

## Architecture

The pipeline uses the following AWS services:
- Amazon S3 (source, intermediate, and archive buckets)
- AWS Glue (data transformation)
- Amazon DynamoDB (KPI storage)

## Prerequisites

- Apache Airflow (Astronomer Runtime 12.7.1)
- Python 3.x
- AWS account with appropriate permissions
- Required Python packages (see `requirements.txt`)

## Project Structure

```
.
├── dags/
│   ├── music_streaming_services_dag.py    # Main DAG file
│   ├── helpers/                           # Helper functions
│   │   ├── extract_and_combine_streams.py
│   │   ├── validate_csv_files.py
│   │   └── ...
├── glue_script/
│   └── glue_script.py                     # AWS Glue transformation script
├── tests/
│   └── dags/
│       └── test_dag_example.py
└── requirements.txt
```

## Environment Setup

1. Create a `.env` file with the following variables:
```
SOURCE_BUCKET_NAME=your-source-bucket
INTERMEDIATE_BUCKET_NAME=your-intermediate-bucket
ARCHIVE_BUCKET_NAME=your-archive-bucket
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## DAG Overview

The `music_streaming_pipeline` DAG performs the following steps:

1. Monitors S3 source bucket for new streaming data
2. Extracts and combines CSV files
3. Validates data structure
4. Moves validated files to intermediate bucket
5. Triggers Glue job for transformation
6. Computes streaming KPIs
7. Stores results in DynamoDB
8. Archives processed data
9. Cleans up intermediate files

## AWS Glue Transformation

The Glue job:
- Processes streaming data, user data, and song data
- Computes daily genre-level KPIs
- Identifies top songs and genres
- Handles data cleaning and missing values

## Running Locally

1. Start Airflow:
```bash
astro dev start
```

2. Access Airflow UI:
- URL: http://localhost:8083
- Default credentials: admin/admin