import os
from datetime import datetime
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from google.cloud import storage

# --- CONFIGURATION ---
# Your GCP Project ID
GCP_PROJECT_ID = "customer-exploration-demo" 
GCS_BUCKET_NAME = f"{GCP_PROJECT_ID}-raw-data"
BIGQUERY_DATASET = "amazon_reviews"

@dag(
    dag_id="amazon_reviews_ingestion",
    start_date=datetime(2025, 8, 6),
    schedule_interval=None,
    catchup=False,
    tags=['customer-exploration'],
)
def data_ingestion_dag():
    @task
    def download_data_to_gcs():
        """
        Downloads 100k records of the Amazon Reviews dataset and uploads to GCS as Parquet.
        """
        from datasets import load_dataset

        print("Loading 100,000 records from Hugging Face...")
        dataset = load_dataset(
            "McAuley-Lab/Amazon-Reviews-2023",
            "raw_review_Clothing_Shoes_and_Jewelry",
            split="train[:100000]", # 100,000 records
            trust_remote_code=True
        )
        df = dataset.to_pandas()

        # Basic processing to add a proper timestamp
        df['timestamp'] = pd.to_datetime(df['unixReviewTime'], unit='s')

        # Save as Parquet and upload to GCS
        file_name = "reviews.parquet"
        gcs_path = f"data/{file_name}"
        df.to_parquet(file_name)

        print(f"Uploading {file_name} to gs://{GCS_BUCKET_NAME}/{gcs_path}")
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(file_name)

        return gcs_path

    load_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="load_gcs_to_bigquery",
        bucket=GCS_BUCKET_NAME,
        source_objects=["{{ task_instance.xcom_pull(task_ids='download_data_to_gcs') }}"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.raw_reviews",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
    )

    transform_in_bigquery = BigQueryExecuteQueryOperator(
        task_id="transform_in_bigquery",
        sql=f"""
            CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.processed_reviews` AS
            SELECT
                user_id,
                parent_asin AS item_id,
                rating,
                timestamp AS event_timestamp
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.raw_reviews`
        """,
        use_legacy_sql=False,
    )

    # Define task dependencies
    download_data_to_gcs() >> load_gcs_to_bigquery >> transform_in_bigquery

data_ingestion_dag()