from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Fix module path — must be BEFORE any local imports
sys.path.insert(0, '/opt/airflow')
os.environ['PYTHONPATH'] = '/opt/airflow'

# Verify path is set (shows in Airflow logs)
print(f"Python path: {sys.path}")

# --- Default settings for the DAG ---
default_args = {
    'owner': 'sithu',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

# --- Define the DAG ---
with DAG(
    dag_id='bangkok_property_pipeline',
    default_args=default_args,
    description='Bangkok Property Market Analytics Pipeline',
    schedule_interval='@daily',       # Runs every day automatically
    start_date=datetime(2024, 1, 1),
    catchup=False,                    # Don't backfill old runs
    tags=['bangkok', 'property', 'analytics'],
) as dag:

    def extract_task():
        df = extract_from_csv('/opt/airflow/data/raw_listings.csv')
        # Save to temp file so next task can read it
        df.to_csv('/opt/airflow/data/extracted.csv', index=False)
        print(f"Extracted {len(df)} rows")

    def transform_task():
        import pandas as pd
        df = pd.read_csv('/opt/airflow/data/extracted.csv')
        df_clean = transform_property_data(df)
        # Save cleaned data for load task
        df_clean.to_csv('/opt/airflow/data/transformed.csv', index=False)
        print(f"Transformed to {len(df_clean)} rows")

    def load_task():
        import pandas as pd
        df = pd.read_csv('/opt/airflow/data/transformed.csv')
        load_to_postgres(df)
        print(f"Loaded {len(df)} rows to PostgreSQL")

    # --- Define tasks ---
    t1_extract = PythonOperator(
        task_id='extract',
        python_callable=extract_task,
    )

    t2_transform = PythonOperator(
        task_id='transform',
        python_callable=transform_task,
    )

    t3_load = PythonOperator(
        task_id='load',
        python_callable=load_task,
    )

    # --- Set task order ---
    t1_extract >> t2_transform >> t3_load