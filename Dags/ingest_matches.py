from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
import logging

# Add the ../scripts directory to Python path to import the custom fetch function
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))

from api_fetcher import fetch_match_data, fetch_match_summaries

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 14), # start time (this is when the club world cup starts)
    "retries": 1, # amount of retries if the task fails
}

# definition of our DAG
with DAG(
    dag_id="ingest_cwc_matches", # unique identifer for our dag
    description="Ingest Club World Cup match data from ESPN API endpoints", # short descirption on what our DAG does
    default_args=default_args, # using our arguments
    schedule_interval="@daily", # set when we want our DAG to run (@daily, @hourly, etc.)
    catchup=False,
    tags=["club_world_cup", "ingestion"],  # Tags for organization in Airflow UI
) as dag:
    
    # Define the task that will run the data fetcher
    fetch_matches = PythonOperator(
        task_id="fetch_match_data",
        python_callable=fetch_match_data,
        provide_context=True
    )
    fetch_summaries = PythonOperator(
        task_id="fetch_match_summaries",
        python_callable=fetch_match_summaries,
        provide_context = True
    )
    fetch_matches >> fetch_summaries


dag = dag