from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

# Add preprocessing module to path if needed, or assume it's installed/available
# Ideally, we would import the pipeline function.
# For this example, we assume the code is deployed where Airflow can reach it.

sys.path.append(r"c:\Users\saade\Documents\bigdata_ProjetFinDeModule\preprocessing")

try:
    from pipeline import main as run_pipeline
except ImportError:
    run_pipeline = None

def preprocessing_task(**kwargs):
    if not run_pipeline:
        raise ImportError("Could not import preprocessing pipeline")
    
    input_file = kwargs.get('input_file', r"c:\Users\saade\Documents\bigdata_ProjetFinDeModule\kafka project\ai_vs_human_news.csv")
    output_file = kwargs.get('output_file', r"c:\Users\saade\Documents\bigdata_ProjetFinDeModule\preprocessing\processed_news.csv")
    
    run_pipeline(input_file, output_file)

default_args = {
    'owner': 'member2',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'preprocessing_pipeline',
    default_args=default_args,
    description='A simple preprocessing DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['bigdata', 'preprocessing'],
) as dag:

    t1 = PythonOperator(
        task_id='run_preprocessing',
        python_callable=preprocessing_task,
        op_kwargs={
            'input_file': r"c:\Users\saade\Documents\bigdata_ProjetFinDeModule\kafka project\ai_vs_human_news.csv",
            'output_file': r"c:\Users\saade\Documents\bigdata_ProjetFinDeModule\preprocessing\processed_news.csv"
        }
    )

    t1
