"""
Sports Analytics Pipeline DAG
Orchestrates data ingestion, transformation, and export for NBA analytics
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from dotenv import load_dotenv

# Load environment variables
load_dotenv('/opt/airflow/.env')

# Add scripts directory to Python path
sys.path.insert(0, '/opt/airflow/scripts')

# Default arguments for the DAG
default_args = {
    'owner': 'analytics',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'sla': timedelta(hours=2),  # Alert if task runs longer than 2 hours
}

# Create DAG
dag = DAG(
    'sports_pipeline_dag',
    default_args=default_args,
    description='Daily sports analytics pipeline with Airflow and dbt',
    schedule_interval=os.getenv('SCHEDULE_CRON', '0 3 * * *'),
    catchup=False,
    tags=['sports', 'analytics', 'dbt'],
)

def log_task_duration(context):
    """Log task execution duration"""
    task_instance = context['task_instance']
    duration = task_instance.end_date - task_instance.start_date
    print(f"Task {task_instance.task_id} completed in {duration.total_seconds():.2f} seconds")

def failure_callback(context):
    """Handle task failures with clear logging"""
    task_instance = context['task_instance']
    exception = context.get('exception', 'Unknown error')
    print(f"Task {task_instance.task_id} failed with error: {exception}")
    print("Troubleshooting hint: Check data source URLs, file permissions, and dbt profiles")

# Task 1: Fetch raw data from FiveThirtyEight
fetch_raw_data = BashOperator(
    task_id='fetch_raw_data',
    bash_command='cd /opt/airflow && python scripts/fetch_538.py',
    dag=dag,
    on_success_callback=log_task_duration,
    on_failure_callback=failure_callback,
)

# Task 2: Load data to warehouse
load_to_warehouse = BashOperator(
    task_id='load_to_warehouse',
    bash_command='cd /opt/airflow && python scripts/load_duckdb.py',
    dag=dag,
    on_success_callback=log_task_duration,
    on_failure_callback=failure_callback,
)

# Task group for dbt operations
with TaskGroup('dbt_transformations', dag=dag) as dbt_group:
    # Task 3: dbt seed
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command='cd /opt/airflow/dbt && export DBT_PROFILES_DIR=/opt/airflow/dbt && dbt seed',
        dag=dag,
        on_success_callback=log_task_duration,
        on_failure_callback=failure_callback,
    )
    
    # Task 4: dbt run
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt && export DBT_PROFILES_DIR=/opt/airflow/dbt && dbt run',
        dag=dag,
        on_success_callback=log_task_duration,
        on_failure_callback=failure_callback,
    )
    
    # Task 5: dbt test
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt && export DBT_PROFILES_DIR=/opt/airflow/dbt && dbt test',
        dag=dag,
        on_success_callback=log_task_duration,
        on_failure_callback=failure_callback,
    )
    
    # Set dependencies within dbt group
    dbt_seed >> dbt_run >> dbt_test

# Task 6: Export metrics
export_metrics = BashOperator(
    task_id='export_metrics',
    bash_command='cd /opt/airflow && python scripts/export_metrics.py',
    dag=dag,
    on_success_callback=log_task_duration,
    on_failure_callback=failure_callback,
)

# Task 7: Success notification (log only)
def success_notify(**context):
    """Log successful pipeline completion"""
    print("Sports analytics pipeline completed successfully!")
    print(f"Execution date: {context['execution_date']}")
    print(f"Run ID: {context['run_id']}")
    print("Metrics exported to data/exports/")

success_notification = PythonOperator(
    task_id='success_notification',
    python_callable=success_notify,
    dag=dag,
    trigger_rule='all_success',
)

# Set task dependencies
fetch_raw_data >> load_to_warehouse >> dbt_group >> export_metrics >> success_notification