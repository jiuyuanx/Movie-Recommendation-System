from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 24),
    'email': ['your-email'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'RecSys_Train_DAG',
    default_args=default_args,
    description='A simple DAG to train models',
    schedule_interval=timedelta(hours=12),
    start_date=days_ago(1),
    tags=['example'],
)

t1 = BashOperator(
    task_id='Kafka_stream_to_RDS',
    bash_command='python3 /opt/airflow/dags/rds_Kafka.py 0.5 0 True True',
    dag=dag,
)

t2 = BashOperator(
    task_id='Fetch_Train_DATA_SQL',
    bash_command='python3 /opt/airflow/dags/Train_Data.py',
    dag=dag,
)

t3 = BashOperator(
    task_id='Train_Model_sagemaker',
    bash_command='python3 /opt/airflow/dags/Train_SVD_SageMaker.py',
    dag=dag,
)

t1 >> t2 >> t3
