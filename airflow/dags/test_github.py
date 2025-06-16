from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator 
import pandas as pd

def extract_data():
    df = pd.read_csv("https://cdn.wsform.com/wp-content/uploads/2020/06/industry.csv")
    df.to_csv("chipotle.csv")



# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Instantiate the DAG object
with DAG(
    'dags-testdag.py',
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule_interval=None,
    catchup=False
) as dag:

    # Define the tasks
    hello_task = BashOperator(
        task_id='hello_world_task',
        bash_command='echo "Hello, World!"'
    )
    
    clean_task = BashOperator(
      task_id = "clean_up_task",
      bash_command = 'echo "Clean up after Hello World"'
    )

    extract_data_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_data
    )
    # Set the task dependencies (>> used to set dependency)
    hello_task >> clean_task >> extract_data_task
