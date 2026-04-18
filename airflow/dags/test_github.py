from datetime import datetime
    2 import pandas as pd
    3 from airflow.decorators import dag, task
    4
    5 # Define the DAG using the TaskFlow API decorator
    6 @dag(
    7     dag_id='test_dag_v3',
    8     start_date=datetime(2023, 5, 1),
    9     schedule=None,  # Modern replacement for schedule_interval
   10     catchup=False,
   11     tags=['example', 'airflow_3'],
   12     default_args={
   13         'owner': 'airflow',
   14         'retries': 1
   15     }
   16 )
   17 def my_dag():
   18
   19     # Using @task.bash for shell commands
   20     @task.bash
   21     def hello_world_task():
   22         return 'echo "Hello, World!"'
   23
   24     @task.bash
   25     def clean_up_task():
   26         return 'echo "Clean up after Hello World"'
   27
   28     # Standard @task decorator for Python logic
   29     @task(task_id='extract_data_task')
   30     def extract_data():
   31         url = "https://cdn.wsform.com/wp-content/uploads/2020/06/industry.csv"
   32         df = pd.read_csv(url)
   33         # Note: In a production Airflow 3 environment, you would typically 
   34         # write to a remote object store (S3/GCS) or use Airflow Assets.
   35         df.to_csv("chipotle.csv", index=False)
   36         return "chipotle.csv"
   37
   38     # Set dependencies using bitshift operators on the function calls
   39     hello_world_task() >> clean_up_task() >> extract_data()
   40
   41 # Instantiate the DAG
   42 my_dag()
