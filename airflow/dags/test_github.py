from datetime import datetime
import pandas as pd
from airflow.decorators import dag, task

@dag(
    dag_id='testdag',
    start_date=datetime(2023, 5, 1),
    schedule=None,
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1
    }
)
def my_dag():

    @task.bash
    def hello_world_task():
        return 'echo "Hello, World!"'

    @task.bash
    def clean_up_task():
        return 'echo "Clean up after Hello World"'

    @task(task_id='extract_data_task')
    def extract_data():
        url = "https://cdn.wsform.com/wp-content/uploads/2020/06/industry.csv"
        df = pd.read_csv(url)
        df.to_csv("chipotle.csv", index=False)
        return "chipotle.csv"

    hello_world_task() >> clean_up_task() >> extract_data()

my_dag()
