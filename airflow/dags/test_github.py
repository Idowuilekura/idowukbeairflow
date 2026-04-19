from datetime import datetime
import pandas as pd
from airflow.decorators import dag, task
import os

dags_path = os.path.join(os.getcwd(), 'dags/repo/airflow/dags')
pod_path = os.path.join(dags_path,"pod_py.yaml")

print(os.listdir(dags_path))

# @dag(
#     dag_id='testdag',
#     start_date=datetime(2023, 5, 1),
#     schedule=None,
#     catchup=False,
#     default_args={
#         'owner': 'airflow',
#         'retries': 1
#     }
# )
# def my_dag():

#     @task.bash
#     def hello_world_task():
#         return 'echo "Hello, World!"'

#     @task.bash
#     def clean_up_task():
#         return 'echo "Clean up after Hello World"'

#     @task(task_id='extract_data_task')
#     def extract_data():
#         url = "https://cdn.wsform.com/wp-content/uploads/2020/06/industry.csv"
#         df = pd.read_csv(url)
#         df.to_csv("chipotle.csv", index=False)
#         return "chipotle.csv"

#     hello_world_task() >> clean_up_task() >> extract_data()

# my_dag()
# from airflow.sdk import DAG 
# from datetime import datetime
# from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


# dag = DAG(dag_id="kube_run",start_date=datetime(2025,2,1),schedule=None,default_args={
#         'owner': 'airflow',
#         'retries': 1
#     })


# kube_task = KubernetesPodOperator(name="hello-dry-run",
#     image="debian",
#     cmds=["bash", "-cx"],
#     arguments=["echo", "10"],
#     labels={"foo": "bar"},
#     task_id="dry_run_demo",
#     do_xcom_push=True,
#     on_finish_action="delete_pod",
#     dag=dag
# )

# kube_task 


from airflow.sdk import DAG 
from datetime import datetime
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


dag = DAG(dag_id="kube_run",start_date=datetime(2025,2,1),schedule=None,default_args={
        'owner': 'airflow',
        'retries': 1
    })
# image="debian",
# #     cmds=["bash", "-cx"],
# #     arguments=["echo", "10"],
# #     labels={"foo": "bar"},
# #     task_id="dry_run_demo",
# #     do_xcom_push=True,
# #     on_finish_action="delete_pod",
# #     dag=dag

kube_task = KubernetesPodOperator(
    task_id="run_with_template",
    pod_template_file=pod_path, # Path to your YAML
    name="templated-pod",
    on_finish_action="delete_pod",
    do_xcom_push = True,
    arguments=['echo \'{"key": "value"}\' > /airflow/xcom/return.json'],
    dag=dag,
     
)

kube_task
