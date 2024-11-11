from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator


# creating a object which will have unique configuration for this workflow
dag = DAG(
    dag_id="hello_world_dag", schedule_interval="@daily", start_date="days_ago(1)"
)

## creating what task should be performed

task1 = BashOperator(task_id="t1", bash_command="echo Hello World", dag=dag)


# init -> t1 -> complete , this workflow is like this
