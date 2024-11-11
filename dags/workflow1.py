from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator


# creating a object which will have unique configuration for this workflow
dag = DAG(
    dag_id="hello_world_dag", schedule_interval="@daily", start_date="days_ago(1)"
)

## creating what task should be performed

task1 = BashOperator(
    task_id="t1", bash_command="echo Hello World", dag=dag, retries=3
)  # retries parameter will let the task retry it for given number

task2 = BashOperator(task_id="t2", bash_command="echo t2", dag=dag)

task3 = BashOperator(
    task_id="t3", bash_command="echo t3", dag=dag, trigger_rule="all_failed"
)  # trigger_rule parameter tells the airflow when to run this task, by default it is all_success, here it is all_failed (which means if parent task fails it will run)
# note: trigger_rule: is always done on child task


# setting precendence as to which task should perform first if not set each task will be performed parallely
# task1 will be performed first then task2 and task3 will be performed parallely
task1 >> [task2, task3]


# init -> t1 -> t2,t3 -> complete , this workflow is like this
