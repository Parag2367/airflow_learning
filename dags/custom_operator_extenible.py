from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

# this is for creating custome operator. extends baseoperator
from airflow.models import BaseOperator
from airflow.utils import apply_defaults  # for applying default parameters


# creating a object which will have unique configuration for this workflow
dag = DAG(
    dag_id="hello_world_dag", schedule_interval="@daily", start_date="days_ago(1)"
)


# creating a custom operator of our own using baseoperator, this is aloowed by airflow this makes it extensible
class MyOperator(BaseOperator):  # inheritance
    @apply_defaults
    def __init__(self, name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name

    def execute(self, context):
        message = "Hello {}".format(self.name)
        print(message)
        return message


## creating what task should be performed

task1 = BashOperator(task_id="t1", bash_command="echo Hello World", dag=dag, retries=3)

task2 = BashOperator(task_id="t2", bash_command="echo t2", dag=dag)

task3 = BashOperator(
    task_id="t3", bash_command="echo t3", dag=dag, trigger_rule="all_faled"
)

task4 = MyOperator(name="Parag", task_id="t4", dag=dag, trigger_rule="one_success")


# setting precendence as to which task should perform first if not set each task will be performed parallely
# task1 will be performed first then task2 and task3 will be performed parallely
task1 >> [task2, task3] >> task4


# init -> t1 -> t2,t3 -> complete , this workflow is like this
