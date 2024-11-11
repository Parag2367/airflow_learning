from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

# this is for creating custome operator. extends baseoperator
from airflow.models import BaseOperator
from airflow.utils import apply_defaults  # for applying default parameters

# we used sensor it is a similar to operator , it is like something that keeps a watch
from airflow.sensorr.http_sensor import HttpSensor
from datetime import timedelta


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

# in this task we are passing a variable in place of command , this variable is defined in airflow ui
# we can go to admin >> variables >> then create (key, value).
# the keyname is passed as variable in place of command
task1 = BashOperator(
    task_id="t1", bash_command={{var.value.mycommand}}, dag=dag, retries=3
)

task2 = BashOperator(task_id="t2", bash_command="echo t2", dag=dag)

task3 = BashOperator(
    task_id="t3", bash_command="echo t3", dag=dag, trigger_rule="all_faled"
)

task4 = MyOperator(name="Parag", task_id="t4", dag=dag, trigger_rule="one_success")


# added sensor as to use sensor in this workflow
sensor = HttpSensor(
    task_id="http_sensor",
    endpoint="/",
    http_conn_id="my_httpcon",
    dag=dag,
    retries=20,
    retry_delay=timedelta(seconds=10),
    # added this for retry delay
)
# after this go to UI , go to admin , go to connection and create a connection


# setting precendence as to which task should perform first if not set each task will be performed parallely
# task1 will be performed first then task2 and task3 will be performed parallely
sensor >> task1 >> [task2, task3] >> task4


# init -> t1 -> t2,t3 -> complete , this workflow is like this
