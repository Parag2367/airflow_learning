from airflow import DAG, task
from airflow.operators.bash_operator import BashOperator
from pendulum import datetime
import requests


@dag(
    startdate=datetime(2023, 8, 1),
    schedule="@daily",  # various options are there for scheduling
    catchup=False,
)
def in_cat_fact():
    @task
    def get_cat_fact():
        r = requests.get("https://catfact.ninja/fact")
        return r.json()["fact"]

    get_cat_fact_obj = get_cat_fact()

    print_cat_fact = BashOperator(
        taskid="print_cat_fact",
        bashcommand=f'echo "{get_cat_fact_obj}"',
    )

    get_cat_fact_obj >> print_cat_fact


in_cat_fact()


"""
AzureData Factory + Airflow:

1. Azure Native ISV service: Astronomer/Astro gives us the platform to use apache airflow on azure
2. Go to orchestration in azure, select apache airflow, configure it create resource.
3. Very easy to use compared to AKS as in aks we have to install etc the software and do many configurations


"""
