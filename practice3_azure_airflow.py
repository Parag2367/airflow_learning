from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator

from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator,
)
from airflow.providers.common.sql.operators.sql import SqlTableCheckOperator

from pendulum import dataetime

dag(
    startdate=datetime(2023, 8, 1),
    schedule="@daily",  # various options are there for scheduling
    catchup=False,
    params={
        "run_adf_ingeston": Param(
            True,
            type="Boolean",
            description_md="set to true to run adf_ingestion , False to skip it, default is true",
        )
    },
)


def ingest_bunny():
    @task.branch
    def get_adf_update_decision(**context):
        adf_update_decision = context["params"]["run_adf_ingestion"]
        if adf_update_decision:
            return "get_pipelines_to_run"
        else:
            return "skip_adf_update"

    skip_adf_update = EmptyOperator(task_id="skip_adf_update")

    @task
    # the data that is available in a file or source
    def get_pipelines_to_run():
        import json

        file_path = "include/dummy.json"

        with open(file_path, "r") as file:
            bunny_list = json.load(file)

        return bunny_list

    get_pipelines_to_run_obj = get_pipelines_to_run()

    chain(get_adf_update_decision(), [get_pipelines_to_run_obj, skip_adf_update])

    # this part connect adf to airflow and run the jobs
    # tasks
    run_extraction_pipeline = AzureDataFactoryRunPipelineOperator.partial(
        task_id="run_extraction_pipeline",
        azure_data_factory_conn_id="azure_data_factory",
        factory_name="BunnyDF",  # data factory name
        resource_grpoup_name="airflow-adf_rg",
    ).expand(pipeline_name=get_pipelines_to_run_obj)

    # data quality check
    # tasks
    check_fav_food = SqlTableCheckOperator(
        task_id="check_fav_food",
        conn_id="azure_sql_bunnydb",
        table="BunnyObservations",
        checks={"fav_food_check": {"check_statement": "FavFood IN ('carrot', 'kale')"}},
        trigger_rule="none_failed",
        on_failure_callback=lambda context: print("favfood check is failed"),
        outlets=[
            Dataset("azuresql://bunnydataset")
        ],  # this will be upadated by check_fav_food task, if it fails it will not update and the pipeline will not go ahead
    )

    chain(
        get_pipelines_to_run_obj,
        run_extraction_pipeline,
        check_fav_food,
    )

    chain(skip_adf_update, check_fav_food)


ingest_bunny()
