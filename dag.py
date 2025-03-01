# pylint: disable=pointless-statement
import importlib
import json
import os
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

with open(Path(__file__).parent / "dag_config.json", "r", encoding="utf-8") as file:
    dag_config = json.load(file)

PROJECT_NAME = dag_config["project_name"]
CLIENT_NAME = dag_config["client_name"]
CLIENT_DISPLAY_NAME = dag_config["client_display_name"]
CLIENT_ID = dag_config["client_id"]

PRESENTATION_DATASET_NAME = f"{PROJECT_NAME}_presentation"
DASHBOARD_TOPIC_NAME = os.getenv("DASHBOARD_TOPIC_NAME", "dev-edm-insights-dashboards-topic")
PROJECT_ID = os.getenv("BQ_PROJECT_ID", "solutionsdw")
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
LOGS_TAIL = 50

module_path = f"{PROJECT_NAME}.utils.alerts" if ENVIRONMENT == "prod" else "utils.alerts"
helpers_path = f"{PROJECT_NAME}.utils.helpers" if ENVIRONMENT == "prod" else "utils.helpers"

# Import using string because of dynamic module path based on project name
# Once deployed to production, the module path will be the project name, hence the need to import dynamically
module = importlib.import_module(module_path)
helpers = importlib.import_module(helpers_path)
on_task_fail = getattr(module, "on_task_fail")
on_dbt_test_completion = getattr(module, "on_dbt_test_completion")
bq_query_and_publish = getattr(helpers, "bq_query_and_publish")
run_dbt_task = getattr(helpers, "run_dbt_task")

default_args = {
    "owner": CLIENT_DISPLAY_NAME,
    "tags": ["pulse", CLIENT_DISPLAY_NAME, CLIENT_ID]
}

with DAG(
    dag_id=PROJECT_NAME,
    description="Run dbt and publish dashboard tables to Pub/Sub",
    default_args=default_args,
    schedule_interval=dag_config["schedule_interval"],
    max_active_runs=1,  # only one run at a time
    catchup=False,
) as dag:
    # Generate tests exclude selector based on the weekday
    execution_day = "{{ dag_run.execution_date.strftime('%A') }}"
    print("execution_day is:", execution_day)

    tests_to_include = "{{ dag_run.conf.get('tests', {}).get('select', dag_run.conf.get('select', '+Presentation')) }}"
    if execution_day == "Monday":
        tests_to_exclude = "{{ dag_run.conf.get('tests', {}).get('exclude', 'DeprecatedModels,Presentation+') }}"
    else:
        tests_to_exclude = "{{ dag_run.conf.get('tests', {}).get('exclude', 'DeprecatedModels,Presentation+,test_name:relationships') }}"

    dbt_task = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt_task,
        on_failure_callback=on_task_fail,
        op_kwargs={
            "full_refresh": "{{ dag_run.conf.get('full_refresh','False') }}",
            "select": "{{ dag_run.conf.get('select', '+Presentation') }}",
            "exclude": "{{ dag_run.conf.get('exclude', 'DeprecatedModels') }}",
            "target": ENVIRONMENT,
            "skip_command_execution": "{{ dag_run.conf.get('skip_command_execution', 'False') }}",
            "vars": "{{ dag_run.conf.get('vars', '{}') }}",
            "command": "run",
        },
        retries=10,
        retry_delay=pendulum.duration(seconds=30),
    )

    dbt_test_task = PythonOperator(
        task_id="dbt_test",
        python_callable=run_dbt_task,
        on_failure_callback=on_dbt_test_completion,
        op_kwargs={
            "full_refresh": "False",
            "select": tests_to_include,
            "exclude": tests_to_exclude,
            "target": ENVIRONMENT,
            "skip_command_execution": "{{ dag_run.conf.get('skip_command_execution', 'False') }}",
            "vars": "{{ dag_run.conf.get('vars', '{}') }}",
            "command": "test",
        },
        retries=10,
        retry_delay=pendulum.duration(seconds=30),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    send_message = PythonOperator(
        task_id="send_message",
        python_callable=bq_query_and_publish,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    dbt_task >> [send_message, dbt_test_task]
