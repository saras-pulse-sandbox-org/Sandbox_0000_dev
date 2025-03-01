# pylint: disable=pointless-statement
import importlib
import json
import os
import subprocess
import sys
from pathlib import Path

from airflow.exceptions import AirflowFailException
from google.cloud import bigquery, pubsub_v1, storage
from google.oauth2 import service_account

with open(Path(__file__).parent.parent / "dag_config.json", "r", encoding="utf-8") as file:
    dag_config = json.load(file)

PROJECT_NAME = dag_config["project_name"]
CLIENT_NAME = dag_config["client_name"]
CLIENT_ID = dag_config["client_id"]

PRESENTATION_DATASET_NAME = f"{PROJECT_NAME}_presentation"
DASHBOARD_TOPIC_NAME = os.getenv("DASHBOARD_TOPIC_NAME", "dev-edm-insights-dashboards-topic")
PROJECT_ID = os.getenv("BQ_PROJECT_ID", "solutionsdw")
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
LOGS_TAIL = 50

client_dir = f"/home/airflow/gcs/dags/{PROJECT_NAME}" if ENVIRONMENT == "prod" else "/home/airflow/gcs/dags"
target_path = f"{client_dir}/target"
credentials_path = f"{client_dir}/keyfile.json"


def cli(cmd: list[str], skip_execution: bool = False) -> dict:
    print(f"Running command: {cmd}")

    if skip_execution:
        print("Skipping command execution in test environment")
        return 0, False

    _cmd = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    # Stream the output
    file_not_found_error = False
    logs = []
    if _cmd.stdout is not None:
        for line in iter(_cmd.stdout.readline, ""):
            print(line, end="")
            logs.append(line)
            if ("FileNotFoundError" in line) or ("No dbt_project.yml found" in line):
                file_not_found_error = True

    # Wait for the process to finish and get the exit code
    _cmd.wait()
    return_code = _cmd.returncode
    print(f"Exit code: {return_code}")

    if not logs:
        logs.append("No logs found")

    return {"exit_code": return_code, "file_not_found_error": file_not_found_error, "logs": logs[-LOGS_TAIL:]}


def upload_folder_to_gcs(
    local_folder_path: Path,
    bucket_name: str,
    gcs_folder_path: Path,
    service_account_path: str,
):
    """Uploads a folder to a Google Cloud Storage bucket."""
    print(f"Starting upload process for folder: {local_folder_path} to bucket: {bucket_name}")

    # Initialize a client and get the bucket
    client = storage.Client.from_service_account_json(service_account_path)
    bucket = client.bucket(bucket_name)

    if local_folder_path.exists():
        print(f"Folder exists: {local_folder_path}")
    else:
        print(f"Folder does not exist: {local_folder_path}")
        return

    files = [file for file in local_folder_path.rglob("*") if file.is_file()]
    if not files:
        print(f"No files found in folder: {local_folder_path}")
        return

    print(f"Uploading {len(files)} files to bucket: {bucket_name}")
    for file_path in local_folder_path.rglob("*"):
        if file_path.is_file():
            relative_path = file_path.relative_to(local_folder_path)
            gcs_file_path = gcs_folder_path / relative_path
            blob = bucket.blob(str(gcs_file_path))
            blob.upload_from_filename(str(file_path))
            print(f"Finished uploading file: {file_path} to bucket: {bucket_name}")
    print(f"Finished uploading folder: {local_folder_path} to bucket: {bucket_name}")


def run_dbt_task(**kwargs):
    """
    Run dbt task. If full_refresh is set to True, run dbt with --full-refresh option.
    If select is set to a list of models, run dbt with --select option. If exclude is set
    to a list of models, run dbt with --exclude option. Pass the configuration to the task via
    the dag_run.conf (run with config option in UI).

    Args:
        **kwargs: dictionary containing the configuration for the dbt task. The configuration
            is passed via the dag_run.conf (run with config option in UI). Possible keys are:
            ```
            {
                    "full_refresh": str,  # True or False
                    "select": str,  # Comma separated list of models
                    "exclude": str,  # Comma separated list of models
                    "skip_command_execution": str,  # True or False
            }
            ```
    """
    print(f"{kwargs=}")
    run_id = kwargs["run_id"]
    target = kwargs["target"]
    full_refresh = kwargs.get("full_refresh", "True") == "True"
    select_models = kwargs.get("select", "").split(",")
    exclude_models = kwargs.get("exclude", "").split(",")
    command = kwargs.get("command", "run")

    service_account_path = f"{client_dir}/keyfile.json"
    skip_execution = kwargs.get("skip_command_execution", "False") == "True"

    default_options = ["--profiles-dir", client_dir, "--project-dir", client_dir, "--target", target]

    # Run dbt clean command
    dbt_clean_command = [f"{sys.prefix}/bin/dbt", "clean"] + default_options
    cli_output = cli(cmd=dbt_clean_command, skip_execution=skip_execution)
    clean_exit_code = cli_output["exit_code"]
    if clean_exit_code != 0:
        raise AirflowFailException(f"dbt clean command failed with exit code {clean_exit_code}")

    # Run dbt deps command
    dbt_deps_command = [f"{sys.prefix}/bin/dbt", "deps"] + default_options
    cli_output = cli(cmd=dbt_deps_command, skip_execution=skip_execution)
    deps_exit_code = cli_output["exit_code"]
    if deps_exit_code != 0:
        raise AirflowFailException(f"dbt deps command failed with exit code {deps_exit_code}")

    # Check if the dbt packages are installed
    dbt_packages_dir = Path(client_dir) / "dbt_packages"
    print(dbt_packages_dir)
    print(f"edm_data_transformation exists: {(dbt_packages_dir / 'edm_data_transformation/dbt_project.yml').exists()}")
    print(f"dbt_utils exists: {(dbt_packages_dir / 'dbt_utils/dbt_project.yml').exists()}")
    print(f"dbt_expectations exists: {(dbt_packages_dir / 'dbt_expectations/dbt_project.yml').exists()}")
    print(f"dbt_date exists: {(dbt_packages_dir / 'dbt_date/dbt_project.yml').exists()}")

    # Build the dbt command
    dbt_command = [f"{sys.prefix}/bin/dbt", "--no-use-colors", command]
    vars_json = json.loads(kwargs.get("vars", "{}"))
    dbt_command.extend(default_options)

    if full_refresh:
        print(f"Full refresh options: {full_refresh}")
        dbt_command.extend(["--full-refresh"])

    if select_models:
        select_models_option = ["--select"] + select_models
        print(f"Select models options: {select_models_option}")
        dbt_command.extend(select_models_option)

    if exclude_models:
        exclude_models_option = ["--exclude"] + exclude_models
        print(f"Exclude models options: {exclude_models_option}")
        dbt_command.extend(exclude_models_option)

    vars_json = {**vars_json, "AIRFLOW_RUN_ID": run_id}
    vars_json_option = ["--vars", json.dumps(vars_json)]
    print(f"Vars options: {vars_json_option}")
    dbt_command.extend(vars_json_option)

    # Run dbt run command
    cli_output = cli(cmd=dbt_command, skip_execution=skip_execution)
    dbt_run_exit_code = cli_output["exit_code"]

    # Check if it is file_not_found_error
    if cli_output["file_not_found_error"]:
        raise Exception("dbt_project.yml file not found in the client directory")

    # Upload the target folder to GCS
    bucket_name = os.getenv("TARGET_BUCKET_NAME", "pulse-dbt-target-dev")
    gcs_folder_path = Path(f"{PROJECT_NAME}/{run_id}/")
    upload_folder_to_gcs(Path(target_path), bucket_name, gcs_folder_path, service_account_path)

    # Start the process
    if dbt_run_exit_code != 0:
        raise AirflowFailException(f"dbt command failed with exit code {dbt_run_exit_code}")

    return credentials_path


def bq_query_and_publish(**kwargs):
    """
    Get the latest presentation tables for this run ID and publish the table names to
    a Pub/Sub topic.
    """
    run_id = kwargs["run_id"]

    # Create credentials using the service account file
    credentials = service_account.Credentials.from_service_account_file(credentials_path)

    # Initialize BigQuery client with the specified credentials
    bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # Initialize Pub/Sub client with the specified credentials
    pubsub_publisher = pubsub_v1.PublisherClient(credentials=credentials)
    topic_path = pubsub_publisher.topic_path(PROJECT_ID, DASHBOARD_TOPIC_NAME)

    # Get latest presentation tables for this run ID
    query = f"""
    SELECT DISTINCT name
    FROM `{PROJECT_ID}.edm_insights_metadata.dbt_results`
    WHERE
      schema_name LIKE '%{CLIENT_ID}%presentation'
      AND airflow_run_id = '{run_id}'
      AND status = 'success'
    QUALIFY
      RANK() OVER(PARTITION BY airflow_run_id, invocation_id ORDER BY record_created_at DESC) = 1;
    """
    print(f"Query: {query}")
    query_job = bq_client.query(query)
    results = query_job.result()
    query_table_names = [row.name for row in results]

    # Get available tables for this client based on sources
    query = f"""
    WITH flattened_dashboard AS (
        SELECT
            dashboard_id,
            src AS source,
            RTRIM(LTRIM(dataset_name)) AS dataset_name
        FROM
            `{PROJECT_ID}.insights_config.insights_dashboardService_config_test`,
            UNNEST(mandatory_src) AS src,
            UNNEST(dataset_names) AS dataset_name
        UNION ALL
        SELECT
            dashboard_id,
            src AS source,
            RTRIM(LTRIM(dataset_name)) AS dataset_name
        FROM
            `{PROJECT_ID}.insights_config.insights_dashboardService_config_test`,
            UNNEST(optional_src) AS src,
            UNNEST(dataset_names) AS dataset_name
    ),
    expanded_client AS (
        SELECT
            client_id,
            source
        FROM
            `{PROJECT_ID}.edm_insights_metadata.client`,
            UNNEST(sources) AS source
        WHERE
            client_id = '{CLIENT_ID}'
    ),
    dedup AS (
        SELECT
            ec.client_id,
            fd.dataset_name,
            ec.source
        FROM
            expanded_client ec
        JOIN
            flattened_dashboard fd
        ON
            ec.source = fd.source
        QUALIFY ROW_NUMBER() OVER(PARTITION BY fd.dataset_name ORDER BY 1) = 1
        ORDER BY
            ec.client_id, fd.dataset_name
    )

    SELECT dataset_name AS name
    FROM dedup; 
    """
    print(f"Query: {query}")
    query_job = bq_client.query(query)
    results = query_job.result()
    available_tables = [row.name for row in results]

    print(f"Available tables ({len(available_tables)}): {sorted(available_tables)}")
    print(f"Query tables ({len(query_table_names)}): {sorted(query_table_names)}")
    table_names = [name for name in query_table_names if name in available_tables]

    # Construct Pub/Sub message
    message = {
        "client_id": CLIENT_ID,
        "client_name": CLIENT_NAME,
        "project_id": PROJECT_ID,
        "dataset_name": PRESENTATION_DATASET_NAME,
        "table_names": table_names,
        "version": "1.0.0",
        "cogsMargin": 15.0,
    }
    message_data = json.dumps(message).encode("utf-8")

    # Publish message
    print(f"Publishing message to {topic_path} - {message}")
    future = pubsub_publisher.publish(topic_path, message_data)
    future.result()
    print(f"Published message to {DASHBOARD_TOPIC_NAME} ({message}) - {topic_path}")
