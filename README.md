# Pulse Client Template

This repository serves as a template for Pulse client dbt projects that run in Airflow. It provides a standardized structure for data transformation pipelines that publish data to dashboards via Google Cloud Platform services.

## Project Overview

The Pulse Client Template is designed to:
- Execute dbt models in an Airflow environment
- Upload dbt artifacts to Google Cloud Storage
- Publish successful table information to Pub/Sub for dashboard consumption
- Provide robust error handling and alerting

## Repository Structure
```
.
├── .github/                      # GitHub configuration files
├── .vscode/                      # VS Code settings
├── dag.py                        # Main Airflow DAG definition
├── dag_config.json               # Client-specific DAG configuration
├── dbt_project.yml               # dbt project configuration
├── keyfile.json                  # GCP service account credentials (template)
├── models/                       # dbt models directory
├── packages.yml                  # dbt package dependencies
├── profiles.yml                  # dbt connection profiles
└── utils/                        # Utility modules
    ├── __init__.py
    ├── alerts.py                 # Alert handling for failures
    └── helpers.py                # Helper functions for dbt execution
```

## Key Components

### DAG Configuration
The Airflow DAG (`dag.py`) orchestrates:
1. **dbt Run Task**: Executes dbt models with configurable parameters
2. **dbt Test Task**: Runs dbt tests with weekday-based configurations
3. **Send Message Task**: Publishes successful tables to Pub/Sub

### Utility Functions
- **helpers.py**: Contains functions for:
  - Running dbt commands with proper environment setup
  - Uploading artifacts to Google Cloud Storage
  - Querying BigQuery for table information
  - Publishing messages to Pub/Sub

- **alerts.py**: Handles:
  - Failure notifications to Slack
  - Creating tickets in ClickUp for task failures
  - Processing and reporting dbt test results

### Environment Configuration
- Supports both `dev` and `prod` environments
- Uses BigQuery for data storage
- Relies on GCS for artifact storage
- Leverages Pub/Sub for messaging

## Workflow

1. The DAG is triggered (manually or on schedule)
2. dbt models are executed with the specified parameters
3. Results and artifacts are uploaded to GCS
4. Tables are published to Pub/Sub for dashboard consumption
5. Tests are executed against the models
6. Any failures trigger alerts to Slack and ClickUp

## Error Handling

The project includes robust error handling:
- Task failures generate ClickUp tickets
- dbt test failures trigger detailed Slack alerts with Excel reports
- Custom exception handling for various error scenarios

## Usage

This template is intended to be customized for specific clients by:
1. Updating `dag_config.json` with client-specific information
2. Adding client-specific dbt models to the `models` directory
3. Configuring the appropriate environment variables
4. Providing a valid GCP service account with appropriate permissions

## Dependencies

- dbt Core (version between 1.0.0 and 2.0.0)
- Apache Airflow
- Google Cloud libraries (bigquery, pubsub_v1, storage)
- Other Python dependencies (yaml, openpyxl, etc.)

## Future Improvements

This codebase is slated for revamping to:
- Simplify the structure
- Follow best practices more closely
- Improve efficiency in execution
- Enhance documentation and maintainability
