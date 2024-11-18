from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from datetime import datetime

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# DAG definition
with DAG(
    dag_id="gcs_to_bigquery_simple",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="@once",
    catchup=False,
) as dag:

    # Task 1: Create Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id="arcane-world-420020",
        cluster_name="sample-cluster",
        region="us-east1",
        cluster_config={
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-2",
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-2",
            },
            "software_config": {
                "image_version": "2.1.63-debian11",  # Explicitly set image version for Scala 2.12
            },
        },
    )

    # Task 2: Submit PySpark job
    dataproc_job = {
        "reference": {"project_id": "arcane-world-420020"},
        "placement": {"cluster_name": "sample-cluster"},
        "pyspark_job": {
            "main_python_file_uri": "gs://test-composer-sample-spark/scripts/gcs_to_bq.py",
            "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.41.0.jar"],
        },
    }
    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        job=dataproc_job,
        region="us-east1",
        project_id="arcane-world-420020",
    )

    # # Task 3: Delete Dataproc Cluster (executed after the job completes)
    # delete_cluster = DataprocDeleteClusterOperator(
    #     task_id="delete_dataproc_cluster",
    #     project_id="arcane-world-420020",
    #     cluster_name="sample-cluster",
    #     region="us-east1",
    #     trigger_rule="all_done",  # Ensure cluster is deleted even if the job fails
    # )

    # Define task dependencies
    create_cluster >> submit_pyspark_job
