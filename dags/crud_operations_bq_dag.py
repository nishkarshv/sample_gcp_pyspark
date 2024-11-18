from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryDeleteTableOperator
)
from airflow.utils.dates import days_ago

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
with DAG(
    'bigquery_crud_operations',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Create a BigQuery dataset
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        dataset_id='sample_bq_crud',
        project_id='arcane-world-420020',
    )

    # Create a BigQuery table
    create_table = BigQueryCreateEmptyTableOperator(
        task_id='create_table',
        dataset_id='sample_bq_crud',
        table_id='test_bq_table',
        schema_fields=[
            {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        project_id='arcane-world-420020',
    )

    # Insert data into the BigQuery table
    insert_data = BigQueryInsertJobOperator(
        task_id='insert_data',
        configuration={
            "query": {
                "query": """
                    INSERT INTO `arcane-world-420020.sample_bq_crud.test_bq_table` (id, name)
                    VALUES (1, 'John Doe'), (2, 'Jane Doe')
                """,
                "useLegacySql": False,
            }
        },
    )

    # Delete the BigQuery table
    delete_table = BigQueryDeleteTableOperator(
        task_id='delete_table',
        deletion_dataset_table='arcane-world-420020.sample_bq_crud.test_bq_table',
    )

    # Delete the BigQuery dataset
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id='delete_dataset',
        dataset_id='sample_bq_crud',
        project_id='arcane-world-420020',
        delete_contents=True,
    )

    # Define task dependencies
    create_dataset >> create_table >> insert_data >> delete_table >> delete_dataset