from google.cloud import bigquery
import time

# Initialize a BigQuery client
client = bigquery.Client()

# Define project and dataset details
project_id = "arcane-world-420020"
dataset_id = "bq_demo_dataset"
table_id = "demo_table"


# Create a dataset
def create_dataset():
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "us-east1"
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {dataset.dataset_id} created.")


# Create a table
def create_table():
    schema = [
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
    ]
    table_ref = client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)
    print(f"Table {table.table_id} created.")


# Insert data into the table
def insert_data():
    rows_to_insert = [
        {"name": "John Doe", "age": 30, "email": "john.doe@example.com"},
        {"name": "Jane Smith", "age": 25, "email": "jane.smith@example.com"},
    ]
    table_ref = client.dataset(dataset_id).table(table_id)
    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors == []:
        print("Data inserted successfully.")
    else:
        print(f"Encountered errors while inserting rows: {errors}")


# Query data from the table
def query_data():
    query = f"""
    SELECT name, age, email
    FROM `{project_id}.{dataset_id}.{table_id}`
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        print(f"name: {row.name}, age: {row.age}, email: {row.email}")


# Update data in the table
def update_data():
    query = f"""
    UPDATE `{project_id}.{dataset_id}.{table_id}`
    SET age = age + 1
    WHERE name = 'John Doe'
    """
    query_job = client.query(query)
    query_job.result()
    print("Data updated successfully.")


# Delete data from the table
def delete_data():
    query = f"""
    DELETE FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE name = 'Jane Smith'
    """
    query_job = client.query(query)
    query_job.result()
    print("Data deleted successfully.")


# Main function to run all operations
def main():
    # create_dataset()
    # create_table()
    # insert_data()
    # time.sleep(10)
    query_data()
    update_data()
    query_data()
    delete_data()
    query_data()


if __name__ == "__main__":
    main()
