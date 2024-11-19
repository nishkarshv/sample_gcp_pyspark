gcloud composer environments create sample-airflow --location us-east1
gcloud composer environments list --locations us-east1
gcloud composer environments run sample-airflow --location us-east1 list_dags
# set default project and composer location
gcloud config set project arcane-world-420020
gcloud config set composer/location us-east1

# upload one file
gsutil cp deploy.sh gs://test-composer-sample-spark/configs/
# upload directory
gsutil cp -r configs gs://test-composer-sample-spark/

# bq commands with crud operations
# create dataset
# bq mk --dataset <project-id>:<dataset-id>
bq mk --dataset my_project:my_dataset

# create table
# bq mk --table <project-id>:<dataset-id>.<table-id> <schema>
bq mk --table my_project:my_dataset.my_table name:STRING,age:INTEGER

# insert into table
bq query --use_legacy_sql=false \
'INSERT INTO `my_project.my_dataset.my_table` (name, age, email) 
 VALUES ("John Doe", 30, "john.doe@example.com")'


# read data
bq query --use_legacy_sql=false 'SELECT * FROM `my_project.my_dataset.my_table`'

# update table schema
bq update --table my_project:my_dataset.my_table email:STRING

# delete table
bq rm -t my_project:my_dataset.my_table



# Create a Table from CSV: Use the bq load command to create a table from your local CSV file.

# bq load --autodetect --source_format=CSV <project-id>:<dataset-id>.<table-id> <path-to-local-csv-file>
# For example, to create a table named my_table in the dataset my_dataset from a local CSV file data.csv:

bq load --autodetect --source_format=CSV my_project:my_dataset.my_table ./data.csv
bq load --autodetect --source_format=CSV my_project:my_dataset.my_table gs://my-bucket/data.csv
# delete dataset
bq rm -r -d my_project:my_dataset

