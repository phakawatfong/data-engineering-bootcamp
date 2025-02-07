import os
import json

from google.cloud import bigquery
from google.oauth2 import service_account

DATA_FOLDER="data"

keyfile = "/workspaces/data-engineering-bootcamp/02-data-warehouse-and-google-bigquery/examples/loading-data-to-bigquery-from-local/deb2-loading-data-to-bigquery-395008-413be4b1a119.json"
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = "deb2-395008"
client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)

# Addressess
job_config = bigquery.LoadJobConfig(
    skip_leading_rows=1,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
)

data = "addresses"
file_path = f"{DATA_FOLDER}/{data}.csv"
with open(file_path, "rb") as f:
    table_id = f"{project_id}.deb_bootcamp.{data}"
    job = client.load_table_from_file(f, table_id, job_config=job_config)
    job.result()

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

# ----------

# Events
job_config = bigquery.LoadJobConfig(
    skip_leading_rows=1,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="created_at",
    ),
)

dt = "2021-02-10"
partition = dt.replace("-", "")
data = "events"
file_path = f"{DATA_FOLDER}/{data}.csv"
with open(file_path, "rb") as f:
    table_id = f"{project_id}.deb_bootcamp.{data}${partition}"
    job = client.load_table_from_file(f, table_id, job_config=job_config)
    job.result()

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

# ถึงตรงนี้เราโหลดข้อมูลไปแล้ว 2 ชุด ยังเหลืออีก 5 ชุดที่ต้องโหลดเพิ่ม
# YOUR CODE HERE

# order_items table
job_config = bigquery.LoadJobConfig(
    skip_leading_rows = 1,
    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
)

data = "order_items"
file_path = f"{DATA_FOLDER}/{data}.csv"
with open(file_path, "rb") as f:
    table_id = f"{project_id}.deb_bootcamp.{data}"
    job = client.load_table_from_file(f, table_id, job_config=job_config)

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


# orders
job_config = bigquery.LoadJobConfig(
    skip_leading_rows=1,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="created_at",
    ),
)
data = "orders"
# order_list_partition=['2021-02-10','2021-02-11']
partition = '2021-02-10'
partition = dt.replace("-", "")
# for partition in order_list_partition:
file_path = f"{DATA_FOLDER}/{data}.csv"
with open(file_path, "rb") as f:
    table_id = f"{project_id}.deb_bootcamp.{data}${partition}"
    job = client.load_table_from_file(f, table_id, job_config=job_config)
    job.result()

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


# products 
job_config = bigquery.LoadJobConfig(
    skip_leading_rows = 1,
    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format = bigquery.SourceFormat.CSV,
    autodetect=True,
)

data = "products"
file_path = f"{DATA_FOLDER}/{data}.csv"
with open(file_path, "rb") as f:
    table_id = f"{project_id}.deb_bootcamp.{data}"
    job = client.load_table_from_file(f, table_id, job_config=job_config)
    job.result()

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


# promos 
job_config = bigquery.LoadJobConfig(
    skip_leading_rows = 1,
    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format = bigquery.SourceFormat.CSV,
    autodetect=True,
)

data = "promos"
file_path = f"{DATA_FOLDER}/{data}.csv"
with open(file_path, "rb") as f:
    table_id = f"{project_id}.deb_bootcamp.{data}"
    job = client.load_table_from_file(f, table_id, job_config=job_config)
    job.result()

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


# users
job_config = bigquery.LoadJobConfig(
    skip_leading_rows=1,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="created_at",
    ),
)

dt = "2020-10-23"
partition = dt.replace("-", "")
data = "users"
file_path = f"{DATA_FOLDER}/{data}.csv"
with open(file_path, "rb") as f:
    table_id = f"{project_id}.deb_bootcamp.{data}${partition}"
    job = client.load_table_from_file(f, table_id, job_config=job_config)
    job.result()

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")