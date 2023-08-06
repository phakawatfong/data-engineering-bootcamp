import csv
import configparser

import psycopg2


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("postgres_config", "database")
user = parser.get("postgres_config", "username")
password = parser.get("postgres_config", "password")
host = parser.get("postgres_config", "host")
port = parser.get("postgres_config", "port")

conn_str = f"dbname={dbname} user={user} password={password} host={host} port={port}"
conn = psycopg2.connect(conn_str)
cursor = conn.cursor()


DATA_FOLDER = "data"

add_table = "addresses"
add_header = ["address_id", "address", "zipcode", "state", "country"]
with open(f"{DATA_FOLDER}/addresses.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(add_header)

    query = f"select * from {add_table}"
    cursor.execute(query)

    results = cursor.fetchall()
    for each in results:
        writer.writerow(each)

ord_item = "order_items"
order_ord = ["order_id", "product_id", "quantity"]
# ลองดึงข้อมูลจากตาราง order_items และเขียนลงไฟล์ CSV
# YOUR CODE HERE
with open(f"{DATA_FOLDER}/order_items.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(order_ord)

    query = f"select * from {ord_item}"
    cursor.execute(query)

    results = cursor.fetchall()
    for each in results:
        writer.writerow(each)