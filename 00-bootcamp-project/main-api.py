import configparser
import csv

import requests


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
host = parser.get("api_config", "host")
port = parser.get("api_config", "port")

API_URL = f"http://{host}:{port}"
DATA_FOLDER = "data"

### Events
data = "events"
date = "2021-02-10"
response = requests.get(f"{API_URL}/{data}/?created_at={date}")
data = response.json()
with open(f"{DATA_FOLDER}/events.csv", "w") as f:
    writer = csv.writer(f)
    header = data[0].keys()
    writer.writerow(header)

    for each in data:
        writer.writerow(each.values())

### Users
user_data = "users"
user_date = "2020-10-23"
# ลองดึงข้อมูลจาก API เส้น users และเขียนลงไฟล์ CSV
# YOUR CODE HERE
response = requests.get(f"{API_URL}/{user_data}/?created_at={user_date}")
user_data = response.json()
with open(f"{DATA_FOLDER}/users.csv", "w") as f:
    writer = csv.writer(f)
    header = user_data[0].keys()
    writer.writerow(header)

    for each in user_data:
        writer.writerow(each.values())

### Orders
ord_data = "orders"
ord_date = "2021-02-10"
# ลองดึงข้อมูลจาก API เส้น orders และเขียนลงไฟล์ CSV
# YOUR CODE HERE
response = requests.get(f"{API_URL}/{ord_data}/?created_at={ord_date}")
ord_data = response.json()
with open(f"{DATA_FOLDER}/orders.csv", "w") as f:
    writer = csv.writer(f)
    header = ord_data[0].keys()
    writer.writerow(header)

    for each in ord_data:
        writer.writerow(each.values())