import configparser
import csv

import requests

# ดึง data มาจาก api ที่กำหนด และ query ลงใน CSV

parser = configparser.ConfigParser()

# อ่านไฟล์ pipeline ซึ่ง pipeline.conf เป็น ไฟล์ที่ทำให้เราสามารถดึง data มาจาก source
parser.read("pipeline.conf")
host = parser.get("api_config", "host")
port = parser.get("api_config", "port")

# http -> should by https to make it more secured
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
data = "users"
date = "2020-10-23"
# ลองดึงข้อมูลจาก API เส้น users และเขียนลงไฟล์ CSV
# YOUR CODE HERE
response = requests.get(f"{API_URL}/{data}/?created_at={date}")
data = response.json()
with open(f"{DATA_FOLDER}/users.csv", "w") as f:
    writer = csv.writer(f)
    header = data[0].keys()
    writer.writerow(header)

    for each in data:
        writer.writerow(each.values())


### Orders
data = "orders"
date = "2021-02-10"
# ลองดึงข้อมูลจาก API เส้น orders และเขียนลงไฟล์ CSV
# YOUR CODE HERE
response = requests.get(f"{API_URL}/{data}/?created_at={date}")
data = response.json()
with open(f"{DATA_FOLDER}/orders.csv", "w") as f:
    writer = csv.writer(f)
    header = data[0].keys()
    writer.writerow(header)

    for each in data:
        writer.writerow(each.values())