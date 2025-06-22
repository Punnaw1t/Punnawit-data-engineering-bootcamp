import csv
import configparser

import psycopg2

# ดึง data มาจาก postgress Database และ query ลงใน CSV

parser = configparser.ConfigParser()

# อ่านไฟล์ pipeline ซึ่ง pipeline.conf เป็น ไฟล์ที่ทำให้เราสามารถดึง data มาจาก source
parser.read("pipeline.conf")


dbname = parser.get("postgres_config", "database")
user = parser.get("postgres_config", "username")
password = parser.get("postgres_config", "password")
host = parser.get("postgres_config", "host")
port = parser.get("postgres_config", "port")

# create cursor to connect
conn_str = f"dbname={dbname} user={user} password={password} host={host} port={port}"
conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

DATA_FOLDER = "data"

table = "addresses"
header = ["address_id", "address", "zipcode", "state", "country"]
with open(f"{DATA_FOLDER}/addresses.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(header)

    query = f"select * from {table}"
    cursor.execute(query)

    results = cursor.fetchall()
    for each in results:
        writer.writerow(each)

table = "order_items"
header = ["order_id", "product_id", "quantity"]
# ลองดึงข้อมูลจากตาราง order_items และเขียนลงไฟล์ CSV
# YOUR CODE HERE

with open (f"{DATA_FOLDER}/order_items.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(header)

    query = f"select * from {table}"
    cursor.execute(query)

    results = cursor.fetchall()
    for each in results:
        writer.writerow(each)
