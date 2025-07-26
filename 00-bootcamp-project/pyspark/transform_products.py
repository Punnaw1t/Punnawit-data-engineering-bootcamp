import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType

# กำหนดค่าตัวแปรให้สอดคล้องกับโปรเจกต์
BUSINESS_DOMAIN = "greenery"
BUCKET_NAME = "deb-bootcamp-027" # <--- ตรวจสอบและเปลี่ยน YOUR_STUDENT_ID ให้ถูกต้อง
DATA = "products"
KEYFILE_PATH = "/pyspark/027-upload-to-gcs.json" # <--- ตรวจสอบและเปลี่ยน YOUR_KEY_FILE_PATH ให้ถูกต้อง

# ดึง EXECUTION_DATE ที่ส่งมาจาก Airflow
execution_date = os.getenv('EXECUTION_DATE')

# กำหนด Spark Session
spark = SparkSession.builder.appName("greenery") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "5G") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("google.cloud.auth.service.account.enable", "true") \
    .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
    .getOrCreate()

# กำหนด schema ของข้อมูลในไฟล์ .csv
struct_schema = StructType([
    StructField("product_id", StringType()),
    StructField("name", StringType()),
    StructField("price", StringType()),
    StructField("inventory", StringType()),
])

GCS_FILE_PATH = f"gs://{BUCKET_NAME}/raw/{BUSINESS_DOMAIN}/{DATA}/{execution_date}/{DATA}.csv"
df = spark.read.option("header", True).schema(struct_schema).csv(GCS_FILE_PATH)

# สร้าง temporary table เพื่อ query ข้อมูล
df.createOrReplaceTempView("ADDRESSES_TABLE")
result = spark.sql("""
    select
        *
    from ADDRESSES_TABLE
""")

# เขียนข้อมูลออกเป็น parquet
OUTPUT_PATH = f"gs://{BUCKET_NAME}/cleaned/{BUSINESS_DOMAIN}/{DATA}/{execution_date}/"
result.write.mode("overwrite").parquet(OUTPUT_PATH)