from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, TimestampType


KEYFILE_PATH = "/opt/spark/pyspark/uploading-gcs.json"


# GCS Connector Path (on Spark): /opt/spark/jars/gcs-connector-hadoop3-latest.jar
# GCS Connector Path (on Airflow): /home/airflow/.local/lib/python3.9/site-packages/pyspark/jars/gcs-connector-hadoop3-latest.jar
# spark = SparkSession.builder.appName("demo") \
#     .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
#     .config("spark.memory.offHeap.enabled", "true") \
#     .config("spark.memory.offHeap.size", "5g") \
#     .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
#     .config("google.cloud.auth.service.account.enable", "true") \
#     .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
#     .getOrCreate()

spark = SparkSession.builder.appName("demo_gcs_airflow") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "5g") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("google.cloud.auth.service.account.enable", "true") \
    .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
    .getOrCreate()

# Example schema for Greenery users data
# struct_schema = StructType([
#     StructField("user_id", StringType()),
#     StructField("first_name", StringType()),
#     StructField("last_name", StringType()),
#     StructField("email", StringType()),
#     StructField("phone_number", StringType()),
#     StructField("created_at", TimestampType()),
#     StructField("updated_at", TimestampType()),
#     StructField("address_id", StringType()),
# ])

GCS_FILE_PATH = "gs://deb-bootcamp-027/raw/greenery/addresses/addresses.csv"

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(GCS_FILE_PATH)

# df = spark.read \
#     .option("header", True) \
#     .schema(struct_schema) \
#     .csv(GCS_FILE_PATH)

df.show()
df.printSchema()

df.createOrReplaceTempView("my_addresses")
result = spark.sql("""
    select
        *

    from my_addresses
""")

OUTPUT_PATH = "gs://deb-bootcamp-027/my_spark"
result.write.mode("overwrite").parquet(OUTPUT_PATH)
