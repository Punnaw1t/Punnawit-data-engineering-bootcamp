import json

from google.cloud import storage
from google.oauth2 import service_account


DATA_FOLDER = "data"
BUSINESS_DOMAIN = "greenery"
project_id = "poetic-fact-462916-g2"
location = "asia-southeast1"
bucket_name = "deb-bootcamp-027"

data = ["addresses", "events", "order_items", "orders", "products", "promos", "users"]

  # Prepare and Load Credentials to Connect to GCP Services
keyfile_gcs = "027-uploaing-gcs.json"
service_account_info_gcs = json.load(open(keyfile_gcs))
credentials_gcs = service_account.Credentials.from_service_account_info(
    service_account_info_gcs
)

# Load data from Local to GCS
storage_client = storage.Client(
    project=project_id,
    credentials=credentials_gcs,
)

bucket = storage_client.bucket(bucket_name)


for i in data:
 # Use 'i' instead of 'data'

    # Partition to events.csv
    if i == "events":
        dt = "2021-02-10"
        partition = dt.replace("-", "")
        file_path = f"{DATA_FOLDER}/{i}.csv"
        destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{i}/{dt}/{i}.csv"
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)
    
    # Partition to orders.csv
    elif i == "orders":
        dt = "2021-02-10"
        partition = dt.replace("-", "")
        file_path = f"{DATA_FOLDER}/{i}.csv"
        destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{i}/{dt}/{i}.csv"
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)
    
    # Partition to users.csv
    elif i == "users":
        dt = "2020-10-23"
        partition = dt.replace("-", "")
        file_path = f"{DATA_FOLDER}/{i}.csv"
        destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{i}/{dt}/{i}.csv"
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)

    #Others files that didn't Partition
    else:
        file_path = f"{DATA_FOLDER}/{i}.csv"
        destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{i}/{i}.csv"

        # YOUR CODE HERE TO LOAD DATA TO GCS
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)