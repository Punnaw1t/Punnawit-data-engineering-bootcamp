import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


import pandas as pd
from google import genai
from google.genai import types
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime


GCP_PROJECT_ID = "poetic-fact-462916-g2"
DATASET_ID = "deb_bootcamp"
TABLE_ID = "greenery_embeddings"
KEYFILE = "/opt/airflow/dags/deb-loading-data-to-bq-secured.json"
# api_key = os.environ.get("GEMINI_API_KEY")
GEMINI_API_KEY = "AIzaSyCRbdOQqakgNCsfq-MZciuxAwR4au4-iBA"
DAGS_FOLDER = "/opt/airflow/dags"


def _gather_data(dataset_id, table_id, ds):
    service_account_info = json.load(open(KEYFILE))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    bigquery_client = bigquery.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials,
    )

    month = 2
    year = 2021

    query = f"""
        SELECT
              product_name
              , count(1) as record_count
        FROM {GCP_PROJECT_ID}.{dataset_id}.{table_id}
        WHERE
            state = 'California'
            AND EXTRACT(MONTH FROM order_created_at_utc) = {month}
            AND EXTRACT(YEAR FROM order_created_at_utc) = {year}
        GROUP BY product_name
        ORDER BY 2 DESC
        LIMIT 3
    """
    query_job = bigquery_client.query(query)
    results = query_job.result()

    products = []
    for row in results:
        products.append(row.product_name)

    houseplants = ", ".join(products)

    query = f"""
        SELECT
            count(1) as record_count
        FROM {GCP_PROJECT_ID}.{dataset_id}.{table_id}
        WHERE
            state = 'California'
            AND EXTRACT(MONTH FROM order_created_at_utc) = {month}
            AND EXTRACT(YEAR FROM order_created_at_utc) = {year}
    """
    query_job = bigquery_client.query(query)
    results = query_job.result()

    data = []
    for row in results:
        data.append(row.record_count)

    number_of_orders = data[0]

    date_object = datetime.strptime(ds, "%Y-%m-%d")
    formatted_date = date_object.strftime("%B %Y")

    df = pd.DataFrame(data={
        "text": [
            f"In California, the most ordered houseplants are {houseplants}. In {formatted_date}, there were {number_of_orders} orders."
            f" The top 3 houseplants are {houseplants}.",
            f"In {formatted_date}, the number of orders in California was {number_of_orders}.",
            f"In {formatted_date}, the most popular houseplants in California were {houseplants}."

        ]
    })

    # Partition file by date and noon (12:00)
    partition_time = date_object.strftime("%Y-%m-%d-12-00")
    partitioned_filename = f"{DAGS_FOLDER}/greenery-summary-data-{partition_time}.parquet"
    df.to_parquet(partitioned_filename)

    # df.to_parquet(f"{DAGS_FOLDER}/greenery-summary-data.parquet")  # <--- deprecated, see partitioned version above


def _get_embeddings():
    # df = pd.read_parquet(f"{DAGS_FOLDER}/greenery-summary-data.parquet")  # <--- deprecated, see partitioned version below
    from datetime import datetime as dt
    today = dt.now().strftime("%Y-%m-%d-12-00")
    partitioned_filename = f"{DAGS_FOLDER}/greenery-summary-data-{today}.parquet"
    df = pd.read_parquet(partitioned_filename)

    def generate_embeddings(text):
        genai_client = genai.Client(api_key=GEMINI_API_KEY)
        result = genai_client.models.embed_content(
            model="gemini-embedding-exp-03-07",
            contents=text,
        )
        print(text)
        return result.embeddings[0].values

    df["embedding"] = df.text.map(generate_embeddings)
    partitioned_emb_filename = f"{DAGS_FOLDER}/greenery-summary-data-with-embeddings-{today}.parquet"
    df.to_parquet(partitioned_emb_filename, index=False)

    # df.to_parquet(f"{DAGS_FOLDER}/greenery-summary-data-with-embeddings.parquet", index=False)  # <--- deprecated, see partitioned version above


def _load_data_to_bigquery():
    # df = pd.read_parquet(f"{DAGS_FOLDER}/greenery-summary-data-with-embeddings.parquet")  # <--- deprecated, see partitioned version below
    from datetime import datetime as dt
    today = dt.now().strftime("%Y-%m-%d-12-00")
    partitioned_emb_filename = f"{DAGS_FOLDER}/greenery-summary-data-with-embeddings-{today}.parquet"
    df = pd.read_parquet(partitioned_emb_filename)

    service_account_info = json.load(open(KEYFILE))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    bigquery_client = bigquery.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials,
    )

    schema = [
        bigquery.SchemaField("text", "STRING"),
        bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE"
    )
    table_id = f"{GCP_PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    load_job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()

    print(f"Loaded {load_job.output_rows} rows into {table_id}")

with DAG(
    dag_id="greenery_llm_rag_pipeline",
    schedule="@daily",
    start_date=timezone.datetime(2024, 3, 10),
    catchup=False,
    tags=["DEB", "Skooldio"],
):

    gather_data = PythonOperator(
        task_id="gather_data",
        python_callable=_gather_data,
        op_kwargs={
            "dataset_id": DATASET_ID,
            "table_id": "fct_orders",
        },
    )

    get_embeddings = PythonOperator(
        task_id="get_embeddings",
        python_callable=_get_embeddings,
    )

    load_data_to_bigquery = PythonOperator(
        task_id="load_data_to_bigquery",
        python_callable=_load_data_to_bigquery,
    )

    gather_data >> get_embeddings >> load_data_to_bigquery