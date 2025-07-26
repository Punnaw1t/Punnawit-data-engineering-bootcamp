import json
import os

from google import genai
from google.cloud import bigquery
from google.oauth2 import service_account

GCP_PROJECT_ID = "poetic-fact-462916-g2"
DATASET_ID = "deb_bootcamp"
TABLE_ID = "my_embeddings"
KEYFILE = "/workspaces/data-engineering-bootcamp/06-data-infrastructure-for-gen-ai-app/deb-loading-data-to-bq-secured.json"
# api_key = os.environ.get("GEMINI_API_KEY")
GEMINI_API_KEY = "AIzaSyCRbdOQqakgNCsfq-MZciuxAwR4au4-iBA"


def get_embedding(client, model: str = "gemini-embedding-exp-03-07", text: str = "data eng"):
    result = client.models.embed_content(
        model=model,
        contents=text,
    )
    return result.embeddings[0]


service_account_info = json.load(open(KEYFILE))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
bigquery_client = bigquery.Client(
    project=GCP_PROJECT_ID,
    credentials=credentials,
)

# Set up a Gemini client
genai_client = genai.Client(api_key=GEMINI_API_KEY)
vec = get_embedding(genai_client, text="QR codes systems for COVID-19.\nSimple tools for bars, restaurants, offices, and other small proximity businesses.").values

query = f"""
    SELECT
        base.text,
        distance
    FROM
    VECTOR_SEARCH(
        TABLE `{DATASET_ID}.{TABLE_ID}`,
        'embedding',
        (select {vec} as embedding),
        top_k => 3,
        distance_type => 'EUCLIDEAN'
    )
"""

# Run the query
query_job = bigquery_client.query(query)

# Get the results
results = query_job.result()

# Print the results
for row in results:
    print(row.text)
    print(row.distance)
