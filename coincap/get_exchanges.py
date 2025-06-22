# Extract
import csv

import requests


BEARER_TOKEN = "9a2298b82f5087de00150f265baf2136f8621d60a07566e67c2e84a19b284075"

# Read data from API
url = "https://rest.coincap.io/v3/exchanges"
headers = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Content-Type": "application/json"
}
response = requests.get(url, headers=headers)
data = response.json()["data"]

# Write data to CSV
with open("exchanges.csv", "w") as f:
    fieldnames = [
        "exchangeId",
        "name",
        "rank",
        "percentTotalVolume",
        "volumeUsd",
        "tradingPairs",
        "socket",
        "exchangeUrl",
        "updated",
    ]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for each in data:
        writer.writerow(each)