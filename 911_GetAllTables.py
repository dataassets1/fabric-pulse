# 911 - Get All Tables Across All Workspaces in All Lakehouses

# This snippet returns a Spark view with all tables
# It helps solve challenges:
# - Which table has XYZ in the name (e.g. payments, sales, etc)
# - How many tables we have
# - How many duplicated tables we have

import requests
import pandas as pd
from notebookutils import mssparkutils
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

access_token = mssparkutils.credentials.getToken(
    "https://api.fabric.microsoft.com"
)

api_headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

API_ROOT = "https://api.fabric.microsoft.com/v1"

ws_response = requests.get(
    f"{API_ROOT}/workspaces",
    headers=api_headers
)

ws_response.raise_for_status()
workspace_list = ws_response.json().get("value", [])

print(f"Discovered {len(workspace_list)} workspaces\n")

inventory_records = []

for workspace in workspace_list:
    workspace_id = workspace["id"]
    workspace_name = workspace["displayName"]

    print(f"Processing workspace: {workspace_name}")

    workspace_failed = False

    try:
        lakehouse_resp = requests.get(
            f"{API_ROOT}/workspaces/{workspace_id}/lakehouses",
            headers=api_headers
        )
        lakehouse_resp.raise_for_status()

        lakehouse_list = lakehouse_resp.json().get("value", [])

        for lakehouse in lakehouse_list:
            lakehouse_id = lakehouse["id"]
            lakehouse_name = lakehouse["displayName"]

            tables_resp = requests.get(
                f"{API_ROOT}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables",
                headers=api_headers
            )

            tables_resp.raise_for_status()
            table_list = tables_resp.json().get("data", [])

            for table in table_list:
                inventory_records.append({
                    "workspace_name": workspace_name,
                    "lakehouse_name": lakehouse_name,
                    "table_name": table.get("name"),
                    "table_type": table.get("type"),
                    "location": table.get("location"),
                    "format": table.get("format")
                })

        print(f"  ✓ Workspace processed successfully\n")

    except Exception as ex:
        workspace_failed = True
        print(f"  ✗ Workspace failed: {workspace_name}")
        print(f"    Error: {str(ex)}\n")

if inventory_records:
    pandas_df = pd.DataFrame(inventory_records)
    spark_df = spark.createDataFrame(pandas_df)
    spark_df.createOrReplaceTempView("fabric_lakehouse_inventory")

    print("Temp view created: fabric_lakehouse_inventory")
    print(f"Total tables indexed: {len(inventory_records)}")
else:
    print("No lakehouses or tables found.")

query_text = """SELECT
    workspace_name,
    lakehouse_name,
    table_name,
    table_type,
    location,
    format
FROM fabric_lakehouse_inventory
"""

display(spark.sql(query_text))

print("Use that query to get data:")
print("****************")
print("%%sql")
print(query_text)
