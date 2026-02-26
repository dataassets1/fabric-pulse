# 911 - Get Fabric Pipelines FullText Search

# This snippet returns a Spark view with all Data Pipelines, including their JSON code decoded
# It helps solve challenges:
# - Which pipelines use table X
# - Which pipelines use XYZ (doesn't matter what, it supports full-text search through pipeline content)

MAX_RETRIES = 10    # increase if you face 429 error
INITIAL_DELAY = 4   # increase if you face 429 error
# ************************************************
import requests
import json
import base64
import time
from pyspark.sql import Row
from pyspark.sql.types import *
from notebookutils import mssparkutils

BASE_URL = "https://api.fabric.microsoft.com/v1"
def get_headers():
    token = mssparkutils.credentials.getToken(
        "https://analysis.windows.net/powerbi/api"
    )
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

def api_request(method, url, body=None):
    delay = INITIAL_DELAY

    for _ in range(MAX_RETRIES):
        if method == "GET":
            r = requests.get(url, headers=get_headers())
        else:
            r = requests.post(url, headers=get_headers(), json=body)
        if r.status_code in (200, 201):
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(delay)
            delay *= 2
            continue
        print(f"Failed: {r.status_code} - {r.text}")
        return None
    print("Max retries exceeded")
    return None

def decode_definition(definition_response):
    if not definition_response:
        return None
    parts = definition_response.get("definition", {}).get("parts", [])
    decoded_parts = {}

    for part in parts:
        if part.get("payloadType") == "InlineBase64":
            decoded_bytes = base64.b64decode(part["payload"])
            decoded_str = decoded_bytes.decode("utf-8")
            decoded_parts[part["path"]] = json.loads(decoded_str)
    return decoded_parts

def extract_all_pipelines(
    workspace_name_filter: str = None,
    limit_pipelines: int = None
):

    rows = []
    total_processed = 0

    workspaces = api_request("GET", f"{BASE_URL}/workspaces")
    if not workspaces:
        return None

    for ws in workspaces.get("value", []):
        workspace_id = ws["id"]
        workspace_name = ws["displayName"]
        # Optional workspace filter
        if workspace_name_filter and workspace_name != workspace_name_filter:
            continue
        print(f"Scanning workspace: {workspace_name}")
        items = api_request(
            "GET",
            f"{BASE_URL}/workspaces/{workspace_id}/items"
        )
        if not items:
            continue
        pipelines = [
            i for i in items.get("value", [])
            if i.get("type") == "DataPipeline"
        ]
        for pipeline in pipelines:
            if limit_pipelines and total_processed >= limit_pipelines:
                print("Pipeline limit reached.")
                break

            pipeline_id = pipeline["id"]
            pipeline_name = pipeline["displayName"]
            print(f"  Extracting: {pipeline_name}")

            definition_response = api_request(
                "POST",
                f"{BASE_URL}/workspaces/{workspace_id}/items/{pipeline_id}/getDefinition",
                body={"format": "json"}
            )

            decoded = decode_definition(definition_response)
            rows.append(Row(
                workspace_id=workspace_id,
                workspace_name=workspace_name,
                pipeline_id=pipeline_id,
                pipeline_name=pipeline_name,
                description=pipeline.get("description"),
                created_at=pipeline.get("createdDateTime"),
                modified_at=pipeline.get("lastModifiedDateTime"),
                decoded_json=json.dumps(decoded) if decoded else None
            ))

            total_processed += 1
        if limit_pipelines and total_processed >= limit_pipelines:
            break
    schema = StructType([
        StructField("workspace_id", StringType(), True),
        StructField("workspace_name", StringType(), True),
        StructField("pipeline_id", StringType(), True),
        StructField("pipeline_name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("modified_at", StringType(), True),
        StructField("decoded_json", StringType(), True),
    ])
    return spark.createDataFrame(rows, schema)

df = extract_all_pipelines(workspace_name_filter=None, limit_pipelines=None)

df.createOrReplaceTempView("v_pipelines")

sql = """select workspace_id, workspace_name, pipeline_id, pipeline_name, description, created_at, modified_at, decoded_json
from v_pipelines where lower(decoded_json) like '%'"""

display(spark.sql(sql))

print(f'''View v_pipelines was created. Use that query as a template:
********************
%%sql
{sql}''')
