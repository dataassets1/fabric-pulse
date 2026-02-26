# 911 - Get Notebooks FullText Search

# This snippet returns a Spark view with all notebooks, including their JSON code decoded
# It helps solve challenges:
# - Which notebooks use table X
# - Which notebooks use XYZ (doesn't matter what, it supports full-text search through pipeline content)

import requests
import time
import base64
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from notebookutils import mssparkutils
from pyspark.sql import Row

base_url = "https://api.fabric.microsoft.com/v1"
token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com/")
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

workspaces = requests.get(f"{base_url}/workspaces", headers=headers).json().get("value", [])
print(f"Found {len(workspaces)} workspaces\n")

def get_notebook_content(workspace_id, notebook_id):
    start_url = f"{base_url}/workspaces/{workspace_id}/notebooks/{notebook_id}/getDefinition?format=ipynb"
    r = requests.post(start_url, headers=headers)

    if r.status_code == 200:
        definition = r.json()

    elif r.status_code == 202:
        op_id = r.headers.get("x-ms-operation-id")
        retry = int(r.headers.get("Retry-After", "2"))

        while True:
            status = requests.get(
                f"{base_url}/operations/{op_id}",
                headers=headers
            ).json().get("status")

            if status == "Succeeded":
                break
            if status == "Failed":
                raise Exception("Operation failed")

            time.sleep(retry)

        definition = requests.get(
            f"{base_url}/operations/{op_id}/result",
            headers=headers
        ).json()

    else:
        raise Exception(f"{r.status_code} - {r.text}")
    parts = definition.get("definition", {}).get("parts", [])
    if not parts:
        return ""
    ipynb_text = base64.b64decode(parts[0]["payload"]).decode("utf-8", errors="replace")
    nb = json.loads(ipynb_text)

    code_cells = [
        "".join(c["source"])
        for c in nb.get("cells", [])
        if c.get("cell_type") == "code"
    ]
    return "\n\n".join(code_cells)

records = []
total_notebooks = 0

for ws_index, ws in enumerate(workspaces, start=1):
    workspace_id = ws["id"]
    workspace_name = ws["displayName"]

    print(f"\n[{ws_index}/{len(workspaces)}] Processing workspace: {workspace_name}")

    items = requests.get(
        f"{base_url}/workspaces/{workspace_id}/items",
        headers=headers
    ).json().get("value", [])

    notebooks = [i for i in items if i.get("type") == "Notebook"]
    if not notebooks:
        print("  No notebooks found.")
        continue
    print(f"  Found {len(notebooks)} notebooks")
    total_notebooks += len(notebooks)
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {
            pool.submit(get_notebook_content, workspace_id, n["id"]): n
            for n in notebooks
        }
        for i, f in enumerate(as_completed(futures), start=1):
            n = futures[f]
            notebook_name = n.get("displayName")

            print(f"    [{i}/{len(notebooks)}] Extracting: {notebook_name}")
            try:
                content = f.result()
                print(f"        ✓ Success")
            except Exception as e:
                content = None
                print(f"        ✗ Failed: {str(e)}")
            records.append(Row(
                workspace_id=workspace_id,
                workspace_name=workspace_name,
                notebook_id=n["id"],
                notebook_name=notebook_name,
                content=content
            ))

print(f"\nExtraction complete. Total notebooks processed: {len(records)}")

df = spark.createDataFrame(records)
df.createOrReplaceTempView("fabric_notebooks_inventory")
print("Spark view created: fabric_notebooks_inventory")

display(df)
print('''Use that query:
***************
%%sql
select workspace_id, workspace_name, notebook_id, notebook_name, content
from fabric_notebooks_inventory
where lower(content) like '%%'
''')
