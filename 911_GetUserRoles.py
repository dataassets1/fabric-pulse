# 911 - Get All Users Across All Workspaces with Permissions in WS

import requests
import pandas as pd
from notebookutils import mssparkutils

access_token = mssparkutils.credentials.getToken(
    "https://analysis.windows.net/powerbi/api"
)

api_headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

ws_resp = requests.get(
    "https://api.powerbi.com/v1.0/myorg/groups",
    headers=api_headers
)

workspace_items = ws_resp.json().get("value", [])

print(f"Discovered {len(workspace_items)} workspaces\n")

access_records = []

for idx, workspace in enumerate(workspace_items, start=1):
    workspace_id = workspace["id"]
    workspace_name = workspace["name"]

    print(f"[{idx}/{len(workspace_items)}] Processing workspace: {workspace_name}")

    try:
        members_resp = requests.get(
            f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/users",
            headers=api_headers
        )
        members_resp.raise_for_status()

        member_list = members_resp.json().get("value", [])

        for member in member_list:
            access_records.append({
                "workspace_name": workspace_name,
                "principal_id": member.get("identifier"),
                "access_role": member.get("groupUserAccessRight"),
                "principal_type": member.get("principalType")
            })

        print(f"    ✓ Retrieved {len(member_list)} principals\n")

    except Exception as ex:
        print(f"    ✗ Failed to retrieve users for {workspace_name}")
        print(f"      Error: {str(ex)}\n")

pandas_df = pd.DataFrame(access_records)
spark_df = spark.createDataFrame(pandas_df)
spark_df.createOrReplaceTempView("workspace_access_view")

print("Temp view created: workspace_access_view")
print(f"Total principals indexed: {len(access_records)}")
display(spark_df)
print("Use that query:")
print("**************")
print('''%%sql
select workspace_name, principal_id, access_role, principal_type
from workspace_access_view''')
