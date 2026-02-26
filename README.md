# Fabric 911 – Emergency Investigation Toolkit for Microsoft Fabric

Fabric 911 is a lightweight emergency toolkit designed to quickly understand what is happening across Microsoft Fabric workspaces during incidents, audits, or governance reviews.

When documentation is missing and something breaks, these scripts provide immediate visibility into tables, pipelines, notebooks, and workspace access.

## Purpose

This repository helps answer urgent questions such as:
	•	What tables exist across all workspaces?
	•	Which notebooks contain specific logic?
	•	What pipelines are deployed?
	•	Who has access to each workspace?

It enables fast metadata discovery and operational inspection across your Fabric environment.


## Files Overview

### 911_GetAllTables.py

Scans all accessible workspaces and Lakehouses.
Returns table metadata, including name, type, location, and format.

Useful for:
	•	Deployment validation
	•	Schema audits
	•	Storage path inspection
	•	Governance checks

### 911_GetDataPipelines.py

Enumerates Data Pipelines across workspaces.

Useful for:
	•	Dependency discovery
	•	Identifying orphan or unused pipelines
	•	Operational troubleshooting

### 911_GetNotebooks.py

Retrieves notebook definitions and extracts code content.
Creates a Spark view to allow SQL-based code search.

Useful for:
	•	Root cause analysis (what notebooks use specific tables)
	•	Detecting risky operations (overwrite, replaceWhere, hardcoded paths)
	•	Investigating undocumented transformations

### 911_GetUserRoles.py

Extracts users and roles for every workspace.

Useful for:
	•	Access reviews
	•	Security audits
	•	Detecting excessive privileges
	•	Governance validation

How to Use
	1.	Run the scripts inside a Fabric Notebook.
	2.	Each script creates a Spark temporary view.
	3.	Query results using SQL for investigation and analysis.

Fabric 911 is intended for fast, structured investigation of Microsoft Fabric environments, helping reduce time to detect and resolve operational issues.
