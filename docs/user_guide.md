# User Guide: Accessing BERDL JupyterHub

## Prerequisites

Before you begin, ensure you have:
* **KBase Account** - Register at https://narrative.kbase.us/ if you don't have one
* **ORCID Account linked with MFA enabled** - Required for authentication
* **BERDL_USER group membership** - Contact KBase System Admin team for access

## Step-by-Step Guide

### 1. Access BERDL JupyterHub

Navigate to BERDL JupyterHub:

```
https://hub.berdl.kbase.us/
```

> **💡 Accessing Other Environments:**
>
> Development and staging environments require SSH tunnel setup:
> - Development: `https://hub.dev.berdl.kbase.us/`
> - Staging: `https://hub.stage.berdl.kbase.us/`
>
> For SSH tunnel setup instructions, see the [MinIO Guide - Setting Up SSH Tunnel](minio_guide.md#setting-up-ssh-tunnel) section.

This will open the JupyterHub login interface.

<img src="screen_shots/login.png" alt="login" style="max-width: 800px;">

### 2. Log In with Your KBase Credentials

Log in using your KBase credentials (the same 3rd party identity provider, username and password you use for KBase services).

> **⚠️ Important Authentication Requirements:**
> - If you don't have a KBase account yet, you'll need to register at https://narrative.kbase.us/ before proceeding.
> - **Your KBase account must be linked to ORCID**
>   - To link your ORCID account: At https://narrative.kbase.us/ → Click on your username (top right) → My Account → Linked Providers → Link with ORCID
>
>      <img src="screen_shots/link_ORCID.png" alt="link_ORCID" style="max-width: 600px;">
>
> - **Multi-Factor Authentication (MFA) must be enabled on your ORCID account for security**
>   - To enable MFA: Go to https://orcid.org/account → Click on your username → Account settings → Two-factor authentication
>
>      <img src="screen_shots/ORCID_MFA.png" alt="ORCID_MFA" style="max-width: 600px;">
>
> - **Your account must be added to the BERDL_USER group**
>   - If you see an error message like the one below, please [contact the KBase Platform team](https://docs.kbase.us/troubleshooting/support) to have your KBase account added to the appropriate user group.
>
>      <img src="screen_shots/require_role.png" alt="require_role" style="max-width: 400px;">

### 3. Data Access
By default, you have **read/write access** to:
- Your personal SQL warehouse (`s3a://cdm-lake/users-sql-warehouse/{username}/`)
- Any tenant SQL warehouses that you belong to (e.g., `s3a://cdm-lake/tenant-sql-warehouse/kbase/`)

For questions about data access or permissions, please reach out to the BERDL Platform team.

### 4. Access the Workspace

#### 4.1 Home Directory
After logging in, click on `username` (your KBase username) under the `FAVORITES` section to access your personal home directory.
This directory is exclusive to your account and is where you can store your notebooks and files.

#### 4.2 Shared Directory
To access shared resources and example notebooks, click on the `Global Share` folder icon. This directory contains shared content available to all users.

### 5. Using Pre-loaded Functions

BERDL JupyterHub automatically loads helper functions and utilities when you start a notebook. These are imported from [berdl_notebook_utils](../notebook_utils/berdl_notebook_utils) and are immediately available without any imports needed!

#### 5.1 Creating a Spark Session

Use the `get_spark_session()` function to create or get a Spark session with all necessary configurations:

```python
spark = get_spark_session()
```

This automatically configures:
- Spark Connect server connection
- Hive Metastore integration
- MinIO S3 access
- Delta Lake support

#### 5.2 Displaying DataFrames

Use the standard `show()` method to display Spark DataFrames:

```python
# Display DataFrame using show()
df = spark.sql("SELECT * FROM my_namespace.my_table")
df.show()
```

**For formatted, interactive display with CSV-like formatting:**

If you would like to display your DataFrame in a formatted table with sorting, filtering, and pagination capabilities, use `display_df()`:

```python
# Display DataFrame in interactive table format
display_df(df)
```

<img src="screen_shots/display_func.png" alt="display_df" style="max-width: 800px;">

> **⚠️ Warning:** `display_df()` converts Spark DataFrames to pandas DataFrames and loads the entire DataFrame into memory. Only use this for reasonably-sized datasets that can fit in memory. For large datasets, use `show()` or `limit()` the results first:
> ```python
> # Safe for large datasets - limit rows first
> display_df(df.limit(100))
> ```

### 6. Accessing Data

BERDL provides prebuilt functions to explore and query your data. All functions are automatically loaded - no imports needed!

#### 6.1 Listing Databases (Namespaces)

Use `get_databases()` to list all namespaces you have access to:

```python
# Get list of all databases/namespaces
databases = get_databases()
print(f"Available databases: {databases}")
```

#### 6.2 Listing Tables in a Database

Use `get_tables()` to list all tables in a specific namespace:

```python
# List all tables in a namespace
tables = get_tables("my_namespace")
print(f"Tables in my_namespace: {tables}")
```

#### 6.3 Getting Table Schema

Use `get_table_schema()` to view the structure of a table:

```python
# Get schema information for a table
schema = get_table_schema("my_namespace", "my_table")
display_df(schema)
```

#### 6.4 Getting Complete Database Structure

Use `get_db_structure()` to see all tables and their schemas in a namespace:

```python
# Get complete structure of a database
db_structure = get_db_structure("my_namespace")
for table_info in db_structure:
    print(f"Table: {table_info['table']}")
    print(f"Columns: {table_info['columns']}")
```

## Troubleshooting

### Restarting Your Server

If you encounter issues with your notebook environment (such as kernel errors, connection problems, or need to apply environment updates), you can restart your JupyterHub server:

**Step 1:** In JupyterLab, click **File** → **Hub Control Panel**

**Step 2:** Click the **Stop My Server** button

**Step 3:** Wait for the "Stop My Server" button to disappear (this may take 10-30 seconds)

**Step 4:** Once the button disappears, click **My Server** to start a fresh server instance

> **⚠️ Note:** Stopping your server will close all running notebooks and kernels. Make sure to save your work before restarting.

> **💡 Tip:** Your files in your home directory are persistent and will not be deleted when you restart your server.