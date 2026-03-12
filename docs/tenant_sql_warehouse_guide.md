# Tenant SQL Warehouse User's Guide

This guide explains how to configure your Spark session to write tables to either your personal SQL warehouse or a tenant's shared SQL warehouse.

## Overview

The BERDL JupyterHub environment supports two types of SQL warehouses:

1. **User SQL Warehouse**: Your personal workspace for tables
2. **Tenant SQL Warehouse**: Shared workspace for team/organization tables

## Spark Connect Architecture

BERDL uses **Spark Connect**, which provides a client-server architecture:
- **Connection Protocol**: Uses gRPC for efficient communication with remote Spark clusters
- **Spark Connect Server**: Runs locally in your notebook pod as a proxy
- **Spark Connect URL**: `sc://localhost:15002`
- **Driver and Executors**: Runs locally in your notebook pod

### Automatic Credential Management

Your MinIO S3 credentials are automatically configured when your notebook server starts:

1. **JupyterHub** calls the governance API to create/retrieve your MinIO credentials
2. **Environment Variables** are set in your notebook: `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_USERNAME`
3. **Spark Cluster** receives your credentials and configures S3 access for Hive Metastore operations
4. **Notebooks** use credentials from environment (no API call needed)

This ensures secure, per-user S3 access without exposing credentials in code.

## Quick Comparison

| Configuration | Warehouse Location | Tables Location | Default Namespace |
|--------------|-------------------|----------------|-------------------|
| `create_namespace_if_not_exists(spark)` | `s3a://cdm-lake/users-sql-warehouse/{username}/` | Personal workspace | `u_{username}__default` |
| `create_namespace_if_not_exists(spark, tenant_name="kbase")` | `s3a://cdm-lake/tenant-sql-warehouse/kbase/` | Tenant workspace | `kbase_default` |

## Personal SQL Warehouse (Default)

### Usage
```python
from berdl_notebook_utils import get_spark_session
from berdl_notebook_utils.spark.database import create_namespace_if_not_exists

# Create Spark session
spark = get_spark_session("MyPersonalApp")

# Create namespace in your personal SQL warehouse
namespace = create_namespace_if_not_exists(spark)
```

### Where Your Tables Go
- **Warehouse Directory**: `s3a://cdm-lake/users-sql-warehouse/{username}/`
- **Default Namespace**: `u_{username}__default` (username prefix added automatically)
- **Table Location**: `s3a://cdm-lake/users-sql-warehouse/{username}/u_{username}__default.db/your_table/`

### Example
```python
from berdl_notebook_utils import get_spark_session
from berdl_notebook_utils.spark.database import create_namespace_if_not_exists

# Create Spark session
spark = get_spark_session("MyPersonalApp")

# Create default namespace (creates "u_{username}__default" with username prefix)
namespace = create_namespace_if_not_exists(spark)
print(f"Created namespace: {namespace}")  # Output: "Created namespace: u_{username}__default"

# Or create custom namespace (creates "u_{username}__analysis" with username prefix)
analysis_namespace = create_namespace_if_not_exists(spark, namespace="analysis")
print(f"Created namespace: {analysis_namespace}")  # Output: "Created namespace: u_{username}__analysis"

# Create a DataFrame and save as table using returned namespace
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.write.format("delta").saveAsTable(f"{namespace}.my_personal_table")

# Table will be stored at:
# s3a://cdm-lake/users-sql-warehouse/{username}/u_{username}__default.db/my_personal_table/
```

## Tenant SQL Warehouse

### Usage
```python
from berdl_notebook_utils import get_spark_session
from berdl_notebook_utils.spark.database import create_namespace_if_not_exists

# Create Spark session
spark = get_spark_session("TeamAnalysis")

# Create namespace in tenant SQL warehouse (validates membership)
namespace = create_namespace_if_not_exists(spark, tenant_name="kbase")
```

### Where Your Tables Go
- **Warehouse Directory**: `s3a://cdm-lake/tenant-sql-warehouse/{tenant}/`
- **Default Namespace**: `{tenant}_default` (tenant prefix added automatically)
- **Table Location**: `s3a://cdm-lake/tenant-sql-warehouse/{tenant}/{tenant}_default.db/your_table/`

### Requirements
- You must be a member of the specified tenant/group
- The system validates your membership before allowing access

### Example
```python
from berdl_notebook_utils import get_spark_session
from berdl_notebook_utils.spark.database import create_namespace_if_not_exists

# Create Spark session
spark = get_spark_session("TeamAnalysis")

# Create default namespace in tenant warehouse (creates "{tenant}_default" with tenant prefix)
namespace = create_namespace_if_not_exists(spark, tenant_name="kbase")
print(f"Created namespace: {namespace}")  # Output: "Created namespace: kbase_default"

# Or create custom namespace (creates "{tenant}_research" with tenant prefix)
research_namespace = create_namespace_if_not_exists(spark, namespace="research", tenant_name="kbase")
print(f"Created namespace: {research_namespace}")  # Output: "Created namespace: kbase_research"

# Create a DataFrame and save as table using returned namespace
df = spark.createDataFrame([(1, "Dataset A"), (2, "Dataset B")], ["id", "dataset"])
df.write.format("delta").saveAsTable(f"{namespace}.shared_analysis")

# Table will be stored at:
# s3a://cdm-lake/tenant-sql-warehouse/kbase/kbase_default.db/shared_analysis/
```

## Advanced Namespace Management

### Custom Namespaces (Default Behavior)
```python
# Personal warehouse with custom namespace (prefix enabled by default)
spark = get_spark_session()
exp_namespace = create_namespace_if_not_exists(spark, "experiments")  # Returns "u_{username}__experiments"

# Tenant warehouse with custom namespace (prefix enabled by default)
data_namespace = create_namespace_if_not_exists(spark, "research_data", tenant_name="kbase")  # Returns "kbase_research_data"

# Use returned namespace names for table operations
df.write.format("delta").saveAsTable(f"{exp_namespace}.my_experiment_table")
df.write.format("delta").saveAsTable(f"{data_namespace}.shared_dataset")

# Tables will be stored at:
# s3a://cdm-lake/users-sql-warehouse/{username}/u_{username}__experiments.db/my_experiment_table/
# s3a://cdm-lake/tenant-sql-warehouse/kbase/kbase_research_data.db/shared_dataset/
```

## Tips

- **Always use the returned namespace**: `create_namespace_if_not_exists()` returns the actual namespace name (with prefixes applied). Always use this value when creating tables.
- **Permission issues with manual namespaces**: If you create namespaces manually (without using `create_namespace_if_not_exists()`) and the namespace doesn't follow the expected naming rules (e.g., missing the `u_{username}__` or `{tenant}_` prefix), you may not have the correct permissions to read/write to that namespace. The governance system enforces permissions based on namespace naming conventions:
  - User namespaces must start with `u_{username}__` to grant you access
  - Tenant namespaces must start with `{tenant}_` and you must be a member of that tenant
  - Namespaces without proper prefixes will result in "Access Denied" errors from MinIO
- **Tenant membership required**: Attempting to access a tenant warehouse without membership will fail.
- **Credentials are automatic**: MinIO credentials are set by JupyterHub - you don't need to call any API to get them.
- **Spark Connect is default**: All sessions use Spark Connect for better stability and resource isolation.
