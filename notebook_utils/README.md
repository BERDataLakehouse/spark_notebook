# BERDL Notebook Utils

Python package providing comprehensive utilities for BERDL notebook environments. This package enables seamless integration with Spark, MinIO storage, data governance, and cluster management within Jupyter notebooks.

## Features

- **Spark Session Management**: Pre-configured Spark sessions with Delta Lake, MinIO S3 storage, and Hive metastore integration
- **Data Governance Integration**: Complete access to BERDL Data Governance API for workspace and table management
- **Client Management**: Centralized client factories for all BERDL services (MinIO, CDM Task Service, Governance, Spark Cluster Manager)
- **Database Operations**: Namespace and table management utilities with governance integration
- **DataFrame Utilities**: Enhanced DataFrame display and Spark-to-Pandas conversion functions

## Installation

This package is automatically installed in BERDL notebook environments. For development:

```bash
cd notebook_utils
uv sync --locked
```

## Quick Start

### Basic Spark Session

```python
from berdl_notebook_utils import get_spark_session

# Create a Spark session with your personal SQL warehouse
spark = get_spark_session("MyAnalysis")

# Create a DataFrame
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.show()
```

### Group/Tenant Workspace

```python
# Use a shared group SQL warehouse
spark_group = get_spark_session("TeamAnalysis", tenant_name="research_team")
```

### Data Governance Operations

```python
from berdl_notebook_utils.minio_governance import *

# Get your workspace info
workspace = get_my_workspace()
print(f"Username: {workspace.username}")

# Create a tenant and assign users
result = create_tenant_and_assign_users(
    tenant_name="research_team",
    usernames=["alice", "bob", "charlie"]
)

# Share a table with colleagues
share_table("analytics", "user_metrics", with_users=["alice", "bob"])

# Make a table publicly accessible
make_table_public("research", "public_dataset")
```

### Database Management

```python
from berdl_notebook_utils.spark import *

# Create namespace
create_namespace_if_not_exists("analytics")

# List tables
tables = list_tables("analytics")
print(f"Tables in analytics: {[t.name for t in tables]}")

# Check if table exists
if table_exists("analytics", "user_data"):
    info = get_table_info("analytics", "user_data")
    print(f"Table location: {info.location}")
```

## Package Structure

```
berdl_notebook_utils/
├── __init__.py                 # Main exports and settings
├── clients.py                  # Client factories for all services
├── setup_spark_session.py     # Spark session configuration
├── berdl_settings.py          # Environment configuration
├── spark/                     # Spark utilities
│   ├── __init__.py            # Database and cluster operations
│   ├── database.py            # Namespace and table management
│   ├── dataframe.py           # DataFrame utilities
│   └── cluster.py             # Spark cluster management
└── minio_governance/          # Data governance integration
    ├── __init__.py            # Governance operations exports
    └── operations.py          # API wrapper functions
```

## Configuration

The package uses Pydantic Settings for configuration management. Required environment variables are automatically validated:

### Core Authentication
- `KBASE_AUTH_TOKEN`: KBase authentication token
- `CDM_TASK_SERVICE_URL`: CDM Task Service endpoint
- `USER`: KBase username

### MinIO Configuration
- `MINIO_ENDPOINT_URL`: MinIO endpoint (hostname:port)
- `MINIO_ACCESS_KEY`: MinIO access key
- `MINIO_SECRET_KEY`: MinIO secret key
- `MINIO_SECURE`: Use secure connection (default: False)

### Spark Configuration
- `BERDL_POD_IP`: Pod IP address for Spark driver
- `SPARK_MASTER_URL`: Spark cluster master URL
- `BERDL_HIVE_METASTORE_URI`: Hive metastore connection URI

### Service Endpoints
- `SPARK_CLUSTER_MANAGER_API_URL`: Spark cluster management API
- `GOVERNANCE_API_URL`: Data governance API endpoint

## Development

### Running Tests

```bash
uv run pytest
```

### Code Quality

```bash
# Check code style
uv run ruff check .

# Format code
uv run ruff format .

# Auto-fix issues
uv run ruff check . --fix
```

### Building Package

```bash
# Install in development mode
uv sync --locked

# Build wheel
uv build
```

## API Reference

### Client Factories

```python
from berdl_notebook_utils.clients import (
    get_minio_client,           # MinIO S3 client
    get_task_service_client,    # CDM Task Service client
    get_governance_client,      # Data Governance client
    get_spark_cluster_client,   # Spark Cluster Manager client
)
```

### Spark Utilities

```python
from berdl_notebook_utils.spark import (
    # Database operations
    create_namespace_if_not_exists, table_exists, remove_table,
    list_tables, list_namespaces, get_table_info,
    
    # DataFrame operations  
    spark_to_pandas, display_df, display_namespace_viewer, read_csv,
    
    # Cluster management
    check_api_health, get_cluster_status, create_cluster, delete_cluster,
)
```

### Data Governance

```python
from berdl_notebook_utils.minio_governance import (
    # Health and credentials
    check_governance_health, get_minio_credentials,

    # Workspace management
    get_my_sql_warehouse, get_group_sql_warehouse,
    get_my_workspace, get_my_policies, get_my_groups,

    # Tenant management
    create_tenant_and_assign_users,

    # Table sharing
    share_table, unshare_table, make_table_public, make_table_private,
    get_table_access_info,
)
```

## Integration with BERDL Platform

This package is designed to work seamlessly with the BERDL platform:

- **JupyterHub Integration**: Automatically configured via IPython startup scripts
- **Authentication**: Uses KBase tokens for all service authentication
- **Resource Management**: Integrates with Kubernetes-based Spark clusters
- **Data Governance**: Full integration with MinIO policy management and audit logging
- **Metadata Management**: Connected to Hive metastore for structured data access

## Troubleshooting

### Common Issues

1. **Missing Environment Variables**: Check if all required environment variables are set
2. **Authentication Failures**: Verify KBase token validity
3. **Service Connectivity**: Ensure service URLs are accessible
4. **Import Errors**: Package installed correctly in notebook environment

### Health Checks

```python
# Check governance API health
from berdl_notebook_utils.minio_governance import check_governance_health
health = check_governance_health()
print(f"API Status: {health.status}")

# Check Spark cluster manager health
from berdl_notebook_utils.spark import check_api_health
cluster_health = check_api_health()
print(f"Cluster API Status: {cluster_health.status}")
```

### Debug Configuration

```python
# Inspect current settings
from berdl_notebook_utils import get_settings
settings = get_settings()
print(f"MinIO Endpoint: {settings.MINIO_ENDPOINT_URL}")
print(f"Governance API: {settings.GOVERNANCE_API_URL}")
```