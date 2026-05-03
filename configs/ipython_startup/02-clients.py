"""
Initialize all BERDL service clients for easy access in notebooks.

This runs after credentials are set up in 01-minio-credentials.py.
All imports are available from 00-notebookutils.py.
"""

# Setup logging
import logging

logger = logging.getLogger("berdl.startup")

# Initialize MinIO client
try:
    minio = get_s3_client()  # noqa: F821
    logger.info("✅ MinIO client initialized")
except Exception as e:
    logger.error(f"❌ Failed to initialize MinIO client: {e}")
    minio = None

# Initialize governance client (required for most operations)
try:
    governance = get_governance_client()  # noqa: F821
    logger.info("✅ Governance client initialized")
except Exception as e:
    logger.error(f"❌ Failed to initialize governance client: {e}")
    governance = None

# Initialize Hive Metastore client
try:
    hms_client = get_hive_metastore_client()  # noqa: F821
    logger.info("✅ Hive Metastore client initialized")
except Exception as e:
    logger.error(f"❌ Failed to initialize Hive Metastore client: {e}")
    hms_client = None

# Initialize Task Service client (optional - may not be available in local dev)
try:
    task_service = get_task_service_client()  # noqa: F821
    logger.info("✅ Task Service client initialized")
except Exception as e:
    logger.warning(f"⚠️  Task Service client not available: {e}")
    task_service = None

# Initialize Spark Cluster Manager client (optional - may not be available in local dev)
try:
    spark_cluster = get_spark_cluster_client()  # noqa: F821
    logger.info("✅ Spark Cluster Manager client initialized")
except Exception as e:
    logger.warning(f"⚠️  Spark Cluster Manager client not available: {e}")
    spark_cluster = None

# Initialize Datalake MCP Server client (optional - may not be available in local dev)
try:
    mcp_client = get_datalake_mcp_client()  # noqa: F821
    logger.info("✅ Datalake MCP Server client initialized")
except Exception as e:
    logger.warning(f"⚠️  Datalake MCP Server client not available: {e}")
    mcp_client = None

logger.info("🔧 All available clients initialized and ready for use!")
