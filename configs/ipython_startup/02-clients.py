"""
Initialize all BERDL service clients for easy access in notebooks.

This runs after credentials are set up in 01-minio-credentials.py.
All imports are available from 00-notebookutils.py.
"""

# Initialize MinIO client
try:
    minio = get_minio_client() # type: ignore
    print("✅ MinIO client initialized")
except Exception as e:
    print(f"❌ Failed to initialize MinIO client: {e}")
    minio = None

# Initialize governance client (required for most operations)
try:
    governance = get_governance_client() # type: ignore
    print("✅ Governance client initialized")
except Exception as e:
    print(f"❌ Failed to initialize governance client: {e}")
    governance = None

# Initialize Hive Metastore client
try:
    hms_client = get_hive_metastore_client() # type: ignore
    print("✅ Hive Metastore client initialized")
except Exception as e:
    print(f"❌ Failed to initialize Hive Metastore client: {e}")
    hms_client = None

# Initialize Task Service client (optional - may not be available in local dev)
try:
    task_service = get_task_service_client() # type: ignore
    print("✅ Task Service client initialized")
except Exception as e:
    print(f"⚠️  Task Service client not available: {e}")
    task_service = None

# Initialize Spark Cluster Manager client (optional - may not be available in local dev)
try:
    spark_cluster = get_spark_cluster_client()
    print("✅ Spark Cluster Manager client initialized")
except Exception as e:
    print(f"⚠️  Spark Cluster Manager client not available: {e}")
    spark_cluster = None

print("🔧 All available clients initialized and ready for use!")
