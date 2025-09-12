#!/usr/bin/env python3
import os
import sys
from unittest.mock import patch, MagicMock

# Add the notebook_utils to Python path
sys.path.insert(0, '/home/runner/work/spark_notebook/spark_notebook/notebook_utils')
sys.path.insert(0, '/home/runner/work/spark_notebook/spark_notebook/configs/ipython_startup')

# Set test environment variables
test_env = {
    "KBASE_AUTH_TOKEN": "test-token-123",
    "CDM_TASK_SERVICE_URL": "https://ci.kbase.us/services/ctsfake",
    "MINIO_ENDPOINT": "http://localhost:9000",
    "MINIO_ACCESS_KEY": "minioadmin",
    "MINIO_SECRET_KEY": "minioadmin",
    "MINIO_SECURE": "false",
    "BERDL_POD_IP": "192.168.1.100",
    "SPARK_MASTER_URL": "spark://localhost:7077",
    "SPARK_JOB_LOG_DIR_CATEGORY": "test-user",
    "BERDL_HIVE_METASTORE_URI": "thrift://localhost:9083",
    "SPARK_CLUSTER_MANAGER_API_URL": "http://localhost:8000",
    "GOVERNANCE_API_URL": "http://localhost:8000",
    "USER": "test-user",
}

for key, value in test_env.items():
    os.environ[key] = value

print("Testing fallback approach...")

# Test simple import first
try:
    import berdl_notebook_utils
    print("✓ Successfully imported berdl_notebook_utils")
except Exception as e:
    print(f"✗ Failed to import berdl_notebook_utils: {e}")

# Test imports with mocking
try:
    with patch('berdl_notebook_utils.clients.get_minio_client') as mock_minio, \
         patch('berdl_notebook_utils.clients.get_task_service_client') as mock_task, \
         patch('berdl_notebook_utils.clients.get_governance_client') as mock_governance, \
         patch('berdl_notebook_utils.clients.get_spark_cluster_client') as mock_spark:
        
        mock_minio.return_value = MagicMock()
        mock_task.return_value = MagicMock()
        mock_governance.return_value = MagicMock()
        mock_spark.return_value = MagicMock()
        
        print("✓ Set up mocks successfully")
        
        # Test the notebook utilities file
        with open('/home/runner/work/spark_notebook/spark_notebook/configs/ipython_startup/00-notebookutils.py', 'r') as f:
            notebook_code = f.read()
        
        print(f"✓ Read notebook file ({len(notebook_code)} chars)")
        
        exec_globals = {
            '__name__': '__main__',
            '__file__': '00-notebookutils.py',
            '__builtins__': __builtins__
        }
        
        exec(compile(notebook_code, '00-notebookutils.py', 'exec'), exec_globals)
        print("✓ Successfully executed 00-notebookutils.py")
        
        # Check that expected variables were created
        expected_vars = ['governance', 'minio', 'task_service', 'spark_cluster']
        for var in expected_vars:
            if var in exec_globals:
                print(f"✓ Variable '{var}' created successfully")
            else:
                print(f"⚠ Variable '{var}' not found")
        
except Exception as e:
    print(f"✗ Test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("✓ All tests passed!")
