#!/usr/bin/env python3
"""
Test script to validate 00-notebookutils.py imports work correctly.
This script runs inside the container and tests the notebook utilities.
"""
import sys
import os
import traceback
from unittest.mock import patch, MagicMock


def test_imports():
    """Test that all imports in 00-notebookutils.py work without errors."""
    print("Testing imports from 00-notebookutils.py...")
    
    # Set up test environment variables
    test_env = {
        "KBASE_AUTH_TOKEN": "test-token-123",
        "CDM_TASK_SERVICE_URL": "https://ci.kbase.us/services/ctsfake",
        "MINIO_ENDPOINT": "http://localhost:9000",
        "MINIO_ACCESS_KEY": "minioadmin",
        "MINIO_SECRET_KEY": "minioadmin",
        "MINIO_SECURE": "false",
        "MINIO_SECURE_FLAG": "false",
        "BERDL_POD_IP": "192.168.1.100",
        "SPARK_MASTER_URL": "spark://localhost:7077",
        "SPARK_JOB_LOG_DIR_CATEGORY": "test-user",
        "BERDL_HIVE_METASTORE_URI": "thrift://localhost:9083",
        "SPARK_CLUSTER_MANAGER_API_URL": "http://localhost:8000",
        "GOVERNANCE_API_URL": "http://localhost:8000",
        "USER": "test-user",
    }
    
    # Set environment variables
    for key, value in test_env.items():
        os.environ[key] = value
    
    print(f"✓ Set {len(test_env)} environment variables")
    
    try:
        # Check if the notebook utils file exists
        notebook_file = '/configs/ipython_startup/00-notebookutils.py'
        if not os.path.exists(notebook_file):
            print(f"✗ File not found: {notebook_file}")
            print("Available files in /configs/ipython_startup/:")
            if os.path.exists('/configs/ipython_startup'):
                for f in os.listdir('/configs/ipython_startup'):
                    print(f"  - {f}")
            return False
        
        print(f"✓ Found notebook file: {notebook_file}")
        
        # Add the path to Python path
        sys.path.insert(0, '/home')  # Add the working directory
        
        # Try to import individual components first to see what works
        print("Testing individual imports...")
        
        try:
            from berdl_notebook_utils.berdl_settings import BERDLSettings, get_settings
            print("✓ Successfully imported BERDLSettings")
        except Exception as e:
            print(f"⚠ Warning: Could not import BERDLSettings: {e}")
        
        # Mock the client creation functions to prevent network calls
        with patch('berdl_notebook_utils.clients.get_minio_client') as mock_minio, \
             patch('berdl_notebook_utils.clients.get_task_service_client') as mock_task, \
             patch('berdl_notebook_utils.clients.get_governance_client') as mock_governance, \
             patch('berdl_notebook_utils.clients.get_spark_cluster_client') as mock_spark:
            
            # Set up mock return values
            mock_minio.return_value = MagicMock()
            mock_task.return_value = MagicMock()
            mock_governance.return_value = MagicMock()
            mock_spark.return_value = MagicMock()
            
            print("✓ Set up mocks for external clients")
            
            # Read and execute the notebook utilities code
            print("Executing 00-notebookutils.py...")
            with open(notebook_file, 'r') as f:
                notebook_code = f.read()
            
            print(f"✓ Read {len(notebook_code)} characters from notebook file")
            
            # Execute the notebook utilities code in a controlled environment
            exec_globals = {
                '__name__': '__main__',
                '__file__': notebook_file,
                '__builtins__': __builtins__
            }
            
            exec(compile(notebook_code, notebook_file, 'exec'), exec_globals)
            print("✓ Successfully executed 00-notebookutils.py")
            
            # Verify that the expected variables are created
            expected_vars = ['governance', 'minio', 'task_service', 'spark_cluster']
            for var in expected_vars:
                if var in exec_globals:
                    print(f"✓ Variable '{var}' created successfully")
                else:
                    print(f"⚠ Warning: Expected variable '{var}' not found")
            
            return True
            
    except Exception as e:
        print(f"✗ Error testing imports: {e}")
        print("Traceback:")
        traceback.print_exc()
        return False


def main():
    """Main test function."""
    print("Starting notebook utils test...")
    print(f"Python version: {sys.version}")
    print(f"Python path: {sys.path}")
    print(f"Current working directory: {os.getcwd()}")
    
    success = test_imports()
    
    if success:
        print("\n✓ All tests passed!")
        sys.exit(0)
    else:
        print("\n✗ Tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()