"""
Initialize MinIO credentials and basic MinIO client.

This runs after 00-notebookutils.py loads all the imports, so get_minio_credentials
is already available in the global namespace.
"""

try:
    # Set MinIO credentials to environment - also creates user if they don't exist
    credentials = get_minio_credentials()  # noqa: F821
    print(f"✅ MinIO credentials set for user: {credentials.username}")

except Exception as e:
    import warnings

    warnings.warn(f"Failed to set MinIO credentials: {str(e)}", UserWarning)
    print(f"❌ Failed to set MinIO credentials: {str(e)}")
    credentials = None
