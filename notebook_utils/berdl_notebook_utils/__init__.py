from berdl_settings import BERDLSettings, get_settings
from clients import get_minio_client, get_task_service_client
from setup_spark_session import get_spark_session
from spark_utils import display_df, spark_to_pandas


def help():
    print(
        """
    berdl_notebook_utils

    A collection of utilities for working with BERDL in Jupyter notebooks.

    Available functions:
    - get_settings(): Get the BERDL settings from environment variables.
    - get_minio_client(): Get a MinIO client instance.
    - get_task_service_client(): Get a CDM Task Service client instance.
    - get_spark_session(): Get a configured Spark session.

    Example usage:
    from berdl_notebook_utils import get_settings, get_minio_client, get_task_service_client, get_spark_session

    settings = get_settings()
    minio_client = get_minio_client()
    cts_client = get_task_service_client()
    spark = get_spark_session()
    """
    )
