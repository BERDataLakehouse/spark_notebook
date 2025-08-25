from berdl_notebook_utils.berdl_settings import BERDLSettings, get_settings
from berdl_notebook_utils.clients import get_minio_client, get_task_service_client
from berdl_notebook_utils.setup_spark_session import get_spark_session
from berdl_notebook_utils.spark_utils import display_df, spark_to_pandas, read_csv

# add these to all!

__all__ = [
    "BERDLSettings",
    "get_settings",
    "get_minio_client",
    "get_task_service_client",
    "get_spark_session",
    "display_df",
    "spark_to_pandas",
    "read_csv",
]


def help():
    print(
        """
    berdl_notebook_utils
    ====================

    A collection of utilities for working with Spark, MinIO, and other services in a Jupyter notebook environment.

    Modules:
    --------
    - BERDLSettings: Configuration settings for the BERDL environment.
    - get_settings: Function to retrieve the current settings.
    - get_minio_client: Function to get a MinIO client instance.
    - get_task_service_client: Function to get a CDM Task Service client instance.
    - get_spark_session: Function to create or retrieve a Spark session.
    - display_df: Function to display pandas or Spark DataFrames in a Jupyter notebook using itables.
    - spark_to_pandas: Function to convert a Spark DataFrame to a pandas DataFrame.

    Usage:
    ------
    Import the desired functions or classes from the module and use them in your notebook.

    Example:
    --------
    from berdl_notebook_utils import get_spark_session, display_df

    spark = get_spark_session()
    df = spark.read.csv("s3a://your-bucket/your-file.csv")
    display_df(df)

    For more detailed documentation, refer to the individual module docstrings.
    """
    )
