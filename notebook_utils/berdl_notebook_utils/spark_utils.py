import csv
from threading import RLock
from typing import Optional

import itables.options as opt
from IPython.core.display_functions import clear_output
from IPython.display import display
from ipywidgets import Accordion, VBox, HTML
from itables import init_notebook_mode, show
from pandas import DataFrame as PandasDataFrame
from pyspark.sql import DataFrame as SparkDataFrame, SparkSession, DataFrame
from sidecar import Sidecar

from berdl_notebook_utils import get_minio_client
from berdl_notebook_utils.setup_spark_session import get_spark_session

lock = RLock()


def spark_to_pandas(spark_df: SparkDataFrame, limit: int = 1000, offset: int = 0) -> PandasDataFrame:
    """
    Convert a Spark DataFrame to a pandas DataFrame.

    :param spark_df: a Spark DataFrame
    :param limit: the number of rows to fetch
    :param offset: the number of rows to skip
    :return: a pandas DataFrame
    """

    return spark_df.offset(offset).limit(limit).toPandas()


def display_df(
    df: PandasDataFrame | SparkDataFrame,
    layout: dict = None,
    buttons: list = None,
    length_menu: list = None,
) -> None:
    """
    Display a pandas DataFrame using itables.
    iTables project page: https://github.com/mwouts/itables

    Notice itables.show() function is not compatible with Spark DataFrames. If a Spark DataFrame is passed to this
    function, it will be converted to a pandas DataFrame (first 1000 rows) before displaying it.

    :param df: a pandas DataFrame or a Spark DataFrame
    :param layout: layout options, refer to https://datatables.net/reference/option/layout
    :param buttons: buttons options, options refer to https://datatables.net/reference/button/
    :param length_menu: length menu options, refer to https://datatables.net/reference/option/lengthMenu
    :return:
    """
    # convert Spark DataFrame to pandas DataFrame
    if isinstance(df, SparkDataFrame):
        if df.count() > 1000:
            print("Converting first 1000 rows from Spark to Pandas DataFrame...")
        df = spark_to_pandas(df)

    # initialize itables for the notebook
    init_notebook_mode(all_interactive=False)

    # set default values if options are not provided
    default_layout = {
        "topStart": "search",
        "topEnd": "buttons",
        "bottomStart": "pageLength",
        "bottomEnd": "paging",
        "bottom2Start": "info",
    }
    default_buttons = ["csvHtml5", "excelHtml5", "print"]
    default_length_menu = [5, 10, 20]

    layout = layout or default_layout
    buttons = buttons or default_buttons
    length_menu = length_menu or default_length_menu

    with lock:
        opt.layout = layout
        show(df, buttons=buttons, lengthMenu=length_menu)


def _create_namespace_accordion(spark: SparkSession, namespaces: list) -> Accordion:
    """
    Create an Accordion widget displaying namespaces and their tables.

    ref: https://ipywidgets.readthedocs.io/en/latest/examples/Widget%20List.html#accordion
    """
    accordion = Accordion()

    for namespace in namespaces:
        namespace_name = namespace.namespace
        tables_df = spark.sql(f"SHOW TABLES IN {namespace_name}").toPandas()

        table_content = "<br>".join(tables_df["tableName"]) if not tables_df.empty else "No tables available"
        table_list = HTML(value=table_content)

        namespace_section = VBox([table_list])
        accordion.children += (namespace_section,)
        accordion.set_title(len(accordion.children) - 1, f"Namespace: {namespace_name}")

    return accordion


def _update_namespace_view(
    sidecar: Sidecar,
) -> None:
    """
    Update the namespace viewer in the sidecar with the latest namespaces and tables.
    """
    with get_spark_session() as spark:
        namespaces = spark.sql("SHOW DATABASES").collect()
        print("Available Namespaces:", [ns.namespace for ns in namespaces])
        updated_accordion = _create_namespace_accordion(spark, namespaces)

    ui = VBox([updated_accordion])

    with sidecar:
        clear_output(wait=True)
        display(ui)


def display_namespace_viewer() -> None:
    """
    Display the namespace viewer in the Jupyter Notebook Sidecar.
    """
    sidecar = Sidecar(title="Database Namespaces & Tables")
    _update_namespace_view(sidecar)


def read_csv(
    spark: SparkSession,
    path: str,
    header: bool = True,
    sep: Optional[str] = None,
    **kwargs,
) -> DataFrame:
    """
    Read CSV file from MinIO into a Spark DataFrame with automatic delimiter detection.

    Args:
        spark: Spark session instance
        path: MinIO path to CSV file (e.g., "s3a://bucket/file.csv" or "bucket/file.csv")
        header: Whether CSV file has header row
        sep: CSV delimiter. If None, will attempt auto-detection
        minio_url: MinIO URL (uses MINIO_URL env var if None)
        access_key: MinIO access key (uses MINIO_ACCESS_KEY env var if None)
        secret_key: MinIO secret key (uses MINIO_SECRET_KEY env var if None)
        **kwargs: Additional arguments passed to spark.read.csv()

    Returns:
        Spark DataFrame containing CSV data

    Example:
        >>> # Basic usage with auto-detection
        >>> df = read_csv(spark, "s3a://my-bucket/data.csv")

        >>> # With explicit delimiter
        >>> df = read_csv(spark, "s3a://my-bucket/data.tsv", sep="\\t")

        >>> # With custom MinIO credentials
        >>> df = read_csv(
        ...     spark,
        ...     "s3a://my-bucket/data.csv",
        ...     minio_url="http://localhost:9000",
        ...     access_key="my-key",
        ...     secret_key="my-secret"
        ... )
    """
    # Auto-detect delimiter if not provided
    if sep is None:
        client = get_minio_client()

        # Parse S3 path to get bucket and key
        s3_path = path.replace("s3a://", "")
        bucket, key = s3_path.split("/", 1)

        # Sample file to detect delimiter
        obj = client.get_object(bucket, key)
        sample = obj.read(8192).decode()
        sep = _detect_csv_delimiter(sample)
        print(f"Auto-detected CSV delimiter: '{sep}'")

    # Read CSV into DataFrame
    return spark.read.csv(path, header=header, sep=sep, **kwargs)


def _detect_csv_delimiter(sample: str) -> str:
    """
    Detect CSV delimiter from a sample string.

    Args:
        sample: Sample string from CSV file

    Returns:
        Detected delimiter character

    Raises:
        ValueError: If delimiter cannot be detected
    """
    try:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample)
        return dialect.delimiter
    except Exception as e:
        raise ValueError(f"Could not detect CSV delimiter: {e}. Please provide delimiter explicitly.") from e
