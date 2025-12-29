"""
Tests for spark/dataframe.py - DataFrame utilities.
"""

from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import pytest
from pyspark.sql import DataFrame as SparkDataFrame

from berdl_notebook_utils.spark.dataframe import (
    spark_to_pandas,
    display_df,
    _detect_csv_delimiter,
    read_csv,
    _create_namespace_accordion,
    _update_namespace_view,
    display_namespace_viewer,
)


class TestSparkToPandas:
    """Tests for spark_to_pandas function."""

    def test_spark_to_pandas_basic(self):
        """Test basic Spark to Pandas conversion."""
        mock_spark_df = Mock()
        mock_offset_df = Mock()
        mock_limit_df = Mock()
        mock_pandas_df = Mock()

        mock_spark_df.offset.return_value = mock_offset_df
        mock_offset_df.limit.return_value = mock_limit_df
        mock_limit_df.toPandas.return_value = mock_pandas_df

        result = spark_to_pandas(mock_spark_df)

        assert result == mock_pandas_df
        mock_spark_df.offset.assert_called_once_with(0)
        mock_offset_df.limit.assert_called_once_with(1000)

    def test_spark_to_pandas_with_limit_and_offset(self):
        """Test Spark to Pandas with custom limit and offset."""
        mock_spark_df = Mock()
        mock_offset_df = Mock()
        mock_limit_df = Mock()
        mock_pandas_df = Mock()

        mock_spark_df.offset.return_value = mock_offset_df
        mock_offset_df.limit.return_value = mock_limit_df
        mock_limit_df.toPandas.return_value = mock_pandas_df

        spark_to_pandas(mock_spark_df, limit=500, offset=100)

        mock_spark_df.offset.assert_called_once_with(100)
        mock_offset_df.limit.assert_called_once_with(500)


class TestDisplayDf:
    """Tests for display_df function."""

    @patch("berdl_notebook_utils.spark.dataframe.show")
    @patch("berdl_notebook_utils.spark.dataframe.init_notebook_mode")
    def test_display_pandas_df(self, mock_init, mock_show):
        """Test displaying a pandas DataFrame."""
        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

        display_df(df)

        mock_init.assert_called_once_with(all_interactive=False)
        mock_show.assert_called_once()

    @patch("berdl_notebook_utils.spark.dataframe.spark_to_pandas")
    @patch("berdl_notebook_utils.spark.dataframe.show")
    @patch("berdl_notebook_utils.spark.dataframe.init_notebook_mode")
    def test_display_spark_df_converts(self, mock_init, mock_show, mock_convert):
        """Test displaying a Spark DataFrame converts to pandas."""
        mock_spark_df = Mock(spec=SparkDataFrame)
        mock_spark_df.count.return_value = 500
        mock_pandas_df = Mock()
        mock_convert.return_value = mock_pandas_df

        display_df(mock_spark_df)

        mock_convert.assert_called_once_with(mock_spark_df)
        mock_show.assert_called_once()

    @patch("berdl_notebook_utils.spark.dataframe.spark_to_pandas")
    @patch("berdl_notebook_utils.spark.dataframe.show")
    @patch("berdl_notebook_utils.spark.dataframe.init_notebook_mode")
    def test_display_large_spark_df_prints_message(self, mock_init, mock_show, mock_convert, capsys):
        """Test displaying large Spark DataFrame prints conversion message."""
        mock_spark_df = Mock(spec=SparkDataFrame)
        mock_spark_df.count.return_value = 5000
        mock_pandas_df = Mock()
        mock_convert.return_value = mock_pandas_df

        display_df(mock_spark_df)

        captured = capsys.readouterr()
        assert "Converting first 1000 rows" in captured.out

    @patch("berdl_notebook_utils.spark.dataframe.show")
    @patch("berdl_notebook_utils.spark.dataframe.init_notebook_mode")
    def test_display_df_custom_layout(self, mock_init, mock_show):
        """Test displaying with custom layout options."""
        df = pd.DataFrame({"col1": [1]})
        custom_layout = {"topStart": "info"}

        display_df(df, layout=custom_layout)

        mock_show.assert_called_once()


class TestDetectCsvDelimiter:
    """Tests for _detect_csv_delimiter function."""

    def test_detect_comma_delimiter(self):
        """Test detecting comma delimiter."""
        sample = "col1,col2,col3\nval1,val2,val3"

        result = _detect_csv_delimiter(sample)

        assert result == ","

    def test_detect_tab_delimiter(self):
        """Test detecting tab delimiter."""
        sample = "col1\tcol2\tcol3\nval1\tval2\tval3"

        result = _detect_csv_delimiter(sample)

        assert result == "\t"

    def test_detect_semicolon_delimiter(self):
        """Test detecting semicolon delimiter."""
        sample = "col1;col2;col3\nval1;val2;val3"

        result = _detect_csv_delimiter(sample)

        assert result == ";"

    def test_detect_delimiter_failure(self):
        """Test raises error when delimiter cannot be detected."""
        # Empty string causes csv.Sniffer to fail
        sample = ""

        with pytest.raises(ValueError, match="Could not detect CSV delimiter"):
            _detect_csv_delimiter(sample)


class TestReadCsv:
    """Tests for read_csv function."""

    @patch("berdl_notebook_utils.spark.dataframe.get_minio_client")
    def test_read_csv_with_auto_detect(self, mock_get_client):
        """Test read_csv with automatic delimiter detection."""
        mock_spark = Mock()
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_obj = Mock()
        mock_obj.read.return_value = b"col1,col2\nval1,val2"
        mock_client.get_object.return_value = mock_obj

        mock_df = Mock()
        mock_spark.read.csv.return_value = mock_df

        read_csv(mock_spark, "s3a://bucket/file.csv")

        mock_spark.read.csv.assert_called_once()
        call_kwargs = mock_spark.read.csv.call_args[1]
        assert call_kwargs["sep"] == ","

    def test_read_csv_with_explicit_delimiter(self):
        """Test read_csv with explicit delimiter."""
        mock_spark = Mock()
        mock_df = Mock()
        mock_spark.read.csv.return_value = mock_df

        read_csv(mock_spark, "s3a://bucket/file.csv", sep="\t")

        mock_spark.read.csv.assert_called_once()
        call_kwargs = mock_spark.read.csv.call_args[1]
        assert call_kwargs["sep"] == "\t"

    def test_read_csv_header_option(self):
        """Test read_csv passes header option."""
        mock_spark = Mock()
        mock_df = Mock()
        mock_spark.read.csv.return_value = mock_df

        read_csv(mock_spark, "s3a://bucket/file.csv", sep=",", header=False)

        call_kwargs = mock_spark.read.csv.call_args[1]
        assert call_kwargs["header"] is False


class TestCreateNamespaceAccordion:
    """Tests for _create_namespace_accordion function."""

    @patch("berdl_notebook_utils.spark.dataframe.Accordion")
    @patch("berdl_notebook_utils.spark.dataframe.VBox")
    @patch("berdl_notebook_utils.spark.dataframe.HTML")
    def test_create_namespace_accordion(self, mock_html, mock_vbox, mock_accordion):
        """Test creating namespace accordion widget."""
        mock_spark = Mock()
        mock_tables_df = pd.DataFrame({"tableName": ["table1", "table2"]})
        mock_spark.sql.return_value.toPandas.return_value = mock_tables_df

        mock_accordion_instance = Mock()
        mock_accordion_instance.children = ()
        mock_accordion.return_value = mock_accordion_instance

        namespace = Mock()
        namespace.namespace = "test_ns"

        _create_namespace_accordion(mock_spark, [namespace])

        mock_html.assert_called()
        mock_accordion_instance.set_title.assert_called_with(0, "Namespace: test_ns")


class TestUpdateNamespaceView:
    """Tests for _update_namespace_view function."""

    @patch("berdl_notebook_utils.spark.dataframe.display")
    @patch("berdl_notebook_utils.spark.dataframe.clear_output")
    @patch("berdl_notebook_utils.spark.dataframe.VBox")
    @patch("berdl_notebook_utils.spark.dataframe._create_namespace_accordion")
    @patch("berdl_notebook_utils.spark.dataframe.get_spark_session")
    def test_update_namespace_view(self, mock_get_spark, mock_create_accordion, mock_vbox, mock_clear, mock_display):
        """Test updating namespace viewer."""
        mock_spark = Mock()
        mock_get_spark.return_value = mock_spark

        namespace = Mock()
        namespace.namespace = "test_ns"
        mock_spark.sql.return_value.collect.return_value = [namespace]

        mock_sidecar = MagicMock()
        mock_create_accordion.return_value = Mock()
        mock_vbox.return_value = Mock()

        _update_namespace_view(mock_sidecar)

        mock_clear.assert_called_once_with(wait=True)
        mock_display.assert_called_once()


class TestDisplayNamespaceViewer:
    """Tests for display_namespace_viewer function."""

    @patch("berdl_notebook_utils.spark.dataframe._update_namespace_view")
    @patch("berdl_notebook_utils.spark.dataframe.Sidecar")
    def test_display_namespace_viewer(self, mock_sidecar_class, mock_update):
        """Test displaying namespace viewer creates sidecar."""
        mock_sidecar = Mock()
        mock_sidecar_class.return_value = mock_sidecar

        display_namespace_viewer()

        mock_sidecar_class.assert_called_once_with(title="Database Namespaces & Tables")
        mock_update.assert_called_once_with(mock_sidecar)
