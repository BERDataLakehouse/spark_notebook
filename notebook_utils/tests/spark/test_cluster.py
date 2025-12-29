"""
Tests for spark/cluster.py - Spark cluster management.
"""

from unittest.mock import Mock, patch, MagicMock
import pytest


class TestRaiseApiError:
    """Tests for _raise_api_error helper function."""

    def test_raises_with_status_code(self):
        """Test raises error with status code."""
        from berdl_notebook_utils.spark.cluster import _raise_api_error

        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.content = None

        with pytest.raises(ValueError, match="API Error \\(HTTP 500\\)"):
            _raise_api_error(mock_response)

    def test_raises_with_content(self):
        """Test raises error with content message."""
        from berdl_notebook_utils.spark.cluster import _raise_api_error

        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.content = b"Bad request"

        with pytest.raises(ValueError, match="Bad request"):
            _raise_api_error(mock_response)


class TestCheckApiHealth:
    """Tests for check_api_health function."""

    @patch("berdl_notebook_utils.spark.cluster.get_spark_cluster_client")
    @patch("berdl_notebook_utils.spark.cluster.health_check_health_get")
    def test_check_api_health_success(self, mock_health_check, mock_get_client):
        """Test health check returns response on success."""
        from berdl_notebook_utils.spark.cluster import check_api_health

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.parsed = Mock(status="healthy")
        mock_health_check.sync_detailed.return_value = mock_response

        result = check_api_health()

        assert result.status == "healthy"

    @patch("berdl_notebook_utils.spark.cluster.get_spark_cluster_client")
    @patch("berdl_notebook_utils.spark.cluster.health_check_health_get")
    def test_check_api_health_failure(self, mock_health_check, mock_get_client):
        """Test health check raises error on failure."""
        from berdl_notebook_utils.spark.cluster import check_api_health

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_response = Mock()
        mock_response.status_code = 503
        mock_response.content = b"Service unavailable"
        mock_health_check.sync_detailed.return_value = mock_response

        with pytest.raises(ValueError, match="API Error"):
            check_api_health()


class TestGetClusterStatus:
    """Tests for get_cluster_status function."""

    @patch("berdl_notebook_utils.spark.cluster.get_spark_cluster_client")
    @patch("berdl_notebook_utils.spark.cluster.get_cluster_status_clusters_get")
    def test_get_cluster_status_success(self, mock_get_status, mock_get_client):
        """Test get cluster status returns status on success."""
        from berdl_notebook_utils.spark.cluster import get_cluster_status

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.parsed = Mock(status="running", worker_count=2)
        mock_get_status.sync_detailed.return_value = mock_response

        result = get_cluster_status()

        assert result.status == "running"
        assert result.worker_count == 2

    @patch("berdl_notebook_utils.spark.cluster.get_spark_cluster_client")
    @patch("berdl_notebook_utils.spark.cluster.get_cluster_status_clusters_get")
    def test_get_cluster_status_failure(self, mock_get_status, mock_get_client):
        """Test get cluster status raises error on failure."""
        from berdl_notebook_utils.spark.cluster import get_cluster_status

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.content = b"Cluster not found"
        mock_get_status.sync_detailed.return_value = mock_response

        with pytest.raises(ValueError, match="API Error"):
            get_cluster_status()


class TestCreateCluster:
    """Tests for create_cluster function."""

    @patch("berdl_notebook_utils.spark.cluster.os")
    @patch("berdl_notebook_utils.spark.cluster.get_spark_cluster_client")
    @patch("berdl_notebook_utils.spark.cluster.create_cluster_clusters_post")
    def test_create_cluster_with_force(self, mock_create, mock_get_client, mock_os):
        """Test create cluster with force=True skips confirmation."""
        from berdl_notebook_utils.spark.cluster import create_cluster

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.parsed = Mock(master_url="spark://master:7077")
        mock_create.sync_detailed.return_value = mock_response

        result = create_cluster(
            worker_count=2,
            worker_cores=2,
            worker_memory="8GiB",
            force=True,
        )

        assert result.master_url == "spark://master:7077"
        mock_os.environ.__setitem__.assert_called()

    @patch("builtins.input", return_value="n")
    def test_create_cluster_aborted(self, mock_input):
        """Test create cluster aborted by user."""
        from berdl_notebook_utils.spark.cluster import create_cluster

        result = create_cluster(force=False)

        assert result is None

    @patch("berdl_notebook_utils.spark.cluster.os")
    @patch("berdl_notebook_utils.spark.cluster.get_spark_cluster_client")
    @patch("berdl_notebook_utils.spark.cluster.create_cluster_clusters_post")
    def test_create_cluster_failure(self, mock_create, mock_get_client, mock_os):
        """Test create cluster raises error on API failure."""
        from berdl_notebook_utils.spark.cluster import create_cluster

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.content = b"Internal server error"
        mock_create.sync_detailed.return_value = mock_response

        with pytest.raises(ValueError, match="API Error"):
            create_cluster(force=True)


class TestDeleteCluster:
    """Tests for delete_cluster function."""

    @patch("berdl_notebook_utils.spark.cluster.os")
    @patch("berdl_notebook_utils.spark.cluster.get_spark_cluster_client")
    @patch("berdl_notebook_utils.spark.cluster.delete_cluster_clusters_delete")
    def test_delete_cluster_success(self, mock_delete, mock_get_client, mock_os):
        """Test delete cluster success."""
        from berdl_notebook_utils.spark.cluster import delete_cluster

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.parsed = Mock(message="Cluster deleted successfully")
        mock_delete.sync_detailed.return_value = mock_response

        result = delete_cluster()

        assert result.message == "Cluster deleted successfully"
        mock_os.environ.pop.assert_called_with("SPARK_MASTER_URL", None)

    @patch("berdl_notebook_utils.spark.cluster.get_spark_cluster_client")
    @patch("berdl_notebook_utils.spark.cluster.delete_cluster_clusters_delete")
    def test_delete_cluster_failure(self, mock_delete, mock_get_client):
        """Test delete cluster raises error on failure."""
        from berdl_notebook_utils.spark.cluster import delete_cluster

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.content = b"Cluster not found"
        mock_delete.sync_detailed.return_value = mock_response

        with pytest.raises(ValueError, match="API Error"):
            delete_cluster()
