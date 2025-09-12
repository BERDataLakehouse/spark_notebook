from functools import lru_cache

from cdmtaskserviceclient.client import CTSClient
from minio import Minio
from spark_manager_client.client import AuthenticatedClient as SparkAuthenticatedClient

from berdl_notebook_utils import BERDLSettings, get_settings
from governance_client import AuthenticatedClient as GovernanceAuthenticatedClient


@lru_cache(maxsize=1)
def get_task_service_client(settings: BERDLSettings | None = None) -> CTSClient:
    """Get an instance of the CDM Task Service client.
    See:
    https://github.com/kbase/cdm-task-service-client/
    https://github.com/kbase/cdm-task-service/
    https://github.com/kbase/cdm-spark-events
    Returns:"""
    if settings is None:
        settings = get_settings()
    return CTSClient(settings.KBASE_AUTH_TOKEN, url=str(settings.CDM_TASK_SERVICE_URL))


@lru_cache(maxsize=1)
def get_minio_client(settings: BERDLSettings | None = None) -> Minio:
    """
    * Get an instance of the Minio client.
    * Note: Your minio credentials are refreshed on each restart of the jupyter notebook.
    * That means any running jobs with these credentials will fail when the credentials change
    See: Governance API
    """
    if settings is None:
        settings = get_settings()

    return Minio(
        endpoint=str(settings.MINIO_ENDPOINT),
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY,
        secure=settings.MINIO_SECURE,
    )


@lru_cache(maxsize=1)
def get_governance_client(
    settings: BERDLSettings | None = None,
) -> GovernanceAuthenticatedClient:
    """
    Get the governance client for MinIO data management.

    This provides access to all governance API endpoints including credentials,
    workspace management, and data sharing operations.
    """
    if settings is None:
        settings = get_settings()

    return GovernanceAuthenticatedClient(
        base_url=str(settings.GOVERNANCE_API_URL),
        token=settings.KBASE_AUTH_TOKEN,
    )


@lru_cache(maxsize=1)
def get_spark_cluster_client(
    settings: BERDLSettings | None = None,
) -> SparkAuthenticatedClient:
    """
    Get an authenticated Spark Cluster Manager API client.

    Args:
        settings: Optional BERDLSettings instance. If None, reads from environment.

    Returns:
        SparkAuthenticatedClient with KBase authentication
    """
    if settings is None:
        settings = get_settings()

    return SparkAuthenticatedClient(
        base_url=str(settings.SPARK_CLUSTER_MANAGER_API_URL),
        token=settings.KBASE_AUTH_TOKEN,
    )
