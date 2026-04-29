import warnings
from functools import lru_cache

from cdmtaskserviceclient.client import CTSClient
from governance_client import AuthenticatedClient as GovernanceAuthenticatedClient
from hmsclient import HMSClient
from minio import Minio
from spark_manager_client.client import AuthenticatedClient as SparkAuthenticatedClient

from berdl_notebook_utils import BERDLSettings, get_settings
from berdl_notebook_utils.cache import kbase_token_dependent, sync_kbase_token_before_call
from berdl_notebook_utils.hms_pool import HMSClientPool, build_pool_from_settings


@sync_kbase_token_before_call
@kbase_token_dependent
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
        endpoint=settings.MINIO_ENDPOINT_URL.replace("https://", "").replace("http://", ""),
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY,
        secure=settings.MINIO_SECURE,
    )


@sync_kbase_token_before_call
@kbase_token_dependent
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


@sync_kbase_token_before_call
@kbase_token_dependent
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


@lru_cache(maxsize=1)
def get_hive_metastore_pool(settings: BERDLSettings | None = None) -> HMSClientPool:
    """Return the per-process Hive Metastore connection pool.

    Use this in preference to :func:`get_hive_metastore_client`. ``HMSClient``
    is a Thrift client and is **not thread-safe**: a single client instance
    serializes one in-flight call. The pool keeps that invariant while
    allowing many concurrent callers in the same process. See
    :class:`berdl_notebook_utils.hms_pool.HMSClientPool` for the correctness
    rationale and tunables.
    """
    return build_pool_from_settings(settings)


def get_hive_metastore_client(
    settings: BERDLSettings | None = None,  # noqa: ARG001 - kept for API compat
) -> HMSClient:
    """**DEPRECATED**: returns a single shared, non-thread-safe ``HMSClient``.

    Concurrent callers using the same instance will corrupt the underlying
    Thrift socket — see :mod:`berdl_notebook_utils.hms_pool`. Use
    :func:`get_hive_metastore_pool` instead and check out connections per
    call::

        with get_hive_metastore_pool().acquire() as client:
            return client.get_databases("*")

    This function is retained only so that downstream code that explicitly
    serializes its own access to a single client can continue to work during
    the transition. New code MUST NOT call it.
    """
    warnings.warn(
        "get_hive_metastore_client() returns a non-thread-safe singleton and "
        "is deprecated. Use get_hive_metastore_pool().acquire() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    pool = get_hive_metastore_pool(settings)
    # Build one fresh client (not from the pool) so the caller owns its full
    # lifecycle and the pool's invariants are not violated.
    # Disable lint of private call: documented escape hatch for legacy callers.
    return pool._new_client()  # type: ignore[attr-defined]  # noqa: SLF001
