import warnings
from functools import lru_cache

from cdmtaskserviceclient.client import CTSClient
from governance_client import AuthenticatedClient as GovernanceAuthenticatedClient
from hmsclient import HMSClient
from minio import Minio
from spark_manager_client.client import AuthenticatedClient as SparkAuthenticatedClient

from berdl_notebook_utils import BERDLSettings, get_settings
from berdl_notebook_utils.kbase_token_cache import kbase_token_dependent, sync_kbase_token_before_call
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
def get_s3_client(settings: BERDLSettings | None = None) -> Minio:
    """
    * Get an instance of the Minio client.
    * Note: Your minio credentials are refreshed on each restart of the jupyter notebook.
    * That means any running jobs with these credentials will fail when the credentials change
    See: Governance API
    """
    if settings is None:
        settings = get_settings()

    return Minio(
        endpoint=settings.S3_ENDPOINT_URL.replace("https://", "").replace("http://", ""),
        access_key=settings.S3_ACCESS_KEY,
        secret_key=settings.S3_SECRET_KEY,
        secure=settings.S3_SECURE,
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
def _get_hive_metastore_pool_cached() -> HMSClientPool:
    """Internal: lru-cached pool built from default (env-driven) settings.

    Kept private because :class:`BERDLSettings` is a Pydantic ``BaseSettings``
    model and is not hashable, so we cannot ``lru_cache`` a function that
    accepts one as an argument. The public :func:`get_hive_metastore_pool`
    wrapper bypasses this cache when a custom ``settings`` is supplied.
    """
    return build_pool_from_settings(None)


def get_hive_metastore_pool(settings: BERDLSettings | None = None) -> HMSClientPool:
    """Return the per-process Hive Metastore connection pool.

    Use this in preference to :func:`get_hive_metastore_client`. ``HMSClient``
    is a Thrift client and is **not thread-safe**: a single client instance
    serializes one in-flight call. The pool keeps that invariant while
    allowing many concurrent callers in the same process. See
    :class:`berdl_notebook_utils.hms_pool.HMSClientPool` for the correctness
    rationale and tunables.

    With the default (no-argument) form, the pool is constructed once per
    process and cached. When ``settings`` is supplied, a fresh pool is
    built each call (the cache is bypassed because ``BERDLSettings`` is
    not hashable).
    """
    if settings is None:
        return _get_hive_metastore_pool_cached()
    return build_pool_from_settings(settings)


def get_hive_metastore_client(
    settings: BERDLSettings | None = None,
) -> HMSClient:
    """**DEPRECATED**: returns a fresh, single-owner ``HMSClient`` per call.

    The previous implementation returned an ``@lru_cache``'d singleton that
    was unsafe to share across threads — concurrent callers corrupted its
    Thrift socket (see :mod:`berdl_notebook_utils.hms_pool` for details).
    To keep the old call sites working without keeping the unsafe behavior,
    each call now constructs and returns a **new** ``HMSClient``; the
    caller is responsible for its full lifecycle (``open()``/``close()``)
    and MUST NOT share it across threads.

    New code should use :func:`get_hive_metastore_pool` instead::

        with get_hive_metastore_pool().acquire() as client:
            return client.get_databases("*")
    """
    warnings.warn(
        "get_hive_metastore_client() is deprecated. It now returns a fresh, "
        "single-owner HMSClient per call (the previous singleton was not "
        "thread-safe). Use get_hive_metastore_pool().acquire() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    pool = get_hive_metastore_pool(settings)
    # Return a fresh, UNOPENED client built through the pool's factory.
    # The legacy contract was: caller does open()/.../close(). We preserve
    # that contract while still applying socket_timeout_ms so legacy callers
    # also get wedge-protection.
    # Disable lint of private call: documented escape hatch for legacy callers.
    return pool._build_client()  # type: ignore[attr-defined]  # noqa: SLF001
