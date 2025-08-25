from functools import lru_cache

from cdmtaskserviceclient.client import CTSClient
from minio import Minio

from berdl_notebook_utils import get_settings, BERDLSettings


@lru_cache(maxsize=1)
def get_task_service_client(settings: BERDLSettings = None) -> CTSClient:
    """ Get an instance of the CDM Task Service client.
    See:
    https://github.com/kbase/cdm-task-service-client/
    https://github.com/kbase/cdm-task-service/
    https://github.com/kbase/cdm-spark-events
    Returns:"""
    if settings is None:
        settings = get_settings()
    return CTSClient(settings.KBASE_AUTH_TOKEN, url=settings.CDM_TASK_SERVICE_URL)


@lru_cache(maxsize=1)
def get_minio_client(settings: BERDLSettings = None) -> Minio:
    """
    * Get an instance of the Minio client.
    * Note: Your minio credentials are refreshed on each restart of the jupyter notebook.
    * That means any running jobs with these credentials will fail when the credentials change
    See: Governance API
    """
    if settings is None:
        settings = get_settings()

    return Minio(endpoint=settings.MINIO_ENDPOINT, access_key=settings.MINIO_ACCESS_KEY, secret_key=settings.MINIO_SECRET_KEY,
                 secure=settings.MINIO_SECURE)
