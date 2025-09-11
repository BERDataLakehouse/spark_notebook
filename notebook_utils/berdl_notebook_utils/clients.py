from functools import lru_cache
from typing import Optional

from cdmtaskserviceclient.client import CTSClient
from minio import Minio

from berdl_notebook_utils import BERDLSettings, get_settings
from berdl_notebook_utils.minio_governance.client import DataGovernanceClient


@lru_cache(maxsize=1)
def get_task_service_client(settings: Optional[BERDLSettings] = None) -> CTSClient:
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
def get_minio_client(settings: Optional[BERDLSettings] = None) -> Minio:
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
def get_governance_client(settings: Optional[BERDLSettings] = None) -> DataGovernanceClient:
    """
    Get an instance of the Data Governance client.

    The governance client is used for managing MinIO permissions, user workspaces,
    and data sharing operations in the BERDL environment.
    """
    if settings is None:
        settings = get_settings()

    return DataGovernanceClient(kbase_token=settings.KBASE_AUTH_TOKEN)
