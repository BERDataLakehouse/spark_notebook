from berdl_notebook_utils.berdl_settings import BERDLSettings, get_settings  # noqa F401
from berdl_notebook_utils.clients import get_minio_client, get_task_service_client  # noqa F401
from berdl_notebook_utils.setup_spark_session import get_spark_session  # noqa F401
from berdl_notebook_utils.spark import (  # noqa F401
    # Database operations
    create_namespace_if_not_exists,
    table_exists,
    remove_table,
    list_tables,
    list_namespaces,
    get_table_info,
    # DataFrame operations
    display_df,
    spark_to_pandas,
    read_csv,
    display_namespace_viewer,
)

from berdl_notebook_utils import berdl_notebook_help  # noqa F401

# MinIO Data Governance integration
from berdl_notebook_utils.minio_governance.client import DataGovernanceClient  # noqa F401
from berdl_notebook_utils.minio_governance.utils import (  # noqa F401
    check_governance_health,
    get_minio_credentials,
    get_my_sql_warehouse,
    get_my_workspace,
    get_my_policies,
    get_table_access_info,
    share_table,
    unshare_table,
    make_table_public,
    make_table_private,
)

# Pre-initialize data governance client for easy access
governance = DataGovernanceClient()  # noqa F401

from berdl_notebook_utils.spark.cluster import (  # noqa F401
    # Cluster management
    check_api_health,
    get_cluster_status,
    create_cluster,
    delete_cluster,
)
