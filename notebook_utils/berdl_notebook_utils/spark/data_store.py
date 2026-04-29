"""Module for interacting with Spark databases and tables.

This module lists databases (across Iceberg catalogs *and* the Hive
metastore), tables, and their schemas. Filtering is driven entirely by
``BERDLSettings`` (``USER``, ``POLARIS_PERSONAL_CATALOG``,
``POLARIS_TENANT_CATALOGS``) — there are no governance-API calls on the
listing path.

Output identifiers:

- Iceberg namespaces are returned in ``catalog.namespace`` form
  (e.g. ``my.demo``, ``globalusers.shared_data``).
- Hive databases (Delta tables registered under ``spark_catalog``) are
  returned as flat names (e.g. ``u_alice__demo``).

Both forms can be used directly in queries:
``SELECT * FROM {database}.{table}``.
"""

import json
import logging
import re
from typing import Any, Dict, List, Optional, Union

from pyspark.sql import SparkSession

from .. import hive_metastore
from ..berdl_settings import BERDLSettings, get_settings
from ..setup_spark_session import (
    _get_personal_catalog_aliases,
    _get_tenant_catalog_alias,
    get_spark_session,
)

logger = logging.getLogger(__name__)

# Catalogs to exclude from Iceberg discovery (non-Iceberg catalogs that
# happen to be registered under spark.sql.catalog.*).
_EXCLUDED_CATALOGS = {"spark_catalog"}

# Pattern matching only top-level Spark catalog registration keys
# (``spark.sql.catalog.<name>``) — sub-properties like ``.uri`` are skipped.
_CATALOG_KEY_PATTERN = re.compile(r"^spark\.sql\.catalog\.([a-zA-Z_][a-zA-Z0-9_]*)$")


# =============================================================================
# Helpers
# =============================================================================


def _format_output(data: Any, return_json: bool = True) -> Union[str, Any]:
    """Format the output based on the return_json flag."""
    return json.dumps(data) if return_json else data


def _ensure_spark(spark: Optional[SparkSession]) -> SparkSession:
    """Return ``spark`` if provided, else create one via ``get_spark_session``."""
    return spark if spark is not None else get_spark_session()


def _list_iceberg_catalogs(spark: SparkSession) -> List[str]:
    """List all Iceberg catalogs registered in Spark, excluding ``spark_catalog``.

    In Spark Connect mode ``SHOW CATALOGS`` only returns catalogs the client
    session has touched, so we instead introspect ``spark.sql.catalog.<name>``
    keys via ``SET`` to discover all server-registered catalogs reliably.
    """
    rows = spark.sql("SET").collect()
    catalogs: set[str] = set()
    for row in rows:
        match = _CATALOG_KEY_PATTERN.match(row["key"])
        if match:
            catalogs.add(match.group(1))
    logger.info(
        f"Discovered {len(catalogs)} catalog(s) from Spark config: "
        f"{sorted(catalogs)}"
    )
    return sorted(c for c in catalogs if c not in _EXCLUDED_CATALOGS)


def _list_iceberg_namespaces(spark: SparkSession) -> List[str]:
    """List Iceberg namespaces across all registered catalogs.

    Returns entries in ``catalog.namespace`` format. A failure on a single
    catalog is logged and skipped so the rest of the listing still surfaces.
    """
    out: List[str] = []
    for catalog in _list_iceberg_catalogs(spark):
        try:
            rows = spark.sql(f"SHOW NAMESPACES IN {catalog}").collect()
            for row in rows:
                out.append(f"{catalog}.{row['namespace']}")
        except Exception as e:
            logger.warning(f"Failed to list namespaces in catalog '{catalog}': {e}")
    return out


def _list_hive_databases() -> List[str]:
    """List Hive databases via the HMS Thrift client (no Spark required).

    Returns flat names like ``u_alice__demo``. Failures are logged and
    return an empty list so the Iceberg listing still surfaces.
    """
    try:
        return sorted(hive_metastore.get_databases())
    except Exception as e:
        logger.warning(f"Failed to list Hive databases: {e}")
        return []


def _settings_get(settings: Any, key: str) -> Any:
    """Read ``key`` from a BERDLSettings instance or a dict."""
    if isinstance(settings, dict):
        return settings.get(key)
    return getattr(settings, key, None)


def _aliases_for_user(settings: Any) -> tuple[set[str], set[str]]:
    """Return ``(personal_aliases, tenant_aliases)`` for the current user.

    Personal aliases include ``my`` plus the sanitized stripped form of
    ``POLARIS_PERSONAL_CATALOG`` (e.g. ``user_alice`` → ``alice``). Tenant
    aliases are derived from each entry of ``POLARIS_TENANT_CATALOGS`` with
    the ``tenant_`` prefix stripped (e.g. ``tenant_globalusers`` →
    ``globalusers``).
    """
    personal = set(
        _get_personal_catalog_aliases(_settings_get(settings, "POLARIS_PERSONAL_CATALOG"))
    )
    tenant: set[str] = set()
    raw_tenants = _settings_get(settings, "POLARIS_TENANT_CATALOGS")
    if raw_tenants:
        for cat in str(raw_tenants).split(","):
            cat = cat.strip()
            if cat:
                alias = _get_tenant_catalog_alias(cat)
                if alias:
                    tenant.add(alias)
    return personal, tenant


def _filter_to_user_namespaces(
    databases: List[str],
    username: str,
    personal_aliases: set[str],
    tenant_aliases: set[str],
) -> List[str]:
    """Filter ``databases`` to those owned by the user or accessible via tenants.

    Iceberg ``catalog.namespace`` entries are kept when the catalog matches
    the user's personal or tenant aliases. Hive flat names are kept when
    they start with ``u_{username}__`` (own personal) or ``{tenant}_`` for
    any tenant the user belongs to.
    """
    user_prefix = f"u_{username}__"
    allowed_catalogs = personal_aliases | tenant_aliases

    result: List[str] = []
    for db in databases:
        if "." in db:
            catalog = db.split(".", 1)[0]
            if catalog in allowed_catalogs:
                result.append(db)
        else:
            if db.startswith(user_prefix):
                result.append(db)
            elif any(db.startswith(f"{t}_") for t in tenant_aliases):
                result.append(db)
    return result


# =============================================================================
# Public API
# =============================================================================


def get_databases(
    spark: Optional[SparkSession] = None,
    use_hms: bool = True,  # noqa: ARG001 — kept for backward compatibility, ignored
    return_json: bool = True,
    filter_by_namespace: bool = True,
    tenant: Optional[str] = None,
    settings: Optional[BERDLSettings] = None,
) -> Union[str, List[str]]:
    """List all accessible databases across Iceberg catalogs and Hive.

    Iceberg namespaces are returned in ``catalog.namespace`` format
    (e.g. ``my.demo``, ``globalusers.shared_data``). Hive databases (Delta
    tables registered under ``spark_catalog``) are returned as flat names
    (e.g. ``u_alice__demo``). All forms can be used directly in table
    references: ``SELECT * FROM {database}.{table}``.

    Args:
        spark: SparkSession to use. If ``None``, a session is obtained via
            ``get_spark_session()``. A Spark session is required to
            discover and query Iceberg catalogs.
        use_hms: Deprecated. Retained for backward compatibility and
            ignored — both Iceberg (via Spark) and Hive (via direct HMS
            Thrift client) are always queried.
        return_json: Whether to return a JSON string or raw list.
        filter_by_namespace: When True (default), restrict results to
            databases owned by the current user or accessible via the
            user's tenant catalogs. Filtering is purely prefix-based and
            uses ``BERDLSettings`` (no governance API calls).
        tenant: Optional single-tenant filter. When provided, only
            databases matching that tenant are returned: Iceberg catalog
            ``{tenant}`` and Hive databases prefixed ``{tenant}_``. Takes
            precedence over ``filter_by_namespace``.
        settings: Optional ``BERDLSettings`` instance. Defaults to
            ``get_settings()`` (env-driven).

    Returns:
        Sorted list of database identifiers (Iceberg ``catalog.namespace``
        and Hive flat names interleaved), as a JSON string when
        ``return_json=True``.
    """
    spark = _ensure_spark(spark)
    settings = settings or get_settings()

    # Concatenate into a fresh list to avoid mutating helper return values.
    databases = _list_iceberg_namespaces(spark) + _list_hive_databases()

    # Single-tenant filter takes precedence over the general user filter.
    if tenant is not None:
        hive_prefix = f"{tenant}_"
        iceberg_prefix = f"{tenant}."
        databases = sorted(
            db
            for db in databases
            if db.startswith(iceberg_prefix) or db.startswith(hive_prefix)
        )
        return _format_output(databases, return_json)

    if filter_by_namespace:
        username = _settings_get(settings, "USER")
        if not username:
            raise ValueError(
                "settings.USER must be set when filter_by_namespace=True"
            )
        personal, tenant_aliases = _aliases_for_user(settings)
        databases = _filter_to_user_namespaces(
            databases, username, personal, tenant_aliases
        )

    return _format_output(sorted(databases), return_json)


def get_tables(
    database: str,
    spark: Optional[SparkSession] = None,
    use_hms: bool = True,  # noqa: ARG001 — kept for backward compatibility, ignored
    return_json: bool = True,
) -> Union[str, List[str]]:
    """List tables in a database (Iceberg namespace or Hive database).

    The lookup path is chosen by the shape of ``database``:

    - dotted (``catalog.namespace``, e.g. ``my.demo``) → Spark SQL
      ``SHOW TABLES IN {database}`` against the Iceberg catalog.
    - flat (e.g. ``u_alice__demo``) → direct HMS Thrift client.

    Args:
        database: Either an Iceberg ``catalog.namespace`` or a Hive flat
            database name.
        spark: SparkSession to use. Required for Iceberg lookups; if
            ``None`` and the database is Iceberg-style, a session is
            created via ``get_spark_session()``. Unused for Hive.
        use_hms: Deprecated. Retained for backward compatibility and
            ignored — the path is now derived from ``database``'s shape.
        return_json: Whether to return a JSON string or raw list.

    Returns:
        Sorted list of table names.
    """
    if "." in database:
        spark = _ensure_spark(spark)
        try:
            rows = spark.sql(f"SHOW TABLES IN {database}").collect()
            tables = sorted(row["tableName"] for row in rows)
        except Exception as e:
            logger.warning(f"Failed to list tables in '{database}': {e}")
            tables = []
    else:
        try:
            tables = sorted(hive_metastore.get_tables(database))
        except Exception as e:
            logger.warning(f"Failed to list tables in Hive database '{database}': {e}")
            tables = []

    return _format_output(tables, return_json)


def get_table_schema(
    database: str,
    table: str,
    spark: Optional[SparkSession] = None,
    return_json: bool = True,
    detailed: bool = False,
) -> Union[str, List[str], List[Dict[str, Any]]]:
    """Get the schema of a specific table.

    Works for both Iceberg ``catalog.namespace`` databases and Hive flat
    databases. Uses ``spark.catalog.listColumns`` with the
    fully-qualified table name; pyspark routes to the correct catalog.

    Args:
        database: Either an Iceberg ``catalog.namespace`` (e.g. ``my.demo``)
            or a Hive flat database name (e.g. ``u_alice__demo``).
        table: Table name.
        spark: SparkSession to use; if ``None``, one is created via
            ``get_spark_session()``.
        return_json: Whether to return a JSON string or raw list.
        detailed: If ``True``, return a list of per-column dicts (name,
            dataType, nullable, isPartition, isBucket, description) via
            ``Column._asdict()`` instead of just column names.

    Returns:
        List of column names, or list of column metadata dicts when
        ``detailed=True``. Empty list on lookup failure (logged).
    """
    spark = _ensure_spark(spark)
    fq_table = f"{database}.{table}"
    try:
        cols = spark.catalog.listColumns(tableName=fq_table)
        if detailed:
            data: Union[List[str], List[Dict[str, Any]]] = [c._asdict() for c in cols]
        else:
            data = [c.name for c in cols]
    except Exception:
        logger.error(
            f"Error retrieving schema for table {table} in database {database}"
        )
        data = []
    return _format_output(data, return_json)


def get_db_structure(
    with_schema: bool = False,
    use_hms: bool = True,  # noqa: ARG001 — kept for backward compatibility, ignored
    return_json: bool = True,
    filter_by_namespace: bool = True,
    spark: Optional[SparkSession] = None,
    settings: Optional[BERDLSettings] = None,
) -> Union[str, Dict]:
    """Get the structure of all accessible databases.

    Args:
        with_schema: Whether to include each table's column names.
        use_hms: Deprecated. Retained for backward compatibility and
            ignored — the listing always covers Iceberg + Hive.
        return_json: Whether to return the result as a JSON string.
        filter_by_namespace: Whether to filter to databases the user owns
            or can access via tenants (delegates to ``get_databases``).
        spark: SparkSession to use; if ``None``, one is created.
        settings: Optional ``BERDLSettings`` instance.

    Returns:
        Dictionary mapping database identifier to table list (or to a
        ``{table: [columns]}`` dict when ``with_schema=True``).
    """
    spark = _ensure_spark(spark)
    databases = get_databases(
        spark=spark,
        return_json=False,
        filter_by_namespace=filter_by_namespace,
        settings=settings,
    )

    structure: Dict[str, Any] = {}
    for db in databases:
        tables = get_tables(database=db, spark=spark, return_json=False)
        if with_schema:
            structure[db] = {
                tbl: get_table_schema(
                    database=db, table=tbl, spark=spark, return_json=False
                )
                for tbl in tables
            }
        else:
            structure[db] = tables

    return _format_output(structure, return_json)
