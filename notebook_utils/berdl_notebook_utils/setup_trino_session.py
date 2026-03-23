"""
Trino connection setup for BERDL notebooks.

Provides per-user Trino connections with dynamic catalogs that use
the user's own MinIO credentials, mirroring how get_spark_session()
configures per-user S3 access for Spark.

Architecture:
    1. Fetch user's MinIO credentials from governance API
    2. Create a per-user dynamic catalog via CREATE CATALOG
    3. Return a trino.dbapi connection + catalog name

The user's catalog (e.g., "u_tgu_lake") has their own MinIO credentials,
so S3 access is scoped to what the governance API grants them — same
security model as Spark sessions.
"""

import logging
import re
from typing import NamedTuple

import trino

from .berdl_settings import BERDLSettings, get_settings
from .minio_governance.operations import get_minio_credentials

logger = logging.getLogger(__name__)

# Connectors allowed in CREATE CATALOG statements (SQL-injection allowlist).
#
# Each user gets a per-user dynamic catalog (e.g. "u_{username}_lake") that points
# at the shared Hive Metastore but uses the user's own MinIO credentials.
# The berdl-namespace-isolation access-control plugin then filters visibility:
#   - Personal namespaces:  schemas matching  u_{username}__*
#   - Tenant namespaces:    schemas matching  {tenant}_*
#   - Public namespaces:    schemas matching  globalusers_*
#
# Both delta_lake and hive connectors read metadata from Hive Metastore and
# data from MinIO/S3, so the same namespace isolation applies to either one.
# delta_lake is the default (matches Spark's Delta write format); hive is
# available for querying legacy non-Delta tables (Parquet, ORC, CSV, etc.).
ALLOWED_CONNECTORS = frozenset(
    {
        "delta_lake",
        "hive",
    }
)


class TrinoSession(NamedTuple):
    """Return type for get_trino_connection()."""

    connection: trino.dbapi.Connection
    catalog: str


def _sanitize_identifier(value: str) -> str:
    """
    Sanitize a string into a valid Trino identifier component.

    Trino catalog names must be lowercase alphanumeric + underscores.
    Used for both usernames and catalog suffixes.
    """
    return re.sub(r"[^a-z0-9_]", "_", value.lower())


def _build_catalog_properties(
    settings: BERDLSettings,
    access_key: str,
    secret_key: str,
) -> dict[str, str]:
    """Build the WITH properties for CREATE CATALOG."""
    endpoint_url = str(settings.MINIO_ENDPOINT_URL)
    if not endpoint_url.startswith("http"):
        protocol = "https" if settings.MINIO_SECURE else "http"
        endpoint_url = f"{protocol}://{endpoint_url}"

    return {
        "hive.metastore.uri": str(settings.BERDL_HIVE_METASTORE_URI),
        "fs.native-s3.enabled": "true",
        "s3.endpoint": endpoint_url,
        "s3.aws-access-key": access_key,
        "s3.aws-secret-key": secret_key,
        "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    }


def _catalog_exists(cursor: trino.dbapi.Cursor, catalog_name: str) -> bool:
    """Check if a Trino catalog already exists (e.g. from a static .properties file)."""
    cursor.execute("SHOW CATALOGS")
    catalogs = [row[0] for row in cursor.fetchall()]
    return catalog_name in catalogs


def _escape_sql_string(value: str) -> str:
    """Escape single quotes in a SQL string literal by doubling them."""
    return value.replace("'", "''")


def _validate_connector(connector: str) -> None:
    """Validate connector name against the allowlist.

    Raises:
        ValueError: If the connector is not in ALLOWED_CONNECTORS.
    """
    if connector not in ALLOWED_CONNECTORS:
        raise ValueError(
            f"Connector {connector!r} is not allowed. Must be one of: {', '.join(sorted(ALLOWED_CONNECTORS))}"
        )


def _create_dynamic_catalog(
    cursor: trino.dbapi.Cursor,
    catalog_name: str,
    connector: str,
    properties: dict[str, str],
) -> None:
    """
    Create a dynamic catalog if it doesn't already exist.

    Skips creation if the catalog is already loaded (e.g. from a static
    .properties file), since CREATE CATALOG requires system-level privileges
    that file-based access control may not grant.
    """
    if _catalog_exists(cursor, catalog_name):
        logger.info(f"Catalog '{catalog_name}' already exists, skipping creation")
        return

    _validate_connector(connector)

    props_sql = ",\n        ".join(f"\"{k}\" = '{_escape_sql_string(v)}'" for k, v in properties.items())

    sql = f"""CREATE CATALOG IF NOT EXISTS "{catalog_name}" USING {connector}
    WITH (
        {props_sql}
    )"""

    logger.info(f"Creating dynamic Trino catalog: {catalog_name} (connector={connector})")
    cursor.execute(sql)
    cursor.fetchall()
    logger.info(f"Catalog '{catalog_name}' ready")


def get_trino_connection(
    host: str | None = None,
    port: int | None = None,
    catalog_suffix: str = "lake",
    connector: str = "delta_lake",
    settings: BERDLSettings | None = None,
) -> TrinoSession:
    """
    Create a Trino connection with a per-user dynamic catalog.

    The user's MinIO credentials are fetched from the governance API and
    injected into a dynamic catalog, giving each user isolated S3 access
    — the same model as get_spark_session().

    Args:
        host: Trino coordinator hostname. Defaults to TRINO_HOST env var or "trino".
        port: Trino coordinator port. Defaults to TRINO_PORT env var or 8080.
        catalog_suffix: Suffix for the catalog name. The full name is
                        "u_{username}_{suffix}" (e.g., "u_tgu_lake").
        connector: Trino connector to use. Defaults to "delta_lake".
                   Use "hive" if you need Hive connector instead.
        settings: BERDLSettings instance. If None, reads from environment.

    Returns:
        TrinoSession(connection, catalog) — a named tuple with:
            - connection: trino.dbapi.Connection ready for queries
            - catalog: str catalog name to use in queries

    Example:
        >>> conn, catalog = get_trino_connection()
        >>> cursor = conn.cursor()
        >>> cursor.execute(f"SHOW SCHEMAS FROM {catalog}")
        >>> print(cursor.fetchall())

        >>> # Query a table
        >>> cursor.execute(f"SELECT * FROM {catalog}.my_schema.my_table LIMIT 10")
        >>> df = cursor.fetch_pandas_all()

        >>> # With tenant warehouse
        >>> conn, catalog = get_trino_connection(catalog_suffix="research_team")
    """
    if settings is None:
        get_settings.cache_clear()
        settings = get_settings()

    # Resolve host/port (use `is not None` so callers can intentionally pass falsy values)
    trino_host = host if host is not None else settings.TRINO_HOST
    trino_port = port if port is not None else settings.TRINO_PORT

    # Fetch user's MinIO credentials (same flow as Spark)
    credentials = get_minio_credentials()
    username = settings.USER

    # Build catalog name: u_{username}_{suffix}
    safe_username = _sanitize_identifier(username)
    safe_suffix = _sanitize_identifier(catalog_suffix)
    catalog_name = f"u_{safe_username}_{safe_suffix}"

    logger.info(f"Setting up Trino connection for user={username}, catalog={catalog_name}")

    # Create connection
    conn = trino.dbapi.connect(
        host=trino_host,
        port=trino_port,
        user=username,
    )

    # Create per-user dynamic catalog with user's MinIO credentials
    properties = _build_catalog_properties(
        settings=settings,
        access_key=credentials.access_key,
        secret_key=credentials.secret_key,
    )

    cursor = conn.cursor()
    _create_dynamic_catalog(cursor, catalog_name, connector, properties)

    logger.info(f"Trino session ready: host={trino_host}:{trino_port}, user={username}, catalog={catalog_name}")

    return TrinoSession(connection=conn, catalog=catalog_name)
