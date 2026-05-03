"""
Trino connection setup for BERDL notebooks.

Provides per-user Trino connections with dynamic catalogs that use
the user's own MinIO credentials, mirroring how get_spark_session()
configures per-user S3 access for Spark.

Architecture:
    1. Fetch user's MinIO credentials from governance API
    2. Create a per-user Delta/Hive dynamic catalog via CREATE CATALOG
    3. If Polaris is configured, create Iceberg REST catalogs for the personal
       and tenant Polaris warehouses
    4. Return a trino.dbapi Connection configured to use the per-user Delta/Hive
       catalog as the default

The user's catalog (e.g., "u_tgu") has their own MinIO credentials,
so S3 access is scoped to what the governance API grants them — same
security model as Spark sessions.
"""

import logging
import re

import trino

from .berdl_settings import BERDLSettings, get_settings
from .governance.operations import get_credentials

logger = logging.getLogger(__name__)

# Connectors allowed in CREATE CATALOG statements (SQL-injection allowlist).
#
# Each user gets a per-user dynamic catalog (e.g. "u_{username}") that points
# at the shared Hive Metastore but uses the user's own MinIO credentials.
# The berdl-namespace-isolation access-control plugin then filters visibility:
#   - Personal namespaces:  schemas matching  u_{username}__*
#   - Tenant namespaces:    schemas matching  {tenant}_*  (resolved via governance API,
#                           includes globalusers and any other tenant groups)
#
# Both delta_lake and hive connectors read metadata from Hive Metastore and
# data from MinIO/S3, so the same namespace isolation applies to either one.
# delta_lake is the default (matches Spark's Delta write format); hive is
# available for querying legacy non-Delta tables (Parquet, ORC, CSV, etc.).
#
# The iceberg connector is used only for Polaris REST catalogs.  Those catalogs
# are named with portable aliases that match Spark's cross-engine aliases:
#   - Personal: {username} -> Polaris warehouse user_{username}
#   - Tenant:   {tenant}   -> Polaris warehouse tenant_{tenant}
# The Spark-only "my" alias is intentionally not created in Trino because Trino
# catalog names are global on the coordinator.
ALLOWED_CONNECTORS = frozenset(
    {
        "delta_lake",
        "hive",
        "iceberg",
    }
)


def _sanitize_identifier(value: str) -> str:
    """
    Sanitize a string into a valid Trino identifier component.

    Trino catalog names must be lowercase alphanumeric + underscores.

    IMPORTANT: This logic must match ``sanitizeIdentifier()`` in the Trino
    access control plugin (BerdlSystemAccessControl.java) so that catalog
    names built here align with the ownership prefixes checked there.
    """
    return re.sub(r"[^a-z0-9_]", "_", value.lower())


def _build_catalog_properties(
    settings: BERDLSettings,
    access_key: str,
    secret_key: str,
) -> dict[str, str]:
    """Build the WITH properties for CREATE CATALOG."""
    endpoint_url = str(settings.S3_ENDPOINT_URL)
    if not endpoint_url.startswith("http"):
        protocol = "https" if settings.S3_SECURE else "http"
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


def _get_polaris_uri(settings: BERDLSettings) -> str | None:
    """Return the configured Polaris REST catalog URI without a trailing slash."""
    if not settings.POLARIS_CATALOG_URI:
        return None
    return str(settings.POLARIS_CATALOG_URI).rstrip("/")


def _get_polaris_oauth2_server_uri(settings: BERDLSettings) -> str | None:
    """Return the Polaris OAuth2 token endpoint for Iceberg REST clients."""
    polaris_uri = _get_polaris_uri(settings)
    if not polaris_uri:
        return None
    if polaris_uri.endswith("/v1"):
        return f"{polaris_uri}/oauth/tokens"
    return f"{polaris_uri}/v1/oauth/tokens"


def _get_personal_catalog_alias(personal_catalog: str | None) -> str | None:
    """Return the portable Trino alias for a personal Polaris catalog."""
    if not personal_catalog:
        return None

    alias = personal_catalog.strip()
    if alias.startswith("user_"):
        alias = alias[len("user_") :]
    alias = _sanitize_identifier(alias).strip("_")
    return alias or None


def _get_tenant_catalog_alias(tenant_catalog: str) -> str:
    """Return the portable Trino alias for a tenant Polaris catalog."""
    alias = tenant_catalog.strip()
    if alias.startswith("tenant_"):
        alias = alias[len("tenant_") :]
    return _sanitize_identifier(alias).strip("_")


def _iter_tenant_catalogs(tenant_catalogs: str | None) -> list[str]:
    """Parse configured Polaris tenant catalog names."""
    if not tenant_catalogs:
        return []
    return [catalog.strip() for catalog in tenant_catalogs.split(",") if catalog.strip()]


def _build_iceberg_catalog_properties(
    settings: BERDLSettings,
    warehouse: str,
    access_key: str,
    secret_key: str,
) -> dict[str, str]:
    """Build the WITH properties for a Trino Iceberg REST catalog backed by Polaris."""
    polaris_uri = _get_polaris_uri(settings)
    oauth2_server_uri = _get_polaris_oauth2_server_uri(settings)
    if not polaris_uri or not oauth2_server_uri:
        raise ValueError("POLARIS_CATALOG_URI is required to create an Iceberg REST catalog")

    s3_properties = _build_catalog_properties(settings, access_key, secret_key)
    s3_properties.pop("hive.metastore.uri", None)

    return {
        "iceberg.catalog.type": "rest",
        "iceberg.rest-catalog.uri": polaris_uri,
        "iceberg.rest-catalog.warehouse": warehouse,
        "iceberg.rest-catalog.security": "OAUTH2",
        "iceberg.rest-catalog.oauth2.credential": settings.POLARIS_CREDENTIAL or "",
        "iceberg.rest-catalog.oauth2.scope": "PRINCIPAL_ROLE:ALL",
        "iceberg.rest-catalog.oauth2.server-uri": oauth2_server_uri,
        "iceberg.rest-catalog.oauth2.token-refresh-enabled": "false",
        "iceberg.rest-catalog.oauth2.token-exchange-enabled": "false",
        "iceberg.rest-catalog.vended-credentials-enabled": "false",
        "iceberg.security": "read_only",
        **s3_properties,
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

    Skips creation if the catalog is already loaded (e.g. from a previous
    session or static .properties file).  The access control plugin allows
    CREATE CATALOG for catalogs matching u_{user}*.
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


def _create_polaris_catalogs(
    cursor: trino.dbapi.Cursor,
    settings: BERDLSettings,
    access_key: str,
    secret_key: str,
) -> None:
    """Create Trino Iceberg catalogs for configured Polaris warehouses."""
    if not settings.POLARIS_CATALOG_URI or not settings.POLARIS_CREDENTIAL:
        return

    personal_catalog = settings.POLARIS_PERSONAL_CATALOG
    personal_alias = _get_personal_catalog_alias(personal_catalog)
    if personal_catalog and personal_alias:
        properties = _build_iceberg_catalog_properties(
            settings=settings,
            warehouse=personal_catalog,
            access_key=access_key,
            secret_key=secret_key,
        )
        _create_dynamic_catalog(cursor, personal_alias, "iceberg", properties)

    for tenant_catalog in _iter_tenant_catalogs(settings.POLARIS_TENANT_CATALOGS):
        tenant_alias = _get_tenant_catalog_alias(tenant_catalog)
        if not tenant_alias:
            continue
        properties = _build_iceberg_catalog_properties(
            settings=settings,
            warehouse=tenant_catalog,
            access_key=access_key,
            secret_key=secret_key,
        )
        _create_dynamic_catalog(cursor, tenant_alias, "iceberg", properties)


def get_trino_connection(
    host: str | None = None,
    port: int | None = None,
    connector: str = "delta_lake",
    settings: BERDLSettings | None = None,
) -> trino.dbapi.Connection:
    """
    Create a Trino connection with a per-user dynamic catalog.

    The user's MinIO credentials are fetched from the governance API and
    injected into a dynamic catalog, giving each user isolated S3 access
    — the same model as get_spark_session().

    The connection's default catalog is set automatically, so queries
    can keep using ``schema.table`` format for Delta/Hive tables.
    Polaris/Iceberg tables use explicit cross-engine catalog aliases such
    as ``{username}.schema.table`` or ``{tenant}.schema.table``.

    Args:
        host: Trino coordinator hostname. Defaults to TRINO_HOST env var or "trino".
        port: Trino coordinator port. Defaults to TRINO_PORT env var or 8080.
        connector: Trino connector to use. Defaults to "delta_lake".
                   Use "hive" if you need Hive connector instead.
        settings: BERDLSettings instance. If None, reads from environment.

    Returns:
        trino.dbapi.Connection with the default catalog set to the user's
        dynamic catalog.

    Example:
        >>> conn = get_trino_connection()
        >>> cursor = conn.cursor()
        >>> cursor.execute("SELECT * FROM my_schema.my_table LIMIT 10")
        >>> df = cursor.fetch_pandas_all()

        >>> # SHOW SCHEMAS also works without catalog prefix
        >>> cursor.execute("SHOW SCHEMAS")
        >>> print(cursor.fetchall())
    """
    if settings is None:
        get_settings.cache_clear()
        settings = get_settings()

    # Resolve host/port (use `is not None` so callers can intentionally pass falsy values)
    trino_host = host if host is not None else settings.TRINO_HOST
    trino_port = port if port is not None else settings.TRINO_PORT

    # Fetch user's MinIO credentials (same flow as Spark)
    credentials = get_credentials()
    username = settings.USER

    # Build catalog name: u_{username}
    safe_username = _sanitize_identifier(username)
    catalog_name = f"u_{safe_username}"

    logger.info(f"Setting up Trino connection for user={username}, catalog={catalog_name}")

    # Create connection.
    # Pass KBase auth token as an extra credential so the access control plugin
    # can call the governance API to resolve tenant group memberships.
    conn = trino.dbapi.connect(
        host=trino_host,
        port=trino_port,
        user=username,
        extra_credential=[("kbase_auth_token", settings.KBASE_AUTH_TOKEN)],
    )

    # Create per-user Delta/Hive dynamic catalog with user's MinIO credentials
    properties = _build_catalog_properties(
        settings=settings,
        access_key=credentials.s3_access_key,
        secret_key=credentials.s3_secret_key,
    )

    cursor = conn.cursor()
    _create_dynamic_catalog(cursor, catalog_name, connector, properties)
    _create_polaris_catalogs(
        cursor=cursor,
        settings=settings,
        access_key=credentials.s3_access_key,
        secret_key=credentials.s3_secret_key,
    )

    # Set the default catalog on the connection so users can write
    # schema.table Delta/Hive queries without a catalog prefix — same UX as Spark.
    conn._client_session.catalog = catalog_name

    logger.info(f"Trino session ready: host={trino_host}:{trino_port}, user={username}, catalog={catalog_name}")

    return conn
