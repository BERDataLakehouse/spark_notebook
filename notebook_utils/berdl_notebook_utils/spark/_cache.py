"""In-process TTL caches for read-heavy Spark data-store calls.

The tenant-data-browser hits get_databases / get_tables / get_table_schema
every time a tree node is expanded. These short-TTL caches dedupe rapid
repeat clicks and panel switches without holding stale metadata for long.

Caches are keyed by the *semantic* inputs only (database, table, filter
flags, USER, POLARIS_PERSONAL_CATALOG, POLARIS_TENANT_CATALOGS). The
unhashable / output-formatting params (`spark`, `settings`, `return_json`)
are intentionally excluded — we cache the raw value and format on read.

Mutations made inside the same notebook process (CREATE/DROP DATABASE,
CREATE/DROP TABLE, schema changes) are NOT detected automatically; callers
can pass ``force_refresh=True`` or call :func:`invalidate_all` to bust the
caches.

The cache instances are owned by this module but registered in the central
:mod:`berdl_notebook_utils.caches` registry so they can be discovered and
wiped uniformly with all other BERDL TTL caches.
"""

from ..caches import clear_all_caches, register_cache

_TTL_SECONDS = 60
_NAME_PREFIX = "spark.data_store."

databases_cache = register_cache(
    f"{_NAME_PREFIX}databases",
    maxsize=128,
    ttl_seconds=_TTL_SECONDS,
    description=(
        "get_databases(): list of accessible Iceberg+Hive databases keyed by "
        "(filter_by_namespace, tenant, USER, POLARIS_PERSONAL_CATALOG, "
        "POLARIS_TENANT_CATALOGS)."
    ),
)
tables_cache = register_cache(
    f"{_NAME_PREFIX}tables",
    maxsize=512,
    ttl_seconds=_TTL_SECONDS,
    description="get_tables(): table names keyed by database identifier.",
)
schema_cache = register_cache(
    f"{_NAME_PREFIX}schema",
    maxsize=2048,
    ttl_seconds=_TTL_SECONDS,
    description=("get_table_schema(): column names/details keyed by (database, table, detailed)."),
)
structure_cache = register_cache(
    f"{_NAME_PREFIX}structure",
    maxsize=64,
    ttl_seconds=_TTL_SECONDS,
    description=(
        "get_db_structure(): full {db: tables} or {db: {table: cols}} tree keyed by "
        "(with_schema, filter_by_namespace, USER, POLARIS_PERSONAL_CATALOG, "
        "POLARIS_TENANT_CATALOGS)."
    ),
)


def invalidate_all() -> None:
    """Clear all spark.data_store caches (databases + tables + schema + structure)."""
    clear_all_caches(prefix=_NAME_PREFIX)
