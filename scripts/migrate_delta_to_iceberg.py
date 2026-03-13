"""
Delta Lake to Iceberg migration utilities for BERDL Phase 4.

This script migrates Delta Lake tables (Hive Metastore) to Iceberg tables
(Polaris REST catalog), preserving partitions and validating row counts.

Functions:
    migrate_table   - Migrate a single Delta table to an Iceberg catalog
    migrate_user    - Migrate all of a user's Delta databases to their Iceberg catalog
    migrate_tenant  - Migrate all of a tenant's Delta databases to their Iceberg catalog

Usage in the migration notebook (migration_phase4.ipynb):

    # Import after adding scripts/ to sys.path
    from migrate_delta_to_iceberg import MigrationTracker, migrate_user, migrate_tenant

    tracker = MigrationTracker()

    # Migrate all tables for a user (idempotent — skips existing tables)
    migrate_user(spark, "tgu2", target_catalog="user_tgu2", tracker=tracker)

    # Migrate all tables for a tenant
    migrate_tenant(spark, "globalusers", target_catalog="tenant_globalusers", tracker=tracker)

    # View results
    tracker.to_dataframe(spark).show(truncate=False)
    print(tracker.summary())

Force re-migration (drops existing Iceberg table and re-copies from Delta):

    # Force re-migrate a single user table
    migrate_table(
        spark,
        hive_db="u_tian_gu_test__demo_personal",
        table_name="personal_test_table",
        target_catalog="user_tian_gu_test",
        target_ns="demo_personal",
        tracker=tracker,
        force=True,
    )

    # Force re-migrate a single tenant table
    migrate_table(
        spark,
        hive_db="globalusers_demo_shared",
        table_name="tenant_test_table",
        target_catalog="tenant_globalusers",
        target_ns="demo_shared",
        tracker=tracker,
        force=True,
    )

    # Force re-migrate all tables for a user
    migrate_user(spark, "tian_gu_test", target_catalog="user_tian_gu_test", tracker=tracker, force=True)

    # Force re-migrate all tables for a tenant
    migrate_tenant(spark, "globalusers", target_catalog="tenant_globalusers", tracker=tracker, force=True)

Note:
    - Requires an admin Spark session configured with cross-user catalog access
      (see Section 3 of migration_phase4.ipynb)
    - By default, migration is idempotent: tables that already exist in the target
      catalog are skipped. Use force=True to drop and re-migrate.
    - DROP TABLE PURGE does not delete S3 data files due to an Iceberg bug (#14743).
      To fully clean up, delete files from S3 directly using get_minio_client().
"""

import logging
from dataclasses import dataclass, field

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def _clean_error(e: Exception, max_len: int = 150) -> str:
    """Extract a short, readable error message without JVM stacktraces."""
    msg = str(e)
    # Strip everything after "JVM stacktrace:" if present
    if "JVM stacktrace:" in msg:
        msg = msg[: msg.index("JVM stacktrace:")].strip()
    # Take only the first line
    msg = msg.split("\n")[0].strip()
    if len(msg) > max_len:
        msg = msg[:max_len] + "..."
    return msg


@dataclass
class TableResult:
    """Result of a single table migration."""

    source: str
    target: str
    status: str  # "migrated", "skipped", "failed"
    row_count: int = 0
    error: str = ""


@dataclass
class MigrationTracker:
    """Tracks migration progress across users and tenants."""

    results: list[TableResult] = field(default_factory=list)

    @property
    def migrated(self) -> list[TableResult]:
        return [r for r in self.results if r.status == "migrated"]

    @property
    def skipped(self) -> list[TableResult]:
        return [r for r in self.results if r.status == "skipped"]

    @property
    def failed(self) -> list[TableResult]:
        return [r for r in self.results if r.status == "failed"]

    def add(self, result: TableResult):
        self.results.append(result)

    def summary(self) -> str:
        return (
            f"Total: {len(self.results)} | "
            f"Migrated: {len(self.migrated)} | "
            f"Skipped: {len(self.skipped)} | "
            f"Failed: {len(self.failed)}"
        )

    def to_dataframe(self, spark: SparkSession):
        """Convert results to a Spark DataFrame for notebook display."""
        rows = [(r.source, r.target, r.status, r.row_count, r.error) for r in self.results]
        return spark.createDataFrame(rows, ["source", "target", "status", "row_count", "error"])


def _validate_target_catalog(spark: SparkSession, target_catalog: str) -> None:
    """Raise ValueError if target_catalog is not configured in the Spark session.

    Uses spark.conf.get() instead of SHOW CATALOGS because Spark lazily loads
    catalogs — they won't appear in SHOW CATALOGS until first access.
    """
    config_key = f"spark.sql.catalog.{target_catalog}"
    try:
        spark.conf.get(config_key)
    except Exception:
        raise ValueError(
            f"Catalog '{target_catalog}' is not configured in the current Spark session "
            f"(no {config_key} property found). "
            f"Did you run Section 3 (Configure Admin Spark) and restart Spark Connect?"
        )


def table_exists_in_catalog(spark: SparkSession, catalog: str, namespace: str, table_name: str) -> bool:
    """Check if a table already exists in the target Iceberg catalog.

    Uses SHOW TABLES instead of DESCRIBE TABLE to avoid JVM-level
    TABLE_OR_VIEW_NOT_FOUND error logs when the table doesn't exist.
    """
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{namespace}").collect()
        return any(row["tableName"] == table_name for row in tables)
    except Exception:
        return False


def migrate_table(
    spark: SparkSession,
    hive_db: str,
    table_name: str,
    target_catalog: str,
    target_ns: str,
    tracker: MigrationTracker | None = None,
    force: bool = False,
):
    """
    Migrate a single Delta table to Iceberg via Polaris, preserving partitions.

    Args:
        spark: Active Spark session
        hive_db: Original Hive/Delta database name
        table_name: Original table name
        target_catalog: Target Iceberg catalog name (e.g., 'my')
        target_ns: Target namespace in Iceberg (e.g., 'test_db')
        tracker: Optional MigrationTracker for progress tracking
        force: If True, drop existing target table and re-migrate
    """
    source_ref = f"{hive_db}.{table_name}"
    target_table_ref = f"{target_catalog}.{target_ns}.{table_name}"
    print(f"  {source_ref} -> {target_table_ref}")
    logger.info(f"Starting migration for {source_ref} -> {target_table_ref}")

    # 0. Idempotency: skip if target already exists (unless force=True)
    if table_exists_in_catalog(spark, target_catalog, target_ns, table_name):
        if force:
            logger.info(f"Force mode: dropping existing {target_table_ref}")
            spark.sql(f"DROP TABLE {target_table_ref} PURGE")
        else:
            logger.info(f"Skipping {target_table_ref} — already exists in target catalog")
            if tracker:
                tracker.add(TableResult(source=source_ref, target=target_table_ref, status="skipped"))
            return

    # 1. Read from Delta using spark.table fallback
    try:
        df = spark.table(source_ref)
    except Exception as e:
        err_str = str(e)
        if "DELTA_READ_TABLE_WITHOUT_COLUMNS" in err_str:
            msg = "Delta table has no columns (empty schema) — skipping"
            logger.warning(f"Skipping {source_ref} — {msg}")
            if tracker:
                tracker.add(TableResult(source=source_ref, target=target_table_ref, status="skipped", error=msg))
            return
        short_err = _clean_error(e)
        print(f"    FAILED (read): {short_err}")
        logger.error(f"Failed to read source table {source_ref}: {short_err}")
        if tracker:
            tracker.add(TableResult(source=source_ref, target=target_table_ref, status="failed", error=short_err))
        return

    # 1b. Skip tables with empty schema (corrupt Delta tables with no columns)
    if len(df.columns) == 0:
        msg = "Delta table has no columns (empty schema)"
        logger.warning(f"Skipping {source_ref} — {msg}")
        if tracker:
            tracker.add(TableResult(source=source_ref, target=target_table_ref, status="skipped", error=msg))
        return

    # 2. Extract partition columns from the original Delta table
    partition_cols: list[str] = []
    try:
        partition_cols = [row.name for row in spark.catalog.listColumns(f"{hive_db}.{table_name}") if row.isPartition]
        if partition_cols:
            logger.info(f"Found partition columns: {partition_cols}")
    except Exception as e:
        logger.warning(f"Could not fetch partitions for {source_ref} via catalog API: {e}")

    # 3. Create target namespace, write data, and validate
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {target_catalog}.{target_ns}")

        # 4. Write as Iceberg (applying partition logic if it existed)
        writer = df.writeTo(target_table_ref)
        if partition_cols:
            writer = writer.partitionedBy(*partition_cols)

        logger.info(f"Writing data to {target_table_ref}...")
        writer.create()
        logger.info(f"Write completed for {target_table_ref}")

        # 5. Validate row counts
        original_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {source_ref}").collect()[0]["cnt"]
        migrated_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table_ref}").collect()[0]["cnt"]

        if original_count != migrated_count:
            msg = f"Row count mismatch: {original_count} vs {migrated_count}"
            logger.error(f"Validation FAILED: {msg}")
            if tracker:
                tracker.add(
                    TableResult(
                        source=source_ref,
                        target=target_table_ref,
                        status="failed",
                        row_count=migrated_count,
                        error=msg,
                    )
                )
            raise ValueError(msg)

        logger.info(f"Validation SUCCESS: {migrated_count} rows migrated exactly.")
        if tracker:
            tracker.add(
                TableResult(
                    source=source_ref,
                    target=target_table_ref,
                    status="migrated",
                    row_count=migrated_count,
                )
            )
    except Exception as e:
        short_err = _clean_error(e)
        print(f"    FAILED: {short_err}")
        logger.error(f"Failed to migrate {source_ref} -> {target_table_ref}: {short_err}")
        if tracker and not any(r.target == target_table_ref for r in tracker.results):
            tracker.add(TableResult(source=source_ref, target=target_table_ref, status="failed", error=short_err))


def migrate_user(
    spark: SparkSession,
    username: str,
    target_catalog: str = "my",
    tracker: MigrationTracker | None = None,
    force: bool = False,
):
    """
    Migrate all of a user's Delta databases to their Iceberg catalog.

    Args:
        spark: Active SparkSession
        username: The user's username
        target_catalog: The target catalog (e.g., 'user_{username}')
        tracker: Optional MigrationTracker for progress tracking
        force: If True, drop existing target tables and re-migrate
    """
    _validate_target_catalog(spark, target_catalog)

    prefix = f"u_{username}__"
    databases = [db[0] for db in spark.sql("SHOW DATABASES").collect() if db[0].startswith(prefix)]

    if not databases:
        logger.info(f"No databases found for username {username} with prefix {prefix}")
        return

    for hive_db in databases:
        iceberg_ns = hive_db.replace(prefix, "", 1)  # "u_tgu2__test_db" -> "test_db"
        logger.info(f"Scanning database {hive_db}...")
        try:
            tables = spark.sql(f"SHOW TABLES IN {hive_db}").collect()
        except Exception as e:
            print(f"  Error listing tables in {hive_db}: {_clean_error(e)}")
            continue

        for table_row in tables:
            table_name = table_row["tableName"]
            try:
                migrate_table(spark, hive_db, table_name, target_catalog, iceberg_ns, tracker, force=force)
            except Exception as e:
                print(f"    FAILED (unexpected): {_clean_error(e)}")


def migrate_tenant(
    spark: SparkSession,
    tenant_name: str,
    target_catalog: str,
    tracker: MigrationTracker | None = None,
    force: bool = False,
):
    """
    Migrate all Delta databases for a tenant to their Iceberg catalog.

    Tenant databases follow the pattern: {tenant_name}_{dbname} in Hive.
    The {tenant_name}_ prefix is stripped to get the Iceberg namespace.

    Args:
        spark: Active SparkSession
        tenant_name: The tenant/group name (e.g., 'kbase')
        target_catalog: Target Iceberg catalog (e.g., 'tenant_kbase')
        tracker: Optional MigrationTracker for progress tracking
        force: If True, drop existing target tables and re-migrate
    """
    _validate_target_catalog(spark, target_catalog)

    prefix = f"{tenant_name}_"
    databases = [db[0] for db in spark.sql("SHOW DATABASES").collect() if db[0].startswith(prefix)]

    if not databases:
        logger.info(f"No databases found for tenant {tenant_name} with prefix {prefix}")
        return

    for hive_db in databases:
        iceberg_ns = hive_db.replace(prefix, "", 1)
        logger.info(f"Scanning tenant database {hive_db}...")
        try:
            tables = spark.sql(f"SHOW TABLES IN {hive_db}").collect()
        except Exception as e:
            print(f"  Error listing tables in {hive_db}: {_clean_error(e)}")
            continue

        for table_row in tables:
            table_name = table_row["tableName"]
            try:
                migrate_table(spark, hive_db, table_name, target_catalog, iceberg_ns, tracker, force=force)
            except Exception as e:
                print(f"    FAILED (unexpected): {_clean_error(e)}")


if __name__ == "__main__":
    # If this is run independently we set up basic print logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    print("Migration functions are loaded. Supply an active spark session to begin.")
