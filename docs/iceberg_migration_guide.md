# Iceberg Catalog Migration Guide

## Why We Migrated

KBERDL previously used **Delta Lake + Hive Metastore** with namespace isolation enforced by naming conventions — every database had to be prefixed with `u_{username}__` (personal) or `{tenant}_` (shared). This worked but had several limitations:

- **Naming conventions are fragile** — isolation depends on every user following prefix rules correctly
- **No catalog-level boundaries** — all users share the same Hive Metastore, so a misconfigured namespace could leak data
- **Single-engine lock-in** — Delta Lake tables are only accessible through Spark with the Delta extension
- **No time travel or schema evolution** — Delta supports these, but Hive Metastore doesn't track them natively

We migrated to **Apache Iceberg + Apache Polaris**, which provides:

- **Catalog-level isolation** — each user gets their own Polaris catalog (`my`), and each tenant gets a shared catalog (e.g., `kbase`). No naming prefixes needed.
- **Multi-engine support** — Iceberg tables can be read by Spark, Trino, DuckDB, PyIceberg, and other engines
- **Native time travel** — query any previous snapshot of your data
- **Schema evolution** — add, rename, or drop columns without rewriting data
- **ACID transactions** — concurrent reads and writes are safe

## What Changed

| Aspect | Before (Delta/Hive) | After (Iceberg/Polaris) |
|--------|---------------------|------------------------|
| **Metadata catalog** | Hive Metastore | Apache Polaris (Iceberg REST catalog) |
| **Table format** | Delta Lake | Apache Iceberg |
| **Isolation model** | Naming prefixes (`u_user__`, `tenant_`) | Separate catalogs per user/tenant |
| **Your personal catalog** | Hive (shared, prefix-isolated) | `my` (dedicated Polaris catalog) |
| **Tenant catalogs** | Hive (shared, prefix-isolated) | One catalog per tenant (e.g., `kbase`) |
| **Credentials** | MinIO S3 keys only | MinIO S3 keys + Polaris OAuth2 (auto-provisioned) |
| **Spark session** | `get_spark_session()` | `get_spark_session()` (unchanged) |

> **Migration complete:** All existing Delta Lake tables have been migrated to Iceberg in Polaris. Your data is available in the new Iceberg catalogs (`my` for personal, tenant name for shared). The original Delta tables remain accessible during the dual-read period for backward compatibility, but all new tables should be created in Iceberg.

## Side-by-Side Comparison

### Create a Namespace

The function call is **unchanged** — only the return value format differs.

<table>
<tr><th>Before (Delta/Hive)</th><th>After (Iceberg/Polaris)</th></tr>
<tr>
<td>

```python
spark = get_spark_session()

# Personal namespace
ns = create_namespace_if_not_exists(
    spark, "analysis"
)
# Returns: "u_tgu2__analysis"

# Tenant namespace
ns = create_namespace_if_not_exists(
    spark, "research",
    tenant_name="kbase"
)
# Returns: "kbase_research"
```

</td>
<td>

```python
spark = get_spark_session()

# Personal namespace (same call)
ns = create_namespace_if_not_exists(
    spark, "analysis"
)
# Returns: "my.analysis"

# Tenant namespace (same call)
ns = create_namespace_if_not_exists(
    spark, "research",
    tenant_name="kbase"
)
# Returns: "kbase.research"
```

</td>
</tr>
</table>

### Write a Table

<table>
<tr><th>Before (Delta/Hive)</th><th>After (Iceberg/Polaris)</th></tr>
<tr>
<td>

```python
ns = create_namespace_if_not_exists(
    spark, "analysis"
)
# ns = "u_tgu2__analysis"

df = spark.createDataFrame(data, columns)

# Delta format
df.write.format("delta").saveAsTable(
    f"{ns}.my_table"
)
```

</td>
<td>

```python
ns = create_namespace_if_not_exists(
    spark, "analysis"
)
# ns = "my.analysis"

df = spark.createDataFrame(data, columns)

# Iceberg format (via writeTo API)
df.writeTo(f"{ns}.my_table").createOrReplace()

# Or append to existing table
df.writeTo(f"{ns}.my_table").append()
```

</td>
</tr>
</table>

### Read a Table

<table>
<tr><th>Before (Delta/Hive)</th><th>After (Iceberg/Polaris)</th></tr>
<tr>
<td>

```python
# Query with prefixed namespace
df = spark.sql("""
    SELECT * FROM u_tgu2__analysis.my_table
""")

# Or use the variable
df = spark.sql(
    f"SELECT * FROM {ns}.my_table"
)
```

</td>
<td>

```python
# Query with catalog.namespace
df = spark.sql("""
    SELECT * FROM my.analysis.my_table
""")

# Or use the variable
df = spark.sql(
    f"SELECT * FROM {ns}.my_table"
)
```

</td>
</tr>
</table>

### Cross-Catalog Queries

<table>
<tr><th>Before (Delta/Hive)</th><th>After (Iceberg/Polaris)</th></tr>
<tr>
<td>

```python
# Everything in one Hive catalog
# Must know the naming convention
spark.sql("""
    SELECT u.name, d.dept_name
    FROM u_tgu2__analysis.users u
    JOIN kbase_shared.depts d
    ON u.dept_id = d.id
""")
```

</td>
<td>

```python
# Explicit catalog boundaries
spark.sql("""
    SELECT u.name, d.dept_name
    FROM my.analysis.users u
    JOIN kbase.shared.depts d
    ON u.dept_id = d.id
""")
```

</td>
</tr>
</table>

### List Namespaces and Tables

<table>
<tr><th>Before (Delta/Hive)</th><th>After (Iceberg/Polaris)</th></tr>
<tr>
<td>

```python
# Lists all Hive databases
# (filtered by u_{user}__ prefix)
list_namespaces(spark)

# List tables in a namespace
list_tables(spark, "u_tgu2__analysis")
```

</td>
<td>

```python
# List namespaces in your catalog
spark.sql("SHOW NAMESPACES IN my")

# List tables in a namespace
spark.sql(
    "SHOW TABLES IN my.analysis"
)

# list_tables still works
list_tables(spark, "my.analysis")
```

</td>
</tr>
</table>

### Drop a Table

<table>
<tr><th>Before (Delta/Hive)</th><th>After (Iceberg/Polaris)</th></tr>
<tr>
<td>

```python
spark.sql(
    "DROP TABLE IF EXISTS "
    "u_tgu2__analysis.my_table"
)
```

</td>
<td>

```python
spark.sql(
    "DROP TABLE IF EXISTS "
    "my.analysis.my_table"
)
```

> **Note:** `DROP TABLE` removes the catalog entry but does **not** delete the underlying S3 data files. `DROP TABLE ... PURGE` also does not delete files due to a [known Iceberg bug](https://github.com/apache/iceberg/issues/14743). To fully remove data, delete files from S3 directly using `get_minio_client()`.

</td>
</tr>
</table>

## Iceberg-Only Features

These features are only available with Iceberg tables.

### Time Travel

Query a previous version of your table:

```python
# View snapshot history
spark.sql("SELECT * FROM my.analysis.my_table.snapshots")

# Read data as it was at a specific snapshot
spark.sql("""
    SELECT * FROM my.analysis.my_table
    VERSION AS OF 1234567890
""")

# Read data as it was at a specific timestamp
spark.sql("""
    SELECT * FROM my.analysis.my_table
    TIMESTAMP AS OF '2026-03-01 12:00:00'
""")
```

### Schema Evolution

Modify table schema without rewriting data:

```python
# Add a column
spark.sql("ALTER TABLE my.analysis.my_table ADD COLUMN email STRING")

# Rename a column
spark.sql("ALTER TABLE my.analysis.my_table RENAME COLUMN name TO full_name")

# Drop a column
spark.sql("ALTER TABLE my.analysis.my_table DROP COLUMN temp_field")
```

### Snapshot Management

```python
# View snapshot history
display_df(spark.sql("SELECT * FROM my.analysis.my_table.snapshots"))

# View file-level details
display_df(spark.sql("SELECT * FROM my.analysis.my_table.files"))

# View table history
display_df(spark.sql("SELECT * FROM my.analysis.my_table.history"))
```

## Complete Example

```python
# 1. Create a Spark session
print("1. Creating Spark session...")
spark = get_spark_session("MyAnalysis")
print("   Spark session ready.")

# 2. Create a personal namespace
print("\n2. Creating personal namespace...")
ns = create_namespace_if_not_exists(spark, "demo")
print(f"   Namespace: {ns}")  # "my.demo"

# 3. Create a table
print(f"\n3. Creating table {ns}.employees...")
data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]
df = spark.createDataFrame(data, ["id", "name", "age"])
df.writeTo(f"{ns}.employees").createOrReplace()
print(f"   Table {ns}.employees created with {df.count()} rows.")

# 4. Query the table
print(f"\n4. Querying {ns}.employees:")
result = spark.sql(f"SELECT * FROM {ns}.employees")
display_df(result)

# 5. Append more data
print(f"\n5. Appending data to {ns}.employees...")
new_data = [(4, "Diana", 28)]
new_df = spark.createDataFrame(new_data, ["id", "name", "age"])
new_df.writeTo(f"{ns}.employees").append()
print(f"   Appended {new_df.count()} row(s). Total: {spark.sql(f'SELECT * FROM {ns}.employees').count()} rows.")
display_df(spark.sql(f"SELECT * FROM {ns}.employees"))

# 6. View snapshots and files (two snapshots now: create + append)
print(f"\n6. Viewing snapshots and files for {ns}.employees:")
print("   Snapshots:")
display_df(spark.sql(f"SELECT * FROM {ns}.employees.snapshots"))
print("   Data files:")
display_df(spark.sql(f"SELECT * FROM {ns}.employees.files"))

# 7. Time travel to the original version
print(f"\n7. Time travel to original version (before append)...")
first_snapshot = spark.sql(
    f"SELECT snapshot_id FROM {ns}.employees.snapshots "
    f"ORDER BY committed_at LIMIT 1"
).collect()[0]["snapshot_id"]
print(f"   First snapshot ID: {first_snapshot}")
original = spark.sql(
    f"SELECT * FROM {ns}.employees VERSION AS OF {first_snapshot}"
)
print(f"   Original version ({original.count()} rows, before append):")
display_df(original)

# 8. Tenant namespace (shared with your team)
print("\n8. Creating tenant namespace and shared table...")
tenant_ns = create_namespace_if_not_exists(
    spark, "shared_data", tenant_name="globalusers"
)
print(f"   Tenant namespace: {tenant_ns}")
df.writeTo(f"{tenant_ns}.team_employees").createOrReplace()
print(f"   Table {tenant_ns}.team_employees created.")

# 9. Cross-catalog query
print(f"\n9. Cross-catalog query ({ns} + {tenant_ns}):")
cross_result = spark.sql(f"""
    SELECT * FROM {ns}.employees
    UNION ALL
    SELECT * FROM {tenant_ns}.team_employees
""")
display_df(cross_result)

# 10. Schema evolution
print(f"\n10. Adding 'email' column to {ns}.employees...")
spark.sql(f"ALTER TABLE {ns}.employees ADD COLUMN email STRING")
print(f"   Updated schema:")
spark.sql(f"DESCRIBE {ns}.employees").show()
display_df(spark.sql(f"SELECT * FROM {ns}.employees"))
print("Done!")
```

## FAQ

**Q: Do I need to change my `get_spark_session()` call?**
No. `get_spark_session()` automatically configures both Delta and Iceberg catalogs. Your Polaris catalogs are ready to use.

**Q: Can I still access my old Delta tables?**
Yes. During the dual-read period, your Delta tables remain accessible at their original names (e.g., `u_{username}__analysis.my_table`). Iceberg copies are at `my.analysis.my_table`.

**Q: What happened to my namespace prefixes (`u_{username}__`)?**
They're no longer needed. With Iceberg, your personal catalog `my` is isolated at the catalog level — only you can access it. No prefix is required.

**Q: How do I share data with my team?**
Create a table in a tenant catalog:
```python
ns = create_namespace_if_not_exists(
    spark, "shared_data", tenant_name="kbase"
)
df.writeTo(f"{ns}.my_shared_table").createOrReplace()
```
All members of the `kbase` tenant can read this table.

**Q: Why does `DROP TABLE PURGE` leave files on S3?**
This is a [known Iceberg bug](https://github.com/apache/iceberg/issues/14743) — Spark's `SparkCatalog` ignores the `PURGE` flag when talking to REST catalogs. `DROP TABLE` only removes the catalog entry. To delete the S3 files, use the MinIO client directly.

**Q: Can I use `df.write.format("iceberg").saveAsTable(...)` instead of `writeTo`?**
Yes, both work. `writeTo` is the recommended Iceberg API since it supports `createOrReplace()` and `append()` natively.
