"""Module for querying Hive metastore information using direct HMS client."""

from typing import List

from .clients import get_hive_metastore_client


def get_databases() -> List[str]:
    """Get list of databases from Hive metastore.

    Returns:
        List of database names
    """
    client = get_hive_metastore_client()
    try:
        client.open()
        databases = client.get_databases("*")
        return databases
    except Exception as e:
        raise e
    finally:
        client.close()


def get_tables(database: str) -> List[str]:
    """Get list of tables in a database from Hive metastore.

    Args:
        database: Name of the database

    Returns:
        List of table names
    """
    client = get_hive_metastore_client()
    try:
        client.open()
        tables = client.get_tables(database, "*")
        return tables
    except Exception as e:
        raise e
    finally:
        client.close()
