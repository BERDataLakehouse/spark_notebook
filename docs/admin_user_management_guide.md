# JupyterHub Admin: User & Tenant Management Guide

This guide covers administrative operations for managing users, groups (tenants), and their access in BERDL. These operations require **JupyterHub Admin** privileges.

---

## Prerequisites

### Required Role: `CDM_JUPYTERHUB_ADMIN`

To perform user and tenant management operations, you must have the `CDM_JUPYTERHUB_ADMIN` role assigned via the **KBase Auth Server**.

> [!IMPORTANT]
> The `CDM_JUPYTERHUB_ADMIN` role must be granted by a **KBase System Administrator**. Contact your system administrator to request this role.

Once you have the admin role, you can access management operations from within a BERDL JupyterHub notebook.

---

## Available Management Operations

Import the management functions:

```python
from berdl_notebook_utils.minio_governance import (
    # List operations
    list_users,
    list_groups,
    # Group member management
    add_group_member,
    remove_group_member,
    # Tenant creation
    create_tenant_and_assign_users,
)
```

---

## User Management

### List All Users

Retrieve all users registered in the system:

```python
from berdl_notebook_utils.minio_governance import list_users

users = list_users()
print(users)
```

---

## Group (Tenant) Management

### List All Groups

View all existing groups and their members:

```python
from berdl_notebook_utils.minio_governance import list_groups

groups = list_groups()
print(groups)
# Example output: {'research_team': ['alice', 'bob'], 'data_engineers': ['charlie']}
```

### Create a New Tenant

Create a new tenant (group) with optional initial members:

```python
from berdl_notebook_utils.minio_governance import create_tenant_and_assign_users

# Create tenant without members
result = create_tenant_and_assign_users("kbase")

# Create tenant with initial members
result = create_tenant_and_assign_users(
    tenant_name="kbase",
    usernames=["alice", "bob", "charlie"]
)

# Check results
print(result["create_tenant"])  # Tenant creation status
print(result["add_members"])    # List of (username, status) tuples
```

### Add Users to a Group

Add users to an existing group:

```python
from berdl_notebook_utils.minio_governance import add_group_member
from governance_client.models import GroupManagementResponse

results = add_group_member(
    group_name="kbase",
    usernames=["alice", "bob", "charlie"]
)

for username, response in results:
    if isinstance(response, GroupManagementResponse):
        print(f"Successfully added {username}")
    else:
        print(f"Error adding {username}: {response}")
```

### Remove Users from a Group

Remove users from a group:

```python
from berdl_notebook_utils.minio_governance import remove_group_member
from governance_client.models import GroupManagementResponse

results = remove_group_member(
    group_name="kbase",
    usernames=["alice", "bob"]
)

for username, response in results:
    if isinstance(response, GroupManagementResponse):
        print(f"Successfully removed {username}")
    else:
        print(f"Error removing {username}: {response}")
```

---

## Operations Summary

| Operation | Function | Description |
|-----------|----------|-------------|
| List users | `list_users()` | Get all system users |
| List groups | `list_groups()` | Get all groups with members |
| Create tenant | `create_tenant_and_assign_users(name, users)` | Create new group with optional members |
| Add members | `add_group_member(group, usernames)` | Add users to existing group |
| Remove members | `remove_group_member(group, usernames)` | Remove users from group |

---

## Requesting Admin Access

To obtain the `CDM_JUPYTERHUB_ADMIN` role:

1. Contact the KBase System Administrators in the [#sysadmin Slack channel](https://kbase.slack.com/archives/C02MHNMCQ)
2. Request the `CDM_JUPYTERHUB_ADMIN` role assignment
3. The admin will assign the role via the **KBase Auth Server**
4. Log out and log back into JupyterHub for changes to take effect

> [!NOTE]
> Admin operations will fail with an authentication error if you don't have the required role.
