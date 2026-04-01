# Trino Resource Groups Configuration

Documentation for `resource-groups.json`. Trino's JSON parser does not support
comments, so this companion file explains each setting.

## Structure

```
global (parent group)
  └── user-{username} (per-user child group, auto-created)
```

## Root Group: `global`

| Setting | Value | Meaning |
|---------|-------|---------|
| `softMemoryLimit` | `100%` | This group can use all cluster query memory |
| `hardConcurrencyLimit` | `20` | Max 20 queries running concurrently across ALL users |
| `maxQueued` | `200` | Max 200 queries waiting in queue. Beyond this, new queries are rejected |
| `schedulingPolicy` | `weighted_fair` | Each active user gets an equal share of capacity. 3 active users = ~1/3 each |

## Per-User Group: `user-${USER}`

`${USER}` is replaced by the Trino username from the connection.

| Setting | Value | Meaning |
|---------|-------|---------|
| `softMemoryLimit` | `30%` | One user's queries can use at most 30% of cluster memory. Soft = won't kill running queries, but blocks new ones |
| `hardConcurrencyLimit` | `5` | Max 5 concurrent queries per user. 6th query queues |
| `maxQueued` | `20` | Max 20 queued queries per user. Beyond this, rejected immediately |

## Selectors

Selectors route incoming queries to resource groups. Evaluated in order, first match wins.

| Setting | Value | Meaning |
|---------|-------|---------|
| `user` | `.*` | Matches all usernames (regex) |
| `group` | `global.user-${USER}` | Routes to per-user sub-group (e.g., user `tgu` → `global.user-tgu`) |

To add special handling for specific users, add a selector above the catch-all:
```json
{"user": "admin", "group": "global.admin-group"}
```
