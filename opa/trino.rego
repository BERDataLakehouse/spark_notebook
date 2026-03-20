package trino

default allow := false

user := input.context.identity.user
operation := input.action.operation

catalog_name := c if {
  c := input.action.resource.catalog.name
} else := c if {
  c := input.action.resource.catalog.catalogName
} else := c if {
  c := input.action.resource.schema.catalogName
} else := c if {
  c := input.action.resource.table.catalogName
}

schema_name := s if {
  s := input.action.resource.schema.schemaName
} else := s if {
  s := input.action.resource.table.schemaName
}

table_name := t if {
  t := input.action.resource.table.tableName
}

is_delta if {
  catalog_name == "delta"
}

allowed_schema(schema) if {
  owner := data.schema_owner[schema]
  owner.type == "tenant"
  some i
  data.tenant_membership[user][i] == owner.id
}

allowed_schema(schema) if {
  owner := data.schema_owner[schema]
  owner.type == "user"
  owner.id == user
}

allowed_schema(schema) if {
  startswith(schema, sprintf("u_%s__", [user]))
}

allow if {
  user == "amkhan"
  operation == "ExecuteQuery"
}

allow if {
  user == "amkhan"
  operation == "AccessCatalog"
  is_delta
}

allow if {
  user == "amkhan"
  operation == "FilterCatalogs"
  is_delta
}

allow if {
  user == "amkhan"
  operation == "FilterSchemas"
  is_delta
  schema_name != ""
  allowed_schema(schema_name)
}

# Allow the SHOW SCHEMAS command itself
allow if {
  input.context.identity.user == "amkhan"
  input.action.operation == "ShowSchemas"
  input.action.resource.catalog.name == "delta"
}

# Restrict which schemas are visible
allow if {
  input.context.identity.user == "amkhan"
  input.action.operation == "FilterSchemas"
  input.action.resource.schema.catalogName == "delta"
  input.action.resource.schema.schemaName == "information_schema"
}

allow if {
  input.context.identity.user == "amkhan"
  input.action.operation == "FilterSchemas"
  input.action.resource.schema.catalogName == "delta"
  allowed_schema(input.action.resource.schema.schemaName)
}


# information_schema reads for metadata discovery
allow if {
  user == "amkhan"
  operation == "SelectFromColumns"
  is_delta
  schema_name == "information_schema"
  table_name == "schemata"
}

allow if {
  user == "amkhan"
  operation == "SelectFromColumns"
  is_delta
  schema_name == "information_schema"
  table_name == "tables"
}

allow if {
  user == "amkhan"
  operation == "SelectFromColumns"
  is_delta
  schema_name == "information_schema"
  table_name == "columns"
}

# likely needed so SHOW TABLES returns visible tables
allow if {
  user == "amkhan"
  operation == "FilterTables"
  is_delta
  schema_name != ""
  allowed_schema(schema_name)
}

allow if {
  operation == "ShowTables"
  is_delta
  schema_name != ""
  allowed_schema(schema_name)
}

allow if {
  operation == "SelectFromColumns"
  is_delta
  schema_name != ""
  schema_name != "information_schema"
  allowed_schema(schema_name)
}

allow if {
  operation == "DescribeTable"
  is_delta
  schema_name != ""
  allowed_schema(schema_name)
}

allow if {
  operation == "ShowCreateTable"
  is_delta
  schema_name != ""
  allowed_schema(schema_name)
}