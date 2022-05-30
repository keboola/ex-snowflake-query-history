# Snowflake Query History Extractor

[![Build Status](https://travis-ci.org/keboola/ex-snowflake-query-history.svg?branch=master)](https://travis-ci.org/keboola/ex-snowflake-query-history)

This extractor is designed for continuous fetching of Snowflake query history. Every time the extractor configuration is executed queries which have completed since last extractor run are fetched. Extractor utilizes [Query history table functions](https://docs.snowflake.net/manuals/sql-reference/functions/query_history.html)

### Configuration
Extractor requires Snowflake credentials, database and usage permissions for warehouse.
Which queries will be extracted depends on provided user's [permissions](https://docs.snowflake.net/manuals/sql-reference/functions/query_history.html#usage-notes).

Following queries will create user with access to all queries associated for one warehouse:
```
create role keboola_monitoring;
create database keboola_monitoring;
grant ownership on database keboola_monitoring to role keboola_monitoring;

# this is a warehouse where the QUERY_HISTORY queries wil be executed
# this warehouse will be used in configuration
grant usage on warehouse some_warehouse to role keboola_monitoring;

# add monitor permission to access all queries executed in the warehouse
grant monitor on warehouse some_warehouse to role keboola_monitoring;


create user keboola_monitoring
password = 'PASSWORD'
default_role = 'KEBOOLA_MONITORING';

grant role keboola_monitoring to user keboola_monitoring;
```


### Configuration Schema

```json
{
  "title": "Parameters",
  "type": "object",
  "required": [
    "host",
    "user",
    "database",
    "#password",
    "warehouse"
  ],
  "properties": {
    "host": {
      "title": "Hostname",
      "type": "string",
      "minLength": 1,
      "default": ""
    },
    "username": {
      "title": "User",
      "type": "string",
      "minLength": 1,
      "default": ""
    },
    "#password": {
      "title": "Password",
      "type": "string",
      "format": "password",
      "minLength": 1,
      "default": ""
    },
    "database": {
      "title": "Database",
      "type": "string",
      "minLength": 1,
      "default": ""
    },
    "warehouse": {
      "title": "Warehouse",
      "type": "string",
      "minLength": 1,
      "default": ""
    }
  }
}
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
