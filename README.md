# Snowflake Query History Extractor

[![Build Status](https://travis-ci.org/keboola/ex-snowflake-query-history.svg?branch=master)](https://travis-ci.org/keboola/ex-snowflake-query-history)


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