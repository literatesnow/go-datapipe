# Data Pipe

Data pipe copies data from one database table to a table in another database using bulk insert statements or faster methods if available (for example, COPY IN for the Postgres driver).

*The destination table is truncated!*

## Database Support

* Postgres DB_DRIVER=postgres, [URI example](https://godoc.org/github.com/lib/pq)
* MS SQL server DB_DRIVER=mssql, [URI example](https://github.com/denisenkom/go-mssqldb)
* Any database which Go supports (source modification required)

## Configuration

Configuration is done by environment variables

|Env Var           |Description                                                                  |Default|
|------------------|-----------------------------------------------------------------------------|-------|
|SRC_DB_DRIVER     |Source database driver name                                                  |       |
|SRC_DB_URI        |Source database driver URI                                                   |       |
|SRC_DB_SELECT_SQL |Select statement to query rows from source database                          |       |
|DST_DB_DRIVER     |Destination database driver name                                             |       |
|DST_DB_URI        |Destination database driver URI                                              |       |
|DST_DB_SCHEMA     |Destination database schema name                                             |       |
|DST_DB_TABLE      |Destination database table name (without schema)                             |       |
|MAX_ROW_BUF_SZ    |Maximum number of rows to buffer at a time                                   |100    |
|MAX_ROW_TX_COMMIT |Maximum number of rows to process before committing the database transaction |500    |

## Performance

* MAX_ROW_BUF_SZ or MAX_ROW_TX_COMMIT too low could cause slow performance.
* MAX_ROW_BUF_SZ too high could cause memory issues on the machine where this program is running.
* MAX_ROW_TX_COMMIT too high could cause the destination database's transaction logs to fill up.

## Example

```bash
# All rows from SQL server dbo.source_table_name to Postgres master.destination_table_name
export SRC_DB_DRIVER="mssql"
export SRC_DB_URI="server=localhost;port=1433;database=Test;user id=domain\user_name;password=test"
export SRC_DB_SELECT_SQL="SELECT * FROM dbo.source_table_name WHERE thing >= 100"

export DST_DB_DRIVER="postgres"
export DST_DB_URI="user=test password=test dbname=test_db host=localhost"
export DST_DB_SCHEMA="master"
export DST_DB_TABLE="destination_table_name"

export MAX_ROW_BUF_SZ=100
export MAX_ROW_TX_COMMIT=500

./go-datapipe
```
