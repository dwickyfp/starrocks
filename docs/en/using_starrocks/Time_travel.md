---
displayed_sidebar: docs
description: "Read native tables as they were at a past point in time with AT, BEFORE and FOR TIMESTAMP AS OF, and set data retention."
---

# Time travel on native tables

Time travel reads a native table as it was at a point in time in the past, like Snowflake's `AT` and `BEFORE`.

## Retention

How long a table keeps its history is set in days by `data_retention_time_in_days`:

- The global variable applies to all native tables. It is 0 (time travel off) by default and is persisted:

  ```sql
  SET GLOBAL data_retention_time_in_days = 1;
  ```

- The table property overrides the global variable for one table, including 0 to turn time travel off for it:

  ```sql
  CREATE TABLE orders (...) PROPERTIES ("data_retention_time_in_days" = "7");
  ALTER TABLE orders SET ("data_retention_time_in_days" = "3");
  ```

Both accept 0 up to the FE configuration `time_travel_max_retention_days` (90). The history starts when retention is turned on: a table can be read as of any point since the last load before that.

## Queries

```sql
SELECT * FROM orders AT(TIMESTAMP => '2026-11-21 08:00:00');
SELECT * FROM orders AT(OFFSET => -3600);                  -- one hour ago
SELECT * FROM orders BEFORE(TIMESTAMP => '2026-11-21 08:00:00');
SELECT * FROM orders FOR TIMESTAMP AS OF '2026-11-21 08:00:00';
INSERT INTO orders_restored SELECT * FROM orders AT(OFFSET => -600);
```

A point in the future, or older than the retention period or the kept history, is an error, as is reading one table both now and with time travel (or at two points) in one statement. Views and materialized views cannot use time travel. Materialized view rewrite, the metadata scan rewrite of simple aggregations, the query cache and short-circuit reads are skipped for queries with time travel.

## Recovering a dropped table

`UNDROP TABLE`, `UNDROP DATABASE` and `UNDROP PARTITION ... FROM` are the Snowflake names of [RECOVER](../sql-reference/sql-statements/backup_restore/RECOVER.md). A dropped object can be recovered for `catalog_trash_expire_second`, not for `data_retention_time_in_days`.

## How it works

Each physical partition records the (version, time) at which its versions became visible, for tables with retention. A query resolves the point in time to the version of each partition and scans that version. BEs keep old versions for `tablet_rowset_stale_sweep_time_sec`, which the leader FE raises to the longest retention (see `enable_time_travel_be_config_sync`). This raises disk usage on all BEs, since that BE configuration is not per table.

## Limitations

- Shared-nothing clusters only.
- A partition whose data was replaced keeps no history from before the replacement: reading it as of an earlier point is an error. This applies to `TRUNCATE`, `INSERT OVERWRITE`, the replacement of temporary partitions (also by `OPTIMIZE` and partition merges) and schema changes that rewrite the data. Reading it as of a later point works.
- The current schema is used.
- `FOR VERSION AS OF` is not supported on native tables.
