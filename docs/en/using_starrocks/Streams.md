---
displayed_sidebar: docs
description: "Track the rows inserted, updated and deleted in a native table with Snowflake style streams and the CHANGES clause, and consume them with DML statements."
---

# Streams on native tables

A stream returns the changes of a native table since its offset, like a Snowflake stream: the rows inserted, deleted and updated since then. A DML statement that reads the stream consumes those changes: when its transaction commits, the offset of the stream moves to the point the statement read the table up to, so the next read returns only the changes after it.

A stream reads the table with [time travel](./Time_travel.md): the table must keep its history, through the table property or the global variable `data_retention_time_in_days`. Streams are not supported in shared-data mode.

## Creating a stream

```sql
CREATE [OR REPLACE] STREAM [IF NOT EXISTS] [db.]name ON TABLE [db.]table
    [AT (TIMESTAMP => ...) | AT (OFFSET => ...) | BEFORE (TIMESTAMP => ...)]
    [APPEND_ONLY = TRUE | FALSE]
    [SHOW_INITIAL_ROWS = TRUE | FALSE]
    [COMMENT 'comment']
```

- The offset of a new stream is the table as it is now, so it returns no changes until the table changes. `AT` and `BEFORE` set the offset to a point in the past within the retention period of the table, and `SHOW_INITIAL_ROWS = TRUE` to the empty table: the first read returns every row as an INSERT.
- A stream lives in the database of its table, next to its tables, and a table of the same name hides it.
- Creating a stream needs the `SELECT` privilege on the table.

```sql
CREATE STREAM orders_changes ON TABLE orders;
CREATE STREAM new_events ON TABLE events APPEND_ONLY = TRUE;
CREATE STREAM orders_backfill ON TABLE orders SHOW_INITIAL_ROWS = TRUE;
CREATE STREAM since_yesterday ON TABLE orders AT (OFFSET => -86400);
```

## Reading a stream

A stream is read as a table. It has the columns of its table, followed by:

| Column | Description |
| --- | --- |
| `METADATA$ACTION` | `INSERT` or `DELETE`. |
| `METADATA$ISUPDATE` | `TRUE` for the `DELETE` and the `INSERT` row of an updated row. |
| `METADATA$ROW_ID` | An id of the row: the `DELETE` and `INSERT` rows of an update share it. |

```sql
SELECT * FROM orders_changes WHERE METADATA$ACTION = 'INSERT';
```

The changes are the net changes between the offset and now:

- Primary Key, Unique Key and Aggregate tables compare the rows by key. A key that is new is an `INSERT`, a key that is gone a `DELETE`, and a key whose row changed both, with `METADATA$ISUPDATE`. A row inserted and deleted again since the offset, or changed back to the row it had, is no change.
- Duplicate Key tables compare the rows as a multiset: a row the table has more copies of now is an `INSERT` for each extra copy, one it has fewer copies of a `DELETE`.
- An `APPEND_ONLY` stream returns the `INSERT` rows only: the rows of the keys that are new, or the rows added to a Duplicate Key table. Use it for tables whose rows are only inserted, or whose value columns cannot be compared (`HLL`, `BITMAP` and `PERCENTILE`), which a standard stream on a table with keys rejects.

Only the partitions that changed since the offset are read.

A stream cannot be read with `AT`, `BEFORE` or a `PARTITION` clause. A view or a materialized view cannot read a stream.

## The CHANGES clause

The `CHANGES` clause reads the changes of a table between two points in time, without a stream and without an offset, for example for an audit or a backfill:

```sql
SELECT ... FROM [db.]table
    CHANGES (INFORMATION => DEFAULT | APPEND_ONLY)
    AT (TIMESTAMP => ...) | AT (OFFSET => ...) | BEFORE (TIMESTAMP => ...) | AT (STREAM => '[db.]stream')
    [END (TIMESTAMP => ... | OFFSET => ...)]
    [[AS] alias]
```

- It returns the rows a stream returns, with the same `METADATA$` columns: `INFORMATION => DEFAULT` the changes of a standard stream, `APPEND_ONLY` those of an append-only stream.
- The changes start at the point of `AT` or `BEFORE`, or at the offset of a stream on the table with `AT (STREAM => ...)`, and end at the point of `END`, or now. Both points must be within the retention period of the table, and the end must not be before the start.
- Reading the changes does not move the offset of any stream, also in a DML statement.
- The table must keep its history, as for a stream. The changes cannot span a `TRUNCATE`, an `INSERT OVERWRITE` or a schema change of the table, and the rows of a partition dropped between the two points are not returned as deleted.
- A `CHANGES` clause cannot be used with a `PARTITION` clause, in a view or in a materialized view. Its time travel points must be constants, as for [time travel](./Time_travel.md).

```sql
-- the changes of yesterday
SELECT * FROM orders CHANGES (INFORMATION => DEFAULT)
    AT (TIMESTAMP => '2026-11-21 00:00:00') END (TIMESTAMP => '2026-11-22 00:00:00');

-- the rows added in the last hour
SELECT * FROM events CHANGES (INFORMATION => APPEND_ONLY) AT (OFFSET => -3600);

-- what the stream orders_changes would return, without consuming it
SELECT count(*) FROM orders CHANGES (INFORMATION => DEFAULT) AT (STREAM => 'orders_changes');
```

## Consuming a stream

A `SELECT` does not move the offset. An `INSERT`, `UPDATE`, `DELETE` or `MERGE` that reads the stream moves it when its transaction commits:

```sql
INSERT INTO orders_audit
SELECT order_id, status, METADATA$ACTION, METADATA$ISUPDATE FROM orders_changes;
```

- A statement that fails, or a transaction that is rolled back, does not move the offset.
- In an explicit transaction (`BEGIN` ... `COMMIT`), every read of the stream returns the same changes, and the offset moves when the transaction commits.
- Two transactions that read the stream at the same offset cannot both commit: the second one to commit fails, and its statement can run again to read the changes after the first one.

## Running tasks on streams

`SYSTEM$STREAM_HAS_DATA('[db.]stream')` returns `TRUE` when the stream has changes since its offset. It reads only the metadata of the stream, and fails on a stale stream. In the `WHEN` condition of a [task](./Snowflake_tasks.md#conditions-on-streams), it skips the scheduled runs while the stream has no data, and a task without `SCHEDULE` runs when the stream has data:

```sql
CREATE TASK audit_orders WHEN SYSTEM$STREAM_HAS_DATA('orders_changes') AS
INSERT INTO orders_audit
SELECT order_id, status, METADATA$ACTION, METADATA$ISUPDATE FROM orders_changes;
ALTER TASK audit_orders RESUME;
```

## Stale streams

A stream is stale when the changes since its offset are no longer known: when its offset is older than the retention period of its table, when time travel is turned off for the table, when the data of the table was replaced since the offset (`TRUNCATE`, `INSERT OVERWRITE`, a replace of temporary partitions or a schema change), or when the table was dropped. A stale stream cannot be read; recreate it. Consuming a stream regularly keeps its offset within the retention period.

## Showing and dropping streams

```sql
SHOW STREAMS [FROM db] [LIKE 'pattern'];
DESC STREAM [db.]name;
DROP STREAM [IF EXISTS] [db.]name;
```

`SHOW STREAMS` and `DESC STREAM` return `created_on`, `name`, `database_name`, `owner`, `comment`, `table_name`, `type`, `stale`, `mode` (`DEFAULT` or `APPEND_ONLY`), `stale_after` (when the offset leaves the retention period), `invalid_reason` (why the stream is stale) and `offset`.
