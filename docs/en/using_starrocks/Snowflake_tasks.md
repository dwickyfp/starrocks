---
displayed_sidebar: docs
description: "Create Snowflake style tasks with multi-statement bodies, schedules, task graphs, retries and return values."
---

# Snowflake style tasks

A task created with `CREATE TASK ... AS` runs a SQL body on a schedule, by `EXECUTE TASK`, or after other tasks in a task graph, like Snowflake's tasks.

## Create and manage

```sql
CREATE [OR REPLACE] TASK [IF NOT EXISTS] [db.]name
  [option = value ...]
  [AFTER task, ...]
  [WHEN <condition>]
AS <statement> | $$ <statement>; <statement>; ... $$;

ALTER TASK name RESUME | SUSPEND;
ALTER TASK name SET option = value ...;
ALTER TASK name UNSET option, ...;
ALTER TASK name ADD AFTER task, ... | REMOVE AFTER task, ...;
ALTER TASK name MODIFY WHEN <condition> | REMOVE WHEN;
ALTER TASK name MODIFY AS <body>;
EXECUTE TASK name;
SHOW TASKS [FROM db] [LIKE 'pattern'];
DESC TASK name;
DROP TASK name;
```

A new task is suspended. The statements of the body run in order in one session and the run stops at the first failed statement. Task runs are listed in `information_schema.task_runs`.

Options:

| Option | Meaning |
| --- | --- |
| `SCHEDULE` | `'<n> MINUTE'` (also `SECOND`, `HOUR`, `DAY`) or `'USING CRON <minute> <hour> <day of month> <month> <day of week> <time zone>'`. |
| `WAREHOUSE` | The warehouse the runs use. |
| `COMMENT` | A comment. |
| `USER_TASK_TIMEOUT_MS` | The timeout of a run. |
| `SUSPEND_TASK_AFTER_NUM_FAILURES` | Suspends the task after this many failed runs in a row (10 by default, 0 never). |
| `TASK_AUTO_RETRY_ATTEMPTS` | Set on a root task: how many times (0 to 30, 0 by default) a failed run of the root, a child task or the finalizer runs again before it counts as failed. |
| `USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS` | The shortest time between two runs of a triggered task (30 by default). |
| `FINALIZE` | Makes the task the finalizer of the named root task. |

Any other option must be a session variable, which is set for every run of the task.

## Task graphs

A task with `AFTER` is a child task. It runs after its predecessors, when its `WHEN` condition holds on their states. The default condition is `ALL_SUCCEEDED`. The other conditions are `AT_LEAST_ONE_FAILED`, `ALL_FAILED`, `ALL_DONE` and `NONE_FAILED_MIN_ONE_SUCCEEDED`. A child whose condition does not hold is skipped, and its tasks after it are decided on that. A skipped child gets a run with state `SKIPPED` in `information_schema.task_runs`, whose error message gives the reason. The finalizer runs once after all other tasks of the graph run have finished.

Only resumed tasks take part. The root and the children of a started root cannot be changed until the root is suspended. `EXECUTE TASK` on a suspended root runs the whole graph.

## Conditions on streams

A `WHEN` condition can also test [streams](./Streams.md) with `SYSTEM$STREAM_HAS_DATA('[db.]stream')`, combined with `AND`, `OR`, `NOT` and parentheses. In a child task, `SYSTEM$PREDECESSORS_STATE('<state>')` can be one more condition joined with `AND`. A stream name without a database is looked up in the database of the task.

```sql
CREATE TASK load_changes
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('orders_changes') OR SYSTEM$STREAM_HAS_DATA('items_changes')
AS INSERT INTO orders_audit SELECT order_id, status, METADATA$ACTION FROM orders_changes;

CREATE TASK publish AFTER load_changes
  WHEN SYSTEM$PREDECESSORS_STATE('ALL_DONE') AND SYSTEM$STREAM_HAS_DATA('audit_changes')
AS INSERT INTO published SELECT * FROM audit_changes;
```

- A scheduled run of a task whose streams have no data is skipped, and gets a run with state `SKIPPED` in `information_schema.task_runs` whose error message gives the condition. A child task is skipped the same way.
- A root or standalone task without `SCHEDULE` whose condition tests streams is a triggered task: once resumed, it runs when the condition holds, at most once per `USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS`, and not while a run of it is pending or running. Its body should consume the streams, so the condition is false again after the run.
- The condition only reads the metadata of the streams. A stream that is missing or stale makes the condition an error: the run is skipped with the reason.
- `EXECUTE TASK` runs the task whatever its condition.

`SYSTEM$STREAM_HAS_DATA` can also be used in queries: it returns `TRUE` when the stream has changes since its offset.

## Return values

A run sets its return value with `CALL SYSTEM$SET_RETURN_VALUE('<value>')`. The value is shown as the run's `EXTRA_MESSAGE` in `information_schema.task_runs`. The tasks after it in the same graph run read it with `SYSTEM$GET_PREDECESSOR_RETURN_VALUE('<task>')`, which is NULL when the task set no value. The task name can be left out when the task has exactly one predecessor. A finalizer can read any task of its graph run.

```sql
CREATE TASK load_orders AS $$
  INSERT INTO orders SELECT * FROM staging_orders;
  CALL SYSTEM$SET_RETURN_VALUE('batch-7');
$$;
CREATE TASK log_load AFTER load_orders AS
  INSERT INTO audit_log VALUES (now(), SYSTEM$GET_PREDECESSOR_RETURN_VALUE('load_orders'));
```

`SYSTEM$SET_RETURN_VALUE` takes one literal of at most 10 kB. `SYSTEM$GET_PREDECESSOR_RETURN_VALUE` is available only in runs started by a task graph.

## Limitations

- A graph run in progress is kept by the leader FE in memory: after a leader change, the graph runs in progress stop, and the next run of each root starts a new one.
- Triggered tasks are checked by the leader FE every second.
- A run of a root while its graph still runs is skipped (Snowflake's `NO_OVERLAP`). `ALLOW_OVERLAPPING_EXECUTION` is not supported.
