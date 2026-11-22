# Snowflake compatibility end-to-end tests

Shell scripts that check stored procedures, time travel, streams, tasks and the Snowflake query syntax against a running cluster through the
`mysql` client. Each prints a PASS or FAIL line per check, ends with `ALL CHECKS PASSED`, and exits with the
number of failed checks.

| Script | Usage | Covers |
| --- | --- | --- |
| `procedure_e2e.sh` | `procedure_e2e.sh <fe host>` | CREATE / CALL / DROP PROCEDURE, exceptions, cursors, transactions, owner's and caller's rights, RESULTSET, RETURNS TABLE, EXECUTE IMMEDIATE ... USING |
| `time_travel_e2e.sh` | `time_travel_e2e.sh <fe host>` | AT / BEFORE / FOR TIMESTAMP AS OF, retention settings, compaction, TRUNCATE / INSERT OVERWRITE / schema change, UNDROP |
| `task_graph_e2e.sh` | `task_graph_e2e.sh <fe host>` | task graphs, WHEN conditions, finalizers, schedules, retries, return values, SKIPPED runs |
| `dialect_e2e.sh` | `[AUDIT_LOG=<fe.audit.log>] dialect_e2e.sh <fe host>` | QUALIFY, star modifiers, ILIKE and LIKE ANY / ALL, compatibility functions, UNPIVOT, SHOW TABLES HISTORY, query_tag; each result is compared with an equivalent query |
| `merge_e2e.sh` | `merge_e2e.sh <fe host>` | MERGE INTO a Primary Key table: conditional UPDATE / DELETE / INSERT compared with an oracle, NOT MATCHED BY SOURCE, UPDATE SET * / INSERT *, generated and AUTO_INCREMENT columns, the duplicate row error, transactions, failures that change nothing |
| `insert_multi_e2e.sh` | `insert_multi_e2e.sh <fe host>` | INSERT ALL and INSERT FIRST: every target table compared with an oracle, ELSE, DEFAULT, several INTO clauses for one table, the affected row count, failures that change no table, explicit transactions |
| `history_restore_e2e.sh` | `history_restore_e2e.sh <fe host>` | DESCRIBE HISTORY and RESTORE TABLE: the commits listed, restores compared with time travel reads, undoing a restore, Primary Key and Duplicate Key tables, the errors |
| `stream_e2e.sh` | `stream_e2e.sh <fe host> [port] [other fe hosts...]` | CREATE / SHOW / DESCRIBE / DROP STREAM: changes compared with the difference of the table and a copy at the offset, Primary Key, Duplicate Key and partitioned tables, APPEND_ONLY, SHOW_INITIAL_ROWS, consumption by DML and explicit transactions, concurrent consumers, stale streams |
| `in_all_null_e2e.sh` | `HOST=<fe host> in_all_null_e2e.sh` | IN lists of NULL values |
| `multi_fe_e2e.sh` | `LEADER=<host> FOLLOWER=<host> multi_fe_e2e.sh` | the above driven through a follower FE |
| `failover_e2e.sh` | `failover_e2e.sh prepare <fe>`, then `failover_e2e.sh verify <leader> <other fes>` | state kept across a leader failover, a restart and a checkpoint |

All FEs must listen on MySQL port 9030 (or set `PORT`). The scripts drop and recreate their own databases and tasks.
