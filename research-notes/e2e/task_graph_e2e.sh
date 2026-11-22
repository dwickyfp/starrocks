#!/usr/bin/env bash
# End-to-end test of Snowflake style tasks on a running StarRocks cluster (FE from feature/snowflake-task).
# Usage: task_graph_e2e.sh [host] [port]
# Exits non-zero on the first failed check.
set -uo pipefail

HOST=${1:-127.0.0.1}
PORT=${2:-9030}
DB=task_e2e
FAILURES=0

# The mysql CLI splits its input on ';' even inside $$ ... $$, so a statement with a
# dollar-quoted body is sent under a different delimiter.
wrap() { case "$1" in *'$$'*) printf 'DELIMITER //\n%s //' "$1" ;; *) printf '%s' "$1" ;; esac; }
sql() { mysql -h "$HOST" -P "$PORT" -uroot -N -B -e "$(wrap "$1")" 2>&1; }
sqldb() { mysql -h "$HOST" -P "$PORT" -uroot -N -B -D "$DB" -e "$(wrap "$1")" 2>&1; }

check() { # check <description> <expected> <actual>
    if [[ "$2" == "$3" ]]; then
        echo "  PASS  $1"
    else
        echo "  FAIL  $1"
        echo "        expected: $(echo "$2" | tr '\n' '|')"
        echo "        actual:   $(echo "$3" | tr '\n' '|')"
        FAILURES=$((FAILURES + 1))
    fi
}

# wait_runs <task> <min runs> <timeout s>: waits until the task has finished at least <min runs> runs
wait_runs() {
    local deadline=$(( $(date +%s) + $3 ))
    while (( $(date +%s) < deadline )); do
        local n
        n=$(sql "SELECT count(*) FROM information_schema.task_runs WHERE task_name = '$1'
                 AND create_time >= '$START' AND state IN ('SUCCESS', 'FAILED')")
        if [[ "$n" =~ ^[0-9]+$ ]] && (( n >= $2 )); then
            return 0
        fi
        sleep 2
    done
    echo "  timeout waiting for $2 finished run(s) of $1"
    return 1
}

echo "== setup"
# task_runs keeps the history of dropped tasks too, so only runs of this test run count.
START=$(sql "SELECT now()")
for t in t_load t_summary t_bad t_publish t_alert t_final t_tick t_tick_child t_rv_root t_rv_child t_rv_flaky t_rv_next; do
    sql "DROP TASK IF EXISTS $t" > /dev/null
done
sql "DROP DATABASE IF EXISTS $DB FORCE" > /dev/null
sql "CREATE DATABASE $DB"
sqldb "
CREATE TABLE orders (id INT, status VARCHAR(20), amount DECIMAL(10, 2))
  PRIMARY KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1');
CREATE TABLE daily_summary (status VARCHAR(20), cnt BIGINT, total DECIMAL(18, 2))
  PRIMARY KEY (status) DISTRIBUTED BY HASH(status) BUCKETS 1 PROPERTIES ('replication_num' = '1');
CREATE TABLE audit_log (ts DATETIME, task VARCHAR(64), msg VARCHAR(256))
  DUPLICATE KEY (ts) DISTRIBUTED BY HASH(ts) BUCKETS 1 PROPERTIES ('replication_num' = '1');
"

echo "== create the task graph"
# The root runs INSERT, UPDATE and DELETE in one script.
sqldb "CREATE TASK t_load AS \$\$
  INSERT INTO orders VALUES (1, 'NEW', 10.50), (2, 'NEW', 20.00), (3, 'NEW', 30.00), (4, 'NEW', 40.00);
  UPDATE orders SET status = 'PAID' WHERE id IN (1, 2);
  DELETE FROM orders WHERE id = 4;
  INSERT INTO audit_log VALUES (now(), 't_load', 'loaded');
\$\$"
# Runs when t_load succeeds.
sqldb "CREATE TASK t_summary AFTER t_load AS \$\$
  INSERT INTO daily_summary SELECT status, count(*), sum(amount) FROM orders GROUP BY status;
  INSERT INTO audit_log VALUES (now(), 't_summary', 'summarized');
\$\$"
# Fails at its second statement; the first one stays, the third never runs.
sqldb "CREATE TASK t_bad AFTER t_load AS \$\$
  INSERT INTO audit_log VALUES (now(), 't_bad', 'before failure');
  INSERT INTO table_that_does_not_exist VALUES (1);
  INSERT INTO audit_log VALUES (now(), 't_bad', 'must not run');
\$\$"
# Needs both to succeed (default ALL_SUCCEEDED): skipped.
sqldb "CREATE TASK t_publish AFTER t_summary, t_bad AS
  INSERT INTO audit_log VALUES (now(), 't_publish', 'published')"
# The failure branch: runs because t_bad failed.
sqldb "CREATE TASK t_alert AFTER t_summary, t_bad WHEN SYSTEM\$PREDECESSORS_STATE('AT_LEAST_ONE_FAILED') AS
  INSERT INTO audit_log VALUES (now(), 't_alert', 'a predecessor failed')"
# Runs last, whatever happened.
sqldb "CREATE TASK t_final FINALIZE = t_load AS \$\$
  UPDATE orders SET status = 'ARCHIVED' WHERE status = 'NEW';
  INSERT INTO audit_log VALUES (now(), 't_final', 'finalized');
\$\$"

echo "== SHOW TASKS"
sqldb "SELECT 1" > /dev/null
mysql -h "$HOST" -P "$PORT" -uroot -D "$DB" -e "SHOW TASKS FROM $DB" 2>&1 \
    | awk -F'\t' 'NR == 1 {for (i = 1; i <= NF; i++) col[$i] = i}
                  {print $col["NAME"] "\t" $col["STATE"] "\t" $col["PREDECESSORS"] "\t" $col["CONDITION"] "\t" $col["FINALIZE"]}'

echo "== checks before running"
check "child cannot have SCHEDULE" "1" \
    "$(sqldb "CREATE TASK t_x SCHEDULE = '1 MINUTE' AFTER t_load AS SELECT 1" | grep -c 'cannot have both SCHEDULE and AFTER')"
check "no cycles" "1" "$(sqldb "ALTER TASK t_load ADD AFTER t_summary" | grep -c 'cycle')"

for t in t_summary t_bad t_publish t_alert t_final; do
    sqldb "ALTER TASK $t RESUME"
done

echo "== EXECUTE TASK t_load (the root stays suspended; the whole graph runs)"
sqldb "EXECUTE TASK t_load"
wait_runs t_final 1 300 || FAILURES=$((FAILURES + 1))
sleep 2

echo "== results"
check "orders after INSERT, UPDATE, DELETE and the finalizer's UPDATE" \
    "$(printf '1\tPAID\t10.50\n2\tPAID\t20.00\n3\tARCHIVED\t30.00')" \
    "$(sqldb "SELECT id, status, amount FROM orders ORDER BY id")"
check "summary written by the child, before the finalizer" \
    "$(printf 'NEW\t1\t30.00\nPAID\t2\t30.50')" \
    "$(sqldb "SELECT status, cnt, total FROM daily_summary ORDER BY status")"
check "audit log: what ran, and what did not" \
    "$(printf 't_alert\ta predecessor failed\nt_bad\tbefore failure\nt_final\tfinalized\nt_load\tloaded\nt_summary\tsummarized')" \
    "$(sqldb "SELECT task, msg FROM audit_log ORDER BY task")"
check "task run states" \
    "$(printf 't_alert\tSUCCESS\nt_bad\tFAILED\nt_final\tSUCCESS\nt_load\tSUCCESS\nt_publish\tSKIPPED\nt_summary\tSUCCESS')" \
    "$(sql "SELECT task_name, state FROM information_schema.task_runs
            WHERE task_name LIKE 't\\_%' AND create_time >= '$START' ORDER BY task_name")"
check "t_bad reports the failed statement" "1" \
    "$(sql "SELECT error_message FROM information_schema.task_runs WHERE task_name = 't_bad' AND create_time >= '$START'" | grep -c 'Statement 2 of 3 failed')"
check "the finalizer ran last" "t_final" \
    "$(sql "SELECT task_name FROM information_schema.task_runs WHERE task_name LIKE 't\\_%'
            AND create_time >= '$START' ORDER BY finish_time DESC LIMIT 1")"

echo "== retries, return values and skipped runs"
sqldb "CREATE TASK t_rv_root TASK_AUTO_RETRY_ATTEMPTS = 2 AS \$\$
  INSERT INTO audit_log VALUES (now(), 't_rv_root', 'root');
  CALL SYSTEM\$SET_RETURN_VALUE('batch-7');
\$\$"
sqldb "CREATE TASK t_rv_child AFTER t_rv_root AS
  INSERT INTO audit_log VALUES (now(), 't_rv_child', SYSTEM\$GET_PREDECESSOR_RETURN_VALUE('t_rv_root'))"
sqldb "CREATE TASK t_rv_flaky AFTER t_rv_root AS INSERT INTO table_that_does_not_exist VALUES (1)"
sqldb "CREATE TASK t_rv_next AFTER t_rv_flaky AS INSERT INTO audit_log VALUES (now(), 't_rv_next', 'must not run')"
for t in t_rv_child t_rv_flaky t_rv_next; do sqldb "ALTER TASK $t RESUME"; done
sqldb "EXECUTE TASK t_rv_root" > /dev/null
wait_runs t_rv_flaky 3 300 || FAILURES=$((FAILURES + 1))
wait_runs t_rv_child 1 120 || FAILURES=$((FAILURES + 1))
sleep 3
check "the child read the root's return value" "batch-7" \
    "$(sqldb "SELECT msg FROM audit_log WHERE task = 't_rv_child'")"
check "the return value is the run's extra message" "batch-7" \
    "$(sql "SELECT extra_message FROM information_schema.task_runs WHERE task_name = 't_rv_root' AND create_time >= '$START'")"
check "a failed child runs again TASK_AUTO_RETRY_ATTEMPTS times" "3" \
    "$(sql "SELECT count(*) FROM information_schema.task_runs WHERE task_name = 't_rv_flaky'
            AND create_time >= '$START' AND state = 'FAILED'")"
check "the task after it is recorded as SKIPPED" "SKIPPED" \
    "$(sql "SELECT state FROM information_schema.task_runs WHERE task_name = 't_rv_next' AND create_time >= '$START'")"
check "and did not run" "0" "$(sqldb "SELECT count(*) FROM audit_log WHERE task = 't_rv_next'")"

echo "== a scheduled graph: cron every minute"
sqldb "CREATE TASK t_tick SCHEDULE = 'USING CRON * * * * * UTC' AS
  INSERT INTO audit_log VALUES (now(), 't_tick', 'tick')"
sqldb "CREATE TASK t_tick_child AFTER t_tick AS
  UPDATE orders SET amount = amount + 1 WHERE id = 1"
sqldb "ALTER TASK t_tick_child RESUME"
sqldb "ALTER TASK t_tick RESUME"
check "child cannot change while its root is started" "1" \
    "$(sqldb "ALTER TASK t_tick_child SUSPEND" | grep -c 'must be suspended first')"
echo "  waiting for the scheduler (up to 3 minutes)..."
wait_runs t_tick_child 1 180 || FAILURES=$((FAILURES + 1))
sqldb "ALTER TASK t_tick SUSPEND"
ticks=$(sqldb "SELECT count(*) FROM audit_log WHERE task = 't_tick'")
amount=$(sqldb "SELECT amount FROM orders WHERE id = 1")
check "the scheduled root ran on its own" "yes" "$([[ "$ticks" -ge 1 ]] && echo yes || echo "no ($ticks)")"
check "its child ran after it (amount 10.50 + runs)" "yes" \
    "$(awk -v a="$amount" 'BEGIN {print (a >= 11.50) ? "yes" : "no (" a ")"}')"

echo
if (( FAILURES == 0 )); then
    echo "ALL CHECKS PASSED"
else
    echo "$FAILURES CHECK(S) FAILED"
fi
exit "$FAILURES"
