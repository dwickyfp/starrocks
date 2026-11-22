#!/usr/bin/env bash
# End-to-end test of tasks driven by streams on a running StarRocks cluster: SYSTEM$STREAM_HAS_DATA, triggered tasks
# (a task with a WHEN condition on streams and no SCHEDULE runs when its streams have data, at most once per its
# minimum trigger interval, and consumes them), scheduled tasks whose runs are SKIPPED while the streams have no data,
# child tasks gated on streams, and the errors. With other FE hosts, it also creates tasks and reads through them.
# Usage: stream_task_e2e.sh [host] [port] [other fe hosts...]
set -uo pipefail

HOST=${1:-127.0.0.1}
PORT=${2:-9030}
shift 2 2>/dev/null
OTHERS=("$@")
DB=stream_task_e2e
FAILURES=0
TASKS="st_trig st_sched st_root st_child st_exec st_follower"

sqlon() { mysql -h "$1" -P "$PORT" -uroot -N -B -D "$DB" -e "$2" 2>&1; }
sqldb() { sqlon "$HOST" "$1"; }
sql() { mysql -h "$HOST" -P "$PORT" -uroot -N -B -e "$1" 2>&1; }

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

contains() { # contains <description> <needle> <haystack>
    if [[ "$3" == *"$2"* ]]; then
        echo "  PASS  $1"
    else
        echo "  FAIL  $1"
        echo "        expected to contain: $2"
        echo "        actual: $(echo "$3" | tr '\n' '|')"
        FAILURES=$((FAILURES + 1))
    fi
}

runs() { # runs <task> [states]: the runs of the task in this test run
    sql "SELECT count(*) FROM information_schema.task_runs WHERE task_name = '$1' AND create_time >= '$START'
         AND state IN (${2:-'SUCCESS', 'FAILED'})"
}

# wait_for <description> <timeout s> <command...>: waits until the command prints "yes"
wait_for() {
    local desc=$1 deadline=$(( $(date +%s) + $2 ))
    shift 2
    while (( $(date +%s) < deadline )); do
        [[ "$("$@")" == yes ]] && return 0
        sleep 1
    done
    echo "  timeout waiting for $desc"
    return 1
}

sink_rows_at_least() { local n; n=$(sqldb "SELECT count(*) FROM sink"); [[ "$n" =~ ^[0-9]+$ ]] && (( n >= $1 )) && echo yes; }
runs_at_least() { local n; n=$(runs "$1" "${3:-}"); [[ "$n" =~ ^[0-9]+$ ]] && (( n >= $2 )) && echo yes; }
log_rows_at_least() { local n; n=$(sqldb "SELECT count(*) FROM log WHERE task = '$1'"); [[ "$n" =~ ^[0-9]+$ ]] && (( n >= $2 )) && echo yes; }
has_data() { sqlon "${2:-$HOST}" "SELECT SYSTEM\$STREAM_HAS_DATA('$1')"; }

echo "== setup"
START=$(sql "SELECT now()")
for t in $TASKS; do sql "DROP TASK IF EXISTS $t" > /dev/null; done
sql "DROP DATABASE IF EXISTS $DB FORCE; CREATE DATABASE $DB"
RETAIN="PROPERTIES ('replication_num' = '1', 'data_retention_time_in_days' = '1')"
sqldb "
CREATE TABLE src (k INT NOT NULL, v VARCHAR(32)) PRIMARY KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 2 $RETAIN;
CREATE TABLE quiet (k INT, v VARCHAR(32)) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 1 $RETAIN;
CREATE TABLE sink (k INT, v VARCHAR(32), action VARCHAR(8), ts DATETIME) DUPLICATE KEY (k)
  DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1');
CREATE TABLE log (task VARCHAR(32), ts DATETIME) DUPLICATE KEY (task)
  DISTRIBUTED BY HASH(task) BUCKETS 1 PROPERTIES ('replication_num' = '1');
CREATE STREAM s_src ON TABLE src;
CREATE STREAM s_quiet ON TABLE quiet;
CREATE STREAM s_gate ON TABLE quiet;
"

echo "== SYSTEM\$STREAM_HAS_DATA"
check "a new stream has no data" "0" "$(has_data s_src)"
sqldb "INSERT INTO src VALUES (1, 'a'), (2, 'b')"
check "it has data after an insert" "1" "$(has_data s_src)"
check "with a database name" "1" "$(has_data $DB.s_src)"
check "in a WHERE clause" "2" "$(sqldb "SELECT count(*) FROM src WHERE SYSTEM\$STREAM_HAS_DATA('s_src')")"
check "a SELECT of the stream does not consume it" "1" \
    "$(sqldb "SELECT count(*) FROM s_src WHERE k = 1" >/dev/null; has_data s_src)"
for fe in "${OTHERS[@]}"; do
    check "the same through FE $fe" "1|0" "$(has_data s_src "$fe")|$(has_data s_quiet "$fe")"
done
contains "a missing stream" "Stream nosuch does not exist in database $DB" "$(has_data nosuch)"
contains "the argument is a string literal" "takes the name of a stream as a string literal" \
    "$(sqldb "SELECT SYSTEM\$STREAM_HAS_DATA(concat('s_', 'src'))")"

echo "== errors"
contains "WHEN on a missing stream" "Stream nosuch does not exist" \
    "$(sqldb "CREATE TASK st_bad WHEN SYSTEM\$STREAM_HAS_DATA('nosuch') AS SELECT 1")"
contains "a predecessors state needs AFTER" "needs AFTER" \
    "$(sqldb "CREATE TASK st_bad WHEN SYSTEM\$PREDECESSORS_STATE('ALL_DONE') AS SELECT 1")"
contains "other conditions are rejected" "Unsupported task condition" \
    "$(sqldb "CREATE TASK st_bad WHEN 1 = 1 AS SELECT 1")"
contains "the minimum trigger interval is positive" "USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS" \
    "$(sqldb "CREATE TASK st_bad USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS = 0
              WHEN SYSTEM\$STREAM_HAS_DATA('s_src') AS SELECT 1")"

echo "== a triggered task consumes the stream"
sqldb "CREATE TASK st_trig USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS = 2
  WHEN SYSTEM\$STREAM_HAS_DATA('s_src') AS
  INSERT INTO sink SELECT k, v, METADATA\$ACTION, now() FROM s_src"
contains "DESC TASK shows the condition" "SYSTEM\$STREAM_HAS_DATA('s_src')" \
    "$(sqldb "DESC TASK st_trig")"
sleep 4
check "a suspended task is not triggered" "0" "$(runs st_trig)"
sqldb "ALTER TASK st_trig RESUME"
wait_for "the first run" 30 sink_rows_at_least 2 || FAILURES=$((FAILURES + 1))
wait_for "the stream to be consumed" 30 bash -c "[[ \$(mysql -h $HOST -P $PORT -uroot -N -B -D $DB -e \
    \"SELECT SYSTEM\\\$STREAM_HAS_DATA('s_src')\") == 0 ]] && echo yes" || FAILURES=$((FAILURES + 1))
check "the sink has the inserted rows" "$(printf '1\ta\tINSERT\n2\tb\tINSERT')" \
    "$(sqldb "SELECT k, v, action FROM sink ORDER BY k")"
first=$(runs st_trig)
sleep 6
check "no runs while the stream has no data" "$first" "$(runs st_trig)"
sqldb "UPDATE src SET v = 'bb' WHERE k = 2; DELETE FROM src WHERE k = 1; INSERT INTO src VALUES (3, 'c')"
wait_for "the second run" 30 sink_rows_at_least 6 || FAILURES=$((FAILURES + 1))
sleep 3
check "the next changes are consumed once" \
    "$(printf '1\ta\tDELETE\n1\ta\tINSERT\n2\tb\tDELETE\n2\tb\tINSERT\n2\tbb\tINSERT\n3\tc\tINSERT')" \
    "$(sqldb "SELECT k, v, action FROM sink ORDER BY k, action, v")"
check "every run succeeded" "0" "$(runs st_trig "'FAILED'")"
check "the stream has no data again" "0" "$(has_data s_src)"
# many small commits within the interval: few runs, every row consumed exactly once
before=$(runs st_trig)
for i in $(seq 10 19); do sqldb "INSERT INTO src VALUES ($i, 'x$i')"; done
wait_for "the burst to be consumed" 40 sink_rows_at_least 16 || FAILURES=$((FAILURES + 1))
sleep 4
check "each row of the burst once" "$(printf '10\t10')" \
    "$(sqldb "SELECT count(*), count(DISTINCT k) FROM sink WHERE k >= 10")"
check "in fewer runs than commits" "yes" "$(n=$(( $(runs st_trig) - before )); (( n >= 1 && n < 10 )) && echo yes || echo "no ($n)")"
sqldb "ALTER TASK st_trig SUSPEND"

echo "== EXECUTE TASK runs whatever the condition"
sqldb "CREATE TASK st_exec WHEN SYSTEM\$STREAM_HAS_DATA('s_quiet') AS INSERT INTO log VALUES ('st_exec', now())"
sqldb "EXECUTE TASK st_exec" > /dev/null
wait_for "the run" 30 log_rows_at_least st_exec 1 || FAILURES=$((FAILURES + 1))
check "it ran" "1" "$(sqldb "SELECT count(*) FROM log WHERE task = 'st_exec'")"

echo "== a scheduled task is skipped while its stream has no data"
sqldb "CREATE TASK st_sched SCHEDULE = 'USING CRON * * * * * UTC' WHEN SYSTEM\$STREAM_HAS_DATA('s_quiet') AS
  INSERT INTO log SELECT 'st_sched', now() FROM s_quiet"
sqldb "ALTER TASK st_sched RESUME"
echo "  waiting for the scheduler (up to 2 minutes)..."
wait_for "a SKIPPED run" 130 runs_at_least st_sched 1 "'SKIPPED'" || FAILURES=$((FAILURES + 1))
check "it did not run" "0|0" "$(runs st_sched)|$(sqldb "SELECT count(*) FROM log WHERE task = 'st_sched'")"
contains "the run says why" "SYSTEM\$STREAM_HAS_DATA('s_quiet') is false" \
    "$(sql "SELECT error_message FROM information_schema.task_runs WHERE task_name = 'st_sched'
            AND create_time >= '$START' AND state = 'SKIPPED' LIMIT 1")"
sqldb "INSERT INTO quiet VALUES (1, 'q')"
echo "  waiting for the next minute..."
wait_for "a run with data" 130 runs_at_least st_sched 1 || FAILURES=$((FAILURES + 1))
sqldb "ALTER TASK st_sched SUSPEND"
check "it ran and consumed the stream" "SUCCESS|1|0" \
    "$(sql "SELECT state FROM information_schema.task_runs WHERE task_name = 'st_sched' AND create_time >= '$START'
            AND state != 'SKIPPED' LIMIT 1")|$(sqldb "SELECT count(*) FROM log WHERE task = 'st_sched'")|$(has_data s_quiet)"

echo "== a child task gated on a stream"
sqldb "CREATE TASK st_root AS INSERT INTO log VALUES ('st_root', now())"
sqldb "CREATE TASK st_child AFTER st_root WHEN SYSTEM\$PREDECESSORS_STATE('ALL_DONE') AND SYSTEM\$STREAM_HAS_DATA('s_gate')
  AS INSERT INTO log SELECT 'st_child', now() FROM s_gate"
sqldb "ALTER TASK st_child RESUME"
contains "the condition is kept" "SYSTEM\$PREDECESSORS_STATE('ALL_DONE') AND SYSTEM\$STREAM_HAS_DATA('s_gate')" \
    "$(sqldb "DESC TASK st_child")"
# s_gate saw the insert into quiet above, so the first graph run consumes it; the second is skipped
sqldb "EXECUTE TASK st_root" > /dev/null
wait_for "the child to run" 30 runs_at_least st_child 1 || FAILURES=$((FAILURES + 1))
sqldb "EXECUTE TASK st_root" > /dev/null
wait_for "the child to be skipped" 30 runs_at_least st_child 1 "'SKIPPED'" || FAILURES=$((FAILURES + 1))
check "the child ran once, the root twice" "1|2" \
    "$(sqldb "SELECT count(*) FROM log WHERE task = 'st_child'")|$(sqldb "SELECT count(*) FROM log WHERE task = 'st_root'")"

if (( ${#OTHERS[@]} > 0 )); then
    fe=${OTHERS[0]}
    echo "== through FE $fe"
    sqlon "$fe" "CREATE TASK st_follower USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS = 1
      WHEN SYSTEM\$STREAM_HAS_DATA('s_quiet') AS INSERT INTO log SELECT 'st_follower', now() FROM s_quiet"
    sqlon "$fe" "ALTER TASK st_follower RESUME"
    for other in "$HOST" "${OTHERS[@]}"; do
        contains "FE $other knows the task" "SYSTEM\$STREAM_HAS_DATA('s_quiet')" "$(sqlon "$other" "DESC TASK st_follower")"
    done
    sqlon "$fe" "INSERT INTO quiet VALUES (2, 'r')"
    wait_for "the follower's task to run" 30 runs_at_least st_follower 1 || FAILURES=$((FAILURES + 1))
    sleep 3
    check "the leader triggered it once" "1|0" \
        "$(sqldb "SELECT count(*) FROM log WHERE task = 'st_follower'")|$(has_data s_quiet "$fe")"
    sqlon "$fe" "ALTER TASK st_follower SUSPEND"
fi

echo "== a dropped stream"
sqldb "CREATE TASK st_bad2 WHEN SYSTEM\$STREAM_HAS_DATA('s_gate') AS SELECT 1" >/dev/null
sqldb "DROP STREAM s_gate"
contains "DESC TASK still shows the condition" "SYSTEM\$STREAM_HAS_DATA('s_gate')" "$(sqldb "DESC TASK st_bad2")"
sql "DROP TASK IF EXISTS st_bad2" >/dev/null

for t in $TASKS; do sql "DROP TASK IF EXISTS $t" > /dev/null; done
echo
if (( FAILURES == 0 )); then
    echo "ALL CHECKS PASSED"
else
    echo "$FAILURES CHECK(S) FAILED"
fi
exit $FAILURES
