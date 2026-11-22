#!/usr/bin/env bash
# End-to-end test of streams on a running StarRocks cluster: CREATE / SHOW / DESCRIBE / DROP STREAM, the changes a
# stream returns compared with the difference of the table and a copy of it at the offset (the oracle), Primary Key,
# Duplicate Key and partitioned tables, APPEND_ONLY and SHOW_INITIAL_ROWS, consumption by DML statements and explicit
# transactions (the offset moves only when they commit), concurrent consumers, stale streams and the errors.
# Usage: stream_e2e.sh [host] [port] [other fe hosts...]
set -uo pipefail

HOST=${1:-127.0.0.1}
PORT=${2:-9030}
shift 2 2>/dev/null
OTHERS=("$@")
DB=stream_e2e
FAILURES=0

sqlon() { mysql -h "$1" -P "$PORT" -uroot -N -B -D "$DB" -e "$2" 2>&1; }
sqldb() { sqlon "$HOST" "$1"; }
# runs every statement, also after one fails (mysql -e stops at the first error)
sqlforce() { echo "$1" | mysql -h "$HOST" -P "$PORT" -uroot -N -B -D "$DB" --force 2>&1; }

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

# empty_diff <description> <query a> <query b>: the two queries return the same multiset of (k, v, n)
same() {
    local diff
    diff=$(sqldb "SELECT count(*) FROM (
        SELECT k, v, n, count(*) c FROM ($2) a GROUP BY k, v, n
        EXCEPT SELECT k, v, n, count(*) c FROM ($3) b GROUP BY k, v, n
        UNION ALL
        SELECT k, v, n, count(*) c FROM ($3) b GROUP BY k, v, n
        EXCEPT SELECT k, v, n, count(*) c FROM ($2) a GROUP BY k, v, n) d")
    check "$1 ($(sqldb "SELECT count(*) FROM ($2) a") rows)" "0" "$diff"
}

# copy <name> <table>: a copy of the rows of the table now, the oracle of the offset of a stream made with it
copy() {
    sqldb "DROP TABLE IF EXISTS $1; CREATE TABLE $1 (k INT, v VARCHAR(32), n INT) DUPLICATE KEY (k)
        DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES ('replication_num' = '1');
        INSERT INTO $1 SELECT k, v, n FROM $2" >/dev/null
}

# the multiset difference a - b of two tables of (k, v, n)
minus() {
    echo "SELECT a.k, a.v, a.n FROM (SELECT k, v, n, row_number() OVER (PARTITION BY k, v, n) rn FROM $1) a
        LEFT JOIN (SELECT k, v, n, count(*) c FROM $2 GROUP BY k, v, n) b
        ON b.k <=> a.k AND b.v <=> a.v AND b.n <=> a.n WHERE a.rn > coalesce(b.c, 0)"
}

echo "== setup"
mysql -h "$HOST" -P "$PORT" -uroot -e "DROP DATABASE IF EXISTS $DB FORCE; CREATE DATABASE $DB"
RETAIN="PROPERTIES ('replication_num' = '1', 'data_retention_time_in_days' = '1')"
sqldb "
CREATE TABLE pk (k INT NOT NULL, v VARCHAR(32), n INT) PRIMARY KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 4 $RETAIN;
CREATE TABLE dup (k INT, v VARCHAR(32), n INT) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 4 $RETAIN;
CREATE TABLE day (dt DATE NOT NULL, k INT NOT NULL, v VARCHAR(32), n INT) PRIMARY KEY (dt, k)
  PARTITION BY RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2024-01-02'), PARTITION p2 VALUES LESS THAN ('2024-01-03'))
  DISTRIBUTED BY HASH(k) BUCKETS 2 $RETAIN;
CREATE TABLE plain (k INT, v VARCHAR(32), n INT) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 1
  PROPERTIES ('replication_num' = '1');
CREATE TABLE sink (k INT, v VARCHAR(32), n INT, action VARCHAR(8), isupdate BOOLEAN, row_id VARCHAR(32))
  DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES ('replication_num' = '1');
"
sqldb "INSERT INTO pk SELECT g, concat('v', g), if(g % 13 = 0, NULL, g % 100) FROM TABLE(generate_series(1, 20000)) s(g)
       " >/dev/null
sqldb "INSERT INTO dup SELECT g % 500, concat('v', g % 7), g % 3 FROM TABLE(generate_series(1, 5000)) s(g)" >/dev/null
sqldb "INSERT INTO day SELECT if(g % 2 = 0, '2024-01-01', '2024-01-02'), g, 'd', g FROM TABLE(generate_series(1, 1000)) s(g)
       " >/dev/null

echo "== CREATE / SHOW / DESCRIBE STREAM"
copy o_pk pk
sqldb "CREATE STREAM s_pk ON TABLE pk COMMENT 'pk changes'" >/dev/null
copy o_dup dup
sqldb "CREATE STREAM s_dup ON TABLE dup; CREATE STREAM s_dup_ao ON TABLE dup APPEND_ONLY = TRUE;
       CREATE STREAM s_init ON TABLE pk SHOW_INITIAL_ROWS = TRUE; CREATE STREAM s_day ON TABLE day" >/dev/null
check "SHOW STREAMS" "s_day day DEFAULT|s_dup dup DEFAULT|s_dup_ao dup APPEND_ONLY|s_init pk DEFAULT|s_pk pk DEFAULT" \
    "$(sqldb "SHOW STREAMS" | awk -F'\t' '{print $2, $6, $9}' | tr '\n' '|' | sed 's/|$//')"
check "SHOW STREAMS LIKE" "s_dup|s_dup_ao" "$(sqldb "SHOW STREAMS FROM $DB LIKE 's_dup%'" | cut -f2 | tr '\n' '|' | sed 's/|$//')"
desc=$(sqldb "DESC STREAM s_pk")
check "DESCRIBE STREAM" "s_pk pk changes pk DELTA false" "$(echo "$desc" | awk -F'\t' '{print $2, $5, $6, $7, $8}')"
check "a new stream has no changes" "0" "$(sqldb "SELECT count(*) FROM s_pk")"
check "SHOW_INITIAL_ROWS returns every row" "20000 INSERT" \
    "$(sqldb "SELECT count(*), max(METADATA\$ACTION) FROM s_init" | tr '\t' ' ')"
same "SHOW_INITIAL_ROWS rows" "SELECT k, v, n FROM s_init" "SELECT k, v, n FROM pk"

echo "== the changes of a Primary Key table"
sqldb "UPDATE pk SET v = 'upd', n = n + 1 WHERE k % 3 = 0" >/dev/null
sqldb "DELETE FROM pk WHERE k % 5 = 0" >/dev/null
sqldb "INSERT INTO pk SELECT g, 'new', 7 FROM TABLE(generate_series(20001, 21000)) s(g)" >/dev/null
# a key inserted and deleted since the offset is no change; one updated back to its old row neither
sqldb "INSERT INTO pk VALUES (30000, 'gone', 1); DELETE FROM pk WHERE k = 30000;
       UPDATE pk SET v = 'tmp' WHERE k = 1; UPDATE pk SET v = 'v1' WHERE k = 1" >/dev/null
same "INSERT rows are the rows the table has and its copy at the offset not" \
    "SELECT k, v, n FROM s_pk WHERE METADATA\$ACTION = 'INSERT'" "$(minus pk o_pk)"
same "DELETE rows are the rows the copy has and the table not" \
    "SELECT k, v, n FROM s_pk WHERE METADATA\$ACTION = 'DELETE'" "$(minus o_pk pk)"
check "updates are a DELETE and an INSERT with METADATA\$ISUPDATE" \
    "$(sqldb "SELECT 2 * count(*) FROM o_pk WHERE k % 3 = 0 AND k % 5 <> 0")" \
    "$(sqldb "SELECT count(*) FROM s_pk WHERE METADATA\$ISUPDATE")"
check "the two rows of an update share METADATA\$ROW_ID" "0" \
    "$(sqldb "SELECT count(*) FROM (SELECT METADATA\$ROW_ID FROM s_pk WHERE METADATA\$ISUPDATE GROUP BY 1
        HAVING count(*) <> 2 OR count(DISTINCT METADATA\$ACTION) <> 2) x")"
check "METADATA\$ROW_ID is unique per action" "0" \
    "$(sqldb "SELECT count(*) - count(DISTINCT METADATA\$ACTION, METADATA\$ROW_ID) FROM s_pk")"
check "net change: keys 1 and 30000 are not in the stream" "0" "$(sqldb "SELECT count(*) FROM s_pk WHERE k IN (1, 30000)")"
total=$(sqldb "SELECT count(*) FROM s_pk")
check "a SELECT does not consume the stream" "$total" "$(sqldb "SELECT count(*) FROM s_pk")"
for fe in "${OTHERS[@]}"; do
    check "the stream returns the same changes on $fe" "$total" "$(sqlon "$fe" "SELECT count(*) FROM s_pk")"
done

echo "== consumption"
copy o_pk2 pk
sqldb "INSERT INTO sink SELECT k, v, n, METADATA\$ACTION, METADATA\$ISUPDATE, METADATA\$ROW_ID FROM s_pk" >/dev/null
check "the INSERT wrote every change" "$total" "$(sqldb "SELECT count(*) FROM sink")"
check "the INSERT consumed the stream" "0" "$(sqldb "SELECT count(*) FROM s_pk")"
for fe in "${OTHERS[@]}"; do
    check "the stream is consumed on $fe" "0" "$(sqlon "$fe" "SELECT count(*) FROM s_pk")"
done
sqldb "INSERT INTO pk VALUES (40000, 'later', 1)" >/dev/null
check "changes after the consumption" "40000 later INSERT" \
    "$(sqldb "SELECT k, v, METADATA\$ACTION FROM s_pk" | tr '\t' ' ')"
same "the stream after consumption is the difference with the table at the consumption" \
    "SELECT k, v, n FROM s_pk WHERE METADATA\$ACTION = 'INSERT'" "$(minus pk o_pk2)"
# a failed DML statement does not move the offset
out=$(sqlforce "INSERT INTO sink (k, v, n, action, isupdate, row_id) SELECT k, v, n, METADATA\$ACTION, METADATA\$ISUPDATE,
       assert_true(k < 0, 'stop') FROM s_pk;")
contains "a failing INSERT fails" "stop" "$out"
check "a failing INSERT does not consume the stream" "1" "$(sqldb "SELECT count(*) FROM s_pk")"

echo "== explicit transactions"
sqldb "TRUNCATE TABLE sink" >/dev/null
sqldb "BEGIN; INSERT INTO sink SELECT k, v, n, METADATA\$ACTION, METADATA\$ISUPDATE, METADATA\$ROW_ID FROM s_pk; ROLLBACK"
check "ROLLBACK keeps the offset" "1 0" "$(sqldb "SELECT (SELECT count(*) FROM s_pk), (SELECT count(*) FROM sink)" | tr '\t' ' ')"
sqldb "BEGIN; INSERT INTO sink SELECT k, v, n, METADATA\$ACTION, METADATA\$ISUPDATE, METADATA\$ROW_ID FROM s_pk;
       INSERT INTO pk VALUES (40001, 'in txn', 1); COMMIT" >/dev/null 2>&1
check "COMMIT moves the offset" "1" "$(sqldb "SELECT count(*) FROM sink")"
contains "the change written in the transaction stays in the stream" "40001" "$(sqldb "SELECT k FROM s_pk")"

echo "== Duplicate Key tables"
sqldb "INSERT INTO dup SELECT g % 500, concat('v', g % 7), g % 3 FROM TABLE(generate_series(1, 300)) s(g)" >/dev/null
sqldb "DELETE FROM dup WHERE k < 20" >/dev/null
same "standard: INSERT rows are the multiset difference table - copy" \
    "SELECT k, v, n FROM s_dup WHERE METADATA\$ACTION = 'INSERT'" "$(minus dup o_dup)"
same "standard: DELETE rows are the multiset difference copy - table" \
    "SELECT k, v, n FROM s_dup WHERE METADATA\$ACTION = 'DELETE'" "$(minus o_dup dup)"
check "standard: no update" "0" "$(sqldb "SELECT count(*) FROM s_dup WHERE METADATA\$ISUPDATE")"
check "standard: unique row ids" "0" "$(sqldb "SELECT count(*) - count(DISTINCT METADATA\$ACTION, METADATA\$ROW_ID) FROM s_dup")"
check "append only: the inserted rows, no deletes" "$(sqldb "SELECT count(*) FROM ($(minus dup o_dup)) x") 0" \
    "$(sqldb "SELECT count(*), count(*) - sum(if(METADATA\$ACTION = 'INSERT', 1, 0)) FROM s_dup_ao" | tr '\t' ' ')"

echo "== partitioned tables"
sqldb "UPDATE day SET v = 'x' WHERE dt = '2024-01-02' AND k < 100" >/dev/null
check "changes of one partition" "50 50" "$(sqldb "SELECT sum(if(METADATA\$ACTION = 'INSERT', 1, 0)),
    sum(if(METADATA\$ACTION = 'DELETE', 1, 0)) FROM s_day" | tr '\t' ' ')"
contains "only the changed partition is read" "partitions=1/2" "$(sqldb "EXPLAIN SELECT * FROM s_day")"

echo "== concurrent consumers"
sqldb "TRUNCATE TABLE sink" >/dev/null
sqldb "INSERT INTO pk SELECT g, 'c', 1 FROM TABLE(generate_series(50000, 50999)) s(g)" >/dev/null
pending=$(sqldb "SELECT count(*) FROM s_pk")
for i in 1 2 3 4; do
    sqldb "INSERT INTO sink SELECT k, v, n, METADATA\$ACTION, METADATA\$ISUPDATE, METADATA\$ROW_ID FROM s_pk" \
        > "/tmp/stream_e2e_$i.out" &
done
wait
check "every change is consumed exactly once" "$pending 0" \
    "$(sqldb "SELECT (SELECT count(*) FROM sink), (SELECT count(*) FROM s_pk)" | tr '\t' ' ')"
check "the duplicate rows check" "0" "$(sqldb "SELECT count(*) - count(DISTINCT k) FROM sink")"

echo "== stale streams and errors"
sqldb "CREATE STREAM s_stale ON TABLE dup" >/dev/null
sqldb "TRUNCATE TABLE dup" >/dev/null
check "TRUNCATE makes the stream stale" "true" "$(sqldb "DESC STREAM s_stale" | cut -f8)"
contains "a stale stream cannot be read" "Stream s_stale is stale" "$(sqlforce "SELECT * FROM s_stale;")"
contains "no stream on a table without time travel" "time travel is not enabled" \
    "$(sqlforce "CREATE STREAM e ON TABLE plain;")"
contains "a stream needs a table" "Table nosuch does not exist" "$(sqlforce "CREATE STREAM e ON TABLE nosuch;")"
contains "a stream is not read with time travel" "cannot be read with time travel" \
    "$(sqlforce "SELECT * FROM s_pk AT(OFFSET => -1);")"
contains "the name of a stream exists" "already exists" "$(sqlforce "CREATE STREAM s_pk ON TABLE pk;")"
sqldb "CREATE OR REPLACE STREAM s_pk ON TABLE pk AT(OFFSET => -1)" >/dev/null
contains "CREATE OR REPLACE ... AT" "s_pk" "$(sqldb "SHOW STREAMS LIKE 's_pk'")"
sqldb "DROP STREAM s_dup; DROP STREAM IF EXISTS s_dup" >/dev/null
contains "DROP STREAM" "does not exist" "$(sqlforce "DROP STREAM s_dup;")"
contains "a dropped stream cannot be read" "s_dup" "$(sqlforce "SELECT * FROM s_dup;")"

if [[ $FAILURES -eq 0 ]]; then
    echo "ALL CHECKS PASSED"
else
    echo "$FAILURES CHECKS FAILED"
fi
exit $FAILURES
