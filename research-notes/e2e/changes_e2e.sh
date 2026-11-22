#!/usr/bin/env bash
# End-to-end test of the CHANGES clause on a running StarRocks cluster: the changes of a table between two points in
# time compared with the difference of the table read with time travel at those points (the oracle), Primary Key,
# Duplicate Key and partitioned tables, INFORMATION => DEFAULT and APPEND_ONLY, AT / BEFORE / OFFSET / END, the
# offset of a stream as the start (which CHANGES does not move), CHANGES in INSERT, joins and views, and the errors.
# Usage: changes_e2e.sh [host] [port] [other fe hosts...]
set -uo pipefail

HOST=${1:-127.0.0.1}
PORT=${2:-9030}
shift 2 2>/dev/null
OTHERS=("$@")
DB=changes_e2e
FAILURES=0

sqlon() { mysql -h "$1" -P "$PORT" -uroot -N -B -D "$DB" -e "$2" 2>&1; }
sqldb() { sqlon "$HOST" "$1"; }
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

# same <description> <query a> <query b>: the two queries return the same multiset of (k, v, n)
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

# snap <table> <point name> <time>: a copy of the table at the time, read with time travel: the oracle at the point
snap() {
    sqldb "CREATE TABLE $1_$2 (k INT, v VARCHAR(32), n INT) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 4
        PROPERTIES ('replication_num' = '1'); INSERT INTO $1_$2 SELECT k, v, n FROM $1 AT(TIMESTAMP => '$3')" >/dev/null
}
# minus <a> <b>: the multiset difference a - b of two relations of (k, v, n)
minus() {
    echo "SELECT a.k, a.v, a.n FROM (SELECT k, v, n, row_number() OVER (PARTITION BY k, v, n) rn FROM $1) a
        LEFT JOIN (SELECT k, v, n, count(*) c FROM $2 GROUP BY k, v, n) b
        ON b.k <=> a.k AND b.v <=> a.v AND b.n <=> a.n WHERE a.rn > coalesce(b.c, 0)"
}
# changes <table> <action> <clause>: the (k, v, n) of the changes of the table with the action
changes() { echo "SELECT k, v, n FROM $1 CHANGES(INFORMATION => DEFAULT) $3 WHERE METADATA\$ACTION = '$2'"; }
# mark: a point in time between two commits
mark() { sleep 1.2; sqldb "SELECT now()"; sleep 1.2; }

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
CREATE TABLE backfill (k INT, v VARCHAR(32), n INT, action VARCHAR(8), isupdate BOOLEAN) DUPLICATE KEY (k)
  DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1');
"
sqldb "INSERT INTO pk SELECT g, concat('v', g), if(g % 13 = 0, NULL, g % 100) FROM TABLE(generate_series(1, 20000)) s(g)
       " >/dev/null
sqldb "INSERT INTO dup SELECT g % 500, concat('v', g % 7), g % 3 FROM TABLE(generate_series(1, 5000)) s(g)" >/dev/null
sqldb "INSERT INTO day SELECT if(g % 2 = 0, '2024-01-01', '2024-01-02'), g, 'd', g FROM TABLE(generate_series(1, 1000)) s(g)
       " >/dev/null
T0=$(mark)
sqldb "UPDATE pk SET v = 'upd', n = n + 1 WHERE k % 3 = 0" >/dev/null
sqldb "DELETE FROM pk WHERE k % 5 = 0" >/dev/null
sqldb "INSERT INTO pk SELECT g, 'new', 7 FROM TABLE(generate_series(20001, 21000)) s(g)" >/dev/null
sqldb "INSERT INTO dup SELECT g % 500, concat('v', g % 7), g % 3 FROM TABLE(generate_series(1, 300)) s(g)" >/dev/null
sqldb "DELETE FROM dup WHERE k < 20" >/dev/null
sqldb "UPDATE day SET v = 'x' WHERE dt = '2024-01-02' AND k < 100" >/dev/null
sqldb "CREATE STREAM s_pk ON TABLE pk" >/dev/null
T1=$(mark)
# a key inserted and deleted again, and one changed back: no change between T1 and T2
sqldb "INSERT INTO pk VALUES (30000, 'gone', 1); DELETE FROM pk WHERE k = 30000;
       UPDATE pk SET v = 'tmp' WHERE k = 1; UPDATE pk SET v = 'v1' WHERE k = 1" >/dev/null
sqldb "UPDATE pk SET n = -1 WHERE k BETWEEN 100 AND 199" >/dev/null
sqldb "INSERT INTO pk VALUES (40000, 'later', 1)" >/dev/null
T2=$(mark)
echo "  points: $T0 | $T1 | $T2"
for t in pk dup; do snap $t t0 "$T0"; snap $t t1 "$T1"; done

echo "== a Primary Key table between two points"
W01="AT(TIMESTAMP => '$T0') END(TIMESTAMP => '$T1')"
same "INSERT rows are the rows at the end the start does not have" "$(changes pk INSERT "$W01")" \
    "$(minus pk_t1 pk_t0)"
same "DELETE rows are the rows at the start the end does not have" "$(changes pk DELETE "$W01")" \
    "$(minus pk_t0 pk_t1)"
check "updates are a DELETE and an INSERT with METADATA\$ISUPDATE" \
    "$(sqldb "SELECT 2 * count(*) FROM pk_t0 WHERE k % 3 = 0 AND k % 5 <> 0")" \
    "$(sqldb "SELECT count(*) FROM pk CHANGES(INFORMATION => DEFAULT) $W01 WHERE METADATA\$ISUPDATE")"
check "the two rows of an update share METADATA\$ROW_ID" "0" \
    "$(sqldb "SELECT count(*) FROM (SELECT METADATA\$ROW_ID FROM pk CHANGES(INFORMATION => DEFAULT) $W01
        WHERE METADATA\$ISUPDATE GROUP BY 1 HAVING count(*) <> 2 OR count(DISTINCT METADATA\$ACTION) <> 2) x")"
same "up to now without END" "$(changes pk INSERT "AT(TIMESTAMP => '$T1')")" "$(minus pk pk_t1)"
same "up to now: DELETE rows" "$(changes pk DELETE "AT(TIMESTAMP => '$T1')")" "$(minus pk_t1 pk)"
check "net change: keys 1 and 30000 changed back" "0" \
    "$(sqldb "SELECT count(*) FROM pk CHANGES(INFORMATION => DEFAULT) AT(TIMESTAMP => '$T1') WHERE k IN (1, 30000)")"
check "BEFORE(TIMESTAMP => t) is AT just before t" \
    "$(sqldb "SELECT count(*) FROM pk CHANGES(INFORMATION => DEFAULT) $W01")" \
    "$(sqldb "SELECT count(*) FROM pk CHANGES(INFORMATION => DEFAULT) BEFORE(TIMESTAMP => '$T0') END(TIMESTAMP => '$T1')")"
check "AT(OFFSET => -n) and END(OFFSET => -n)" \
    "$(sqldb "SELECT count(*) FROM pk CHANGES(INFORMATION => DEFAULT) AT(TIMESTAMP => '$T1') END(TIMESTAMP => '$T2')")" \
    "$(o1=$(sqldb "SELECT timestampdiff(SECOND, now(), '$T1')"); o2=$(sqldb "SELECT timestampdiff(SECOND, now(), '$T2')")
       sqldb "SELECT count(*) FROM pk CHANGES(INFORMATION => DEFAULT) AT(OFFSET => $o1) END(OFFSET => $o2)")"
check "APPEND_ONLY: the rows of the new keys" \
    "$(sqldb "SELECT count(*) FROM pk_t1 WHERE k NOT IN (SELECT k FROM pk_t0)")|0" \
    "$(sqldb "SELECT count(*), count(*) - sum(if(METADATA\$ACTION = 'INSERT' AND NOT METADATA\$ISUPDATE, 1, 0))
        FROM pk CHANGES(INFORMATION => APPEND_ONLY) $W01" | tr '\t' '|')"
check "an empty range has no changes" "0" \
    "$(sqldb "SELECT count(*) FROM pk CHANGES(INFORMATION => DEFAULT) AT(TIMESTAMP => '$T2')")"
total=$(sqldb "SELECT count(*) FROM pk CHANGES(INFORMATION => DEFAULT) $W01")
for fe in "${OTHERS[@]}"; do
    check "the same changes through FE $fe" "$total" \
        "$(sqlon "$fe" "SELECT count(*) FROM pk CHANGES(INFORMATION => DEFAULT) $W01")"
done

echo "== Duplicate Key and partitioned tables"
same "Duplicate Key: INSERT rows are the multiset difference end - start" "$(changes dup INSERT "$W01")" \
    "$(minus dup_t1 dup_t0)"
same "Duplicate Key: DELETE rows are the multiset difference start - end" "$(changes dup DELETE "$W01")" \
    "$(minus dup_t0 dup_t1)"
check "Duplicate Key APPEND_ONLY: the added rows" \
    "$(sqldb "SELECT count(*) FROM ($(minus dup_t1 dup_t0)) x")" \
    "$(sqldb "SELECT count(*) FROM dup CHANGES(INFORMATION => APPEND_ONLY) $W01")"
check "partitioned: the changes of one partition" "50 50" "$(sqldb "SELECT sum(if(METADATA\$ACTION = 'INSERT', 1, 0)),
    sum(if(METADATA\$ACTION = 'DELETE', 1, 0)) FROM day CHANGES(INFORMATION => DEFAULT) $W01" | tr '\t' ' ')"
contains "only the changed partition is read" "partitions=1/2" \
    "$(sqldb "EXPLAIN SELECT * FROM day CHANGES(INFORMATION => DEFAULT) $W01")"

echo "== the offset of a stream as the start"
check "AT(STREAM => s) returns what the stream returns" \
    "$(sqldb "SELECT count(*), sum(murmur_hash3_32(METADATA\$ROW_ID)) FROM s_pk")" \
    "$(sqldb "SELECT count(*), sum(murmur_hash3_32(METADATA\$ROW_ID)) FROM pk CHANGES(INFORMATION => DEFAULT)
        AT(STREAM => 's_pk')")"
sqldb "INSERT INTO backfill SELECT k, v, n, METADATA\$ACTION, METADATA\$ISUPDATE FROM pk CHANGES(INFORMATION => DEFAULT)
       AT(STREAM => '$DB.s_pk')" >/dev/null
check "an INSERT of CHANGES does not consume the stream" "$(sqldb "SELECT count(*) FROM backfill")" \
    "$(sqldb "SELECT count(*) FROM s_pk")"
sqldb "INSERT INTO backfill SELECT k, v, n, METADATA\$ACTION, METADATA\$ISUPDATE FROM s_pk" >/dev/null
check "after the stream is consumed it has no changes" "0|0" \
    "$(sqldb "SELECT count(*) FROM s_pk")|$(sqldb "SELECT count(*) FROM pk CHANGES(INFORMATION => DEFAULT) AT(STREAM => 's_pk')")"

echo "== CHANGES in queries"
check "a backfill by INSERT ... SELECT" "$total" \
    "$(sqldb "TRUNCATE TABLE backfill; INSERT INTO backfill SELECT k, v, n, METADATA\$ACTION, METADATA\$ISUPDATE
        FROM pk CHANGES(INFORMATION => DEFAULT) $W01; SELECT count(*) FROM backfill")"
check "joined with the table, with an alias" \
    "$(sqldb "SELECT count(*) FROM ($(changes pk INSERT "$W01")) c WHERE k IN (SELECT k FROM pk)")" \
    "$(sqldb "SELECT count(*) FROM pk CHANGES(INFORMATION => DEFAULT) $W01 AS c JOIN pk t ON c.k = t.k
        WHERE c.METADATA\$ACTION = 'INSERT'")"
check "in a CTE and an aggregate" "$(sqldb "SELECT count(DISTINCT k) FROM ($(changes pk DELETE "$W01")) x")" \
    "$(sqldb "WITH c AS (SELECT * FROM pk CHANGES(INFORMATION => DEFAULT) $W01) SELECT count(DISTINCT k) FROM c
        WHERE METADATA\$ACTION = 'DELETE'")"
contains "not in a view, as time travel" "Do not support create view that reads a stream or a CHANGES clause" \
    "$(sqlforce "CREATE VIEW v_changes AS SELECT k FROM pk CHANGES(INFORMATION => DEFAULT) $W01;")"
contains "nor a view on a stream" "Do not support create view that reads a stream" \
    "$(sqlforce "CREATE VIEW v_stream AS SELECT k FROM s_pk;")"

echo "== errors"
contains "END before the start" "END point of CHANGES" \
    "$(sqlforce "SELECT * FROM pk CHANGES(INFORMATION => DEFAULT) AT(TIMESTAMP => '$T1') END(TIMESTAMP => '$T0');")"
contains "beyond the retention" "beyond its data retention time" \
    "$(sqlforce "SELECT * FROM pk CHANGES(INFORMATION => DEFAULT) AT(OFFSET => -864000);")"
contains "a table without time travel" "time travel is not enabled" \
    "$(sqlforce "SELECT * FROM plain CHANGES(INFORMATION => DEFAULT) AT(OFFSET => -1);")"
contains "the stream of another table" "Stream s_pk is not on table dup" \
    "$(sqlforce "SELECT * FROM dup CHANGES(INFORMATION => DEFAULT) AT(STREAM => 's_pk');")"
contains "a stream is not a table" "s_pk is a stream" \
    "$(sqlforce "SELECT * FROM s_pk CHANGES(INFORMATION => DEFAULT) AT(OFFSET => -1);")"
contains "the information" "DEFAULT or APPEND_ONLY" \
    "$(sqlforce "SELECT * FROM pk CHANGES(INFORMATION => FOO) AT(OFFSET => -1);")"
contains "no materialized view on changes" "reads a stream or a CHANGES clause" \
    "$(sqlforce "CREATE MATERIALIZED VIEW mv_c REFRESH ASYNC AS SELECT k FROM pk CHANGES(INFORMATION => DEFAULT)
        AT(OFFSET => -1);")"
sqldb "TRUNCATE TABLE dup" >/dev/null
sqlforce "DROP MATERIALIZED VIEW IF EXISTS mv_c;" >/dev/null
contains "not across a TRUNCATE" "when its data was replaced" \
    "$(sqlforce "SELECT count(*) FROM dup CHANGES(INFORMATION => DEFAULT) $W01;")"

echo
if (( FAILURES == 0 )); then
    echo "ALL CHECKS PASSED"
else
    echo "$FAILURES CHECK(S) FAILED"
fi
exit $FAILURES
