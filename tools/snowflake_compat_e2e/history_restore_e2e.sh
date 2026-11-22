#!/usr/bin/env bash
# End-to-end test of DESCRIBE HISTORY and RESTORE TABLE on a running StarRocks cluster: the commits a table records,
# restores compared with the rows time travel reads at the same point (the oracle), undoing a restore, Primary Key
# and Duplicate Key tables, and the errors.
# Usage: history_restore_e2e.sh [host] [port]
set -uo pipefail

HOST=${1:-127.0.0.1}
PORT=${2:-9030}
DB=history_restore_e2e
FAILURES=0

sqldb() { mysql -h "$HOST" -P "$PORT" -uroot -N -B -D "$DB" -e "$1" 2>&1; }

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

# same <description> <table> <query>: the table holds exactly the rows of the query, duplicates counted
same() {
    local diff
    diff=$(sqldb "SELECT count(*) FROM (
        SELECT k, v, n, count(*) c FROM $2 GROUP BY k, v, n
        EXCEPT SELECT k, v, n, count(*) c FROM ($3) q GROUP BY k, v, n
        UNION ALL
        SELECT k, v, n, count(*) c FROM ($3) q GROUP BY k, v, n
        EXCEPT SELECT k, v, n, count(*) c FROM $2 GROUP BY k, v, n) d")
    check "$1 ($(sqldb "SELECT count(*) FROM $2") rows)" "0" "$diff"
}

# point: the time now, after the last commit and before the next one
point() { sleep 1.2; sqldb "SELECT now()"; sleep 1.2; }

# snapshot <name> <table> <time>: a copy of the rows time travel reads, to compare a restore with
snapshot() {
    sqldb "DROP TABLE IF EXISTS $1; CREATE TABLE $1 (k INT, v VARCHAR(32), n INT) DUPLICATE KEY (k)
        DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES ('replication_num' = '1');
        INSERT INTO $1 SELECT k, v, n FROM $2 FOR TIMESTAMP AS OF '$3'" >/dev/null
}

echo "== setup"
mysql -h "$HOST" -P "$PORT" -uroot -e "DROP DATABASE IF EXISTS $DB FORCE; CREATE DATABASE $DB"
sqldb "
CREATE TABLE pk (k INT NOT NULL, v VARCHAR(32), n INT, g BIGINT AS (n * 2)) PRIMARY KEY (k)
  DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES ('replication_num' = '1', 'data_retention_time_in_days' = '1');
CREATE TABLE dup (k INT, v VARCHAR(32), n INT) DUPLICATE KEY (k)
  DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES ('replication_num' = '1', 'data_retention_time_in_days' = '1');
CREATE TABLE plain (k INT, v VARCHAR(32), n INT) DUPLICATE KEY (k)
  DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1');
CREATE VIEW pv AS SELECT * FROM pk;
"
t0=$(point)
sqldb "INSERT INTO pk (k, v, n) SELECT g, concat('v', g), if(g % 13 = 0, NULL, g % 100)
       FROM TABLE(generate_series(1, 20000)) s(g)" >/dev/null
t1=$(point)
sqldb "UPDATE pk SET v = 'upd', n = n + 1 WHERE k % 3 = 0" >/dev/null
t2=$(point)
sqldb "DELETE FROM pk WHERE k % 5 = 0" >/dev/null
sqldb "INSERT INTO pk (k, v, n) SELECT g, 'new', 7 FROM TABLE(generate_series(20001, 25000)) s(g)" >/dev/null
t3=$(point)
snapshot o1 pk "$t1"
snapshot o2 pk "$t2"
snapshot o3 pk "$t3"
check "snapshots differ" "20000 20000 21000" "$(sqldb "SELECT (SELECT count(*) FROM o1), (SELECT count(*) FROM o2),
  (SELECT count(*) FROM o3)" | tr '\t' ' ')"

echo "== DESCRIBE HISTORY"
history=$(sqldb "DESCRIBE HISTORY pk")
check "one row per commit, newest first" "INSERT_INTO DELETE UPDATE INSERT_INTO" \
    "$(echo "$history" | cut -f2 | tr '\n' ' ' | sed 's/ $//')"
check "rows written" "5000 20000" "$(echo "$history" | sed -n '1p;4p' | cut -f3 | tr '\n' ' ' | sed 's/ $//')"
check "every commit can be restored" "true true true true" \
    "$(echo "$history" | cut -f7 | tr '\n' ' ' | sed 's/ $//')"
check "partitions written" "1" "$(echo "$history" | head -1 | cut -f4)"
check "LIMIT" "2" "$(sqldb "DESC HISTORY $DB.pk LIMIT 2" | wc -l | tr -d ' ')"
check "commit times are in order" "$(echo "$history" | cut -f1 | sort -r)" "$(echo "$history" | cut -f1)"

echo "== RESTORE TABLE on a Primary Key table"
out=$(mysql -h "$HOST" -P "$PORT" -uroot -D "$DB" -vv -e "RESTORE TABLE pk TO TIMESTAMP AS OF '$t2'" 2>&1)
check "affected rows are the restored rows" "1" "$(echo "$out" | grep -c "20000 rows affected")"
same "the rows as of the point" pk "SELECT * FROM o2"
check "generated column computed" "0" "$(sqldb "SELECT count(*) FROM pk WHERE g <=> n * 2 = false")"
check "the restore is in the history" "RESTORE" "$(sqldb "DESCRIBE HISTORY pk LIMIT 1" | cut -f2)"
t4=$(point)
check "time travel still reads before the restore" "21000" "$(sqldb "SELECT count(*) FROM pk FOR TIMESTAMP AS OF '$t3'")"
sqldb "RESTORE TABLE pk TO TIMESTAMP AS OF '$t3'" >/dev/null
same "a restore can be undone" pk "SELECT * FROM o3"
sqldb "RESTORE TABLE pk TO TIMESTAMP AS OF '$t0'" >/dev/null
check "a point before any data restores an empty table" "0" "$(sqldb "SELECT count(*) FROM pk")"
sqldb "RESTORE TABLE pk TO AT(TIMESTAMP => '$t4')" >/dev/null
same "AT(TIMESTAMP => ...)" pk "SELECT * FROM o2"

echo "== RESTORE TABLE on a Duplicate Key table"
sqldb "INSERT INTO dup SELECT g, concat('d', g), g FROM TABLE(generate_series(1, 10000)) s(g);
       INSERT INTO dup SELECT g, concat('d', g), g FROM TABLE(generate_series(1, 100)) s(g)" >/dev/null
d1=$(point)
sqldb "DELETE FROM dup WHERE k <= 5000" >/dev/null
sqldb "INSERT INTO dup VALUES (0, 'x', 0)" >/dev/null
point >/dev/null
sqldb "RESTORE TABLE dup TO TIMESTAMP AS OF '$d1'" >/dev/null
same "the rows as of the point, duplicates kept" dup \
    "SELECT g k, concat('d', g) v, g n FROM TABLE(generate_series(1, 10000)) s(g)
     UNION ALL SELECT g, concat('d', g), g FROM TABLE(generate_series(1, 100)) s(g)"
check "the restore is in the history" "RESTORE" "$(sqldb "DESCRIBE HISTORY dup LIMIT 1" | cut -f2)"
out=$(sqldb "SELECT count(*) FROM dup FOR TIMESTAMP AS OF '$d1'")
check "time travel cannot read before the restore" "1" "$(echo "$out" | grep -c "when its data was replaced")"

echo "== errors"
before=$(sqldb "SELECT count(*) FROM pk")
out=$(sqldb "RESTORE TABLE pk TO TIMESTAMP AS OF date_add(now(), interval 1 day)")
check "a point in the future" "1" "$(echo "$out" | grep -c "is in the future")"
check "a failed restore changes nothing" "$before" "$(sqldb "SELECT count(*) FROM pk")"
out=$(sqldb "RESTORE TABLE pk TO TIMESTAMP AS OF date_sub(now(), interval 2 day)")
check "a point beyond the retention time" "1" "$(echo "$out" | grep -c "beyond its data retention time")"
out=$(sqldb "RESTORE TABLE plain TO TIMESTAMP AS OF now()")
check "a table without time travel" "1" "$(echo "$out" | grep -c "Time travel is not enabled for table plain")"
out=$(sqldb "DESCRIBE HISTORY pv")
check "a view" "1" "$(echo "$out" | grep -c "DESCRIBE HISTORY is only supported for native tables")"
out=$(sqldb "BEGIN; RESTORE TABLE pk TO TIMESTAMP AS OF '$t1'")
check "not in an explicit transaction" "1" "$(echo "$out" | grep -c "Explicit transaction only support")"
check "no staging table is left" "0" "$(sqldb "SHOW TABLES LIKE '__restore%'" | wc -l | tr -d ' ')"

echo
if [[ $FAILURES -eq 0 ]]; then
    echo "ALL PASSED"
else
    echo "$FAILURES FAILED"
    exit 1
fi
