#!/usr/bin/env bash
# End-to-end test of INSERT ALL and INSERT FIRST on a running StarRocks cluster. Each statement is compared with
# the rows a plain query computes for every target table (the oracle), on tens of thousands of rows with NULLs.
# Also checks DEFAULT, several INTO clauses for one table, the affected row count, that a failed statement
# changes no table, and explicit transactions.
# Usage: insert_multi_e2e.sh [host] [port]
set -uo pipefail

HOST=${1:-127.0.0.1}
PORT=${2:-9030}
DB=insert_multi_e2e
FAILURES=0

sqldb() { mysql -h "$HOST" -P "$PORT" -uroot -N -B -D "$DB" -e "$1" 2>&1; }
# sqlforce <sql>: goes on after an error, so the statements after it run in the same session (the mysql client
# stops at the first error of -e even with --force, so the statements are read from stdin)
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

reset() { sqldb "TRUNCATE TABLE a; TRUNCATE TABLE b; TRUNCATE TABLE c; TRUNCATE TABLE p;"; }

echo "== setup"
mysql -h "$HOST" -P "$PORT" -uroot -e "DROP DATABASE IF EXISTS $DB FORCE; CREATE DATABASE $DB"
sqldb "
CREATE TABLE src (id INT, name VARCHAR(32), amount INT)
  DUPLICATE KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES ('replication_num' = '1');
INSERT INTO src SELECT g, concat('n', g), if(g % 17 = 0, NULL, g % 100 - 30) FROM TABLE(generate_series(1, 30000)) s(g);
INSERT INTO src VALUES (NULL, NULL, NULL);
CREATE TABLE a (k INT, v VARCHAR(32), n INT) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 4
  PROPERTIES ('replication_num' = '1');
CREATE TABLE b (k INT, v VARCHAR(32) DEFAULT 'dflt', n INT) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 4
  PROPERTIES ('replication_num' = '1');
CREATE TABLE c (k INT, v VARCHAR(3), n INT) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 4
  PROPERTIES ('replication_num' = '1');
CREATE TABLE p (k INT NOT NULL, v VARCHAR(32), n INT) PRIMARY KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 4
  PROPERTIES ('replication_num' = '1');
"
check "source rows" "30001" "$(sqldb "SELECT count(*) FROM src")"

echo "== INSERT ALL without conditions"
out=$(mysql -h "$HOST" -P "$PORT" -uroot -D "$DB" -vv -e "
INSERT ALL INTO a INTO b (k, n) VALUES (id, amount * 2) INTO p (k, v, n) VALUES (id, upper(name), amount)
SELECT id, name, amount FROM src WHERE id IS NOT NULL" 2>&1)
check "affected rows are summed" "1" "$(echo "$out" | grep -c "90000 rows affected")"
same "a gets every row" a "SELECT id k, name v, amount n FROM src WHERE id IS NOT NULL"
same "b gets the values, DEFAULT for v" b "SELECT id k, 'dflt' v, amount * 2 n FROM src WHERE id IS NOT NULL"
same "p gets the expressions" p "SELECT id k, upper(name) v, amount n FROM src WHERE id IS NOT NULL"

echo "== conditional INSERT ALL, two INTO clauses for one table, ELSE"
reset
sqldb "
INSERT ALL
  WHEN amount > 50 THEN INTO a (k, v, n) VALUES (id, name, amount) INTO b (k, v, n) VALUES (id, 'big', amount)
  WHEN amount > 0 THEN INTO a (n, k, v) VALUES (amount, id, 'positive')
  ELSE INTO b (k, n) VALUES (id, amount) INTO c (k, n) VALUES (id, -1)
SELECT * FROM src" >/dev/null
same "a: every condition that holds" a "
  SELECT id k, name v, amount n FROM src WHERE amount > 50
  UNION ALL SELECT id, 'positive', amount FROM src WHERE amount > 0"
same "b: the first WHEN and ELSE" b "
  SELECT id k, 'big' v, amount n FROM src WHERE amount > 50
  UNION ALL SELECT id, 'dflt', amount FROM src WHERE NOT coalesce(amount > 0, false)"
same "c: ELSE, NULL conditions count as false" c "
  SELECT id k, NULL v, -1 n FROM src WHERE amount <= 0 OR amount IS NULL"

echo "== INSERT FIRST"
reset
sqldb "
INSERT FIRST
  WHEN amount > 50 THEN INTO a
  WHEN amount > 0 THEN INTO b
  WHEN amount > -10 THEN INTO a
  ELSE INTO c (k, v) VALUES (id, 'neg')
SELECT id, name, amount FROM src" >/dev/null
same "a: the first and the third WHEN" a "
  SELECT id k, name v, amount n FROM src WHERE amount > 50 OR (amount > -10 AND amount <= 0)"
same "b: the second WHEN only" b "SELECT id k, name v, amount n FROM src WHERE amount > 0 AND amount <= 50"
same "c: no WHEN holds" c "SELECT id k, 'neg' v, NULL n FROM src WHERE NOT coalesce(amount > -10, false)"
check "every row is written once" "30001" "$(sqldb "SELECT (SELECT count(*) FROM a) + (SELECT count(*) FROM b)
  + (SELECT count(*) FROM c)")"

echo "== a failed statement changes no table"
reset
sqldb "INSERT INTO a VALUES (0, 'keep', 0)" >/dev/null
out=$(sqldb "INSERT ALL INTO a INTO b INTO c SELECT id, name, amount FROM src")
check "too long value for c fails" "1" "$(echo "$out" | grep -ci "error")"
check "a is unchanged" "1" "$(sqldb "SELECT count(*) FROM a")"
check "b is unchanged" "0" "$(sqldb "SELECT count(*) FROM b")"

echo "== explicit transactions"
reset
sqldb "BEGIN; INSERT ALL INTO a INTO b SELECT * FROM src WHERE id <= 10; ROLLBACK;" >/dev/null
check "ROLLBACK drops the rows" "0 0" "$(sqldb "SELECT (SELECT count(*) FROM a), (SELECT count(*) FROM b)" | tr '\t' ' ')"
sqldb "BEGIN; INSERT INTO p VALUES (1, 'x', 1); INSERT ALL INTO a INTO b SELECT * FROM src WHERE id <= 10; COMMIT;" \
    >/dev/null
check "COMMIT keeps every table" "10 10 1" "$(sqldb "SELECT (SELECT count(*) FROM a), (SELECT count(*) FROM b),
  (SELECT count(*) FROM p)" | tr '\t' ' ')"
reset
out=$(sqlforce "BEGIN; INSERT INTO p VALUES (1, 'x', 1); INSERT ALL INTO a INTO c SELECT * FROM src; COMMIT;")
check "a failure after a table is loaded rolls the transaction back" "1" \
    "$(echo "$out" | grep -c "The transaction was rolled back")"
check "nothing of the transaction is kept" "0 0 0" "$(sqldb "SELECT (SELECT count(*) FROM a), (SELECT count(*) FROM c),
  (SELECT count(*) FROM p)" | tr '\t' ' ')"
out=$(sqlforce "BEGIN; INSERT INTO p VALUES (1, 'x', 1); INSERT ALL INTO zz INTO a SELECT * FROM src; COMMIT;")
check "a failure before any table is loaded keeps the transaction" "1 0" \
    "$(sqldb "SELECT (SELECT count(*) FROM p), (SELECT count(*) FROM a)" | tr '\t' ' ')"

echo "== errors"
out=$(sqldb "INSERT FIRST INTO a SELECT * FROM src")
check "INSERT FIRST needs WHEN" "1" "$(echo "$out" | grep -c "INSERT FIRST needs WHEN clauses")"
out=$(sqldb "INSERT ALL INTO a INTO a (k) VALUES (id) SELECT * FROM src")
check "one table, different columns" "1" "$(echo "$out" | grep -c "Every INTO a must list its columns, or none of them")"
out=$(sqldb "INSERT ALL INTO a VALUES (id, DEFAULT, 1) SELECT * FROM src")
check "DEFAULT needs a column list" "1" "$(echo "$out" | grep -c "VALUES with DEFAULT needs a column list")"
out=$(sqldb "INSERT ALL INTO a (k) VALUES (nosuch) SELECT * FROM src")
check "unknown source column" "1" "$(echo "$out" | grep -c "cannot be resolved")"
check "errors load nothing" "0" "$(sqldb "SELECT count(*) FROM a WHERE k > 0")"

echo
if [[ $FAILURES -eq 0 ]]; then
    echo "ALL PASSED"
else
    echo "$FAILURES FAILED"
    exit 1
fi
