#!/usr/bin/env bash
# End-to-end test of MERGE INTO a Primary Key table on a running StarRocks cluster. Each MERGE is compared with
# the table it should leave, computed before it by a plain query (the oracle), on tens of thousands of rows
# with NULLs, deletes, skipped rows and inserts. Also checks NOT MATCHED BY SOURCE, UPDATE SET * and INSERT *,
# generated and AUTO_INCREMENT columns, the duplicate row error, explicit transactions and that a failed MERGE
# changes nothing.
# Usage: merge_e2e.sh [host] [port]
set -uo pipefail

HOST=${1:-127.0.0.1}
PORT=${2:-9030}
DB=merge_e2e
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

# diff_count <table> <expected table> <columns>: the rows that are in only one of them
diff_count() {
    sqldb "SELECT (SELECT count(*) FROM (SELECT $3 FROM $1 EXCEPT SELECT $3 FROM $2) a)
                + (SELECT count(*) FROM (SELECT $3 FROM $2 EXCEPT SELECT $3 FROM $1) b)"
}

checksum() { sqldb "SELECT count(*), sum(murmur_hash3_32(concat_ws('|', $2))) FROM $1"; }

echo "== setup"
mysql -h "$HOST" -P "$PORT" -uroot -e "DROP DATABASE IF EXISTS $DB FORCE; CREATE DATABASE $DB"
sqldb "
CREATE TABLE t (k INT NOT NULL, v VARCHAR(32) NOT NULL, n INT, d DATETIME DEFAULT CURRENT_TIMESTAMP)
  PRIMARY KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES ('replication_num' = '1');
INSERT INTO t (k, v, n)
  SELECT g, concat('v', g), if(g % 13 = 0, NULL, g % 97) FROM TABLE(generate_series(1, 50000)) s(g);
CREATE TABLE s (k INT, v VARCHAR(32), n INT, op VARCHAR(8))
  DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES ('replication_num' = '1');
INSERT INTO s
  SELECT g, concat('s', g), if(g % 11 = 0, NULL, g % 101),
         CASE WHEN g % 5 = 0 THEN 'del' WHEN g % 7 = 0 THEN 'skip' ELSE 'up' END
  FROM TABLE(generate_series(40001, 60000)) s(g);
INSERT INTO s VALUES (NULL, 'nullkey', 1, 'up');
"
check "target rows" "50000" "$(sqldb "SELECT count(*) FROM t")"

echo "== MERGE with conditional DELETE, UPDATE and INSERT"
sqldb "
CREATE TABLE expect PRIMARY KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES ('replication_num' = '1') AS
SELECT t.k, t.v, t.n FROM t LEFT ANTI JOIN s ON t.k = s.k
UNION ALL
SELECT t.k, if(s.n IS NULL, t.v, s.v), if(s.n IS NULL, -1, t.n + s.n)
FROM t JOIN s ON t.k = s.k WHERE s.op <> 'del' AND s.op <> 'skip'
UNION ALL
SELECT t.k, t.v, t.n FROM t JOIN s ON t.k = s.k WHERE s.op = 'skip'
UNION ALL
SELECT s.k, s.v, s.n FROM s LEFT ANTI JOIN t ON t.k = s.k WHERE s.op <> 'del' AND s.k IS NOT NULL;
"
start=$(date +%s)
out=$(sqldb "
MERGE INTO t USING s ON t.k = s.k
WHEN MATCHED AND s.op = 'del' THEN DELETE
WHEN MATCHED AND s.op = 'skip' THEN UPDATE SET v = t.v
WHEN MATCHED AND s.n IS NULL THEN UPDATE SET n = -1
WHEN MATCHED THEN UPDATE SET v = s.v, n = t.n + s.n
WHEN NOT MATCHED AND s.op <> 'del' AND s.k IS NOT NULL THEN INSERT (k, v, n) VALUES (s.k, s.v, s.n)")
echo "        (merge took $(($(date +%s) - start)) s) $out"
check "rows of the merged table" "$(sqldb "SELECT count(*) FROM expect")" "$(sqldb "SELECT count(*) FROM t")"
check "merged table equals the oracle" "0" "$(diff_count t expect "k, v, n")"
check "deleted rows are gone" "0" "$(sqldb "SELECT count(*) FROM t JOIN s ON t.k = s.k WHERE s.op = 'del'")"
check "inserted rows get the default of d" "0" "$(sqldb "SELECT count(*) FROM t WHERE d IS NULL")"

echo "== a failed MERGE changes nothing"
before=$(checksum t "k, v, n")
out=$(sqldb "MERGE INTO t USING s ON t.k = s.k WHEN NOT MATCHED THEN INSERT (k, v) VALUES (s.k, s.v)")
check "inserting a NULL key fails" "1" "$(echo "$out" | grep -c -i "error")"
check "table unchanged after the failure" "$before" "$(checksum t "k, v, n")"
sqldb "INSERT INTO s VALUES (100, 'dup1', 1, 'up'), (100, 'dup2', 2, 'up')"
out=$(sqldb "MERGE INTO t USING s ON t.k = s.k WHEN MATCHED THEN UPDATE SET v = s.v")
check "two source rows for one target row fail" "1" "$(echo "$out" | grep -c "Duplicate row detected during MERGE INTO")"
check "table unchanged after the duplicate error" "$before" "$(checksum t "k, v, n")"
out=$(sqldb "MERGE INTO t USING (SELECT 70001 k, 'a' v UNION ALL SELECT 70001, 'b') x ON t.k = x.k
             WHEN NOT MATCHED THEN INSERT (k, v) VALUES (x.k, x.v)")
check "two inserts of one key fail" "1" "$(echo "$out" | grep -c "Duplicate row detected during MERGE INTO")"
out=$(sqldb "SET error_on_nondeterministic_merge = false;
             MERGE INTO t USING s ON t.k = s.k AND s.k = 100 WHEN MATCHED THEN UPDATE SET v = s.v;
             SELECT v FROM t WHERE k = 100")
check "error_on_nondeterministic_merge = false writes one of them" "1" "$(echo "$out" | grep -c -E '^dup[12]$')"
sqldb "DELETE FROM s WHERE k = 100"
before=$(checksum t "k, v, n")
out=$(sqldb "BEGIN; MERGE INTO t USING s ON t.k = s.k WHEN MATCHED THEN DELETE; ROLLBACK;")
check "rolled back MERGE changes nothing" "$before" "$(checksum t "k, v, n")"
sqldb "BEGIN; MERGE INTO t USING (SELECT 1 k) x ON t.k = x.k WHEN MATCHED THEN DELETE; COMMIT;" >/dev/null
check "committed MERGE in a transaction" "0" "$(sqldb "SELECT count(*) FROM t WHERE k = 1")"

echo "== NOT MATCHED BY SOURCE, UPDATE SET * and INSERT *"
sqldb "
CREATE TABLE t2 (k INT NOT NULL, v VARCHAR(32), n INT)
  PRIMARY KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 3 PROPERTIES ('replication_num' = '1');
INSERT INTO t2 SELECT g, concat('v', g), g FROM TABLE(generate_series(1, 3000)) s(g);
CREATE TABLE s2 (k INT, v VARCHAR(32), n INT)
  DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 3 PROPERTIES ('replication_num' = '1');
INSERT INTO s2 SELECT g, concat('s', g), if(g % 9 = 0, NULL, -g) FROM TABLE(generate_series(2001, 4000)) s(g);
CREATE TABLE expect2 PRIMARY KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 3 PROPERTIES ('replication_num' = '1') AS
SELECT s2.k, s2.v, s2.n FROM s2
UNION ALL
SELECT t2.k, 'gone', t2.n FROM t2 LEFT ANTI JOIN s2 ON t2.k = s2.k WHERE t2.k % 2 = 0;
"
sqldb "
MERGE INTO t2 AS t USING s2 AS s ON t.k = s.k
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED BY TARGET THEN INSERT *
WHEN NOT MATCHED BY SOURCE AND t.k % 2 = 1 THEN DELETE
WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = 'gone'" >/dev/null
check "merged table equals the oracle" "0" "$(diff_count t2 expect2 "k, v, n")"
check "rows of the merged table" "$(sqldb "SELECT count(*) FROM expect2")" "$(sqldb "SELECT count(*) FROM t2")"

echo "== generated and AUTO_INCREMENT columns, composite key"
sqldb "
CREATE TABLE t3 (k1 INT NOT NULL, k2 VARCHAR(8) NOT NULL, id BIGINT NOT NULL AUTO_INCREMENT, a INT,
                 g BIGINT AS (a * 10))
  PRIMARY KEY (k1, k2) DISTRIBUTED BY HASH(k1) BUCKETS 2 PROPERTIES ('replication_num' = '1');
INSERT INTO t3 (k1, k2, a) VALUES (1, 'x', 1), (1, 'y', 2), (2, 'x', 3);
CREATE TABLE ids PROPERTIES ('replication_num' = '1') AS SELECT k1, k2, id FROM t3;
"
sqldb "
MERGE INTO t3 USING (SELECT 1 k1, 'y' k2, 20 a UNION ALL SELECT 3, 'z', 30) s ON t3.k1 = s.k1 AND t3.k2 = s.k2
WHEN MATCHED THEN UPDATE SET a = s.a
WHEN NOT MATCHED THEN INSERT (k1, k2, a) VALUES (s.k1, s.k2, s.a)" >/dev/null
check "generated column follows the new value" "1	x	1	10
1	y	20	200
2	x	3	30
3	z	30	300" "$(sqldb "SELECT k1, k2, a, g FROM t3 ORDER BY k1, k2")"
check "updated rows keep their AUTO_INCREMENT id" "0" "$(diff_count "(SELECT k1, k2, id FROM t3 WHERE k1 < 3) u" ids "k1, k2, id")"
check "inserted row gets a new id" "1" "$(sqldb "SELECT count(*) FROM t3 WHERE k1 = 3 AND id NOT IN (SELECT id FROM ids)")"

echo "== errors"
out=$(sqldb "MERGE INTO s USING t ON s.k = t.k WHEN MATCHED THEN DELETE")
check "not a Primary Key table" "1" "$(echo "$out" | grep -c "only supported for Primary Key tables and Iceberg tables")"
out=$(sqldb "MERGE INTO t USING s ON t.k = s.k WHEN MATCHED THEN UPDATE SET k = 1")
check "key column" "1" "$(echo "$out" | grep -c "primary key column cannot be updated")"

echo
if [[ $FAILURES -eq 0 ]]; then
    echo "ALL PASSED"
else
    echo "$FAILURES FAILED"
    exit 1
fi
