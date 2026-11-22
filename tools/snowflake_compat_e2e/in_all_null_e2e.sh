#!/usr/bin/env bash
# Checks the IN (NULL, ...) push down fix (#84124) against a running cluster,
# mirroring test/sql/test_push_down_predicate/T/test_in_all_null_push_down.
HOST=${HOST:-127.0.0.1}; PORT=${PORT:-9030}; DB=in_null_e2e; FAILURES=0
sql() { mysql -h "$HOST" -P "$PORT" -uroot -N -B -e "$1" 2>&1; }
sqldb() { mysql -h "$HOST" -P "$PORT" -uroot -N -B -D "$DB" -e "$1" 2>&1; }
check() { if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; else echo "  FAIL  $1 (expected $2, got $3)"; FAILURES=$((FAILURES + 1)); fi; }
sql "DROP DATABASE IF EXISTS $DB FORCE; CREATE DATABASE $DB" > /dev/null
sqldb "CREATE TABLE t (k INT, v SMALLINT NOT NULL, w SMALLINT NULL, s VARCHAR(10) NULL, d DATE NULL)
DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1');
INSERT INTO t VALUES (1, 0, 0, 'a', '2024-01-01'), (2, 5, 5, 'b', '2024-01-02');"
check "v IN (NULL, NULL)" 0 "$(sqldb "SELECT count(*) FROM t WHERE v IN (NULL, NULL)")"
check "w IN (NULL, NULL)" 0 "$(sqldb "SELECT count(*) FROM t WHERE w IN (NULL, NULL)")"
check "s IN (NULL, NULL, NULL)" 0 "$(sqldb "SELECT count(*) FROM t WHERE s IN (NULL, NULL, NULL)")"
check "d IN (NULL, NULL)" 0 "$(sqldb "SELECT count(*) FROM t WHERE d IN (NULL, NULL)")"
check "v IN (NULL, NULL, 5)" 1 "$(sqldb "SELECT count(*) FROM t WHERE v IN (NULL, NULL, 5)")"
check "v NOT IN (NULL, NULL)" 0 "$(sqldb "SELECT count(*) FROM t WHERE v NOT IN (NULL, NULL)")"
sql "DROP DATABASE $DB FORCE" > /dev/null
if (( FAILURES == 0 )); then echo "ALL CHECKS PASSED"; else echo "$FAILURES CHECK(S) FAILED"; fi
exit "$FAILURES"
