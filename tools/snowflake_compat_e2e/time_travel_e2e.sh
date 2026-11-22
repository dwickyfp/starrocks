#!/usr/bin/env bash
# End-to-end test of time travel on native tables on a running shared-nothing StarRocks cluster
# (FE from feature/time-travel). Usage: time_travel_e2e.sh [host] [port]
set -uo pipefail

HOST=${1:-127.0.0.1}
PORT=${2:-9030}
DB=tt_e2e
FAILURES=0

sql() { mysql -h "$HOST" -P "$PORT" -uroot -N -B -e "$1" 2>&1; }
sqldb() { mysql -h "$HOST" -P "$PORT" -uroot -N -B -D "$DB" -e "$1" 2>&1; }
now() { sql "SELECT date_format(now(), '%Y-%m-%d %H:%i:%s')"; }

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

echo "== setup"
sql "ADMIN SET FRONTEND CONFIG ('time_travel_be_config_sync_interval_sec' = '5')" > /dev/null
sql "SET GLOBAL data_retention_time_in_days = 0" > /dev/null
sql "DROP DATABASE IF EXISTS $DB FORCE" > /dev/null
sql "CREATE DATABASE $DB"
sqldb "
CREATE TABLE accounts (id INT, owner VARCHAR(32), balance DECIMAL(12, 2))
  PRIMARY KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 2
  PROPERTIES ('replication_num' = '1', 'data_retention_time_in_days' = '1');
CREATE TABLE events (id INT, kind VARCHAR(16))
  DUPLICATE KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES ('replication_num' = '1');
"

echo "== retention settings"
check "property shown by SHOW CREATE TABLE" "1" \
    "$(sqldb "SHOW CREATE TABLE accounts" | grep -c '"data_retention_time_in_days" = "1"')"
check "events has no time travel yet" "1" \
    "$(sqldb "SELECT * FROM events AT(OFFSET => -1)" | grep -c "not enabled")"
check "the global variable is range checked" "1" \
    "$(sql "SET GLOBAL data_retention_time_in_days = 91" | grep -c "between 0 and 90")"
sql "SET GLOBAL data_retention_time_in_days = 2"
check "SET GLOBAL is persisted and shown" "data_retention_time_in_days	2" \
    "$(sql "SHOW GLOBAL VARIABLES LIKE 'data_retention_time_in_days'")"

echo "== history of a primary key table: insert, update, delete"
sqldb "INSERT INTO accounts VALUES (1, 'ana', 100.00), (2, 'budi', 50.00)"
sleep 2; T1=$(now); sleep 2
sqldb "UPDATE accounts SET balance = balance - 30 WHERE id = 1"
sqldb "UPDATE accounts SET balance = balance + 30 WHERE id = 2"
sleep 2; T2=$(now); sleep 2
sqldb "DELETE FROM accounts WHERE id = 2"
sqldb "INSERT INTO accounts VALUES (3, 'citra', 10.00)"
sleep 2
check "now" "$(printf '1\t70.00\n3\t10.00')" "$(sqldb "SELECT id, balance FROM accounts ORDER BY id")"
check "AT(TIMESTAMP) after the insert" "$(printf '1\t100.00\n2\t50.00')" \
    "$(sqldb "SELECT id, balance FROM accounts AT(TIMESTAMP => '$T1') ORDER BY id")"
check "FOR TIMESTAMP AS OF after the updates" "$(printf '1\t70.00\n2\t80.00')" \
    "$(sqldb "SELECT id, balance FROM accounts FOR TIMESTAMP AS OF '$T2' ORDER BY id")"
check "count(*) in the past is not answered from current metadata" "2" \
    "$(sqldb "SELECT count(*) FROM accounts AT(TIMESTAMP => '$T1')")"
check "an aggregate over the past" "150.00" \
    "$(sqldb "SELECT sum(balance) FROM accounts AT(TIMESTAMP => '$T1')")"
check "before the table had data" "0" \
    "$(sqldb "SELECT count(*) FROM accounts AT(TIMESTAMP => date_sub('$T1', INTERVAL 1 HOUR))")"

echo "== the global variable on a duplicate key table, and OFFSET"
sqldb "INSERT INTO events VALUES (1, 'a'), (2, 'b')"
sleep 3
sqldb "INSERT INTO events VALUES (3, 'c')"
check "OFFSET a moment ago" "2" "$(sqldb "SELECT count(*) FROM events AT(OFFSET => -2)")"
check "current" "3" "$(sqldb "SELECT count(*) FROM events")"

echo "== old versions survive a compaction"
# The leader daemon runs every time_travel_be_config_sync_interval_sec (60s by default).
sweep_raised() { sql "SELECT VALUE FROM information_schema.be_configs \
    WHERE NAME = 'tablet_rowset_stale_sweep_time_sec'" | awk '$1 >= 172800' | sort -u | wc -l | tr -d ' '; }
for _ in $(seq 1 30); do [ "$(sweep_raised)" = "1" ] && break; sleep 5; done
check "the leader raised the BE stale sweep time" "1" "$(sweep_raised)"
sqldb "ALTER TABLE accounts COMPACT" > /dev/null
sleep 20
check "reading the past after compaction" "$(printf '1\t100.00\n2\t50.00')" \
    "$(sqldb "SELECT id, balance FROM accounts AT(TIMESTAMP => '$T1') ORDER BY id")"

echo "== restore from the past"
sqldb "CREATE TABLE accounts_restored LIKE accounts"
sqldb "INSERT INTO accounts_restored SELECT * FROM accounts BEFORE(TIMESTAMP => '$T2')"
check "INSERT ... SELECT from the past" "$(printf '1\t70.00\n2\t80.00')" \
    "$(sqldb "SELECT id, balance FROM accounts_restored ORDER BY id")"

echo "== errors"
check "beyond retention" "1" "$(sqldb "SELECT * FROM accounts AT(OFFSET => -2 * 86400)" | grep -c "beyond its data retention")"
check "in the future" "1" "$(sqldb "SELECT * FROM accounts AT(OFFSET => 600)" | grep -c "in the future")"
check "now and past in one statement" "1" \
    "$(sqldb "SELECT * FROM accounts a JOIN accounts AT(TIMESTAMP => '$T1') b ON a.id = b.id" | grep -c "both now and")"
check "no time travel in a view" "1" \
    "$(sqldb "CREATE VIEW v AS SELECT * FROM accounts AT(TIMESTAMP => '$T1')" | grep -ci "time travel")"

echo "== the past before a TRUNCATE, an INSERT OVERWRITE or a schema change"
sqldb "CREATE TABLE replaced (k INT, v INT) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 1
  PROPERTIES ('replication_num' = '1', 'data_retention_time_in_days' = '1');
INSERT INTO replaced VALUES (1, 10);"
sleep 2; P1=$(now); sleep 2
check "the past reads" "$(printf '1\t10')" "$(sqldb "SELECT * FROM replaced AT(TIMESTAMP => '$P1')")"
sqldb "TRUNCATE TABLE replaced"
sleep 2
check "after TRUNCATE, the past before it is an error, not an empty table" "1" \
    "$(sqldb "SELECT * FROM replaced AT(TIMESTAMP => '$P1')" | grep -c "when its data was replaced")"
sqldb "INSERT INTO replaced VALUES (2, 20)"
sleep 2; P2=$(now); sleep 2
check "the past after the TRUNCATE reads" "$(printf '2\t20')" "$(sqldb "SELECT * FROM replaced AT(TIMESTAMP => '$P2')")"
sqldb "INSERT OVERWRITE replaced VALUES (3, 30)"
sleep 2
check "after INSERT OVERWRITE, the past before it is an error" "1" \
    "$(sqldb "SELECT * FROM replaced AT(TIMESTAMP => '$P2')" | grep -c "when its data was replaced")"
sqldb "INSERT INTO replaced VALUES (4, 40)"
sleep 2; P3=$(now); sleep 2
check "the past after the INSERT OVERWRITE reads" "$(printf '3\t30\n4\t40')" \
    "$(sqldb "SELECT * FROM replaced AT(TIMESTAMP => '$P3') ORDER BY k")"
sqldb "INSERT INTO replaced VALUES (5, 50)"
sleep 2
sqldb "ALTER TABLE replaced MODIFY COLUMN v BIGINT"
for _ in $(seq 1 90); do
    st=$(sqldb "SHOW ALTER TABLE COLUMN WHERE TableName = 'replaced' ORDER BY CreateTime DESC LIMIT 1" | awk -F'\t' '{print $10}')
    [[ "$st" == FINISHED || "$st" == CANCELLED ]] && break; sleep 2
done
check "the schema change finished" "FINISHED" "$st"
sleep 2
check "after a schema change, the past before it is an error" "1" \
    "$(sqldb "SELECT * FROM replaced AT(TIMESTAMP => '$P3')" | grep -c "when its data was replaced")"
check "now still reads" "$(printf '3\t30\n4\t40\n5\t50')" "$(sqldb "SELECT * FROM replaced ORDER BY k")"

echo "== UNDROP"
sqldb "CREATE TABLE gone (k INT) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1');
INSERT INTO gone VALUES (7); DROP TABLE gone;"
check "the dropped table is gone" "1" "$(sqldb "SELECT * FROM gone" | grep -c "Unknown table")"
sqldb "UNDROP TABLE gone"
check "UNDROP TABLE brings it back with its data" "7" "$(sqldb "SELECT * FROM gone")"

echo "== turning it off"
sqldb "ALTER TABLE accounts SET ('data_retention_time_in_days' = '0')"
check "table property 0 turns it off" "1" "$(sqldb "SELECT * FROM accounts AT(OFFSET => -1)" | grep -c "not enabled")"
sql "SET GLOBAL data_retention_time_in_days = 0"
sql "ADMIN SET FRONTEND CONFIG ('time_travel_be_config_sync_interval_sec' = '60')" > /dev/null

echo
if (( FAILURES == 0 )); then
    echo "ALL CHECKS PASSED"
else
    echo "$FAILURES CHECK(S) FAILED"
fi
exit "$FAILURES"
