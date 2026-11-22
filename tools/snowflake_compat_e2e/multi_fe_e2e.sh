#!/usr/bin/env bash
# Multi-FE checks against a running cluster: stored procedures, EXECUTE IMMEDIATE, tasks and
# time travel driven through a follower FE, compared with what the leader sees.
#   LEADER / FOLLOWER: hosts of the leader and of a follower FE, both on MySQL port PORT.
LEADER=${LEADER:-172.17.0.2}
FOLLOWER=${FOLLOWER:-172.17.0.3}
PORT=${PORT:-9030}
DB=multi_fe_e2e
FAILURES=0

# The mysql CLI splits its input on ';' even inside $$ ... $$, so a statement with a
# dollar-quoted body is sent under a different delimiter.
wrap() { case "$1" in *'$$'*) printf 'DELIMITER //\n%s //' "$1" ;; *) printf '%s' "$1" ;; esac; }
on() { local host=$1; shift; mysql -h "$host" -P "$PORT" -uroot -N -B -D "$DB" -e "$(wrap "$1")" 2>&1; }
leader() { on "$LEADER" "$1"; }
follower() { on "$FOLLOWER" "$1"; }
sql() { mysql -h "$LEADER" -P "$PORT" -uroot -N -B -e "$1" 2>&1; }
now() { sql "SELECT now()"; }
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
# wait_follower <sql> <expected>: the follower replays the leader's journal asynchronously
wait_follower() {
    for _ in $(seq 1 30); do [[ "$(follower "$1")" == "$2" ]] && return 0; sleep 1; done
    return 1
}

echo "== setup"
sql "DROP TASK IF EXISTS mfe_task" > /dev/null
sql "DROP DATABASE IF EXISTS $DB FORCE" > /dev/null
sql "CREATE DATABASE $DB"
leader "CREATE TABLE accounts (id INT, balance DECIMAL(12, 2))
  PRIMARY KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1
  PROPERTIES ('replication_num' = '1', 'data_retention_time_in_days' = '1');
CREATE TABLE notes (msg VARCHAR(64)) DUPLICATE KEY (msg) DISTRIBUTED BY HASH(msg) BUCKETS 1
  PROPERTIES ('replication_num' = '1');
INSERT INTO accounts VALUES (1, 100.00), (2, 50.00);"
check "the follower sees the database" "0" "$(wait_follower "SELECT 1" "1"; echo $?)"

echo "== stored procedures through the follower"
out=$(follower "CREATE OR REPLACE PROCEDURE move(src INT, dst INT, amount DECIMAL(12, 2))
RETURNS VARCHAR LANGUAGE SQL AS \$\$
BEGIN
  UPDATE accounts SET balance = balance - :amount WHERE id = :src;
  UPDATE accounts SET balance = balance + :amount WHERE id = :dst;
  CREATE TABLE IF NOT EXISTS moves (src INT, dst INT, amount DECIMAL(12, 2))
    DUPLICATE KEY (src) DISTRIBUTED BY HASH(src) BUCKETS 1 PROPERTIES ('replication_num' = '1');
  INSERT INTO moves VALUES (:src, :dst, :amount);
  RETURN 'moved ' || amount;
END;
\$\$")
check "CREATE PROCEDURE on the follower" "" "$out"
check "the leader has the procedure" "move" "$(leader "SHOW PROCEDURES" | cut -f1)"
check "CALL with DML and DDL on the follower" "moved 30.00" "$(follower "CALL move(1, 2, 30)")"
check "the leader sees the updates" "$(printf '1\t70.00\n2\t80.00')" "$(leader "SELECT * FROM accounts ORDER BY id")"
check "the leader sees the table created by the procedure" "$(printf '1\t2\t30.00')" "$(leader "SELECT * FROM moves")"
check "the follower reads its own writes" "$(printf '1\t70.00\n2\t80.00')" "$(follower "SELECT * FROM accounts ORDER BY id")"
check "EXECUTE IMMEDIATE with DML on the follower" "1" \
    "$(follower "EXECUTE IMMEDIATE \$\$ BEGIN INSERT INTO moves VALUES (9, 9, 1); RETURN SQLROWCOUNT; END \$\$")"
check "a nested CALL on the follower" "moved 5.00" \
    "$(follower "EXECUTE IMMEDIATE \$\$ DECLARE r VARCHAR; BEGIN CALL move(2, 1, 5) INTO :r; RETURN r; END \$\$")"
check "an explicit transaction in a procedure on the follower" "2" \
    "$(follower "EXECUTE IMMEDIATE \$\$ BEGIN BEGIN TRANSACTION; INSERT INTO moves VALUES (7, 7, 1); INSERT INTO notes VALUES ('in a transaction'); COMMIT; RETURN 2; END \$\$")"
check "the transaction's rows landed" "2" \
    "$(leader "SELECT (SELECT count(*) FROM moves WHERE src = 7) + (SELECT count(*) FROM notes)")"
check "DROP PROCEDURE on the follower" "0" \
    "$(follower "CREATE PROCEDURE tmp_p() RETURNS INT LANGUAGE SQL AS 'BEGIN RETURN 1; END'" >/dev/null;
       follower "DROP PROCEDURE tmp_p()" >/dev/null; leader "SHOW PROCEDURES LIKE 'tmp%'" | wc -l | tr -d ' ')"

echo "== time travel: the follower replays the version history"
# now() has second precision: keep T1 clear of the commits around it
sleep 2; T1=$(now); sleep 2
follower "UPDATE accounts SET balance = 0 WHERE id = 1"
sleep 2
check "leader: now" "$(printf '1\t0.00\n2\t75.00')" "$(leader "SELECT * FROM accounts ORDER BY id")"
check "follower: the past (from the replayed history)" "$(printf '1\t75.00\n2\t75.00')" \
    "$(follower "SELECT * FROM accounts AT(TIMESTAMP => '$T1') ORDER BY id")"
check "leader: the past" "$(printf '1\t75.00\n2\t75.00')" \
    "$(leader "SELECT * FROM accounts AT(TIMESTAMP => '$T1') ORDER BY id")"
check "SET GLOBAL on the follower reaches the leader" "3" \
    "$(follower "SET GLOBAL data_retention_time_in_days = 3" >/dev/null;
       sql "SHOW GLOBAL VARIABLES LIKE 'data_retention_time_in_days'" | cut -f2)"
sql "SET GLOBAL data_retention_time_in_days = 0" > /dev/null

echo "== tasks through the follower"
follower "CREATE TASK mfe_task AS \$\$
INSERT INTO moves VALUES (100, 100, 1);
CALL move(1, 2, 0);
\$\$" > /dev/null
check "the leader has the task" "1" "$(sql "SELECT count(*) FROM information_schema.tasks WHERE task_name = 'mfe_task'")"
follower "EXECUTE TASK mfe_task" > /dev/null
for _ in $(seq 1 60); do
    st=$(sql "SELECT state FROM information_schema.task_runs WHERE task_name = 'mfe_task' ORDER BY create_time DESC LIMIT 1")
    [[ "$st" == "SUCCESS" || "$st" == "FAILED" ]] && break; sleep 2
done
check "the task run, started on the follower, succeeded" "SUCCESS" "$st"
check "the task's statements ran" "1" "$(leader "SELECT count(*) FROM moves WHERE src = 100")"
sql "DROP TASK IF EXISTS mfe_task" > /dev/null

echo
if (( FAILURES == 0 )); then
    echo "ALL CHECKS PASSED"
else
    echo "$FAILURES CHECK(S) FAILED"
fi
exit "$FAILURES"
