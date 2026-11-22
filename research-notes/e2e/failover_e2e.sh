#!/usr/bin/env bash
# State that must survive a leader failover, a full restart and a checkpoint (image):
# stored procedures, tasks, time travel history and the retention variable.
#   failover_e2e.sh prepare          creates the state through any FE
#   failover_e2e.sh verify <fe>...   checks it through each given FE (the first must reach the leader)
FES=("${@:2}")
[[ ${#FES[@]} -eq 0 ]] && FES=(172.17.0.2 172.17.0.3 172.17.0.4)
PORT=${PORT:-9030}
DB=failover_e2e
STATE=/opt/e2e/failover.state
FAILURES=0

wrap() { case "$1" in *'$$'*) printf 'DELIMITER //\n%s //' "$1" ;; *) printf '%s' "$1" ;; esac; }
on() { local host=$1; shift; mysql -h "$host" -P "$PORT" -uroot -N -B -D "$DB" -e "$(wrap "$1")" 2>&1; }
check() {
    if [[ "$2" == "$3" ]]; then
        echo "  PASS  $1"
    else
        echo "  FAIL  $1"
        echo "        expected: $(echo "$2" | tr '\n' '|')"
        echo "        actual:   $(echo "$3" | tr '\n' '|')"
        FAILURES=$((FAILURES + 1))
    fi
}

if [[ "$1" == prepare ]]; then
    fe=${FES[0]}
    mysql -h "$fe" -P "$PORT" -uroot -e "DROP TASK IF EXISTS fo_task; DROP DATABASE IF EXISTS $DB FORCE; CREATE DATABASE $DB;
        SET GLOBAL data_retention_time_in_days = 2"
    on "$fe" "CREATE TABLE kv (k INT, v VARCHAR(16)) PRIMARY KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 1
      PROPERTIES ('replication_num' = '1');
    CREATE TABLE log (msg VARCHAR(64)) DUPLICATE KEY (msg) DISTRIBUTED BY HASH(msg) BUCKETS 1
      PROPERTIES ('replication_num' = '1');
    INSERT INTO kv VALUES (1, 'before'), (2, 'before');"
    sleep 2; T1=$(on "$fe" "SELECT now()"); sleep 2
    on "$fe" "UPDATE kv SET v = 'after' WHERE k = 1; DELETE FROM kv WHERE k = 2;"
    on "$fe" "CREATE PROCEDURE put(k INT, v VARCHAR(16)) RETURNS VARCHAR LANGUAGE SQL AS \$\$
BEGIN
  INSERT INTO kv VALUES (:k, :v);
  INSERT INTO log VALUES ('put ' || :k);
  RETURN 'put ' || k;
END;
\$\$"
    on "$fe" "CREATE TASK fo_task AS \$\$
CALL put(10, 'task');
\$\$"
    echo "T1='$T1'" > $STATE
    echo "prepared (T1=$T1)"
    exit 0
fi

source $STATE
n=0
for fe in "${FES[@]}"; do
    n=$((n + 1))
    echo "== through $fe"
    check "the retention variable" "2" \
        "$(mysql -h "$fe" -P "$PORT" -uroot -N -B -e "SHOW GLOBAL VARIABLES LIKE 'data_retention_time_in_days'" | cut -f2)"
    check "now" "$(printf '1\tafter')" "$(on "$fe" "SELECT * FROM kv WHERE k < 10 ORDER BY k")"
    check "the past, from the persisted history" "$(printf '1\tbefore\n2\tbefore')" \
        "$(on "$fe" "SELECT * FROM kv AT(TIMESTAMP => '$T1') ORDER BY k")"
    check "the procedure" "put" "$(on "$fe" "SHOW PROCEDURES" | cut -f1)"
    check "CALL" "put $((100 + n))" "$(on "$fe" "CALL put($((100 + n)), 'call')")"
    check "the task" "1" \
        "$(mysql -h "$fe" -P "$PORT" -uroot -N -B -e "SELECT count(*) FROM information_schema.tasks WHERE task_name = 'fo_task'")"
done
fe=${FES[0]}
on "$fe" "DELETE FROM kv WHERE k = 10" > /dev/null
mysql -h "$fe" -P "$PORT" -uroot -e "EXECUTE TASK fo_task" > /dev/null 2>&1
for _ in $(seq 1 60); do
    [[ "$(on "$fe" "SELECT count(*) FROM kv WHERE k = 10")" == 1 ]] && break; sleep 2
done
check "the task runs on the current leader" "1" "$(on "$fe" "SELECT count(*) FROM kv WHERE k = 10")"

echo
if (( FAILURES == 0 )); then
    echo "ALL CHECKS PASSED"
else
    echo "$FAILURES CHECK(S) FAILED"
fi
exit "$FAILURES"
