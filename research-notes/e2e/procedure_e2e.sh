#!/usr/bin/env bash
# End-to-end test of Snowflake style stored procedures on a running StarRocks cluster
# (FE from feature/stored-procedure). Usage: procedure_e2e.sh [host] [port]
set -uo pipefail

HOST=${1:-127.0.0.1}
PORT=${2:-9030}
DB=proc_e2e
FAILURES=0

# The mysql CLI splits its input on ';' even inside $$ ... $$, so a statement with a
# dollar-quoted body is sent under a different delimiter.
wrap() { case "$1" in *'$$'*) printf 'DELIMITER //\n%s //' "$1" ;; *) printf '%s' "$1" ;; esac; }
sql() { mysql -h "$HOST" -P "$PORT" -uroot -N -B -e "$(wrap "$1")" 2>&1; }
sqldb() { mysql -h "$HOST" -P "$PORT" -uroot -N -B -D "$DB" -e "$(wrap "$1")" 2>&1; }
sqlas() { mysql -h "$HOST" -P "$PORT" -u"$1" -N -B -D "$DB" -e "$(wrap "$2")" 2>&1; }

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
sql "DROP DATABASE IF EXISTS $DB FORCE" > /dev/null
sql "DROP USER IF EXISTS proc_reader" > /dev/null
sql "CREATE DATABASE $DB"
sqldb "
CREATE TABLE accounts (id INT, owner VARCHAR(32), balance DECIMAL(12, 2))
  PRIMARY KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1');
CREATE TABLE transfers (id BIGINT, src INT, dst INT, amount DECIMAL(12, 2), note VARCHAR(64))
  DUPLICATE KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1');
CREATE TABLE audit (msg VARCHAR(256))
  DUPLICATE KEY (msg) DISTRIBUTED BY HASH(msg) BUCKETS 1 PROPERTIES ('replication_num' = '1');
INSERT INTO accounts VALUES (1, 'ana', 100.00), (2, 'budi', 50.00), (3, 'citra', 0.00);
"

echo "== a procedure with DML, SELECT INTO, IF and an exception handler"
sqldb "CREATE OR REPLACE PROCEDURE transfer(src INT, dst INT, amount DECIMAL(12, 2))
RETURNS VARCHAR
LANGUAGE SQL
AS \$\$
DECLARE
  src_balance DECIMAL(12, 2);
  insufficient EXCEPTION (-20001, 'insufficient balance');
  next_id BIGINT;
BEGIN
  SELECT balance INTO :src_balance FROM accounts WHERE id = :src;
  IF (src_balance IS NULL) THEN
    RETURN 'no account ' || src;
  END IF;
  IF (src_balance < amount) THEN
    RAISE insufficient;
  END IF;
  UPDATE accounts SET balance = balance - :amount WHERE id = :src;
  UPDATE accounts SET balance = balance + :amount WHERE id = :dst;
  SELECT coalesce(max(id), 0) + 1 INTO :next_id FROM transfers;
  INSERT INTO transfers VALUES (:next_id, :src, :dst, :amount, 'ok');
  RETURN 'moved ' || amount || ', rows ' || SQLROWCOUNT;
EXCEPTION
  WHEN insufficient THEN
    INSERT INTO audit VALUES ('rejected ' || :src || '->' || :dst || ': ' || :SQLERRM);
    RETURN 'rejected: ' || SQLERRM;
END;
\$\$"
check "transfer 30 from ana to budi" "moved 30.00, rows 1" "$(sqldb "CALL transfer(1, 2, 30)")"
check "transfer 500 is rejected by the handler" "rejected: insufficient balance" "$(sqldb "CALL transfer(2, 3, 500)")"
check "unknown account" "no account 9" "$(sqldb "CALL transfer(9, 1, 1)")"
check "balances after the calls" "$(printf '1\t70.00\n2\t80.00\n3\t0.00')" \
    "$(sqldb "SELECT id, balance FROM accounts ORDER BY id")"
check "the transfer was logged" "$(printf '1\t1\t2\t30.00\tok')" "$(sqldb "SELECT * FROM transfers")"
check "the rejection was audited" "rejected 2->3: insufficient balance" "$(sqldb "SELECT msg FROM audit")"

echo "== a cursor loop, a nested CALL and IDENTIFIER()"
sqldb "CREATE OR REPLACE PROCEDURE pay_interest(rate DECIMAL(5, 2), tbl VARCHAR DEFAULT 'accounts')
RETURNS INT
LANGUAGE SQL
AS \$\$
DECLARE
  c CURSOR FOR SELECT id, balance FROM accounts WHERE balance > 0 ORDER BY id;
  paid INT DEFAULT 0;
BEGIN
  FOR r IN c DO
    UPDATE IDENTIFIER(:tbl) SET balance = balance + :r.balance * :rate / 100 WHERE id = :r.id;
    paid := paid + 1;
  END FOR;
  INSERT INTO audit VALUES ('interest paid to ' || :paid || ' accounts');
  RETURN paid;
END;
\$\$"
sqldb "CREATE OR REPLACE PROCEDURE month_end() RETURNS VARCHAR LANGUAGE SQL AS \$\$
DECLARE
  n INT;
BEGIN
  CALL pay_interest(10) INTO :n;
  RETURN 'paid ' || n;
END;
\$\$"
check "nested call returns the inner value" "paid 2" "$(sqldb "CALL month_end()")"
check "interest applied by the cursor loop" "$(printf '1\t77.00\n2\t88.00\n3\t0.00')" \
    "$(sqldb "SELECT id, balance FROM accounts ORDER BY id")"

echo "== an explicit transaction rolled back when the procedure fails"
sqldb "CREATE OR REPLACE PROCEDURE half_done() RETURNS INT LANGUAGE SQL AS \$\$
BEGIN
  BEGIN TRANSACTION;
  INSERT INTO audit VALUES ('inside the transaction');
  INSERT INTO table_that_does_not_exist VALUES (1);
  COMMIT;
  RETURN 1;
END;
\$\$"
out=$(sqldb "CALL half_done()")
check "the failure is the INSERT, after BEGIN TRANSACTION ran" "1" "$(echo "$out" | grep -c "table_that_does_not_exist")"
check "nothing of the failed transaction stays" "0" \
    "$(sqldb "SELECT count(*) FROM audit WHERE msg = 'inside the transaction'")"

sqldb "CREATE OR REPLACE PROCEDURE all_done() RETURNS INT LANGUAGE SQL AS \$\$
BEGIN
  BEGIN TRANSACTION NAME t1;
  INSERT INTO audit VALUES ('committed 1');
  INSERT INTO transfers VALUES (99, 0, 0, 0, 'committed 2');
  COMMIT;
  RETURN 2;
END;
\$\$"
check "a committed transaction returns" "2" "$(sqldb "CALL all_done()")"
check "both of its rows landed" "2" \
    "$(sqldb "SELECT (SELECT count(*) FROM audit WHERE msg = 'committed 1') + (SELECT count(*) FROM transfers WHERE id = 99)")"

echo "== owner's rights versus caller's rights"
sql "CREATE USER proc_reader"
sql "GRANT SELECT ON ALL TABLES IN DATABASE $DB TO USER proc_reader"
sql "GRANT OPERATE ON SYSTEM TO USER proc_reader"
sqldb "CREATE OR REPLACE PROCEDURE log_as_owner(m VARCHAR) RETURNS VARCHAR LANGUAGE SQL EXECUTE AS OWNER
AS 'BEGIN INSERT INTO audit VALUES (:m); RETURN current_user(); END'"
sqldb "CREATE OR REPLACE PROCEDURE log_as_caller(m VARCHAR) RETURNS VARCHAR LANGUAGE SQL EXECUTE AS CALLER
AS 'BEGIN INSERT INTO audit VALUES (:m); RETURN current_user(); END'"
check "the owner's procedure writes for a read-only user" "1" \
    "$(sqlas proc_reader "CALL log_as_owner('by owner')" | grep -c "root")"
check "the caller's procedure is refused the INSERT" "1" \
    "$(sqlas proc_reader "CALL log_as_caller('by caller')" | grep -ci "denied")"
check "only the owner's write landed" "$(printf 'by owner')" \
    "$(sqldb "SELECT msg FROM audit WHERE msg LIKE 'by %'")"

echo "== SHOW, DESC, persistence and DROP"
check "SHOW PROCEDURES" "$(printf 'all_done\nhalf_done\nlog_as_caller\nlog_as_owner\nmonth_end\npay_interest\ntransfer')" \
    "$(sqldb "SHOW PROCEDURES" | cut -f1)"
check "DESC PROCEDURE shows the body" "1" "$(sqldb "DESC PROCEDURE transfer" | grep -c "RAISE insufficient")"
check "EXECUTE IMMEDIATE runs an anonymous block" "6" \
    "$(sqldb "EXECUTE IMMEDIATE \$\$ DECLARE s INT DEFAULT 0; BEGIN FOR i IN 1 TO 3 DO s := s + i; END FOR; RETURN s; END \$\$")"
sqldb "DROP PROCEDURE half_done()"
check "DROP PROCEDURE" "0" "$(sqldb "SHOW PROCEDURES LIKE 'half%'" | wc -l | tr -d ' ')"

echo "== RESULTSET, RETURN TABLE and EXECUTE IMMEDIATE ... USING"
sqldb "CREATE TABLE items (id INT, name VARCHAR(16), price DECIMAL(8, 2)) DUPLICATE KEY (id)
  DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1');
INSERT INTO items VALUES (1, 'pen', 2.50), (2, 'book', 12.00), (3, 'bag', 30.00);"
sqldb "CREATE PROCEDURE cheaper_than(limit_price DECIMAL(8, 2))
RETURNS TABLE (id INT, name VARCHAR, price DECIMAL(8, 2)) LANGUAGE SQL AS \$\$
DECLARE
  rs RESULTSET DEFAULT (SELECT id, name, price FROM items WHERE price < :limit_price ORDER BY id);
BEGIN
  RETURN TABLE(rs);
END;
\$\$"
check "CALL of a RETURNS TABLE procedure returns its rows" "$(printf '1\tpen\t2.50\n2\tbook\t12.00')" \
    "$(sqldb "CALL cheaper_than(20)")"
check "the result has the declared column names" "id	name	price" \
    "$(mysql -h "$HOST" -P "$PORT" -uroot -B -D "$DB" -e "CALL cheaper_than(5)" 2>&1 | head -1)"
check "FOR over a RESULTSET from CALL" "pen,book," \
    "$(sqldb "EXECUTE IMMEDIATE \$\$ DECLARE s VARCHAR DEFAULT ''; BEGIN
       LET rs RESULTSET := (CALL cheaper_than(20));
       FOR r IN rs DO s := s || r.name || ','; END FOR; RETURN s; END \$\$")"
check "a cursor over a RESULTSET" "44.50" \
    "$(sqldb "EXECUTE IMMEDIATE \$\$ DECLARE total DECIMAL(10, 2) DEFAULT 0; rs RESULTSET;
       BEGIN rs := (SELECT price FROM items);
       LET c CURSOR FOR rs;
       FOR r IN c DO total := total + r.price; END FOR; RETURN total; END \$\$")"
check "EXECUTE IMMEDIATE ... USING binds the placeholders" "$(printf '3\tbag')" \
    "$(sqldb "EXECUTE IMMEDIATE \$\$ DECLARE lo DECIMAL(8, 2) DEFAULT 20; nm VARCHAR DEFAULT 'it''s'; BEGIN
       LET rs RESULTSET := (EXECUTE IMMEDIATE 'SELECT id, name FROM items WHERE price > ? AND name <> ? ORDER BY id'
         USING (lo, nm));
       RETURN TABLE(rs); END \$\$")"
check "EXECUTE IMMEDIATE ... USING with DML" "1" \
    "$(sqldb "EXECUTE IMMEDIATE \$\$ DECLARE i INT DEFAULT 4; n VARCHAR DEFAULT 'cup'; BEGIN
       EXECUTE IMMEDIATE 'INSERT INTO items VALUES (?, ?, 4.00)' USING (i, n);
       RETURN SQLROWCOUNT; END \$\$")"
check "the USING row landed" "cup" "$(sqldb "SELECT name FROM items WHERE id = 4")"
check "CALL ... INTO of a table procedure is refused" "1" \
    "$(sqldb "EXECUTE IMMEDIATE \$\$ DECLARE x INT; BEGIN CALL cheaper_than(1) INTO :x; END \$\$" | grep -c RESULTSET)"
sqldb "DROP PROCEDURE cheaper_than(DECIMAL(8, 2))"

echo
if (( FAILURES == 0 )); then
    echo "ALL CHECKS PASSED"
else
    echo "$FAILURES CHECK(S) FAILED"
fi
exit "$FAILURES"
