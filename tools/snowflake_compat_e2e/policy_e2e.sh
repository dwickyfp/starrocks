#!/usr/bin/env bash
# End-to-end test of masking and row access policies on a running StarRocks cluster: the objects, attaching them to
# tables, what users with different roles read through them (queries, joins, views, CTEs, aggregates, streams), the
# rows UPDATE, DELETE and MERGE change through a row access policy, the SECURITY privilege, the safeguards, and the
# same through the other FEs. With VERIFY_ONLY=1 it only checks that the policies of an earlier run still apply, as
# after a restart.
# Usage: [VERIFY_ONLY=1] policy_e2e.sh [host] [port] [other fe hosts...]
set -uo pipefail

HOST=${1:-127.0.0.1}
PORT=${2:-9030}
shift 2 2>/dev/null
OTHERS=("$@")
DB=policy_e2e
FAILURES=0

as() { mysql -h "${3:-$HOST}" -P "$PORT" -u"$1" -N -B -D "$DB" -e "$2" 2>&1; }
sqldb() { as root "$1"; }
sql() { mysql -h "$HOST" -P "$PORT" -uroot -N -B -e "$1" 2>&1; }

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

verify() { # what each user reads through the policies, on the given FE
    local fe=$1 tag=$2
    check "$tag: eu reads the EU rows, emails and salaries masked" \
        "$(printf '1\tann\t***@a.com\tEU\tNULL\n3\tcid\t***@c.com\tEU\tNULL')" \
        "$(as pol_eu "SELECT id, name, email, region, salary FROM emp ORDER BY id" "$fe")"
    check "$tag: us reads the US rows" "$(printf '2\n4')" "$(as pol_us "SELECT id FROM emp ORDER BY id" "$fe")"
    check "$tag: hr reads every row, unmasked" \
        "$(printf '1\tann@a.com\t100\n2\tbob@b.com\t200\n3\tcid@c.com\t300\n4\tdee@d.com\t400')" \
        "$(as pol_hr "SELECT id, email, salary FROM emp ORDER BY id" "$fe")"
    check "$tag: a user with no region reads no row" "0" "$(as pol_none "SELECT count(*) FROM emp" "$fe")"
}

if [[ "${VERIFY_ONLY:-0}" == 1 ]]; then
    echo "== verify the policies of an earlier run"
    verify "$HOST" "$HOST"
    for fe in "${OTHERS[@]}"; do verify "$fe" "$fe"; done
    check "the policies are listed" "$(printf 'by_region\nmask_email\nmask_salary')" \
        "$(sqldb "SHOW ROW ACCESS POLICIES" | cut -f2; sqldb "SHOW MASKING POLICIES" | cut -f2 | sort)"
    echo
    [[ $FAILURES -eq 0 ]] && echo "ALL CHECKS PASSED" || echo "$FAILURES CHECK(S) FAILED"
    exit $FAILURES
fi

echo "== setup"
sql "DROP DATABASE IF EXISTS $DB FORCE; CREATE DATABASE $DB"
for u in pol_eu pol_us pol_hr pol_none pol_admin; do sql "DROP USER IF EXISTS $u" > /dev/null; done
for r in r_eu r_us r_hr r_admin; do sql "DROP ROLE IF EXISTS $r" > /dev/null; done
sqldb "
CREATE TABLE emp (id INT NOT NULL, name VARCHAR(20), email VARCHAR(50), region VARCHAR(10), salary INT)
  PRIMARY KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 2
  PROPERTIES ('replication_num' = '1', 'data_retention_time_in_days' = '1');
CREATE TABLE events (id INT, region VARCHAR(10), msg VARCHAR(20)) DUPLICATE KEY (id) DISTRIBUTED BY HASH(id)
  BUCKETS 1 PROPERTIES ('replication_num' = '1');
CREATE TABLE region_map (role_name VARCHAR(20), region VARCHAR(10)) DUPLICATE KEY (role_name)
  DISTRIBUTED BY HASH(role_name) BUCKETS 1 PROPERTIES ('replication_num' = '1');
CREATE TABLE bonus (id INT, amount INT) DUPLICATE KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1
  PROPERTIES ('replication_num' = '1');
INSERT INTO emp VALUES (1, 'ann', 'ann@a.com', 'EU', 100), (2, 'bob', 'bob@b.com', 'US', 200),
  (3, 'cid', 'cid@c.com', 'EU', 300), (4, 'dee', 'dee@d.com', 'US', 400);
INSERT INTO events VALUES (1, 'EU', 'e1'), (2, 'US', 'e2');
INSERT INTO region_map VALUES ('r_eu', 'EU'), ('r_us', 'US'), ('r_hr', 'EU'), ('r_hr', 'US'), ('root', 'EU'),
  ('root', 'US');
INSERT INTO bonus VALUES (1, 10), (2, 20), (3, 30), (4, 40);
CREATE VIEW emp_eu_view AS SELECT id, name, salary FROM emp;
"
for r in r_eu r_us r_hr r_admin; do sql "CREATE ROLE $r"; done
for u in pol_eu pol_us pol_hr pol_none pol_admin; do
    sql "CREATE USER $u; GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE $DB TO USER $u;
         GRANT SELECT ON ALL VIEWS IN DATABASE $DB TO USER $u"
done
sql "GRANT r_eu TO USER pol_eu; GRANT r_us TO USER pol_us; GRANT r_hr TO USER pol_hr; GRANT r_admin TO USER pol_admin;
     SET DEFAULT ROLE r_eu TO pol_eu; SET DEFAULT ROLE r_us TO pol_us; SET DEFAULT ROLE r_hr TO pol_hr;
     SET DEFAULT ROLE r_admin TO pol_admin; GRANT SECURITY ON SYSTEM TO ROLE r_admin"

echo "== the SECURITY privilege"
contains "a user without SECURITY cannot create a policy" "Access denied" \
    "$(as pol_hr "CREATE MASKING POLICY x AS (v INT) RETURNS INT -> v")"
contains "nor list them" "Access denied" "$(as pol_eu "SHOW MASKING POLICIES")"
check "a user with SECURITY creates one" "" \
    "$(as pol_admin "CREATE MASKING POLICY mask_salary AS (v INT) RETURNS INT ->
        CASE WHEN is_role_in_session('r_hr') OR is_role_in_session('root') THEN v ELSE NULL END
        COMMENT 'salaries for hr'")"

echo "== policies"
sqldb "
CREATE MASKING POLICY mask_email AS (v VARCHAR(50)) RETURNS VARCHAR(50) ->
  CASE WHEN is_role_in_session('r_hr') OR is_role_in_session('root') THEN v
       ELSE regexp_replace(v, '^[^@]+', '***') END;
CREATE ROW ACCESS POLICY by_region AS (r VARCHAR(10)) RETURNS BOOLEAN ->
  EXISTS (SELECT 1 FROM $DB.region_map m WHERE m.region = r
          AND find_in_set(m.role_name, replace(current_role(), ' ', '')) > 0);
"
check "SHOW MASKING POLICIES" "$(printf 'mask_email\tMASKING\nmask_salary\tMASKING')" \
    "$(sqldb "SHOW MASKING POLICIES" | cut -f2,4 | sort)"
check "DESC MASKING POLICY" "$(printf 'mask_salary\tsalaries for hr\t(v int(11))\tint(11)')" \
    "$(sqldb "DESC MASKING POLICY mask_salary" | cut -f2,6,7,8)"
contains "a row access policy returns BOOLEAN" "returns BOOLEAN" \
    "$(sqldb "CREATE ROW ACCESS POLICY bad AS (r INT) RETURNS INT -> r")"
contains "a body is checked" "Invalid body" \
    "$(sqldb "CREATE MASKING POLICY bad AS (v INT) RETURNS INT -> v + nope")"

echo "== nothing is hidden before the policies are attached"
check "eu reads every row" "4" "$(as pol_eu "SELECT count(*) FROM emp")"

echo "== attach"
check "attach" "" "$(sqldb "
ALTER TABLE emp MODIFY COLUMN email SET MASKING POLICY mask_email;
ALTER TABLE emp MODIFY COLUMN salary SET MASKING POLICY mask_salary;
ALTER TABLE emp ADD ROW ACCESS POLICY by_region ON (region);
ALTER TABLE events ADD ROW ACCESS POLICY by_region ON (region);")"
contains "a user without SECURITY cannot detach one" "Access denied" \
    "$(as pol_hr "ALTER TABLE emp DROP ROW ACCESS POLICY by_region")"
check "attached_to" "$DB.emp, $DB.events" "$(sqldb "DESC ROW ACCESS POLICY by_region" | cut -f10)"
verify "$HOST" "leader"

echo "== queries"
check "aggregate" "$(printf 'EU\t2')" "$(as pol_eu "SELECT region, count(*) FROM emp GROUP BY region")"
check "a filter on a masked column sees the masked value" "0" \
    "$(as pol_eu "SELECT count(*) FROM emp WHERE email = 'ann@a.com'")"
check "join" "$(printf '1\t10\n3\t30')" \
    "$(as pol_eu "SELECT e.id, b.amount FROM emp e JOIN bonus b ON e.id = b.id ORDER BY e.id")"
check "view of root's" "$(printf '1\tann\tNULL\n3\tcid\tNULL')" \
    "$(as pol_eu "SELECT * FROM emp_eu_view ORDER BY id")"
check "CTE and subquery" "2" \
    "$(as pol_eu "WITH c AS (SELECT id FROM emp) SELECT count(*) FROM (SELECT * FROM c) s")"
check "IN subquery" "1" "$(as pol_eu "SELECT count(*) FROM bonus WHERE id IN (SELECT id FROM emp) AND amount < 30")"
check "the other table" "e1" "$(as pol_eu "SELECT msg FROM events")"
check "root reads every row" "4" "$(sqldb "SELECT count(*) FROM emp")"
STREAM_OUT=$(sqldb "CREATE STREAM emp_changes ON TABLE emp" 2>&1)
sqldb "UPDATE emp SET name = 'bob2' WHERE id = 2; UPDATE emp SET name = 'ann2' WHERE id = 1" > /dev/null
check "a stream: the changes of the rows the policy lets through" "$(printf '1\tINSERT\n1\tDELETE')" \
    "$(as pol_eu "SELECT id, METADATA\$ACTION FROM emp_changes ORDER BY id, METADATA\$ACTION DESC")"
check "the CHANGES clause too" "2" \
    "$(as pol_eu "SELECT count(*) FROM emp CHANGES (INFORMATION => DEFAULT) AT (OFFSET => -3600)")"
sqldb "UPDATE emp SET name = 'bob' WHERE id = 2; UPDATE emp SET name = 'ann' WHERE id = 1; DROP STREAM emp_changes" \
    > /dev/null

echo "== DML through the row access policy"
check "eu updates only EU rows" "" "$(as pol_eu "UPDATE emp SET salary = salary + 1 WHERE id > 0")"
check "the salaries" "$(printf '1\t101\n2\t200\n3\t301\n4\t400')" "$(sqldb "SELECT id, salary FROM emp ORDER BY id")"
check "eu deletes only EU rows" "" "$(as pol_eu "DELETE FROM emp WHERE id IN (1, 2)")"
check "the rows" "$(printf '2\n3\n4')" "$(sqldb "SELECT id FROM emp ORDER BY id")"
check "eu merges into EU rows only" "" "$(as pol_eu "MERGE INTO emp t USING bonus s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET salary = s.amount")"
check "the salaries after MERGE" "$(printf '2\t200\n3\t30\n4\t400')" "$(sqldb "SELECT id, salary FROM emp ORDER BY id")"
sqldb "INSERT INTO emp VALUES (1, 'ann', 'ann@a.com', 'EU', 100); UPDATE emp SET salary = 300 WHERE id = 3" > /dev/null
contains "DELETE from a Duplicate Key table with a row access policy" "row access policy" \
    "$(as pol_eu "DELETE FROM events WHERE id = 1")"

echo "== safeguards"
contains "an attached policy cannot be dropped" "attached to" "$(sqldb "DROP MASKING POLICY mask_email")"
contains "nor replaced" "attached to" \
    "$(sqldb "CREATE OR REPLACE ROW ACCESS POLICY by_region AS (r VARCHAR(10)) RETURNS BOOLEAN -> TRUE")"
contains "a column a policy reads cannot be dropped" "detach the policy" "$(sqldb "ALTER TABLE emp DROP COLUMN region")"
contains "nor renamed" "detach the policy" "$(sqldb "ALTER TABLE emp RENAME COLUMN email TO mail")"
contains "a materialized view cannot read a table with policies" "policies" \
    "$(sqldb "CREATE MATERIALIZED VIEW emp_mv REFRESH MANUAL AS SELECT id, salary FROM emp")"
contains "a second mask needs FORCE" "FORCE" "$(sqldb "ALTER TABLE emp MODIFY COLUMN salary SET MASKING POLICY mask_salary")"
contains "the type of the masked column" "is of type" \
    "$(sqldb "ALTER TABLE emp MODIFY COLUMN name SET MASKING POLICY mask_salary")"

echo "== changing a policy"
sqldb "ALTER MASKING POLICY mask_salary SET BODY -> CASE WHEN is_role_in_session('r_hr') OR is_role_in_session('root')
       THEN v ELSE -1 END" > /dev/null
check "the new body applies" "$(printf '1\t-1\n3\t-1')" "$(as pol_eu "SELECT id, salary FROM emp ORDER BY id")"
sqldb "ALTER MASKING POLICY mask_salary SET BODY -> CASE WHEN is_role_in_session('r_hr') OR is_role_in_session('root')
       THEN v ELSE NULL END" > /dev/null

if (( ${#OTHERS[@]} > 0 )); then
    echo "== other FEs"
    for fe in "${OTHERS[@]}"; do verify "$fe" "$fe"; done
    fe=${OTHERS[0]}
    check "a policy is created through $fe" "" "$(as pol_admin "CREATE ROW ACCESS POLICY tmp_rap AS (r VARCHAR(10))
        RETURNS BOOLEAN -> r = 'EU'" "$fe")"
    check "and attached" "" "$(as root "ALTER TABLE bonus ADD ROW ACCESS POLICY tmp_rap ON (id)" "$fe")"
    check "id is cast to the type of the argument" "0" "$(as pol_eu "SELECT count(*) FROM bonus")"
    check "and dropped" "" "$(as root "ALTER TABLE bonus DROP ALL ROW ACCESS POLICIES; DROP ROW ACCESS POLICY tmp_rap" "$fe")"
    check "the leader sees it gone" "" "$(sqldb "SHOW ROW ACCESS POLICIES LIKE 'tmp%'")"
fi

echo "== detach"
sqldb "ALTER TABLE events DROP ROW ACCESS POLICY by_region" > /dev/null
check "events is read in full again" "2" "$(as pol_eu "SELECT count(*) FROM events")"
check "the policies stay for the verify run" "$(printf 'by_region\nmask_email\nmask_salary')" \
    "$(sqldb "SHOW ROW ACCESS POLICIES" | cut -f2; sqldb "SHOW MASKING POLICIES" | cut -f2 | sort)"

echo
[[ $FAILURES -eq 0 ]] && echo "ALL CHECKS PASSED" || echo "$FAILURES CHECK(S) FAILED"
exit $FAILURES
