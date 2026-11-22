#!/usr/bin/env bash
# End-to-end test of the Snowflake dialect extensions on a running StarRocks cluster: QUALIFY, the * modifiers,
# ILIKE and LIKE ANY / ALL, the compatibility functions, UNPIVOT, SHOW TABLES HISTORY and query_tag.
# Each result is compared with the same query written without the extension (the oracle).
# Usage: dialect_e2e.sh [host] [port]. Set AUDIT_LOG to the fe.audit.log of that FE to check query_tag.
set -uo pipefail

HOST=${1:-127.0.0.1}
PORT=${2:-9030}
DB=dialect_e2e
FAILURES=0

sqldb() { mysql -h "$HOST" -P "$PORT" -uroot -N -B -D "$DB" -e "$1" 2>&1; }
# the column names of a query
header() { mysql -h "$HOST" -P "$PORT" -uroot -B -D "$DB" -e "$1" 2>&1 | head -1 | tr '\t' ','; }

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

# same <description> <query> <oracle>: both give the same rows, in any order, and at least one row unless
# EMPTY_OK is set
same() {
    local got want
    got=$(sqldb "$2" | sort)
    want=$(sqldb "$3" | sort)
    if [[ -z "${EMPTY_OK:-}" && -z "$want" ]]; then
        check "$1 (oracle has rows)" "rows" "none"
        return
    fi
    check "$1" "$want" "$got"
}

echo "== setup"
mysql -h "$HOST" -P "$PORT" -uroot -e "DROP DATABASE IF EXISTS $DB FORCE; CREATE DATABASE $DB"
sqldb "
CREATE TABLE sales (id INT, region VARCHAR(16), product VARCHAR(16), amount DECIMAL(10, 2), qty INT,
                    discount DOUBLE, Note VARCHAR(32))
  DUPLICATE KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 3 PROPERTIES ('replication_num' = '1');
INSERT INTO sales VALUES
  (1, 'east', 'Apple', 10.00, 1, 0.1, 'Promo A'), (2, 'east', 'banana', 30.00, 3, NULL, 'promo b'),
  (3, 'east', 'Cherry', 30.00, 2, 0.0, NULL),     (4, 'west', 'apple', 5.50, NULL, 0.2, 'Regular'),
  (5, 'west', 'Durian', 99.99, 9, 0.5, 'PROMO C'), (6, 'west', 'egg', NULL, 4, NULL, 'regular'),
  (7, 'north', 'Fig', 7.25, 0, 0.0, '100%'),      (8, NULL, 'grape', 12.00, 2, 0.3, 'x_y'),
  (9, 'north', 'apple', 7.25, 5, NULL, NULL),     (10, 'south', NULL, NULL, NULL, NULL, NULL);
CREATE TABLE empty_sales LIKE sales;
CREATE TABLE wide (id INT, q1 INT, q2 INT, q3 INT, n1 VARCHAR(8), n2 VARCHAR(8))
  DUPLICATE KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES ('replication_num' = '1');
INSERT INTO wide VALUES (1, 10, NULL, 30, 'a', 'b'), (2, NULL, NULL, NULL, NULL, NULL), (3, 7, 8, 9, 'x', NULL);
"

echo "== QUALIFY"
same "top row per region" \
    "SELECT id, region FROM sales QUALIFY row_number() OVER (PARTITION BY region ORDER BY amount DESC, id) = 1" \
    "SELECT id, region FROM (SELECT id, region, row_number() OVER (PARTITION BY region ORDER BY amount DESC, id) rn
       FROM sales) t WHERE rn = 1"
same "an alias, AND and a column that is not selected" \
    "SELECT id, rank() OVER (PARTITION BY region ORDER BY amount DESC) r FROM sales
       QUALIFY r <= 2 AND qty IS NOT NULL" \
    "SELECT id, r FROM (SELECT id, qty, rank() OVER (PARTITION BY region ORDER BY amount DESC) r FROM sales) t
       WHERE r <= 2 AND qty IS NOT NULL"
same "rank keeps ties" \
    "SELECT id FROM sales QUALIFY rank() OVER (PARTITION BY region ORDER BY amount DESC) = 1" \
    "SELECT id FROM (SELECT id, rank() OVER (PARTITION BY region ORDER BY amount DESC) r FROM sales) t WHERE r = 1"
same "OR of two window functions" \
    "SELECT id FROM sales QUALIFY count(*) OVER (PARTITION BY region) = 1
       OR sum(qty) OVER (PARTITION BY region) > 10" \
    "SELECT id FROM (SELECT id, count(*) OVER (PARTITION BY region) c, sum(qty) OVER (PARTITION BY region) s
       FROM sales) t WHERE c = 1 OR s > 10"
same "QUALIFY before DISTINCT" \
    "SELECT DISTINCT region FROM sales QUALIFY row_number() OVER (PARTITION BY region ORDER BY id) <= 2" \
    "SELECT DISTINCT region FROM (SELECT region, row_number() OVER (PARTITION BY region ORDER BY id) rn
       FROM sales) t WHERE rn <= 2"
same "a window over an aggregate" \
    "SELECT region, sum(amount) total FROM sales GROUP BY region
       QUALIFY dense_rank() OVER (ORDER BY sum(amount) DESC) <= 2" \
    "SELECT region, total FROM (SELECT region, sum(amount) total, dense_rank() OVER (ORDER BY sum(amount) DESC) d
       FROM sales GROUP BY region) t WHERE d <= 2"
same "SELECT * with WHERE, ORDER BY and LIMIT" \
    "SELECT * FROM sales WHERE id > 1 QUALIFY row_number() OVER (PARTITION BY region ORDER BY id) = 1
       ORDER BY id LIMIT 3" \
    "SELECT id, region, product, amount, qty, discount, Note FROM (SELECT *, row_number() OVER (PARTITION BY region
       ORDER BY id) rn FROM sales WHERE id > 1) t WHERE rn = 1 ORDER BY id LIMIT 3"
check "QUALIFY on an empty table" "0" \
    "$(sqldb "SELECT count(*) FROM (SELECT id FROM empty_sales QUALIFY row_number() OVER (ORDER BY id) = 1) t")"
sqldb "CREATE VIEW top_sales AS SELECT region, id FROM sales
       QUALIFY row_number() OVER (PARTITION BY region ORDER BY amount DESC, id) = 1" > /dev/null
same "a view with QUALIFY" "SELECT * FROM top_sales" \
    "SELECT region, id FROM (SELECT region, id, row_number() OVER (PARTITION BY region ORDER BY amount DESC, id) rn
       FROM sales) t WHERE rn = 1"
check "the view has the select columns" "region,id" "$(header "SELECT * FROM top_sales LIMIT 1")"

echo "== * ILIKE / EXCLUDE / REPLACE / RENAME"
check "* ILIKE" "region,product,amount,Note" "$(header "SELECT * ILIKE '%o%' EXCLUDE (discount) FROM sales LIMIT 1")"
check "* EXCLUDE without parentheses" "id,region,product,amount,qty,discount" \
    "$(header "SELECT * EXCLUDE note FROM sales LIMIT 1")"
check "* REPLACE keeps the position and the name" "id,region,product,amount,qty,discount,Note" \
    "$(header "SELECT * REPLACE (upper(product) AS product) FROM sales LIMIT 1")"
same "* REPLACE values" "SELECT * REPLACE (upper(product) AS product, amount * 2 AS amount) FROM sales" \
    "SELECT id, region, upper(product), amount * 2, qty, discount, Note FROM sales"
check "* RENAME" "id,area,product,amount,qty,discount,remark" \
    "$(header "SELECT * RENAME (region AS area, note AS remark) FROM sales LIMIT 1")"
check "all modifiers and other items" "s.id,amt,one" \
    "$(header "SELECT s.* ILIKE '%' EXCLUDE (region, product, qty, discount, note) REPLACE (amount + 1 AS amount)
       RENAME (amount AS amt, id AS \`s.id\`), 1 AS one FROM sales s LIMIT 1")"
same "ORDER BY a replaced column" \
    "SELECT * EXCLUDE (note) REPLACE (-id AS id) FROM sales ORDER BY id LIMIT 3" \
    "SELECT -id, region, product, amount, qty, discount FROM sales ORDER BY -id LIMIT 3"
check "REPLACE of an excluded column fails" "1" \
    "$(sqldb "SELECT * EXCLUDE (id) REPLACE (1 AS id) FROM sales" | grep -c "not in the expansion")"

echo "== ILIKE and LIKE ANY / ALL"
same "ILIKE" "SELECT id FROM sales WHERE note ILIKE 'promo%'" \
    "SELECT id FROM sales WHERE lower(note) LIKE 'promo%'"
same "NOT ILIKE keeps NULL out" "SELECT id FROM sales WHERE note NOT ILIKE 'promo%'" \
    "SELECT id FROM sales WHERE NOT (lower(note) LIKE 'promo%')"
same "LIKE ANY" "SELECT id FROM sales WHERE product LIKE ANY ('a%', '%e', 'F__')" \
    "SELECT id FROM sales WHERE product LIKE 'a%' OR product LIKE '%e' OR product LIKE 'F__'"
same "ILIKE ALL" "SELECT id FROM sales WHERE product ILIKE ALL ('%a%', '%p%')" \
    "SELECT id FROM sales WHERE lower(product) LIKE '%a%' AND lower(product) LIKE '%p%'"
same "NOT LIKE ANY" "SELECT id FROM sales WHERE note NOT LIKE ANY ('Promo%', 'regular')" \
    "SELECT id FROM sales WHERE NOT (note LIKE 'Promo%') OR NOT (note LIKE 'regular')"
same "escaped wildcard" "SELECT id FROM sales WHERE note LIKE ANY ('100\\\\%', 'x\\\\_y')" \
    "SELECT id FROM sales WHERE id IN (7, 8)"
check "NULL LIKE ANY is NULL" "NULL" "$(sqldb "SELECT NULL LIKE ANY ('a', 'b')")"
check "'a' LIKE ANY ('b', NULL) is NULL" "NULL" "$(sqldb "SELECT 'a' LIKE ANY ('b', NULL)")"
check "'a' NOT LIKE ANY ('a', 'b') is true" "1" "$(sqldb "SELECT 'a' NOT LIKE ANY ('a', 'b')")"

echo "== compatibility functions"
check "IFF" "big	small	small" "$(sqldb "SELECT iff(10 > 5, 'big', 'small'), iff(1 > 5, 'big', 'small'),
    iff(NULL, 'big', 'small')")"
check "NVL2" "set	unset" "$(sqldb "SELECT nvl2(1, 'set', 'unset'), nvl2(NULL, 'set', 'unset')")"
check "ZEROIFNULL / NULLIFZERO" "0	5	NULL	5" \
    "$(sqldb "SELECT zeroifnull(NULL), zeroifnull(5), nullifzero(0), nullifzero(5)")"
check "DIV0" "0.0	2.5	NULL" "$(sqldb "SELECT div0(5, 0), div0(5, 2), div0(5, NULL)")"
check "DIV0NULL" "0.0	0.0	2.5" "$(sqldb "SELECT div0null(5, 0), div0null(5, NULL), div0null(5, 2)")"
check "EQUAL_NULL" "1	0	1	0" \
    "$(sqldb "SELECT equal_null(NULL, NULL), equal_null(1, NULL), equal_null('a', 'a'), equal_null('a', 'b')")"
check "TRY_CAST" "NULL	42	NULL" "$(sqldb "SELECT try_cast('abc' AS INT), try_cast('42' AS INT),
    try_cast('2024-13-40' AS DATE)")"
check "TRY_TO_NUMBER / DECIMAL / DOUBLE" "NULL	12.35	1.5	NULL" \
    "$(sqldb "SELECT try_to_number('x1'), try_to_decimal('12.345', 5, 2), try_to_double('1.5'),
    try_to_double('one')")"
check "TRY_TO_DATE" "2024-12-31	2024-02-29	NULL" \
    "$(sqldb "SELECT try_to_date('31/12/2024', 'DD/MM/YYYY'), try_to_date('2024-02-29'), try_to_date('nope')")"
check "TRY_TO_TIMESTAMP" "2024-12-31 23:59:58	NULL" \
    "$(sqldb "SELECT try_to_timestamp('2024.12.31 23:59:58', 'YYYY.MM.DD HH24:MI:SS'), try_to_timestamp('x')")"
same "functions on table columns" \
    "SELECT id, iff(qty > 2, 'many', 'few'), zeroifnull(qty), div0null(amount, qty), equal_null(region, 'east')
       FROM sales" \
    "SELECT id, if(qty > 2, 'many', 'few'), ifnull(qty, 0), if(qty = 0 OR qty IS NULL, 0, amount / qty),
       region <=> 'east' FROM sales"

echo "== UNPIVOT"
same "UNPIVOT leaves NULL values out" \
    "SELECT * FROM wide UNPIVOT (v FOR quarter IN (q1, q2, q3))" \
    "SELECT id, n1, n2, 'q1', q1 FROM wide WHERE q1 IS NOT NULL UNION ALL
     SELECT id, n1, n2, 'q2', q2 FROM wide WHERE q2 IS NOT NULL UNION ALL
     SELECT id, n1, n2, 'q3', q3 FROM wide WHERE q3 IS NOT NULL"
check "UNPIVOT columns" "id,n1,n2,quarter,v" "$(header "SELECT * FROM wide UNPIVOT (v FOR quarter IN (q1, q2, q3))")"
check "INCLUDE NULLS keeps every row" "9" \
    "$(sqldb "SELECT count(*) FROM wide UNPIVOT INCLUDE NULLS (v FOR quarter IN (q1, q2, q3))")"
same "labels, an alias and an aggregate on top" \
    "SELECT u.quarter, sum(u.v) FROM (SELECT id, q1, q2 FROM wide) w
       UNPIVOT (v FOR quarter IN (q1 AS 'first', q2 AS 'second')) u GROUP BY u.quarter" \
    "SELECT 'first', sum(q1) FROM wide UNION ALL SELECT 'second', sum(q2) FROM wide"
same "several value columns" \
    "SELECT id, k, num, name FROM wide UNPIVOT ((num, name) FOR k IN ((q1, n1) AS one, (q2, n2) AS two))" \
    "SELECT id, 'one', q1, n1 FROM wide WHERE q1 IS NOT NULL OR n1 IS NOT NULL UNION ALL
     SELECT id, 'two', q2, n2 FROM wide WHERE q2 IS NOT NULL OR n2 IS NOT NULL"
check "UNPIVOT of an empty table" "0" \
    "$(sqldb "SELECT count(*) FROM empty_sales UNPIVOT (v FOR k IN (qty, id))")"
check "UNPIVOT of an unknown column fails" "1" \
    "$(sqldb "SELECT * FROM wide UNPIVOT (v FOR k IN (q1, nope))" | grep -ci "nope")"

echo "== SHOW TABLES HISTORY"
sqldb "CREATE TABLE gone (k INT) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 1
       PROPERTIES ('replication_num' = '1'); DROP TABLE gone;" > /dev/null
check "a dropped table is listed as recoverable" "gone	BASE TABLE	true" \
    "$(sqldb "SHOW TABLES HISTORY LIKE 'gone'" | awk -F'\t' '{print $1"\t"$2"\t"$4}')"
check "a current table has no drop time" "sales	BASE TABLE	NULL" \
    "$(sqldb "SHOW TABLES HISTORY LIKE 'sales'" | awk -F'\t' '{print $1"\t"$2"\t"$3}')"
sqldb "UNDROP TABLE gone" > /dev/null
check "after UNDROP the table is current" "NULL" "$(sqldb "SHOW TABLES HISTORY LIKE 'gone'" | cut -f3)"
check "SHOW TABLES is unchanged" "1" "$(sqldb "SHOW TABLES LIKE 'gone'" | awk -F'\t' '{print NF}')"

echo "== query_tag"
tag="dialect-e2e-$RANDOM"
check "query_tag is a session variable" "$tag" \
    "$(sqldb "SET query_tag = '$tag'; SELECT @@query_tag")"
if [[ -n "${AUDIT_LOG:-}" ]]; then
    check "the audit log exists" "yes" "$([[ -f "$AUDIT_LOG" ]] && echo yes || echo "no $AUDIT_LOG")"
    sqldb "SET query_tag = '$tag'; SELECT count(*) FROM sales" > /dev/null
    sqldb "SELECT /*+ SET_VAR(query_tag = '$tag-hint') */ 1" > /dev/null
    sleep 2
    check "the audit log has the tag" "1" "$(grep -c "QueryTag=$tag|" "$AUDIT_LOG" | awk '{print ($1 >= 1)}')"
    check "the tag of a SET_VAR hint is logged" "1" \
        "$(grep -c "QueryTag=$tag-hint" "$AUDIT_LOG" | awk '{print ($1 >= 1)}')"
    check "a statement without a tag has none" "0" \
        "$(sqldb "SELECT 'no tag here'" > /dev/null; sleep 2; grep "no tag here" "$AUDIT_LOG" | grep -c "QueryTag=")"
fi

echo
if [[ $FAILURES -eq 0 ]]; then
    echo "ALL CHECKS PASSED"
else
    echo "$FAILURES CHECK(S) FAILED"
fi
exit $FAILURES
