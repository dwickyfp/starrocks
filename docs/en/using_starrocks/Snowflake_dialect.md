---
displayed_sidebar: docs
description: "Snowflake query syntax in StarRocks: QUALIFY, star modifiers, ILIKE and LIKE ANY, UNPIVOT, compatibility functions, SHOW TABLES HISTORY and query_tag."
---

# Snowflake query syntax

StarRocks accepts the following Snowflake query syntax. Each form is rewritten into built-in StarRocks expressions while the statement is parsed, so it works with every table type and needs no extra setting.

## QUALIFY

`QUALIFY` filters the rows of a query on the results of its window functions, like `HAVING` filters on aggregates.

```sql
SELECT region, product, amount
FROM sales
QUALIFY row_number() OVER (PARTITION BY region ORDER BY amount DESC) = 1;

SELECT id, rank() OVER (PARTITION BY region ORDER BY amount DESC) AS r
FROM sales
QUALIFY r <= 3 AND qty IS NOT NULL;
```

The condition is any boolean expression. It can use window functions, the names of the select items, and columns of the `FROM` clause that are not selected. `QUALIFY` is evaluated after `WHERE`, `GROUP BY`, `HAVING` and the window functions, and before `DISTINCT`, `ORDER BY` and `LIMIT`. A window function that is also a select item is computed once. A filter such as `row_number() ... <= n` keeps the partition top-N optimization.

## Star modifiers

```sql
SELECT [table.]* [ILIKE 'pattern'] [EXCLUDE (column, ...) | EXCLUDE column]
                 [REPLACE (expression AS column, ...)] [RENAME (column AS new_name, ...) | RENAME column AS new_name]
```

- `ILIKE` keeps the columns whose name matches the pattern ignoring case (`%` and `_` are wildcards).
- `EXCLUDE` (or `EXCEPT`) leaves columns out.
- `REPLACE` outputs an expression in place of a column, under the column's name and at its position.
- `RENAME` outputs a column under another name.

`REPLACE` and `RENAME` can name only columns that the star expands to. A replaced column can be used by its name in `ORDER BY`.

## ILIKE, LIKE ANY and LIKE ALL

| Syntax | Meaning |
| --- | --- |
| `x [NOT] ILIKE p` | `[NOT] lower(x) LIKE lower(p)` |
| `x LIKE ANY (p1, p2, ...)` | `x LIKE p1 OR x LIKE p2 OR ...` |
| `x LIKE ALL (p1, p2, ...)` | `x LIKE p1 AND x LIKE p2 AND ...` |
| `x NOT LIKE ANY (p1, ...)` | `x NOT LIKE p1 OR ...` |
| `x NOT LIKE ALL (p1, ...)` | `x NOT LIKE p1 AND ...` |

`ILIKE ANY` and `ILIKE ALL` ignore case. The patterns can be any expressions.

## UNPIVOT

`UNPIVOT` turns columns into rows. It follows the `FROM` clause.

```sql
SELECT * FROM quarterly_sales
UNPIVOT [INCLUDE NULLS | EXCLUDE NULLS] (amount FOR quarter IN (q1, q2 AS 'second', q3, q4)) [[AS] alias];

-- several value columns
SELECT * FROM t
UNPIVOT ((amount, units) FOR quarter IN ((q1_amount, q1_units) AS q1, (q2_amount, q2_units) AS q2));
```

The result has the columns that are not unpivoted, then the name column, then the value columns. The name column holds the label of each `IN` value, which is the column name unless `AS` gives another one. By default (`EXCLUDE NULLS`) a row whose values are all `NULL` is left out. The values of one value column must have a common type. Each source row is read once.

## Compatibility functions

| Function | Result |
| --- | --- |
| `IFF(cond, a, b)` | `a` when `cond` is true, else `b` |
| `NVL2(e, a, b)` | `a` when `e` is not `NULL`, else `b` |
| `ZEROIFNULL(x)` | `0` when `x` is `NULL`, else `x` |
| `NULLIFZERO(x)` | `NULL` when `x` is `0`, else `x` |
| `DIV0(a, b)` | `0` when `b` is `0`, else `a / b` |
| `DIV0NULL(a, b)` | `0` when `b` is `0` or `NULL`, else `a / b` |
| `EQUAL_NULL(a, b)` | `a <=> b`: true when both are `NULL` |
| `TRY_CAST(x AS type)` | `CAST(x AS type)` |
| `TRY_TO_NUMBER(x [, precision [, scale]])`, `TRY_TO_DECIMAL`, `TRY_TO_NUMERIC` | `x` as `DECIMAL(precision, scale)`, `DECIMAL(38, 0)` by default |
| `TRY_TO_DOUBLE(x)`, `TRY_TO_BOOLEAN(x)` | `x` as `DOUBLE` or `BOOLEAN` |
| `TRY_TO_DATE(x [, format])`, `TRY_TO_TIMESTAMP(x [, format])` | `x` as `DATE` or `DATETIME` |

A value that cannot be converted gives `NULL`. The format of `TRY_TO_DATE` and `TRY_TO_TIMESTAMP` uses the Snowflake elements `YYYY`, `YY`, `MM`, `MON`, `MMMM`, `DD`, `DY`, `HH24`, `HH12`, `HH`, `MI`, `SS`, `FF`, `AM` and `PM`. Text in double quotes is kept as it is.

## SHOW TABLES HISTORY

```sql
SHOW TABLES HISTORY [FROM db] [LIKE 'pattern'];
```

Lists the tables of a database and the dropped tables that are still in the recycle bin. `Dropped_on` is the time a table was dropped, `NULL` for a current table, and `Recoverable` tells whether `UNDROP TABLE` (or `RECOVER TABLE`) can bring it back. A table dropped with `FORCE` is not listed.

## query_tag

The session variable `query_tag` labels the statements of a session, like Snowflake's `QUERY_TAG`. A statement run with a non-empty tag has a `QueryTag` field in the audit log.

```sql
SET query_tag = 'nightly-etl';
SELECT /*+ SET_VAR(query_tag = 'adhoc') */ count(*) FROM sales;
```

## Limitations

- `TRY_CAST` and the `TRY_TO_*` functions give `NULL` for a value that cannot be converted only while the SQL mode `ALLOW_THROW_EXCEPTION` is off, which is the default.
- `TRY_TO_NUMBER` does not take a format model.
- The compatibility functions are resolved before user-defined functions: a user-defined function with one of their names must be called with its database name, such as `db.iff(...)`.
- In `QUALIFY`, an aggregate outside a window function is not supported. In a select list with a star, a column in `QUALIFY` must be one of the columns the star expands to.
- `UNPIVOT` names columns, not expressions, and is supported after the `FROM` clause of a query, not after a single relation in a join.
