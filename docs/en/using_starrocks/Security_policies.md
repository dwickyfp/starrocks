---
displayed_sidebar: docs
description: "Hide rows and mask column values of native tables with Snowflake style masking policies and row access policies."
---

# Masking and row access policies

A masking policy masks the values of a column, and a row access policy hides rows of a table, as in Snowflake. A policy is a SQL expression of its arguments, and the columns of the table it is attached to are passed to it. It applies to every query of the table: a join, a view, a CTE, a subquery, a stream, a `CHANGES` clause, and the statements of tasks and stored procedures. Its body decides what each user sees, usually with `current_user()`, `current_role()` or `is_role_in_session()`, and it can read other tables, a mapping of roles to regions, say.

Policies apply to native tables in the default catalog. They work together with the policies of an access controller such as Apache Ranger: a masking policy of a column replaces the one of the access controller, and a row must pass both row access policies.

## The SECURITY privilege

Creating, changing, dropping, attaching, detaching, showing and describing policies needs the `SECURITY` privilege on the system. It does not depend on the privileges on the tables: whoever owns a table cannot detach a policy from it.

```sql
GRANT SECURITY ON SYSTEM TO ROLE policy_admin;
```

## Creating a policy

```sql
CREATE [OR REPLACE] MASKING POLICY [IF NOT EXISTS] [db.]name
    AS (arg type [, arg type ...]) RETURNS type -> body [COMMENT 'comment']

CREATE [OR REPLACE] ROW ACCESS POLICY [IF NOT EXISTS] [db.]name
    AS (arg type [, arg type ...]) RETURNS BOOLEAN -> body [COMMENT 'comment']
```

- A masking policy returns the type of its first argument, the column it masks; string types are interchangeable. The other arguments are other columns of the row, for a mask that depends on them.
- A row access policy returns `BOOLEAN`. A row is shown when the body is `TRUE`, and hidden when it is `FALSE` or `NULL`.
- The body is checked when the policy is created. The columns are cast to the types of the arguments.
- Masking policies and row access policies have a namespace each, in the database of the policy.
- `OR REPLACE` cannot replace a policy that is attached to a table.

```sql
CREATE MASKING POLICY mask_email AS (v VARCHAR(50)) RETURNS VARCHAR(50) ->
    CASE WHEN is_role_in_session('hr') THEN v ELSE regexp_replace(v, '^[^@]+', '***') END;

CREATE MASKING POLICY mask_salary AS (v INT) RETURNS INT ->
    CASE WHEN is_role_in_session('hr') THEN v ELSE NULL END;

CREATE ROW ACCESS POLICY by_region AS (r VARCHAR(10)) RETURNS BOOLEAN ->
    EXISTS (SELECT 1 FROM security.region_map m
            WHERE m.region = r AND find_in_set(m.role_name, replace(current_role(), ' ', '')) > 0);
```

`is_role_in_session()` takes a constant role name, so a policy that looks roles up in a table matches them against `current_role()`, the active roles separated by commas.

## Attaching a policy

```sql
ALTER TABLE [db.]table MODIFY COLUMN col SET MASKING POLICY [db.]policy [USING (col, other_col ...)] [FORCE];
ALTER TABLE [db.]table MODIFY COLUMN col UNSET MASKING POLICY;

ALTER TABLE [db.]table ADD ROW ACCESS POLICY [db.]policy ON (col [, col ...]);
ALTER TABLE [db.]table DROP ROW ACCESS POLICY [db.]policy;
ALTER TABLE [db.]table DROP ALL ROW ACCESS POLICIES;
```

- A column has one masking policy, and `FORCE` replaces it. `USING` passes the masked column and then other columns of the row to the arguments of the policy. The masked column must be of the type the policy masks.
- A table has one row access policy. Its columns are passed to the arguments of the policy in order.
- An unqualified policy name is a policy of the database of the table.

```sql
ALTER TABLE emp MODIFY COLUMN email SET MASKING POLICY mask_email;
ALTER TABLE emp MODIFY COLUMN salary SET MASKING POLICY mask_salary;
ALTER TABLE emp ADD ROW ACCESS POLICY by_region ON (region);
```

## What the policies change

- A query reads the masked values: a filter or a join on a masked column compares the masked value.
- `UPDATE`, `DELETE` and `MERGE` change only the rows the row access policy of their table shows. A `DELETE` from a table with a row access policy is supported on Primary Key tables only. Masking policies do not apply to the values that `UPDATE` and `MERGE` write. `INSERT` is not restricted by a row access policy.
- A stream or a `CHANGES` clause returns the changes of the rows the policies show, as they show them.
- A materialized view cannot read a table with policies, and policies cannot be attached to a table that a materialized view reads: the materialized view would keep the data without them.
- A column that a policy attached to the table reads cannot be dropped or renamed.
- A table whose policy no longer exists, as after the table was restored with `RECOVER`, cannot be read until the policy is detached.

## Changing, showing and dropping policies

```sql
ALTER MASKING POLICY | ROW ACCESS POLICY [IF EXISTS] [db.]name SET BODY -> body;
ALTER MASKING POLICY | ROW ACCESS POLICY [IF EXISTS] [db.]name SET COMMENT = 'comment';
ALTER MASKING POLICY | ROW ACCESS POLICY [IF EXISTS] [db.]name RENAME TO new_name;

SHOW MASKING POLICIES | ROW ACCESS POLICIES [FROM db] [LIKE 'pattern'];
DESC MASKING POLICY | ROW ACCESS POLICY [db.]name;

DROP MASKING POLICY | ROW ACCESS POLICY [IF EXISTS] [db.]name;
```

- A new body is checked against every table the policy is attached to.
- A policy attached to a table cannot be dropped: detach it first.
- `SHOW` and `DESC` return `created_on`, `name`, `database_name`, `kind`, `owner`, `comment`, `signature`, `return_type`, `body` and `attached_to`, the tables and columns the policy is attached to.
