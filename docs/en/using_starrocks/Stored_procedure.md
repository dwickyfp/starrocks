---
displayed_sidebar: docs
description: "Create and call Snowflake style stored procedures with a SQL scripting body, and run dollar-quoted bodies from the mysql client."
---

# Stored procedures

A stored procedure is a named SQL scripting block, like Snowflake's stored procedures with a Snowflake Scripting body. It can run DML, read values into variables, branch, loop over cursors, raise and handle exceptions, call other procedures and return a value.

## Create, call and drop

```sql
CREATE [OR REPLACE] PROCEDURE [IF NOT EXISTS] [db.]name(arg type [DEFAULT expr], ...)
RETURNS type [NOT NULL] | RETURNS TABLE (col type, ...)
LANGUAGE SQL
[EXECUTE AS OWNER | CALLER]
[COMMENT 'text']
AS $$ ... $$;            -- or AS '...' with a single-quoted body

CALL name(1, 2, 30);
SHOW PROCEDURES [FROM db] [LIKE 'pattern'];
DESC PROCEDURE name;
DROP PROCEDURE [IF EXISTS] name[(type, ...)];

EXECUTE IMMEDIATE $$ BEGIN RETURN 1; END $$;   -- an anonymous block
```

`EXECUTE AS OWNER` (the default) runs the body with the privileges of the procedure's owner; `EXECUTE AS CALLER` runs it with the caller's.

## Example

```sql
CREATE OR REPLACE PROCEDURE transfer(src INT, dst INT, amount DECIMAL(12, 2))
RETURNS VARCHAR
LANGUAGE SQL
AS $$
DECLARE
  src_balance DECIMAL(12, 2);
  insufficient EXCEPTION (-20001, 'insufficient balance');
BEGIN
  SELECT balance INTO :src_balance FROM accounts WHERE id = :src;
  IF (src_balance < amount) THEN
    RAISE insufficient;
  END IF;
  UPDATE accounts SET balance = balance - :amount WHERE id = :src;
  UPDATE accounts SET balance = balance + :amount WHERE id = :dst;
  RETURN 'moved ' || amount || ', rows ' || SQLROWCOUNT;
EXCEPTION
  WHEN insufficient THEN
    RETURN 'rejected: ' || SQLERRM;
END;
$$;
```

Inside SQL statements of the body, variables and arguments are referenced with a colon (`:src`). The body also supports `FOR r IN cursor DO ... END FOR`, `FOR i IN 1 TO n DO ... END FOR`, `CALL other(...) INTO :var`, `IDENTIFIER(:name)` for a table name in a variable, and explicit transactions with `BEGIN TRANSACTION` and `COMMIT`. A transaction left open when the procedure fails is rolled back.

## Result sets and table results

A `RESULTSET` variable holds the rows of a query. A procedure declared `RETURNS TABLE (...)` returns rows with `RETURN TABLE(...)`, and `CALL` shows them as a result set with the declared column names:

```sql
CREATE OR REPLACE PROCEDURE cheaper_than(limit_price DECIMAL(8, 2))
RETURNS TABLE (id INT, name VARCHAR, price DECIMAL(8, 2))
LANGUAGE SQL
AS $$
DECLARE
  rs RESULTSET DEFAULT (SELECT id, name, price FROM items WHERE price < :limit_price ORDER BY id);
BEGIN
  RETURN TABLE(rs);
END;
$$;

CALL cheaper_than(20);
```

A `RESULTSET` is assigned from `(SELECT ...)`, `(EXECUTE IMMEDIATE ...)`, `(CALL p(...))` of a `RETURNS TABLE` procedure or another `RESULTSET`, with `DECLARE rs RESULTSET [DEFAULT ...]`, `LET rs RESULTSET := ...` or `rs := ...`. Its rows are read with `FOR r IN rs DO ... END FOR` or with a cursor declared `FOR rs`. `RETURN TABLE(...)` takes the same sources. The rows of a table result must have as many columns as the `RETURNS TABLE` clause declares. A `RETURNS TABLE` procedure cannot be read with `CALL ... INTO`.

`EXECUTE IMMEDIATE` inside a body binds `?` placeholders to variables with `USING`:

```sql
LET rs RESULTSET := (EXECUTE IMMEDIATE 'SELECT id, name FROM items WHERE price > ?' USING (min_price));
EXECUTE IMMEDIATE 'INSERT INTO items VALUES (?, ?, 4.00)' USING (id, name);
```

The number of placeholders must match the number of `USING` variables. A `?` in a string literal or a comment is not a placeholder.

## Dollar-quoted bodies and the mysql client

The `mysql` command-line client splits its input on `;` itself, also inside `$$ ... $$`, so it sends a dollar-quoted body cut at the first `;`, which fails with `Unexpected input '$$'`. Switch the client's delimiter for such statements:

```sql
DELIMITER //
CREATE OR REPLACE PROCEDURE p() RETURNS INT LANGUAGE SQL AS $$
BEGIN
  RETURN 1;
END;
$$ //
DELIMITER ;
```

JDBC and other clients that send a statement as is need no change.
