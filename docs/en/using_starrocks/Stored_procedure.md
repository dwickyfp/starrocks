---
displayed_sidebar: docs
description: "Create and call Snowflake style stored procedures with a SQL scripting body, and run dollar-quoted bodies from the mysql client."
---

# Stored procedures

A stored procedure is a named SQL scripting block, like Snowflake's stored procedures with a Snowflake Scripting body. It can run DML, read values into variables, branch, loop over cursors, raise and handle exceptions, call other procedures and return a value.

## Create, call and drop

```sql
CREATE [OR REPLACE] PROCEDURE [IF NOT EXISTS] [db.]name(arg type [DEFAULT expr], ...)
RETURNS type [NOT NULL]
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
