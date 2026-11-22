// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.procedure;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.RedirectStatus;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Snowflake style stored procedures on the FE: CREATE / DROP / SHOW / DESC PROCEDURE, CALL and EXECUTE IMMEDIATE
 * with scripts whose expressions fold on the FE (statements on tables need a BE, see the e2e test).
 */
public class StoredProcedureTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("sp_other");
        starRocksAssert.withDatabase("sp").useDatabase("sp");
    }

    @AfterEach
    public void dropProcedures() {
        for (String dbName : List.of("sp", "sp_other")) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
            for (StoredProcedure p : StoredProcedureMgr.list(db)) {
                StoredProcedureMgr.drop(db, p.getName());
            }
        }
    }

    private static ShowResultSet run(String sql) throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        if (stmt instanceof ShowStmt) {
            return ShowExecutor.execute((ShowStmt) stmt, ctx);
        }
        return DDLStmtExecutor.execute(stmt, ctx);
    }

    private static String call(String sql) throws Exception {
        ShowResultSet result = run(sql);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.getResultRows().size());
        return result.getResultRows().get(0).get(0);
    }

    private static String error(String sql) {
        Exception e = Assertions.assertThrows(Exception.class, () -> run(sql));
        return e.getMessage();
    }

    private static Database db(String name) {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(name);
    }

    @Test
    public void testCreateShowDescDrop() throws Exception {
        run("CREATE PROCEDURE add_one(x INT, step INT DEFAULT 1)\n" +
                "RETURNS INT\n" +
                "LANGUAGE SQL\n" +
                "COMMENT 'adds'\n" +
                "AS $$\n" +
                "BEGIN\n" +
                "  RETURN x + step;\n" +
                "END;\n" +
                "$$");
        StoredProcedure p = StoredProcedureMgr.get(db("sp"), "ADD_ONE");
        Assertions.assertNotNull(p, "names are case-insensitive");
        Assertions.assertEquals(2, p.getParameters().size());
        Assertions.assertEquals("1", p.getParameters().get(1).getDefaultSql());
        Assertions.assertTrue(p.isExecuteAsOwner(), "EXECUTE AS OWNER is the default");
        Assertions.assertEquals("adds", p.getComment());

        ShowResultSet show = run("SHOW PROCEDURES");
        Assertions.assertEquals(1, show.getResultRows().size());
        List<String> row = show.getResultRows().get(0);
        Assertions.assertEquals("add_one", row.get(0));
        Assertions.assertEquals("sp", row.get(1));
        Assertions.assertTrue(row.get(2).startsWith("add_one(x INT, step INT DEFAULT 1) RETURN INT"), row.get(2));
        Assertions.assertEquals("OWNER", row.get(5));
        Assertions.assertEquals(0, run("SHOW PROCEDURES LIKE 'nope%'").getResultRows().size());
        Assertions.assertEquals(0, run("SHOW PROCEDURES FROM sp_other").getResultRows().size());

        ShowResultSet desc = run("DESC PROCEDURE add_one(INT, INT)");
        Assertions.assertTrue(desc.getResultRows().stream()
                .anyMatch(r -> r.get(0).equals("body") && r.get(1).contains("RETURN x + step")));

        Assertions.assertTrue(error("CREATE PROCEDURE add_one() RETURNS INT LANGUAGE SQL AS $$ BEGIN RETURN 1; END $$")
                .contains("already exists"));
        run("CREATE PROCEDURE IF NOT EXISTS add_one() RETURNS INT LANGUAGE SQL AS $$ BEGIN RETURN 1; END $$");
        Assertions.assertEquals(2, StoredProcedureMgr.get(db("sp"), "add_one").getParameters().size());

        Assertions.assertTrue(error("DROP PROCEDURE add_one(INT)").contains("does not exist"));
        run("DROP PROCEDURE IF EXISTS add_one(INT)");
        run("DROP PROCEDURE add_one(INT, INT)");
        Assertions.assertNull(StoredProcedureMgr.get(db("sp"), "add_one"));
        run("DROP PROCEDURE IF EXISTS add_one");
    }

    @Test
    public void testCallArgumentsAndDefaults() throws Exception {
        run("CREATE OR REPLACE PROCEDURE sp.add_one(x INT, step INT DEFAULT 1) RETURNS INT LANGUAGE SQL\n" +
                "AS 'BEGIN RETURN x + step; END'");
        Assertions.assertEquals("42", call("CALL add_one(41)"));
        Assertions.assertEquals("50", call("CALL add_one(40, 10)"));
        Assertions.assertEquals("12", call("CALL sp.add_one(step => 2, x => 10)"));
        Assertions.assertEquals("8", call("CALL add_one('7')"), "arguments are cast to their types");
        Assertions.assertTrue(error("CALL add_one()").contains("missing argument x"));
        Assertions.assertTrue(error("CALL add_one(1, 2, 3)").contains("takes 2 arguments"));
        Assertions.assertTrue(error("CALL add_one(y => 1)").contains("has no argument y"));

        // the column is named after the procedure
        ShowResultSet result = run("CALL add_one(1)");
        Assertions.assertEquals("add_one", result.getMetaData().getColumn(0).getName());
    }

    @Test
    public void testControlFlow() throws Exception {
        run("CREATE PROCEDURE flow(n INT)\n" +
                "RETURNS VARCHAR\n" +
                "LANGUAGE SQL\n" +
                "AS $$\n" +
                "DECLARE\n" +
                "  total INT DEFAULT 0;\n" +
                "  i INT DEFAULT 0;\n" +
                "  label VARCHAR;\n" +
                "BEGIN\n" +
                "  FOR k IN 1 TO n DO\n" +
                "    IF (k % 2 = 0) THEN\n" +
                "      CONTINUE;\n" +
                "    ELSEIF (k > 7) THEN\n" +
                "      BREAK;\n" +
                "    END IF;\n" +
                "    total := total + k;          -- 1 + 3 + 5 + 7\n" +
                "  END FOR;\n" +
                "  WHILE (i < 3) DO\n" +
                "    i := i + 1;\n" +
                "  END WHILE;\n" +
                "  REPEAT\n" +
                "    i := i + 10;\n" +
                "  UNTIL (i > 30)\n" +
                "  END REPEAT;\n" +
                "  CASE (total)\n" +
                "    WHEN 16 THEN label := 'sixteen';\n" +
                "    ELSE label := 'other';\n" +
                "  END CASE;\n" +
                "  LET s VARCHAR := label || ':' || total || ':' || i;\n" +
                "  LOOP\n" +
                "    s := s || '!';\n" +
                "    IF (substr(s, 16, 1) <> '') THEN BREAK; END IF;\n" +
                "  END LOOP;\n" +
                "  RETURN s;\n" +
                "END;\n" +
                "$$");
        Assertions.assertEquals("sixteen:16:33!!!", call("CALL flow(10)"));
        Assertions.assertEquals("other:1:33!!!!!!", call("CALL flow(1)"));
    }

    @Test
    public void testReverseForAndLabels() throws Exception {
        run("CREATE PROCEDURE labels() RETURNS VARCHAR LANGUAGE SQL AS $$\n" +
                "DECLARE\n" +
                "  s VARCHAR DEFAULT '';\n" +
                "BEGIN\n" +
                "  FOR i IN REVERSE 1 TO 3 DO\n" +
                "    FOR j IN 1 TO 3 DO\n" +
                "      IF (j = 2) THEN CONTINUE outer_loop; END IF;\n" +
                "      s := s || i || j || ' ';\n" +
                "    END FOR;\n" +
                "  END FOR outer_loop;\n" +
                "  RETURN regexp_replace(s, ' $', '');\n" +
                "END\n" +
                "$$");
        Assertions.assertEquals("31 21 11", call("CALL labels()"));
    }

    @Test
    public void testExceptions() throws Exception {
        run("CREATE PROCEDURE errors(mode INT) RETURNS VARCHAR LANGUAGE SQL AS $$\n" +
                "DECLARE\n" +
                "  my_error EXCEPTION (-20002, 'custom failure');\n" +
                "  result VARCHAR DEFAULT 'none';\n" +
                "BEGIN\n" +
                "  BEGIN\n" +
                "    IF (mode = 1) THEN\n" +
                "      RAISE my_error;\n" +
                "    ELSEIF (mode = 2) THEN\n" +
                "      SELECT 1 FROM table_that_does_not_exist;\n" +
                "    ELSEIF (mode = 3) THEN\n" +
                "      result := undeclared_function_xyz(1);\n" +
                "    END IF;\n" +
                "    result := 'ok';\n" +
                "  EXCEPTION\n" +
                "    WHEN my_error THEN\n" +
                "      result := 'caught ' || SQLCODE || ' ' || SQLERRM;\n" +
                "    WHEN STATEMENT_ERROR OR EXPRESSION_ERROR THEN\n" +
                "      result := 'sql error ' || SQLCODE;\n" +
                "  END;\n" +
                "  IF (mode = 4) THEN\n" +
                "    RAISE my_error;\n" +
                "  END IF;\n" +
                "  RETURN result;\n" +
                "END;\n" +
                "$$");
        Assertions.assertEquals("ok", call("CALL errors(0)"));
        Assertions.assertEquals("caught -20002 custom failure", call("CALL errors(1)"));
        Assertions.assertTrue(call("CALL errors(2)").startsWith("sql error"));
        Assertions.assertTrue(call("CALL errors(3)").startsWith("sql error"));
        String message = error("CALL errors(4)");
        Assertions.assertTrue(message.contains("Uncaught exception of type 'my_error'"), message);
        Assertions.assertTrue(message.contains("line 22"), message);
        Assertions.assertTrue(message.contains("custom failure"), message);
    }

    @Test
    public void testReRaiseAndOther() throws Exception {
        run("CREATE PROCEDURE reraise() RETURNS VARCHAR LANGUAGE SQL AS $$\n" +
                "DECLARE\n" +
                "  e1 EXCEPTION;\n" +
                "BEGIN\n" +
                "  BEGIN\n" +
                "    BEGIN\n" +
                "      RAISE e1;\n" +
                "    EXCEPTION WHEN OTHER THEN\n" +
                "      RAISE;\n" +
                "    END;\n" +
                "  EXCEPTION WHEN e1 THEN\n" +
                "    RETURN 'outer got ' || SQLSTATE;\n" +
                "  END;\n" +
                "END;\n" +
                "$$");
        Assertions.assertEquals("outer got P0001", call("CALL reraise()"));
    }

    @Test
    public void testNestedCallsAndDepth() throws Exception {
        run("CREATE PROCEDURE sq(x INT) RETURNS INT LANGUAGE SQL AS 'BEGIN RETURN x * x; END'");
        run("CREATE PROCEDURE sum_sq(n INT) RETURNS INT LANGUAGE SQL AS $$\n" +
                "DECLARE\n" +
                "  total INT DEFAULT 0;\n" +
                "  v INT;\n" +
                "BEGIN\n" +
                "  FOR i IN 1 TO n DO\n" +
                "    CALL sq(:i) INTO :v;\n" +
                "    total := total + v;\n" +
                "  END FOR;\n" +
                "  RETURN total;\n" +
                "END;\n" +
                "$$");
        Assertions.assertEquals("14", call("CALL sum_sq(3)"));

        run("CREATE PROCEDURE forever(n INT) RETURNS INT LANGUAGE SQL AS 'BEGIN CALL forever(:n); END'");
        Assertions.assertTrue(error("CALL forever(1)").contains("nest deeper than"));
    }

    @Test
    public void testReturnTypeAndNotNull() throws Exception {
        run("CREATE PROCEDURE ret_dec() RETURNS DECIMAL(10, 2) LANGUAGE SQL AS 'BEGIN RETURN 1.5; END'");
        Assertions.assertEquals("1.50", call("CALL ret_dec()"));
        run("CREATE PROCEDURE ret_null() RETURNS INT LANGUAGE SQL AS 'BEGIN NULL; END'");
        Assertions.assertNull(call("CALL ret_null()"));
        run("CREATE PROCEDURE ret_not_null() RETURNS INT NOT NULL LANGUAGE SQL AS 'BEGIN RETURN NULL; END'");
        Assertions.assertTrue(error("CALL ret_not_null()").contains("declared NOT NULL"));
    }

    @Test
    public void testExecuteImmediate() throws Exception {
        ShowResultSet result = run("EXECUTE IMMEDIATE $$\n" +
                "DECLARE\n" +
                "  x INT DEFAULT 6;\n" +
                "BEGIN\n" +
                "  LET q VARCHAR := 'BEGIN RETURN 1; END';\n" +
                "  EXECUTE IMMEDIATE :q;\n" +
                "  RETURN x * 7;\n" +
                "END;\n" +
                "$$");
        Assertions.assertEquals("anonymous block", result.getMetaData().getColumn(0).getName());
        Assertions.assertEquals("42", result.getResultRows().get(0).get(0));
    }

    @Test
    public void testInvalidBodiesAreRejectedAtCreate() {
        Assertions.assertTrue(error("CREATE PROCEDURE bad() RETURNS INT LANGUAGE SQL AS $$\n" +
                "BEGIN\n" +
                "  IF (1 = 1) THEN\n" +
                "    RETURN 1;\n" +
                "END;\n" +
                "$$").contains("Invalid procedure body"));
        Assertions.assertTrue(error("CREATE PROCEDURE bad() RETURNS INT LANGUAGE JAVASCRIPT AS 'return 1'")
                .contains("only SQL"));
        Assertions.assertTrue(error("CREATE PROCEDURE bad(a INT, a INT) RETURNS INT LANGUAGE SQL AS 'BEGIN END'")
                .contains("Duplicate argument"));
        Assertions.assertTrue(error("CREATE PROCEDURE bad(a INT DEFAULT 1, b INT) RETURNS INT LANGUAGE SQL "
                + "AS 'BEGIN RETURN 1; END'").contains("needs a DEFAULT"));
        Assertions.assertTrue(error("CREATE OR REPLACE PROCEDURE IF NOT EXISTS bad() RETURNS INT LANGUAGE SQL "
                + "AS 'BEGIN RETURN 1; END'").contains("cannot be used together"));
    }

    @Test
    public void testUndeclaredVariable() throws Exception {
        run("CREATE PROCEDURE undeclared() RETURNS INT LANGUAGE SQL AS 'BEGIN nope := 1; RETURN 1; END'");
        String message = error("CALL undeclared()");
        Assertions.assertTrue(message.contains("Undeclared variable 'nope'"), message);
    }

    @Test
    public void testPersistence() throws Exception {
        run("CREATE PROCEDURE kept(a VARCHAR DEFAULT 'x') RETURNS VARCHAR LANGUAGE SQL EXECUTE AS CALLER\n" +
                "AS 'BEGIN RETURN a; END'");
        Database db = db("sp");
        StoredProcedure p = StoredProcedureMgr.get(db, "kept");

        // the database image keeps its procedures
        Database copy = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(db), Database.class);
        StoredProcedure restored = copy.getStoredProcedures().get("kept");
        Assertions.assertNotNull(restored);
        Assertions.assertEquals(p.getBody(), restored.getBody());
        Assertions.assertEquals("CALLER", restored.getExecuteAs());
        Assertions.assertEquals(p.getOwner(), restored.getOwner());

        // replaying the edit log adds and removes it
        StoredProcedureMgr.drop(db, "kept");
        Assertions.assertNull(StoredProcedureMgr.get(db, "kept"));
        StoredProcedureMgr.replayCreate(GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(p), StoredProcedure.class));
        Assertions.assertEquals("x", call("CALL kept()"));
        StoredProcedureMgr.replayDrop(new DropProcedureLog(db.getId(), "KEPT"));
        Assertions.assertNull(StoredProcedureMgr.get(db, "kept"));

        // an old image without procedures still loads
        Database old = GsonUtils.GSON.fromJson("{\"i\": 7, \"n\": \"old\"}", Database.class);
        Assertions.assertTrue(old.getStoredProcedures().isEmpty());
    }

    @Test
    public void testOwnerRightsNeedOwnerOrOperate() throws Exception {
        run("CREATE PROCEDURE owner_only() RETURNS INT LANGUAGE SQL AS 'BEGIN RETURN 1; END'");
        run("CREATE PROCEDURE callers() RETURNS INT LANGUAGE SQL EXECUTE AS CALLER AS 'BEGIN RETURN 2; END'");
        run("CREATE USER IF NOT EXISTS sp_user");
        run("GRANT SELECT ON ALL TABLES IN DATABASE sp TO sp_user");
        ConnectContext root = starRocksAssert.getCtx();
        ConnectContext user = UtFrameUtils.createDefaultCtx();
        user.setCurrentUserIdentity(new UserIdentity("sp_user", "%"));
        user.setCurrentRoleIds(GlobalStateMgr.getCurrentState().getAuthorizationMgr()
                .getRoleIdsByUser(user.getCurrentUserIdentity()));
        user.setQualifiedUser("sp_user");
        user.setDatabase("sp");
        try {
            starRocksAssert = new StarRocksAssert(user);
            user.setThreadLocalInfo();
            Assertions.assertEquals("2", call("CALL callers()"));
            Assertions.assertTrue(error("CALL owner_only()").contains("owner's rights"));
            Assertions.assertTrue(error("DROP PROCEDURE callers").contains("Access denied"));
            List<String> names = new ArrayList<>();
            run("SHOW PROCEDURES").getResultRows().forEach(r -> names.add(r.get(0)));
            Assertions.assertEquals(List.of("callers", "owner_only"), names);
        } finally {
            starRocksAssert = new StarRocksAssert(root);
            root.setThreadLocalInfo();
        }
    }

    @Test
    public void testStoredProcedureRunsOnTheLeader() throws Exception {
        run("CREATE PROCEDURE on_leader() RETURNS INT LANGUAGE SQL AS $$ BEGIN RETURN 1; END $$");
        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setThreadLocalInfo();
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(
                SqlParser.parseSingleStatement("CALL on_leader()", ctx.getSessionVariable().getSqlMode())));
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(
                SqlParser.parseSingleStatement("CALL sp.on_leader()", ctx.getSessionVariable().getSqlMode())));
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, RedirectStatus.getRedirectStatus(
                SqlParser.parseSingleStatement("EXECUTE IMMEDIATE $$ BEGIN RETURN 1; END $$",
                        ctx.getSessionVariable().getSqlMode())));
        // a name that is not a stored procedure, such as a connector procedure, stays on this FE
        Assertions.assertEquals(RedirectStatus.NO_FORWARD, RedirectStatus.getRedirectStatus(
                SqlParser.parseSingleStatement("CALL not_a_procedure()", ctx.getSessionVariable().getSqlMode())));
    }
}
