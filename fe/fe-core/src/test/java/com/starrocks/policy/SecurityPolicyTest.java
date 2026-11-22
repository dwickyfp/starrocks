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

package com.starrocks.policy;

import com.starrocks.authorization.SecurityPolicyRewriteRule;
import com.starrocks.catalog.Database;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.MergeIntoStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.policy.SecurityPolicyStmt;
import com.starrocks.sql.ast.policy.ShowSecurityPoliciesStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Masking and row access policies: the objects, attaching them to tables, and the queries and DML statements that
 * read the tables through them.
 */
public class SecurityPolicyTest {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext ctx;
    private static Database db;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("pol").useDatabase("pol");
        starRocksAssert.withTable("CREATE TABLE emp (id INT NOT NULL, name VARCHAR(20), email VARCHAR(50), "
                + "region VARCHAR(10), salary INT) PRIMARY KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1')");
        starRocksAssert.withTable("CREATE TABLE logs (id INT, region VARCHAR(10), msg VARCHAR(20)) DUPLICATE KEY (id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        starRocksAssert.withTable("CREATE TABLE region_map (role_name VARCHAR(20), region VARCHAR(10)) "
                + "DUPLICATE KEY (role_name) DISTRIBUTED BY HASH(role_name) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1')");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("pol");
    }

    @AfterEach
    public void dropPolicies() {
        for (TablePolicies policies : List.copyOf(db.getTablePolicies().values())) {
            SecurityPolicyMgr.setTablePolicies(new TablePolicies(policies.getDbId(), policies.getTableId(), null,
                    Map.of()));
        }
        for (SecurityPolicy policy : List.copyOf(db.getSecurityPolicies().values())) {
            SecurityPolicyMgr.drop(policy);
        }
    }

    private static StatementBase parse(String sql) {
        return SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
    }

    private static void run(String sql) throws Exception {
        ctx.setThreadLocalInfo();
        StatementBase stmt = parse(sql);
        Analyzer.analyze(stmt, ctx);
        SecurityPolicyExecutor.execute(assertInstanceOf(SecurityPolicyStmt.class, stmt), ctx);
    }

    private static String error(String sql) {
        return assertThrows(Exception.class, () -> run(sql)).getMessage();
    }

    private static List<List<String>> show(String sql) {
        ctx.setThreadLocalInfo();
        ShowResultSet result = SecurityPolicyExecutor.show(assertInstanceOf(ShowSecurityPoliciesStmt.class,
                parse(sql)), ctx);
        return result.getResultRows();
    }

    /** The plan of a statement as a connection runs it: with the policies of the tables it reads. */
    private static String plan(String sql) throws Exception {
        ctx.setThreadLocalInfo();
        StatementBase stmt = parse(sql);
        SecurityPolicyRewriteRule.markRelationsForRewrite(stmt);
        return StatementPlanner.plan(stmt, ctx).getExplainString(TExplainLevel.NORMAL);
    }

    /** The query a DML statement runs to find the rows it writes. */
    private static String dml(String sql) {
        ctx.setThreadLocalInfo();
        StatementBase stmt = parse(sql);
        SecurityPolicyRewriteRule.markRelationsForRewrite(stmt);
        Analyzer.analyze(stmt, ctx);
        QueryStatement query = stmt instanceof UpdateStmt u ? u.getQueryStatement()
                : stmt instanceof DeleteStmt d ? d.getQueryStatement() : ((MergeIntoStmt) stmt).getQueryStatement();
        return AstToSQLBuilder.toSQL(query);
    }

    private static String planError(String sql) {
        return assertThrows(Exception.class, () -> plan(sql)).getMessage();
    }

    @Test
    public void testPolicyObjects() throws Exception {
        run("CREATE MASKING POLICY email_mask AS (val VARCHAR(50)) RETURNS VARCHAR(50) -> "
                + "CASE WHEN current_user() LIKE '%root%' THEN val ELSE regexp_replace(val, '.+@', '***@') END "
                + "COMMENT 'emails'");
        run("CREATE ROW ACCESS POLICY by_region AS (r VARCHAR(10)) RETURNS BOOLEAN -> r = 'EU'");
        List<List<String>> rows = show("SHOW MASKING POLICIES");
        assertEquals(1, rows.size());
        assertEquals("email_mask", rows.get(0).get(1));
        assertEquals("pol", rows.get(0).get(2));
        assertEquals("MASKING", rows.get(0).get(3));
        assertEquals("emails", rows.get(0).get(5));
        assertEquals("(val varchar(50))", rows.get(0).get(6));
        assertEquals("", rows.get(0).get(9));
        assertEquals(1, show("SHOW ROW ACCESS POLICIES IN pol LIKE 'by%'").size());
        assertEquals(0, show("SHOW ROW ACCESS POLICIES LIKE 'x%'").size());
        assertEquals("by_region", show("DESC ROW ACCESS POLICY pol.by_region").get(0).get(1));

        // one namespace per kind
        run("CREATE ROW ACCESS POLICY email_mask AS (r VARCHAR(10)) RETURNS BOOLEAN -> TRUE");
        assertTrue(error("CREATE MASKING POLICY email_mask AS (v VARCHAR(50)) RETURNS VARCHAR(50) -> v")
                .contains("already exists"), "duplicate");
        run("CREATE MASKING POLICY IF NOT EXISTS email_mask AS (v INT) RETURNS INT -> v");
        assertEquals("(val varchar(50))", show("DESC MASKING POLICY email_mask").get(0).get(6));
        run("CREATE OR REPLACE MASKING POLICY email_mask AS (v VARCHAR(50)) RETURNS VARCHAR(50) -> upper(v)");
        assertEquals("upper(v)", show("DESC MASKING POLICY email_mask").get(0).get(8));

        run("ALTER MASKING POLICY email_mask SET BODY -> lower(v)");
        run("ALTER MASKING POLICY email_mask SET COMMENT = 'lower'");
        run("ALTER MASKING POLICY email_mask RENAME TO email_lower");
        List<String> row = show("DESC MASKING POLICY email_lower").get(0);
        assertEquals("lower(v)", row.get(8));
        assertEquals("lower", row.get(5));
        run("ALTER MASKING POLICY IF EXISTS nope SET COMMENT 'x'");
        assertTrue(error("ALTER MASKING POLICY nope SET COMMENT 'x'").contains("does not exist"));
        run("DROP ROW ACCESS POLICY email_mask");
        run("DROP ROW ACCESS POLICY IF EXISTS email_mask");
        assertTrue(error("DROP ROW ACCESS POLICY email_mask").contains("does not exist"));

        // the body is checked
        assertTrue(error("CREATE ROW ACCESS POLICY bad AS (r VARCHAR(10)) RETURNS VARCHAR(10) -> r")
                .contains("returns BOOLEAN"));
        assertTrue(error("CREATE MASKING POLICY bad AS (v INT) RETURNS VARCHAR(10) -> 'x'")
                .contains("type of its first argument"));
        assertTrue(error("CREATE MASKING POLICY bad AS (v INT) RETURNS INT -> nope_column + 1")
                .contains("Invalid body"));
        assertTrue(error("CREATE ROW ACCESS POLICY bad AS (r INT) RETURNS BOOLEAN -> r + 1")
                .contains("must be a condition"));
        assertTrue(error("CREATE MASKING POLICY bad AS (v INT, v INT) RETURNS INT -> v").contains("twice"));
        assertTrue(error("CREATE MASKING POLICY nodb.bad AS (v INT) RETURNS INT -> v").contains("Unknown database"));
    }

    @Test
    public void testSubstitute() {
        Map<String, String> args = Map.of("v", "X", "r", "Y");
        assertEquals("X + t.v + f(X) + 'v' + X AS v", SecurityPolicyMgr.substitute("v + t.v + f(v) + 'v' + `v` AS v",
                args));
        assertEquals("v(1) + v.x + Y -- v", SecurityPolicyMgr.substitute("v(1) + v.x + r -- v", args));
    }

    @Test
    public void testMaskingAndRowAccessInQueries() throws Exception {
        run("CREATE MASKING POLICY salary_mask AS (s INT) RETURNS INT -> CASE WHEN s > 100 THEN 0 ELSE s END");
        run("CREATE MASKING POLICY email_mask AS (e VARCHAR(50), r VARCHAR(10)) RETURNS VARCHAR(50) -> "
                + "CASE WHEN r = 'EU' THEN '***' ELSE e END");
        run("CREATE ROW ACCESS POLICY by_region AS (r VARCHAR(10)) RETURNS BOOLEAN -> "
                + "r IN (SELECT region FROM pol.region_map WHERE role_name = current_role())");
        run("ALTER TABLE emp MODIFY COLUMN salary SET MASKING POLICY salary_mask");
        run("ALTER TABLE emp MODIFY COLUMN email SET MASKING POLICY email_mask USING (email, region)");
        run("ALTER TABLE emp ADD ROW ACCESS POLICY by_region ON (region)");

        String explain = plan("SELECT id, salary, email FROM emp WHERE id > 1");
        assertTrue(explain.contains("> 100"), explain);
        assertTrue(explain.contains("'EU'"), explain);
        assertTrue(explain.contains("region_map"), explain);
        // an alias, a join and a view read it the same way
        starRocksAssert.withView("CREATE VIEW emp_v AS SELECT id, salary FROM emp");
        explain = plan("SELECT e.salary FROM emp e JOIN logs l ON e.id = l.id");
        assertTrue(explain.contains("region_map") && explain.contains("> 100"), explain);
        explain = plan("SELECT * FROM emp_v");
        assertTrue(explain.contains("region_map") && explain.contains("> 100"), explain);
        // a statement not run by a connection, the planner's own, is not rewritten
        ctx.setThreadLocalInfo();
        String internal = StatementPlanner.plan(parse("SELECT salary FROM emp"), ctx)
                .getExplainString(TExplainLevel.NORMAL);
        assertFalse(internal.contains("region_map"), internal);

        List<String> attached = List.of(show("DESC ROW ACCESS POLICY by_region").get(0).get(9),
                show("DESC MASKING POLICY email_mask").get(0).get(9));
        assertEquals(List.of("pol.emp", "pol.emp.email"), attached);

        // one per column and table
        assertTrue(error("ALTER TABLE emp MODIFY COLUMN salary SET MASKING POLICY salary_mask")
                .contains("FORCE"));
        run("ALTER TABLE emp MODIFY COLUMN salary SET MASKING POLICY salary_mask FORCE");
        assertTrue(error("ALTER TABLE emp ADD ROW ACCESS POLICY by_region ON (region)").contains("already has"));
        // an attached policy is not dropped, nor replaced
        assertTrue(error("DROP MASKING POLICY salary_mask").contains("attached to pol.emp.salary"));
        assertTrue(error("CREATE OR REPLACE ROW ACCESS POLICY by_region AS (r VARCHAR(10)) RETURNS BOOLEAN -> TRUE")
                .contains("attached"));
        // a body that no longer applies to a table it is attached to
        assertTrue(error("ALTER MASKING POLICY email_mask SET BODY -> e + nope").contains("Invalid body"));
        // the columns
        assertTrue(error("ALTER TABLE emp MODIFY COLUMN name SET MASKING POLICY salary_mask").contains("is of type"));
        assertTrue(error("ALTER TABLE emp MODIFY COLUMN nope SET MASKING POLICY salary_mask").contains("Unknown column"));
        assertTrue(error("ALTER TABLE emp MODIFY COLUMN email SET MASKING POLICY email_mask FORCE")
                .contains("takes 2 columns"));
        assertTrue(error("ALTER TABLE emp MODIFY COLUMN region SET MASKING POLICY email_mask USING (email, region)")
                .contains("first column of USING"));
        assertTrue(error("ALTER TABLE emp DROP ROW ACCESS POLICY nope").contains("does not exist"));

        run("ALTER TABLE emp MODIFY COLUMN salary UNSET MASKING POLICY");
        assertTrue(error("ALTER TABLE emp MODIFY COLUMN salary UNSET MASKING POLICY").contains("has no masking"));
        run("ALTER TABLE emp DROP ROW ACCESS POLICY by_region");
        run("ALTER TABLE emp MODIFY COLUMN email UNSET MASKING POLICY");
        explain = plan("SELECT salary FROM emp");
        assertFalse(explain.contains("region_map") || explain.contains("CASE"), explain);
        run("DROP MASKING POLICY salary_mask");
        starRocksAssert.dropView("emp_v");
    }

    @Test
    public void testDml() throws Exception {
        run("CREATE ROW ACCESS POLICY eu AS (r VARCHAR(10)) RETURNS BOOLEAN -> r = 'EU'");
        run("ALTER TABLE emp ADD ROW ACCESS POLICY eu ON (region)");
        run("ALTER TABLE logs ADD ROW ACCESS POLICY eu ON (region)");
        String explain = dml("UPDATE emp SET salary = 1 WHERE id = 3");
        assertTrue(explain.contains("'EU'"), explain);
        explain = dml("UPDATE emp SET salary = 1 FROM logs WHERE emp.id = logs.id");
        assertTrue(explain.contains("'EU'"), explain);
        explain = dml("DELETE FROM emp WHERE id = 3");
        assertTrue(explain.contains("'EU'"), explain);
        explain = dml("MERGE INTO emp t USING (SELECT 1 AS id) s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET salary = 2 WHEN NOT MATCHED THEN INSERT (id, region) VALUES (s.id, 'EU')");
        assertTrue(explain.contains("'EU'"), explain);
        assertTrue(assertThrows(Exception.class, () -> dml("DELETE FROM logs WHERE id = 3")).getMessage()
                .contains("row access policy"));
        // a materialized view would keep the rows without the policy
        assertTrue(assertThrows(Exception.class, () -> starRocksAssert.withMaterializedView(
                "CREATE MATERIALIZED VIEW emp_mv REFRESH MANUAL AS SELECT id, salary FROM emp"))
                .getMessage().contains("policies"));
        // the columns a policy reads stay
        assertTrue(assertThrows(Exception.class, () -> starRocksAssert.alterTable(
                "ALTER TABLE emp DROP COLUMN region")).getMessage().contains("detach the policy"));
        assertTrue(assertThrows(Exception.class, () -> starRocksAssert.alterTable(
                "ALTER TABLE emp RENAME COLUMN region TO area")).getMessage().contains("detach the policy"));
        run("ALTER TABLE emp DROP ALL ROW ACCESS POLICIES");
        run("ALTER TABLE logs DROP ROW ACCESS POLICY eu");
        assertFalse(dml("DELETE FROM emp WHERE id = 3").contains("'EU'"));
    }

    @Test
    public void testAttachToTableWithMaterializedView() throws Exception {
        starRocksAssert.withTable("CREATE TABLE mvbase (k INT, v INT) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) "
                + "BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mvbase_mv REFRESH MANUAL AS "
                + "SELECT k, sum(v) AS s FROM mvbase GROUP BY k");
        run("CREATE MASKING POLICY m AS (v INT) RETURNS INT -> 0");
        assertTrue(error("ALTER TABLE mvbase MODIFY COLUMN v SET MASKING POLICY m").contains("materialized views"));
        starRocksAssert.dropMaterializedView("mvbase_mv");
        run("ALTER TABLE mvbase MODIFY COLUMN v SET MASKING POLICY m");
    }

    @Test
    public void testPersistence() throws Exception {
        run("CREATE MASKING POLICY m AS (v INT) RETURNS INT -> 0 COMMENT 'zero'");
        run("ALTER TABLE emp MODIFY COLUMN salary SET MASKING POLICY m");
        SecurityPolicy policy = SecurityPolicyMgr.list(db, SecurityPolicyStmt.Kind.MASKING).get(0);
        SecurityPolicy copy = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(policy), SecurityPolicy.class);
        assertEquals(policy.getName(), copy.getName());
        assertEquals(SecurityPolicyStmt.Kind.MASKING, copy.getKind());
        assertEquals(policy.signature(), copy.signature());
        TablePolicies policies = db.getTablePolicies().values().iterator().next();
        TablePolicies tableCopy = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(policies), TablePolicies.class);
        assertEquals(List.of("salary"), tableCopy.referencedColumns());
        assertTrue(tableCopy.getMasks().get("salary").refersTo(policy));

        // replay
        SecurityPolicyMgr.replayDrop(new DropSecurityPolicyLog(db.getId(), policy.getId()));
        assertNull(SecurityPolicyMgr.get(db, SecurityPolicyStmt.Kind.MASKING, "m"));
        // fail closed: the table cannot be read while its policy is gone
        assertTrue(planError("SELECT salary FROM emp").contains("no longer exists"));
        SecurityPolicyMgr.replayPut(copy);
        assertTrue(plan("SELECT salary FROM emp").contains("salary"));
        SecurityPolicyMgr.replaySetTablePolicies(new TablePolicies(db.getId(), policies.getTableId(), null, Map.of()));
        assertTrue(db.getTablePolicies().isEmpty());
        SecurityPolicyMgr.replaySetTablePolicies(tableCopy);
        assertEquals(1, db.getTablePolicies().size());
    }
}
