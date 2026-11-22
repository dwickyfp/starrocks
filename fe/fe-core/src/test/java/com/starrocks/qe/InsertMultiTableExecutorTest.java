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

package com.starrocks.qe;

import com.starrocks.sql.ast.InsertMultiTableStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * INSERT ALL | FIRST: the parser keeps the SQL text of its parts, and every target table gets one INSERT.
 */
public class InsertMultiTableExecutorTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("create table ma (k int, v varchar(20), n int) duplicate key(k) "
                + "distributed by hash(k) buckets 2 properties('replication_num'='1')");
        starRocksAssert.withTable("create table mb (k int, v varchar(20), n int) duplicate key(k) "
                + "distributed by hash(k) buckets 2 properties('replication_num'='1')");
        starRocksAssert.withTable("create table mdef (k int, v varchar(20) default 'dflt', "
                + "d datetime default current_timestamp) duplicate key(k) "
                + "distributed by hash(k) buckets 2 properties('replication_num'='1')");
        starRocksAssert.withTable("create table msource (id int, name varchar(20), amount int) duplicate key(id) "
                + "distributed by hash(id) buckets 2 properties('replication_num'='1')");
    }

    private static InsertMultiTableStmt parse(String sql) {
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        return assertInstanceOf(InsertMultiTableStmt.class, stmt);
    }

    /** The INSERT statements, each analyzed to check that it is valid SQL for the tables. */
    private static List<String> inserts(String sql) throws Exception {
        InsertMultiTableStmt stmt = parse(sql);
        List<String> inserts = InsertMultiTableExecutor.targets(stmt, connectContext).stream()
                .map(t -> InsertMultiTableExecutor.toSql(stmt, t)).collect(Collectors.toList());
        for (String insert : inserts) {
            assertInstanceOf(InsertStmt.class, UtFrameUtils.parseStmtWithNewParser(insert, connectContext), insert);
        }
        return inserts;
    }

    private static void assertError(String sql, String message) {
        Exception e = assertThrows(Exception.class, () -> inserts(sql));
        assertTrue(e.getMessage().contains(message), e.getMessage());
    }

    @Test
    public void testParse() {
        InsertMultiTableStmt stmt = parse("insert first when amount > 10 then into ma (k, v) values (id, upper(name)) "
                + "into mb when amount > 0 then into mb else into ma values (id, DEFAULT, 1) "
                + "select id, name, amount from msource");
        assertTrue(stmt.isFirst());
        assertEquals(3, stmt.getBranches().size());
        assertEquals("amount > 10", stmt.getBranches().get(0).getConditionSql());
        assertEquals(2, stmt.getBranches().get(0).getIntos().size());
        InsertMultiTableStmt.Into into = stmt.getBranches().get(0).getIntos().get(0);
        assertEquals(List.of("k", "v"), into.getColumns());
        assertEquals(List.of("id", "upper(name)"), into.getValueSqls());
        assertNull(stmt.getBranches().get(0).getIntos().get(1).getColumns());
        assertNull(stmt.getBranches().get(0).getIntos().get(1).getValueSqls());
        assertTrue(stmt.getBranches().get(2).isElse());
        assertNull(stmt.getBranches().get(2).getIntos().get(0).getValueSqls().get(1));
        assertEquals("select id, name, amount from msource", stmt.getQuerySql());

        InsertMultiTableStmt all = parse("insert all into ma into test.mb (k) values (id) "
                + "with s as (select * from msource) select * from s");
        assertFalse(all.isFirst());
        assertEquals(1, all.getBranches().size());
        assertNull(all.getBranches().get(0).getConditionSql());
        assertEquals(List.of("test", "mb"), all.getBranches().get(0).getIntos().get(1).getTableNameParts());

        Exception e = assertThrows(Exception.class, () -> parse("insert first into ma select * from msource"));
        assertTrue(e.getMessage().contains("INSERT FIRST needs WHEN clauses"), e.getMessage());
        e = assertThrows(Exception.class, () -> parse("insert all into ma (k, v) values (id) select * from msource"));
        assertTrue(e.getMessage().contains("INTO ma lists 2 columns but 1 values"), e.getMessage());
    }

    @Test
    public void testUnconditional() throws Exception {
        List<String> inserts = inserts("insert all into ma into mb (k, n) values (id, amount * 2) "
                + "select * from msource");
        assertEquals(2, inserts.size());
        assertEquals("INSERT INTO ma WITH `__insert_source` AS (SELECT * FROM (select * from msource) "
                + "`__insert_query`) SELECT * FROM `__insert_source`", inserts.get(0));
        assertTrue(inserts.get(1).startsWith("INSERT INTO mb (`k`, `n`) WITH"), inserts.get(1));
        assertTrue(inserts.get(1).endsWith("SELECT id, amount * 2 FROM `__insert_source`"), inserts.get(1));
    }

    @Test
    public void testConditionalAll() throws Exception {
        // The INTO clauses of one table become one INSERT, as a transaction writes a table once.
        List<String> inserts = inserts("insert all when amount > 10 then into ma (k, v) values (id, name) "
                + "when amount > 0 then into ma (v, k) values ('small', id) into mb "
                + "else into mb select * from msource");
        assertEquals(2, inserts.size());
        assertTrue(inserts.get(0).endsWith("SELECT id, name FROM `__insert_source` WHERE coalesce((amount > 10), false) "
                + "UNION ALL SELECT id, 'small' FROM `__insert_source` WHERE coalesce((amount > 0), false)"),
                inserts.get(0));
        assertTrue(inserts.get(1).endsWith("SELECT * FROM `__insert_source` WHERE coalesce((amount > 0), false) "
                + "UNION ALL SELECT * FROM `__insert_source` WHERE NOT coalesce((amount > 10), false) "
                + "AND NOT coalesce((amount > 0), false)"), inserts.get(1));
    }

    @Test
    public void testFirst() throws Exception {
        List<String> inserts = inserts("insert first when amount > 10 then into ma "
                + "when amount > 0 then into mb else into mb select * from msource");
        assertEquals(2, inserts.size());
        assertTrue(inserts.get(0).endsWith("WHERE coalesce((amount > 10), false)"), inserts.get(0));
        assertTrue(inserts.get(1).contains("WHERE coalesce((amount > 0), false) AND NOT coalesce((amount > 10), false)"),
                inserts.get(1));
        // DEFAULT leaves the column out, so both INTO clauses of mb must leave out the same columns.
        assertError("insert first when amount > 10 then into ma "
                + "when amount > 0 then into mb else into mb (k, v, n) values (id, DEFAULT, 0) "
                + "select * from msource", "Every INTO mb must list its columns, or none of them");
        // The ELSE rows are the rows no condition holds for.
        inserts = inserts("insert first when amount > 0 then into mb (k, n) values (id, amount) "
                + "else into mb (k, v, n) values (id, DEFAULT, 0) select * from msource");
        assertEquals(1, inserts.size());
        assertTrue(inserts.get(0).startsWith("INSERT INTO mb (`k`, `n`)"), inserts.get(0));
        assertTrue(inserts.get(0).endsWith("SELECT id, 0 FROM `__insert_source` WHERE NOT coalesce((amount > 0), false)"),
                inserts.get(0));
    }

    @Test
    public void testDifferentColumns() throws Exception {
        // The INSERT writes every column an INTO clause lists; the others write the default of the column.
        List<String> inserts = inserts("insert all when amount > 0 then into ma (k, v) values (id, name) "
                + "else into ma (n, k) values (amount, id) into mdef (k) values (id) into mdef (k, d) values (id, now()) "
                + "select * from msource");
        assertEquals(2, inserts.size());
        assertTrue(inserts.get(0).startsWith("INSERT INTO ma (`k`, `v`, `n`)"), inserts.get(0));
        assertTrue(inserts.get(0).contains("SELECT id, name, NULL FROM"), inserts.get(0));
        assertTrue(inserts.get(0).contains("SELECT id, NULL, amount FROM"), inserts.get(0));
        assertTrue(inserts.get(1).startsWith("INSERT INTO mdef (`k`, `d`)"), inserts.get(1));
        // CURRENT_TIMESTAMP is the time of the statement, as for an INSERT that leaves the column out.
        assertTrue(inserts.get(1).matches(".*SELECT id, '\\d{4}-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d' FROM.*"),
                inserts.get(1));
        assertTrue(inserts.get(1).contains("SELECT id, now() FROM"), inserts.get(1));

        inserts = inserts("insert all into mdef (k, v) values (id, name) into mdef (k) values (id) select * from msource");
        assertTrue(inserts.get(0).contains("SELECT id, 'dflt' FROM"), inserts.get(0));
    }

    @Test
    public void testErrors() {
        assertError("insert all into ma values (id, DEFAULT, 1) select * from msource",
                "INTO ma: VALUES with DEFAULT needs a column list");
        assertError("insert all into ma (k) values (DEFAULT) select * from msource",
                "INTO ma: at least one value must not be DEFAULT");
        assertError("insert all into ma (k, v) into ma (v, k) select id, name from msource",
                "Every INTO ma without VALUES must list the same columns in the same order");
        assertError("insert all into ma into ma (k) values (id) select * from msource",
                "Every INTO ma must list its columns, or none of them");
        assertError("insert all into ma (k, v) values (id, name) into ma (k, n) select * from msource",
                "Every INTO ma without VALUES must list the same columns in the same order");
        assertError("insert all into ma (k, k) values (id, id) into ma (n) values (1) select * from msource",
                "INTO ma lists a column twice");
        assertError("insert all into ma (k) values (zz) select * from msource", "Column 'zz' cannot be resolved");
        assertError("insert all into nosuch select * from msource", "Table nosuch is not found");
    }
}
