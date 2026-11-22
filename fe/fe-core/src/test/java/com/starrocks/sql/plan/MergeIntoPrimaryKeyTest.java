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

package com.starrocks.sql.plan;

import com.starrocks.common.util.UUIDUtil;
import com.starrocks.load.Load;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MERGE INTO a Primary Key table: the plan writes every column and {@code __op} through an OLAP table sink.
 */
public class MergeIntoPrimaryKeyTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("create table mpk (k int not null, v varchar(20) not null, n int, "
                + "d datetime default current_timestamp) primary key(k) distributed by hash(k) buckets 2 "
                + "properties('replication_num'='1')");
        starRocksAssert.withTable("create table mpk2 (k1 int not null, k2 varchar(10) not null, a int, "
                + "g bigint as (a * 2)) primary key(k1, k2) distributed by hash(k1) buckets 2 "
                + "properties('replication_num'='1')");
        starRocksAssert.withTable("create table msrc (k int, v varchar(20), n int, source int, target int) "
                + "duplicate key(k) distributed by hash(k) buckets 2 properties('replication_num'='1')");
        starRocksAssert.withTable("create table mdup (k int, v int) duplicate key(k) distributed by hash(k) "
                + "buckets 2 properties('replication_num'='1')");
    }

    @AfterEach
    public void reset() {
        connectContext.getSessionVariable().setErrorOnNondeterministicMerge(true);
    }

    private static ExecPlan plan(String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable().getSqlMode()).get(0);
        connectContext.getDumpInfo().setOriginStmt(sql);
        return StatementPlanner.plan(stmt, connectContext);
    }

    private static String explain(String sql) throws Exception {
        return plan(sql).getExplainString(TExplainLevel.NORMAL);
    }

    private static void assertError(String sql, String message) {
        Exception e = assertThrows(Exception.class, () -> plan(sql));
        assertTrue(e.getMessage().contains(message), e.getMessage());
    }

    @Test
    public void testSinkWritesEveryColumnAndOp() throws Exception {
        ExecPlan execPlan = plan("merge into mpk t using msrc s on t.k = s.k "
                + "when matched and s.n < 0 then delete "
                + "when matched then update set v = s.v, n = t.n + s.n "
                + "when not matched then insert (k, v) values (s.k, s.v)");
        OlapTableSink sink = (OlapTableSink) execPlan.getFragments().get(0).getSink();
        List<SlotDescriptor> slots = sink.getTupleDescriptor().getSlots();
        assertEquals(5, slots.size());
        assertEquals(Load.LOAD_OP_COLUMN, slots.get(4).getColumn().getName());
        assertFalse(slots.get(4).getIsNullable());
        assertEquals(5, execPlan.getOutputExprs().size());

        String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
        assertTrue(plan.contains("OLAP TABLE SINK"), plan);
        assertTrue(plan.contains("RIGHT OUTER JOIN") || plan.contains("LEFT OUTER JOIN"), plan);
        // The duplicate check counts the rows of each key after the rows that are left alone are dropped.
        assertTrue(plan.contains("assert_true"), plan);
        assertTrue(plan.contains("count(*)"), plan);
        assertTrue(plan.contains("IS NOT NULL"), plan);
        // The default of d is cast to DATETIME: the current value of d is not cast to a string and back.
        assertFalse(plan.contains("AS VARCHAR"), plan);
    }

    @Test
    public void testNoDuplicateCheck() throws Exception {
        connectContext.getSessionVariable().setErrorOnNondeterministicMerge(false);
        String plan = explain("merge into mpk t using msrc s on t.k = s.k when matched then update set n = s.n");
        assertFalse(plan.contains("assert_true"), plan);
        assertFalse(plan.contains("ANALYTIC"), plan);
    }

    @Test
    public void testNotMatchedBySource() throws Exception {
        String plan = explain("merge into mpk t using msrc s on t.k = s.k "
                + "when matched then update set n = s.n "
                + "when not matched by target then insert (k, v) values (s.k, s.v) "
                + "when not matched by source and t.n > 10 then delete "
                + "when not matched by source then update set v = 'gone'");
        assertTrue(plan.contains("FULL OUTER JOIN"), plan);
        assertTrue(plan.contains("gone"), plan);
    }

    @Test
    public void testStarActions() throws Exception {
        String plan = explain("merge into mpk t using (select k, v, n, now() d from msrc) s on t.k = s.k "
                + "when matched then update set * when not matched then insert *");
        assertTrue(plan.contains("OLAP TABLE SINK"), plan);
        assertError("merge into mpk t using (select k, v from msrc) s on t.k = s.k when matched then update set *",
                "Column '`s`.`n`' cannot be resolved");
        assertError("merge into mpk t using (select k, v, n, d from msrc) on mpk.k = 1 "
                + "when not matched then insert *", "requires an aliased source relation");
    }

    @Test
    public void testCompositeKeyAndGeneratedColumn() throws Exception {
        String plan = explain("merge into mpk2 t using msrc s on t.k1 = s.k and t.k2 = s.v "
                + "when matched then update set a = s.n "
                + "when not matched then insert (k1, k2, a) values (s.k, s.v, s.n)");
        // The generated column is computed from the written value of a.
        assertTrue(plan.contains("* 2"), plan);
        assertTrue(plan.contains("partition by"), plan);
    }

    @Test
    public void testNamesSourceAndTargetStayIdentifiers() throws Exception {
        String plan = explain("merge into mpk as target using msrc as source on target.k = source.source "
                + "when matched and source.target > 0 then delete");
        assertTrue(plan.contains("OLAP TABLE SINK"), plan);
    }

    @Test
    public void testErrors() {
        assertError("merge into mdup t using msrc s on t.k = s.k when matched then delete",
                "only supported for Primary Key tables and Iceberg tables, but 'mdup' is a DUPLICATE KEY table");
        assertError("merge into mpk t using msrc s on t.k = s.k when matched then update set k = 1",
                "primary key column cannot be updated: k");
        assertError("merge into mpk2 t using msrc s on t.k1 = s.k when matched then update set g = 1",
                "generated column cannot be updated: g");
        assertError("merge into mpk2 t using msrc s on t.k1 = s.k when not matched then insert (k1, k2, g) "
                + "values (1, 'a', 2)", "generated column 'g' cannot be inserted");
        assertError("merge into mpk t using msrc s on t.k = s.k when not matched then insert (v) values ('x')",
                "must give a value for the key column 'k'");
        assertError("merge into mpk t using msrc s on t.k = s.k when not matched then insert (k) values (1)",
                "Column 'v' is not nullable and has no default value");
        assertError("merge into mpk t using msrc s on t.k = s.k when not matched then insert (k, v, k) "
                + "values (1, 'a', 2)", "Duplicate column name 'k'");
        assertError("merge into mpk t using msrc s on t.k = s.k when not matched then insert (k, v) values (1)",
                "column count 2 does not match value count 1");
        assertError("merge into mpk t using msrc s on t.k = s.k when not matched then insert values (1, 'a')",
                "VALUES count 2 does not match target table column count 4");
        assertError("merge into mpk t using msrc s on t.k = s.k when matched then update set zz = 1",
                "does not have column 'zz'");
        assertError("merge into mpk t using msrc s on t.k = s.k when matched then delete "
                + "when matched and s.n > 0 then update set n = 1", "Unconditional WHEN MATCHED clause must be the last");
        assertError("merge into mpk t using msrc s on t.k = s.k when not matched by source then delete "
                + "when not matched by source and t.n > 0 then delete",
                "Unconditional WHEN NOT MATCHED BY SOURCE clause must be the last");
        assertError("merge into mpk t using msrc s on t.k = s.k when not matched by foo then insert (k, v) "
                + "values (1, 'a')", "Expected WHEN NOT MATCHED BY TARGET, but got BY foo");
        assertError("merge into mpk t using msrc s on t.k = s.k when not matched by target then delete",
                "Expected WHEN NOT MATCHED BY SOURCE, but got BY target");
    }
}
