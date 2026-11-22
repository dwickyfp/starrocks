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

package com.starrocks.scheduler;

import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.timetravel.TimeTravelRetention;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SYSTEM$STREAM_HAS_DATA, and the tasks whose WHEN condition reads streams: scheduled runs that are skipped while the
 * streams have no data, and triggered tasks, which run when they have.
 */
public class StreamTaskTest {
    private static final long HOUR = 3600_000L;
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext ctx;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("stask").useDatabase("stask");
        starRocksAssert.withTable("CREATE TABLE src (k INT, v INT) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) "
                + "BUCKETS 1 PROPERTIES ('replication_num' = '1', 'data_retention_time_in_days' = '1')");
        starRocksAssert.withTable("CREATE TABLE src2 (k INT, v INT) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) "
                + "BUCKETS 1 PROPERTIES ('replication_num' = '1', 'data_retention_time_in_days' = '1')");
        long base = System.currentTimeMillis();
        for (String name : List.of("src", "src2")) {
            PhysicalPartition partition = table(name).getAllPhysicalPartitions().iterator().next();
            partition.setVisibleVersion(1, base - 4 * HOUR);
            publish(table(name), partition, 2, base - 3 * HOUR);
        }
        run("CREATE STREAM s1 ON TABLE src");
        run("CREATE STREAM s2 ON TABLE src2");
    }

    @AfterEach
    public void dropTasks() throws Exception {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        taskManager.dropTasks(taskManager.filterTasks(null).stream().filter(Task::isUserSqlTask).map(Task::getId)
                .toList());
        // start again from no data
        run("CREATE OR REPLACE STREAM s1 ON TABLE src");
        run("CREATE OR REPLACE STREAM s2 ON TABLE src2");
    }

    private static OlapTable table(String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("stask", name);
    }

    private static void publish(OlapTable table, PhysicalPartition partition, long version, long time) {
        TimeTravelRetention.onVersionVisible(table, partition, version, time);
        partition.updateVisibleVersion(version, time);
        partition.setNextVersion(version + 1);
        for (MaterializedIndex index : partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
            for (Tablet tablet : index.getTablets()) {
                for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                    replica.updateVersionInfo(version, -1, version);
                }
            }
        }
    }

    private static void change(String name) {
        PhysicalPartition partition = table(name).getAllPhysicalPartitions().iterator().next();
        publish(table(name), partition, partition.getVisibleVersion() + 1, System.currentTimeMillis());
    }

    private static ShowResultSet run(String sql) throws Exception {
        ctx.setThreadLocalInfo();
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        if (stmt instanceof ShowStmt) {
            return ShowExecutor.execute((ShowStmt) stmt, ctx);
        }
        return DDLStmtExecutor.execute(stmt, ctx);
    }

    private static String explain(String sql) throws Exception {
        ctx.setThreadLocalInfo();
        StatementBase stmt = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        return StatementPlanner.plan(stmt, ctx).getExplainString(TExplainLevel.NORMAL);
    }

    private static Task task(String name) {
        return GlobalStateMgr.getCurrentState().getTaskManager().getTask(name);
    }

    @Test
    public void testHasDataFunction() throws Exception {
        assertTrue(explain("SELECT SYSTEM$STREAM_HAS_DATA('s1')").contains("FALSE"));
        change("src");
        String plan = explain("SELECT SYSTEM$STREAM_HAS_DATA('stask.s1'), system$stream_has_data('s2')");
        assertTrue(plan.contains("TRUE") && plan.contains("FALSE"), plan);
        // it is a constant: the query reads no table
        assertTrue(!plan.contains("OlapScanNode"), plan);
        assertTrue(explain("SELECT * FROM src WHERE SYSTEM$STREAM_HAS_DATA('s1')").contains("OlapScanNode"));

        Exception e = assertThrows(Exception.class, () -> explain("SELECT SYSTEM$STREAM_HAS_DATA('nosuch')"));
        assertTrue(e.getMessage().contains("Stream nosuch does not exist in database stask"), e.getMessage());
        e = assertThrows(Exception.class, () -> explain("SELECT SYSTEM$STREAM_HAS_DATA(concat('s', '1'))"));
        assertTrue(e.getMessage().contains("takes the name of a stream as a string literal"), e.getMessage());
        e = assertThrows(Exception.class, () -> explain("SELECT SYSTEM$STREAM_HAS_DATA('a.b.c')"));
        assertTrue(e.getMessage().contains("Invalid stream name"), e.getMessage());
    }

    @Test
    public void testConditionText() {
        UserTaskCondition state = UserTaskCondition.parse("ALL_DONE");
        assertEquals(UserTaskSpec.PredecessorsState.ALL_DONE, state.getState());
        assertNull(state.getStreams());
        assertEquals("ALL_DONE", state.toOption());
        assertEquals("SYSTEM$PREDECESSORS_STATE('ALL_DONE')", state.toString());

        String text = "SYSTEM$STREAM_HAS_DATA('s1') OR NOT SYSTEM$STREAM_HAS_DATA('db.s2') "
                + "AND SYSTEM$STREAM_HAS_DATA('s3')";
        UserTaskCondition streams = UserTaskCondition.parse(text);
        assertEquals(List.of("s1", "db.s2", "s3"), streams.streamNames());
        assertEquals("SYSTEM$STREAM_HAS_DATA('s1') OR (NOT SYSTEM$STREAM_HAS_DATA('db.s2') AND "
                + "SYSTEM$STREAM_HAS_DATA('s3'))", streams.toOption());
        // the text it keeps reads back to the same condition
        assertEquals(streams.toOption(), UserTaskCondition.parse(streams.toOption()).toOption());
        assertTrue(streams.testStreams(s -> s.equals("s1")));
        assertTrue(streams.testStreams(s -> s.equals("s3")));
        assertTrue(!streams.testStreams(s -> s.equals("db.s2")));

        UserTaskCondition both = UserTaskCondition.parse("SYSTEM$STREAM_HAS_DATA('s1') AND "
                + "SYSTEM$PREDECESSORS_STATE('all_failed') AND SYSTEM$STREAM_HAS_DATA('s2')");
        assertEquals(UserTaskSpec.PredecessorsState.ALL_FAILED, both.getState());
        assertEquals("SYSTEM$PREDECESSORS_STATE('ALL_FAILED') AND SYSTEM$STREAM_HAS_DATA('s1') AND "
                + "SYSTEM$STREAM_HAS_DATA('s2')", both.toOption());
        assertEquals(both.toOption(), UserTaskCondition.parse(both.toOption()).toOption());
        UserTaskCondition orWithState = UserTaskCondition.parse("SYSTEM$PREDECESSORS_STATE('ALL_DONE') AND "
                + "(SYSTEM$STREAM_HAS_DATA('s1') OR SYSTEM$STREAM_HAS_DATA('s2'))");
        assertEquals(orWithState.toOption(), UserTaskCondition.parse(orWithState.toOption()).toOption());
        assertTrue(orWithState.toOption().endsWith("AND (SYSTEM$STREAM_HAS_DATA('s1') OR "
                + "SYSTEM$STREAM_HAS_DATA('s2'))"), orWithState.toOption());
        Exception e = assertThrows(Exception.class, () -> UserTaskCondition.parse(
                "SYSTEM$PREDECESSORS_STATE('ALL_DONE') AND SYSTEM$PREDECESSORS_STATE('ALL_FAILED')"));
        assertTrue(e.getMessage().contains("once"), e.getMessage());
    }

    @Test
    public void testCreateAlterAndShow() throws Exception {
        // a root task can have a condition on streams, without AFTER
        run("CREATE TASK root1 SCHEDULE = '1 MINUTE' WHEN SYSTEM$STREAM_HAS_DATA('s1') OR "
                + "SYSTEM$STREAM_HAS_DATA('s2') AS INSERT INTO src2 SELECT * FROM s1");
        assertEquals("SYSTEM$STREAM_HAS_DATA('s1') OR SYSTEM$STREAM_HAS_DATA('s2')",
                task("root1").getTaskOption(UserTaskSpec.WHEN));
        List<List<String>> rows = run("DESC TASK root1").getResultRows();
        assertTrue(rows.get(0).contains("SYSTEM$STREAM_HAS_DATA('s1') OR SYSTEM$STREAM_HAS_DATA('s2')"),
                rows.toString());
        run("ALTER TASK root1 MODIFY WHEN SYSTEM$STREAM_HAS_DATA('s2')");
        assertEquals("SYSTEM$STREAM_HAS_DATA('s2')", task("root1").getTaskOption(UserTaskSpec.WHEN));
        // a child task: the state of its predecessors and the streams
        run("CREATE TASK child1 AFTER root1 WHEN SYSTEM$PREDECESSORS_STATE('ALL_DONE') AND "
                + "SYSTEM$STREAM_HAS_DATA('s1') AS SELECT 1");
        assertEquals(UserTaskSpec.PredecessorsState.ALL_DONE, UserTaskSpec.getCondition(task("child1")));
        run("CREATE TASK child2 AFTER root1 WHEN SYSTEM$STREAM_HAS_DATA('s1') AS SELECT 1");
        assertEquals(UserTaskSpec.PredecessorsState.ALL_SUCCEEDED, UserTaskSpec.getCondition(task("child2")));
        run("ALTER TASK child1 MODIFY WHEN SYSTEM$PREDECESSORS_STATE('ALL_FAILED')");
        assertEquals("ALL_FAILED", task("child1").getTaskOption(UserTaskSpec.WHEN));

        Exception e = assertThrows(Exception.class, () -> run("CREATE TASK bad1 WHEN SYSTEM$STREAM_HAS_DATA('nosuch') "
                + "AS SELECT 1"));
        assertTrue(e.getMessage().contains("Stream nosuch does not exist"), e.getMessage());
        e = assertThrows(Exception.class, () -> run("CREATE TASK bad2 WHEN SYSTEM$PREDECESSORS_STATE('ALL_DONE') "
                + "AS SELECT 1"));
        assertTrue(e.getMessage().contains("needs AFTER"), e.getMessage());
        e = assertThrows(Exception.class, () -> run("ALTER TASK root1 MODIFY WHEN SYSTEM$STREAM_HAS_DATA('nosuch')"));
        assertTrue(e.getMessage().contains("Stream nosuch does not exist"), e.getMessage());
        e = assertThrows(Exception.class, () -> run("CREATE TASK bad3 USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS = 0 "
                + "WHEN SYSTEM$STREAM_HAS_DATA('s1') AS SELECT 1"));
        assertTrue(e.getMessage().contains("USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS"), e.getMessage());
    }

    @Test
    public void testBlockingReason() throws Exception {
        run("CREATE TASK b1 WHEN SYSTEM$STREAM_HAS_DATA('s1') AND NOT SYSTEM$STREAM_HAS_DATA('s2') AS SELECT 1");
        Task task = task("b1");
        assertEquals("SYSTEM$STREAM_HAS_DATA('s1') AND NOT SYSTEM$STREAM_HAS_DATA('s2') is false",
                UserTaskCondition.blockingReason(task));
        change("src");
        assertNull(UserTaskCondition.blockingReason(task));
        change("src2");
        assertNotNull(UserTaskCondition.blockingReason(task));
        run("DROP STREAM s2");
        assertTrue(UserTaskCondition.blockingReason(task).contains("cannot be evaluated: Stream s2 does not exist"));
        run("CREATE STREAM s2 ON TABLE src2");
        run("CREATE TASK b2 AS SELECT 1");
        assertNull(UserTaskCondition.blockingReason(task("b2")));
    }

    @Test
    public void testTriggeredTask() throws Exception {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        run("CREATE TASK trig USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS = 3600 WHEN SYSTEM$STREAM_HAS_DATA('s1') "
                + "AS SELECT 1");
        run("CREATE TASK sched SCHEDULE = '1 MINUTE' WHEN SYSTEM$STREAM_HAS_DATA('s1') AS SELECT 1");
        Task trig = task("trig");
        // suspended: not triggered
        change("src");
        taskManager.triggerStreamTasks();
        assertNull(taskManager.getLastTriggerTime(trig.getId()));
        run("CREATE OR REPLACE STREAM s1 ON TABLE src");
        run("ALTER TASK trig RESUME");
        // no data: not triggered
        taskManager.triggerStreamTasks();
        assertNull(taskManager.getLastTriggerTime(trig.getId()));
        change("src");
        taskManager.triggerStreamTasks();
        Long first = taskManager.getLastTriggerTime(trig.getId());
        assertNotNull(first);
        // at most once per its minimum trigger interval
        taskManager.triggerStreamTasks();
        assertEquals(first, taskManager.getLastTriggerTime(trig.getId()));
        // a task with a SCHEDULE is not triggered: its schedule checks the condition
        run("ALTER TASK sched RESUME");
        taskManager.triggerStreamTasks();
        assertNull(taskManager.getLastTriggerTime(task("sched").getId()));
    }
}
