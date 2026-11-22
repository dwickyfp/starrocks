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

import com.starrocks.catalog.View;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.scheduler.UserTaskGraphRunner.RunState;
import com.starrocks.scheduler.UserTaskSpec.PredecessorsState;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Task graphs of Snowflake style tasks: CREATE TASK ... AFTER ... WHEN SYSTEM$PREDECESSORS_STATE(...), FINALIZE,
 * ALTER TASK ADD / REMOVE AFTER and MODIFY / REMOVE WHEN, and running a graph.
 */
public class UserTaskGraphTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withDatabase("ugraph_other");
        starRocksAssert.withDatabase("ugraph").useDatabase("ugraph");
    }

    @AfterEach
    public void dropTasks() {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        List<Long> ids = taskManager.filterTasks(null).stream()
                .filter(Task::isUserSqlTask).map(Task::getId).toList();
        taskManager.dropTasks(ids);
    }

    private static ShowResultSet run(String sql) throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        if (stmt instanceof ShowStmt) {
            return ShowExecutor.execute((ShowStmt) stmt, ctx);
        }
        return DDLStmtExecutor.execute(stmt, ctx);
    }

    private static Task getTask(String name) {
        return GlobalStateMgr.getCurrentState().getTaskManager().getTask(name);
    }

    private static void assertFails(String sql, String message) {
        Exception e = Assertions.assertThrows(Exception.class, () -> run(sql), sql);
        String text = e.getMessage() + (e.getCause() == null ? "" : " " + e.getCause().getMessage());
        Assertions.assertTrue(text.contains(message), sql + " -> " + text);
    }

    private static Map<String, RunState> awaitGraphRun(String root) throws Exception {
        UserTaskGraphRunner runner = GlobalStateMgr.getCurrentState().getTaskManager().getUserTaskGraphRunner();
        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline) {
            Map<String, RunState> states = runner.getLastGraphRunStates(root);
            if (states != null) {
                return states;
            }
            Thread.sleep(200);
        }
        Assertions.fail("graph run of " + root + " did not finish, in progress: " + runner.getGraphRunStates(root));
        return null;
    }

    @Test
    public void testCreateChildTasks() throws Exception {
        run("CREATE TASK g_root SCHEDULE = '1 HOUR' AS SELECT 1");
        run("CREATE TASK g_a AFTER g_root AS SELECT 1");
        run("CREATE TASK g_b AFTER ugraph.g_root WHEN SYSTEM$PREDECESSORS_STATE('at_least_one_failed') AS SELECT 1");
        run("CREATE TASK g_c AFTER g_a, g_b WHEN SYSTEM$PREDECESSORS_STATE('ALL_DONE') AS SELECT 1");
        run("CREATE TASK g_fin FINALIZE = g_root AS SELECT 1");

        Task child = getTask("g_c");
        Assertions.assertEquals(List.of("g_a", "g_b"), UserTaskSpec.getPredecessors(child));
        Assertions.assertEquals(PredecessorsState.ALL_DONE, UserTaskSpec.getCondition(child));
        Assertions.assertEquals(PredecessorsState.ALL_SUCCEEDED, UserTaskSpec.getCondition(getTask("g_a")));
        Assertions.assertEquals(PredecessorsState.AT_LEAST_ONE_FAILED, UserTaskSpec.getCondition(getTask("g_b")));
        Assertions.assertEquals(Constants.TaskState.PAUSE, child.getState());
        Assertions.assertEquals("g_root", UserTaskGraph.rootOf(child, UserTaskGraphTest::getTask).getName());
        Assertions.assertEquals("g_root", UserTaskGraph.rootOf(getTask("g_fin"), UserTaskGraphTest::getTask).getName());

        ShowResultSet show = run("DESC TASK g_c");
        List<String> row = show.getResultRows().get(0);
        int predecessors = show.getMetaData().getColumnIdx("PREDECESSORS");
        Assertions.assertEquals("g_a,g_b", row.get(predecessors));
        Assertions.assertEquals("SYSTEM$PREDECESSORS_STATE('ALL_DONE')", row.get(predecessors + 1));
        Assertions.assertEquals("", row.get(predecessors + 2));
        Assertions.assertEquals("g_root",
                run("DESC TASK g_fin").getResultRows().get(0).get(predecessors + 2));
        Assertions.assertFalse(row.get(show.getMetaData().getColumnIdx("OPTIONS")).contains("AFTER"));
    }

    @Test
    public void testInvalidTaskGraphs() throws Exception {
        run("CREATE TASK g_root AS SELECT 1");
        run("CREATE TASK g_a AFTER g_root AS SELECT 1");
        run("CREATE TASK g_fin FINALIZE = g_root AS SELECT 1");
        run("CREATE TASK g_other AS SELECT 1");

        assertFails("CREATE TASK g_x SCHEDULE = '1 HOUR' AFTER g_root AS SELECT 1", "cannot have both SCHEDULE and AFTER");
        assertFails("CREATE TASK g_x AFTER g_missing AS SELECT 1", "does not exist");
        assertFails("CREATE TASK g_x AFTER g_x AS SELECT 1", "cannot run after itself");
        assertFails("CREATE TASK g_x AFTER ugraph_other.g_root AS SELECT 1", "is not in database");
        assertFails("CREATE TASK g_x WHEN SYSTEM$PREDECESSORS_STATE('ALL_DONE') AS SELECT 1", "WHEN needs AFTER");
        assertFails("CREATE TASK g_x AFTER g_root WHEN SYSTEM$PREDECESSORS_STATE('SOMETIMES') AS SELECT 1",
                "Unknown SYSTEM$PREDECESSORS_STATE value");
        assertFails("CREATE TASK g_x AFTER g_root WHEN SYSTEM$STREAM_HAS_DATA('s') AS SELECT 1",
                "Stream s does not exist in database ugraph");
        assertFails("CREATE TASK g_x AFTER g_root WHEN 1 = 1 AS SELECT 1", "Unsupported task condition");
        assertFails("CREATE TASK g_x AFTER g_root WHEN NOT SYSTEM$PREDECESSORS_STATE('ALL_DONE') AS SELECT 1",
                "must be a condition of its own");
        assertFails("CREATE TASK g_x AFTER = 'g_root' AS SELECT 1", "is a clause, not an option");
        // two roots in one graph
        assertFails("CREATE TASK g_x AFTER g_a, g_other AS SELECT 1", "are in different task graphs");
        // cycle
        assertFails("ALTER TASK g_root ADD AFTER g_a", "cycle");
        // the root of a finalizer stays a root
        assertFails("ALTER TASK g_root ADD AFTER g_other", "is the root task of the finalizer task");
        // finalizer rules
        assertFails("CREATE TASK g_x AFTER g_fin AS SELECT 1", "Finalizer task [g_fin] cannot have child tasks");
        assertFails("CREATE TASK g_fin2 FINALIZE = g_root AS SELECT 1", "already has the finalizer task [g_fin]");
        assertFails("CREATE TASK g_fin2 FINALIZE = g_a AS SELECT 1", "must name a root task");
        assertFails("CREATE TASK g_fin2 SCHEDULE = '1 HOUR' FINALIZE = g_other AS SELECT 1", "cannot have a SCHEDULE");
        assertFails("ALTER TASK g_a SET SCHEDULE = '1 HOUR'", "cannot have both SCHEDULE and AFTER");
        assertFails("ALTER TASK g_a REMOVE AFTER g_other", "does not run after [g_other]");
        Assertions.assertNull(getTask("g_x"));
        Assertions.assertNull(getTask("g_fin2"));
    }

    @Test
    public void testRootMustBeSuspended() throws Exception {
        run("CREATE TASK g_root SCHEDULE = '1 HOUR' AS SELECT 1");
        run("CREATE TASK g_a AFTER g_root AS SELECT 1");
        run("ALTER TASK g_a RESUME");
        run("ALTER TASK g_root RESUME");

        assertFails("CREATE TASK g_b AFTER g_root AS SELECT 1", "Root task [g_root]");
        assertFails("ALTER TASK g_a SUSPEND", "must be suspended first");
        assertFails("CREATE OR REPLACE TASK g_a AFTER g_root AS SELECT 2", "must be suspended first");
        assertFails("CREATE TASK g_fin FINALIZE = g_root AS SELECT 1", "must be suspended first");

        run("ALTER TASK g_root SUSPEND");
        run("ALTER TASK g_a SUSPEND");
        run("ALTER TASK g_a MODIFY WHEN SYSTEM$PREDECESSORS_STATE('ALL_FAILED')");
        Assertions.assertEquals(PredecessorsState.ALL_FAILED, UserTaskSpec.getCondition(getTask("g_a")));
        run("ALTER TASK g_a REMOVE WHEN");
        Assertions.assertEquals(PredecessorsState.ALL_SUCCEEDED, UserTaskSpec.getCondition(getTask("g_a")));
        run("CREATE TASK g_b AS SELECT 1");
        run("ALTER TASK g_b ADD AFTER g_a");
        Assertions.assertEquals(List.of("g_a"), UserTaskSpec.getPredecessors(getTask("g_b")));
        run("ALTER TASK g_b REMOVE AFTER g_a");
        Assertions.assertFalse(UserTaskGraph.isChildOrFinalizer(getTask("g_b")));
    }

    @Test
    public void testDropPredecessorDetachesChildren() throws Exception {
        run("CREATE TASK g_root AS SELECT 1");
        run("CREATE TASK g_a AFTER g_root AS SELECT 1");
        run("CREATE TASK g_b AFTER g_root, g_a WHEN SYSTEM$PREDECESSORS_STATE('ALL_DONE') AS SELECT 1");
        run("CREATE TASK g_fin FINALIZE = g_root AS SELECT 1");
        run("ALTER TASK g_a RESUME");

        run("DROP TASK g_root");
        Task a = getTask("g_a");
        // g_a has no predecessor left: it becomes the root of g_b, and is suspended
        Assertions.assertTrue(UserTaskSpec.getPredecessors(a).isEmpty());
        Assertions.assertEquals(Constants.TaskState.PAUSE, a.getState());
        Assertions.assertEquals(List.of("g_a"), UserTaskSpec.getPredecessors(getTask("g_b")));
        Assertions.assertEquals(PredecessorsState.ALL_DONE, UserTaskSpec.getCondition(getTask("g_b")));
        Assertions.assertNull(UserTaskSpec.getFinalizeRoot(getTask("g_fin")));
    }

    /**
     * g_root succeeds; g_ok succeeds and g_bad fails after it; g_on_fail runs because one of them failed, g_on_ok
     * is skipped because not all succeeded, g_done runs after the skipped g_on_ok because ALL_DONE counts skipped
     * tasks, g_off is suspended and does not take part, and g_fin runs last.
     */
    @Test
    public void testGraphRunFollowsConditions() throws Exception {
        run("CREATE TASK g_root AS CREATE VIEW gv_root AS SELECT 1");
        run("CREATE TASK g_ok AFTER g_root AS CREATE VIEW gv_ok AS SELECT 1");
        run("CREATE TASK g_bad AFTER g_root AS CREATE VIEW gv_bad AS SELECT k FROM gv_missing_table");
        run("CREATE TASK g_on_fail AFTER g_ok, g_bad WHEN SYSTEM$PREDECESSORS_STATE('AT_LEAST_ONE_FAILED')\n" +
                "AS CREATE VIEW gv_on_fail AS SELECT 1");
        run("CREATE TASK g_on_ok AFTER g_ok, g_bad AS CREATE VIEW gv_on_ok AS SELECT 1");
        run("CREATE TASK g_done AFTER g_on_ok WHEN SYSTEM$PREDECESSORS_STATE('ALL_DONE')\n" +
                "AS CREATE VIEW gv_done AS SELECT 1");
        run("CREATE TASK g_off AFTER g_ok AS CREATE VIEW gv_off AS SELECT 1");
        run("CREATE TASK g_fin FINALIZE = g_root AS $$\n" +
                "  CREATE VIEW gv_fin AS SELECT 1;\n" +
                "$$");
        for (String name : List.of("g_ok", "g_bad", "g_on_fail", "g_on_ok", "g_done", "g_fin")) {
            run("ALTER TASK " + name + " RESUME");
        }

        // the root stays suspended: EXECUTE TASK runs the whole graph
        run("EXECUTE TASK g_root");
        Map<String, RunState> states = awaitGraphRun("g_root");

        Assertions.assertEquals(RunState.SUCCEEDED, states.get("g_root"), states.toString());
        Assertions.assertEquals(RunState.SUCCEEDED, states.get("g_ok"), states.toString());
        Assertions.assertEquals(RunState.FAILED, states.get("g_bad"), states.toString());
        Assertions.assertEquals(RunState.SUCCEEDED, states.get("g_on_fail"), states.toString());
        Assertions.assertEquals(RunState.SKIPPED, states.get("g_on_ok"), states.toString());
        Assertions.assertEquals(RunState.SUCCEEDED, states.get("g_done"), states.toString());
        Assertions.assertEquals(RunState.SUCCEEDED, states.get("g_fin"), states.toString());
        Assertions.assertFalse(states.containsKey("g_off"), "a suspended task does not take part");
        Assertions.assertEquals("g_fin", List.copyOf(states.keySet()).get(states.size() - 1), "finalizer runs last");

        for (String view : List.of("gv_root", "gv_ok", "gv_on_fail", "gv_done", "gv_fin")) {
            Assertions.assertNotNull(starRocksAssert.getTable("ugraph", view), view);
        }
        Assertions.assertNull(GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("ugraph", "gv_on_ok"));
        Assertions.assertNull(GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("ugraph", "gv_off"));

        // EXECUTE TASK of a child runs it alone
        run("EXECUTE TASK g_off");
        Assertions.assertNull(GlobalStateMgr.getCurrentState().getTaskManager().getUserTaskGraphRunner()
                .getGraphRunStates("g_off"));
    }

    @Test
    public void testFailedRootStillRunsFailureBranchAndFinalizer() throws Exception {
        run("CREATE TASK gf_root AS CREATE VIEW gf_root AS SELECT k FROM gf_missing_table");
        run("CREATE TASK g_next AFTER gf_root AS CREATE VIEW gf_next AS SELECT 1");
        run("CREATE TASK g_alert AFTER gf_root WHEN SYSTEM$PREDECESSORS_STATE('ALL_FAILED')\n" +
                "AS CREATE VIEW gf_alert AS SELECT 1");
        run("CREATE TASK g_fin FINALIZE = gf_root AS CREATE VIEW gf_fin AS SELECT 1");
        for (String name : List.of("g_next", "g_alert", "g_fin")) {
            run("ALTER TASK " + name + " RESUME");
        }
        run("EXECUTE TASK gf_root");
        Map<String, RunState> states = awaitGraphRun("gf_root");
        Assertions.assertEquals(RunState.FAILED, states.get("gf_root"), states.toString());
        Assertions.assertEquals(RunState.SKIPPED, states.get("g_next"), states.toString());
        Assertions.assertEquals(RunState.SUCCEEDED, states.get("g_alert"), states.toString());
        Assertions.assertEquals(RunState.SUCCEEDED, states.get("g_fin"), states.toString());
        Assertions.assertNotNull(starRocksAssert.getTable("ugraph", "gf_alert"));
        Assertions.assertNotNull(starRocksAssert.getTable("ugraph", "gf_fin"));
    }

    private static List<TaskRunStatus> history(String taskName) throws Exception {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        return taskManager.getTaskRunHistory().getInMemoryHistory().stream()
                .filter(status -> taskName.equals(status.getTaskName()))
                .toList();
    }

    private static List<TaskRunStatus> awaitHistory(String taskName, int count) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        List<TaskRunStatus> runs = history(taskName);
        while (runs.size() < count && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
            runs = history(taskName);
        }
        return runs;
    }

    @Test
    public void testAutoRetryAndSkippedRuns() throws Exception {
        assertFails("CREATE TASK gr_bad_opt TASK_AUTO_RETRY_ATTEMPTS = 31 AS CREATE VIEW gr_x AS SELECT 1",
                "at most 30");
        run("CREATE TASK gr_root TASK_AUTO_RETRY_ATTEMPTS = 2 AS CREATE VIEW gr_root AS SELECT 1");
        run("CREATE TASK gr_flaky AFTER gr_root AS CREATE VIEW gr_flaky AS SELECT k FROM gr_missing_table");
        run("CREATE TASK gr_after AFTER gr_flaky AS CREATE VIEW gr_after AS SELECT 1");
        for (String name : List.of("gr_flaky", "gr_after")) {
            run("ALTER TASK " + name + " RESUME");
        }
        run("EXECUTE TASK gr_root");
        Map<String, RunState> states = awaitGraphRun("gr_root");
        Assertions.assertEquals(RunState.FAILED, states.get("gr_flaky"), states.toString());
        Assertions.assertEquals(RunState.SKIPPED, states.get("gr_after"), states.toString());

        List<TaskRunStatus> flaky = awaitHistory("gr_flaky", 3);
        Assertions.assertEquals(3, flaky.size(), "the first run and 2 retries: " + flaky);
        Assertions.assertTrue(flaky.stream().allMatch(s -> s.getState() == Constants.TaskRunState.FAILED), flaky.toString());
        List<TaskRunStatus> skipped = awaitHistory("gr_after", 1);
        Assertions.assertEquals(1, skipped.size(), skipped.toString());
        Assertions.assertEquals(Constants.TaskRunState.SKIPPED, skipped.get(0).getState());
        Assertions.assertTrue(skipped.get(0).getErrorMessage().contains("PREDECESSORS_STATE"),
                skipped.get(0).getErrorMessage());

        // a root that fails is retried too
        run("CREATE TASK gr_root2 TASK_AUTO_RETRY_ATTEMPTS = 1 AS CREATE VIEW gr_root2 AS SELECT k FROM gr_missing");
        run("EXECUTE TASK gr_root2");
        Assertions.assertEquals(2, awaitHistory("gr_root2", 2).size());
        run("ALTER TASK gr_root2 UNSET TASK_AUTO_RETRY_ATTEMPTS");
        Assertions.assertNull(getTask("gr_root2").getTaskOption(UserTaskSpec.TASK_AUTO_RETRY_ATTEMPTS));
    }

    @Test
    public void testReturnValues() throws Exception {
        run("CREATE TASK gv_root AS $$\n" +
                "  CREATE VIEW gv_r1 AS SELECT 1;\n" +
                "  CALL SYSTEM$SET_RETURN_VALUE('it''s 42');\n" +
                "$$");
        run("CREATE TASK gv_child AFTER gv_root AS $$\n" +
                "  CREATE VIEW gv_c1 AS SELECT SYSTEM$GET_PREDECESSOR_RETURN_VALUE() AS v;\n" +
                "  CALL SYSTEM$SET_RETURN_VALUE('from child');\n" +
                "$$");
        run("CREATE TASK gv_fin FINALIZE = gv_root AS\n" +
                "  CREATE VIEW gv_f1 AS SELECT SYSTEM$GET_PREDECESSOR_RETURN_VALUE('gv_child') AS v");
        run("CREATE TASK gv_bad AFTER gv_root AS CREATE VIEW gv_b1 AS SELECT SYSTEM$GET_PREDECESSOR_RETURN_VALUE('nope')");
        for (String name : List.of("gv_child", "gv_fin", "gv_bad")) {
            run("ALTER TASK " + name + " RESUME");
        }
        run("EXECUTE TASK gv_root");
        Map<String, RunState> states = awaitGraphRun("gv_root");
        Assertions.assertEquals(RunState.SUCCEEDED, states.get("gv_child"), states.toString());
        Assertions.assertEquals(RunState.SUCCEEDED, states.get("gv_fin"), states.toString());
        Assertions.assertEquals(RunState.FAILED, states.get("gv_bad"), states.toString());

        View child = (View) starRocksAssert.getTable("ugraph", "gv_c1");
        Assertions.assertTrue(child.getInlineViewDef().contains("42"), child.getInlineViewDef());
        View fin = (View) starRocksAssert.getTable("ugraph", "gv_f1");
        Assertions.assertTrue(fin.getInlineViewDef().contains("from child"), fin.getInlineViewDef());
        Assertions.assertEquals("it's 42", awaitHistory("gv_root", 1).get(0).getExtraMessage());
        Assertions.assertTrue(awaitHistory("gv_bad", 1).get(0).getErrorMessage().contains("not a predecessor"));

        // outside of a graph run
        run("CREATE TASK gv_alone AS CREATE VIEW gv_a1 AS SELECT SYSTEM$GET_PREDECESSOR_RETURN_VALUE()");
        run("EXECUTE TASK gv_alone");
        Assertions.assertTrue(awaitHistory("gv_alone", 1).get(0).getErrorMessage().contains("only available"));
    }

    @Test
    public void testBindPredecessorReturnValues() throws Exception {
        Assertions.assertEquals("SELECT 'a''b', NULL, 'SYSTEM$GET_PREDECESSOR_RETURN_VALUE()'",
                ScriptTaskRunProcessor.bindPredecessorReturnValues(
                        "SELECT SYSTEM$GET_PREDECESSOR_RETURN_VALUE('t1'), system$get_predecessor_return_value(), "
                                + "'SYSTEM$GET_PREDECESSOR_RETURN_VALUE()'",
                        name -> "t1".equals(name) ? "a'b" : null));
        Assertions.assertThrows(StarRocksException.class, () -> ScriptTaskRunProcessor.bindPredecessorReturnValues(
                "SELECT SYSTEM$GET_PREDECESSOR_RETURN_VALUE(1)", name -> null));
    }

    @Test
    public void testPredecessorsState() {
        // succeeded, failed, skipped
        Assertions.assertTrue(PredecessorsState.ALL_SUCCEEDED.test(2, 0, 0));
        Assertions.assertFalse(PredecessorsState.ALL_SUCCEEDED.test(1, 0, 1));
        Assertions.assertTrue(PredecessorsState.AT_LEAST_ONE_FAILED.test(1, 1, 0));
        Assertions.assertFalse(PredecessorsState.AT_LEAST_ONE_FAILED.test(1, 0, 1));
        Assertions.assertTrue(PredecessorsState.ALL_FAILED.test(0, 2, 0));
        Assertions.assertFalse(PredecessorsState.ALL_FAILED.test(0, 1, 1));
        Assertions.assertTrue(PredecessorsState.ALL_DONE.test(0, 1, 1));
        Assertions.assertTrue(PredecessorsState.NONE_FAILED_MIN_ONE_SUCCEEDED.test(1, 0, 1));
        Assertions.assertFalse(PredecessorsState.NONE_FAILED_MIN_ONE_SUCCEEDED.test(0, 0, 2));
        Assertions.assertFalse(PredecessorsState.NONE_FAILED_MIN_ONE_SUCCEEDED.test(1, 1, 0));
    }
}
