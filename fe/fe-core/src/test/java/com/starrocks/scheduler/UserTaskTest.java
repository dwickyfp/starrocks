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

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.scheduler.persist.TaskSchedule;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Snowflake style tasks: CREATE TASK, ALTER TASK SET / UNSET / MODIFY AS / RESUME / SUSPEND, EXECUTE TASK,
 * SHOW TASKS and DESC TASK, and running a multi statement body.
 */
public class UserTaskTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withDatabase("utask_other");
        starRocksAssert.withDatabase("utask").useDatabase("utask");
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

    private static boolean isScheduled(Task task) {
        return GlobalStateMgr.getCurrentState().getTaskManager().getPeriodFutureMap().containsKey(task.getId());
    }

    @Test
    public void testCreateTaskStartsSuspended() throws Exception {
        run("CREATE TASK ut_basic\n" +
                "  SCHEDULE = '5 MINUTE'\n" +
                "  COMMENT = 'two inserts'\n" +
                "  query_timeout = 100\n" +
                "  USER_TASK_TIMEOUT_MS = 60000\n" +
                "AS $$\n" +
                "  SET query_timeout = 120;\n" +
                "  CREATE TABLE IF NOT EXISTS ut_t1 (k int) DISTRIBUTED BY HASH(k) BUCKETS 1\n" +
                "    PROPERTIES('replication_num' = '1');\n" +
                "$$");
        Task task = getTask("ut_basic");
        Assertions.assertNotNull(task);
        Assertions.assertEquals(Constants.TaskSource.USER_SQL, task.getSource());
        Assertions.assertEquals(Constants.TaskState.PAUSE, task.getState());
        Assertions.assertEquals(Constants.TaskType.PERIODICAL, task.getType());
        Assertions.assertEquals("utask", task.getDbName());
        Assertions.assertEquals(5, task.getSchedule().getPeriod());
        Assertions.assertEquals(TimeUnit.MINUTES, task.getSchedule().getTimeUnit());
        Assertions.assertTrue(task.getDefinition().startsWith("SET query_timeout = 120;"), task.getDefinition());
        Assertions.assertTrue(task.getDefinition().contains("CREATE TABLE IF NOT EXISTS ut_t1"));
        Assertions.assertEquals("two inserts", task.getTaskOption(UserTaskSpec.COMMENT));
        Assertions.assertEquals("100", task.getProperties().get("query_timeout"));
        Assertions.assertEquals(60000, UserTaskSpec.getUserTaskTimeoutMs(task));
        Assertions.assertFalse(isScheduled(task), "a new task must not be scheduled before RESUME");

        run("ALTER TASK ut_basic RESUME");
        Assertions.assertEquals(Constants.TaskState.ACTIVE, task.getState());
        Assertions.assertTrue(isScheduled(task));

        run("ALTER TASK ut_basic SUSPEND");
        Assertions.assertEquals(Constants.TaskState.PAUSE, task.getState());
        Assertions.assertFalse(isScheduled(task));
    }

    @Test
    public void testSingleStatementBody() throws Exception {
        run("CREATE TASK ut_single AS INSERT INTO ut_missing SELECT 1");
        Task task = getTask("ut_single");
        Assertions.assertEquals("INSERT INTO ut_missing SELECT 1", task.getDefinition());
        Assertions.assertEquals(Constants.TaskType.MANUAL, task.getType());
        Assertions.assertEquals(Constants.TaskState.PAUSE, task.getState());
    }

    @Test
    public void testCronSchedule() throws Exception {
        run("CREATE TASK ut_cron SCHEDULE = 'USING CRON 30 2 * * * Asia/Jakarta' AS SELECT 1");
        Task task = getTask("ut_cron");
        TaskSchedule schedule = task.getSchedule();
        Assertions.assertTrue(schedule.isCron());
        Assertions.assertEquals("30 2 * * *", schedule.getCronExpression());
        Assertions.assertEquals("Asia/Jakarta", schedule.getCronTimeZone());

        run("ALTER TASK ut_cron RESUME");
        Assertions.assertTrue(isScheduled(task));
        ZonedDateTime next = ZonedDateTime.ofInstant(java.time.Instant.ofEpochSecond(task.getNextScheduleTime()),
                ZoneId.of("Asia/Jakarta"));
        Assertions.assertEquals(2, next.getHour());
        Assertions.assertEquals(30, next.getMinute());
        Assertions.assertTrue(next.toInstant().isAfter(java.time.Instant.now()));
        run("ALTER TASK ut_cron SUSPEND");
        Assertions.assertFalse(isScheduled(task));
    }

    @Test
    public void testAlterTaskNeedsSuspendedTask() throws Exception {
        run("CREATE TASK ut_alter SCHEDULE = '10 MINUTE' COMMENT = 'old' AS SELECT 1");
        Task task = getTask("ut_alter");
        long id = task.getId();
        run("ALTER TASK ut_alter RESUME");
        Exception e = Assertions.assertThrows(Exception.class, () -> run("ALTER TASK ut_alter SET COMMENT = 'new'"));
        Assertions.assertTrue(e.getMessage().contains("must be suspended"), e.getMessage());

        run("ALTER TASK ut_alter SUSPEND");
        run("ALTER TASK ut_alter SET COMMENT = 'new' SCHEDULE = 'USING CRON 0 * * * * UTC' query_timeout = 30");
        Assertions.assertEquals("new", task.getTaskOption(UserTaskSpec.COMMENT));
        Assertions.assertTrue(task.getSchedule().isCron());
        Assertions.assertEquals("30", task.getProperties().get("query_timeout"));

        run("ALTER TASK ut_alter MODIFY AS $$ SELECT 2; SELECT 3 $$");
        Assertions.assertEquals("SELECT 2; SELECT 3", task.getDefinition());

        run("ALTER TASK ut_alter UNSET COMMENT, SCHEDULE, query_timeout");
        Assertions.assertNull(task.getTaskOption(UserTaskSpec.COMMENT));
        Assertions.assertNull(task.getSchedule());
        Assertions.assertEquals(Constants.TaskType.MANUAL, task.getType());
        Assertions.assertNull(task.getProperties().get("query_timeout"));
        Assertions.assertEquals(id, getTask("ut_alter").getId());
    }

    @Test
    public void testCreateOrReplace() throws Exception {
        run("CREATE TASK ut_replace SCHEDULE = '10 MINUTE' AS SELECT 1");
        Task task = getTask("ut_replace");
        long id = task.getId();
        run("ALTER TASK ut_replace RESUME");

        Exception e = Assertions.assertThrows(Exception.class, () -> run("CREATE TASK ut_replace AS SELECT 2"));
        Assertions.assertTrue(e.getMessage().contains("already exists"), e.getMessage());
        run("CREATE TASK IF NOT EXISTS ut_replace AS SELECT 2");
        Assertions.assertEquals("SELECT 1", task.getDefinition());

        run("CREATE OR REPLACE TASK ut_replace AS SELECT 3");
        Task replaced = getTask("ut_replace");
        Assertions.assertEquals(id, replaced.getId());
        Assertions.assertEquals("SELECT 3", replaced.getDefinition());
        Assertions.assertEquals(Constants.TaskState.PAUSE, replaced.getState());
        Assertions.assertEquals(Constants.TaskType.MANUAL, replaced.getType());
        Assertions.assertFalse(isScheduled(replaced));
    }

    @Test
    public void testShowAndDescTasks() throws Exception {
        run("CREATE TASK ut_show_a SCHEDULE = '15 MINUTE' COMMENT = 'first' AS SELECT 1");
        run("CREATE TASK ut_show_b AS SELECT 2");
        ShowResultSet result = run("SHOW TASKS IN utask LIKE 'ut_show_%'");
        List<List<String>> rows = result.getResultRows();
        Assertions.assertEquals(2, rows.size());
        List<String> first = rows.get(0);
        Assertions.assertEquals("ut_show_a", first.get(1));
        Assertions.assertEquals("utask", first.get(2));
        Assertions.assertEquals("first", first.get(4));
        Assertions.assertEquals("15 MINUTE", first.get(6));
        Assertions.assertEquals("SUSPENDED", first.get(7));
        Assertions.assertEquals("SELECT 1", first.get(8));
        Assertions.assertEquals("USER_SQL", first.get(9));

        result = run("DESC TASK ut_show_b");
        Assertions.assertEquals(1, result.getResultRows().size());
        Assertions.assertEquals("SELECT 2", result.getResultRows().get(0).get(8));
        Assertions.assertThrows(Exception.class, () -> run("DESC TASK ut_show_missing"));
    }

    @Test
    public void testScriptRunsInOrderInOneSession() throws Exception {
        run("CREATE TASK ut_script AS $$\n" +
                "  CREATE TABLE ut_script_t (k int) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num' = '1');\n" +
                "  CREATE VIEW ut_script_v AS SELECT k FROM ut_script_t;\n" +
                "  USE utask_other;\n" +
                "  CREATE VIEW ut_script_v2 AS SELECT 1 AS one;\n" +
                "$$");
        Task task = getTask("ut_script");
        // EXECUTE TASK runs a suspended task.
        GlobalStateMgr.getCurrentState().getTaskManager().executeTaskSync(task);
        Assertions.assertNotNull(starRocksAssert.getTable("utask", "ut_script_t"));
        Assertions.assertNotNull(starRocksAssert.getTable("utask", "ut_script_v"));
        // USE in the script applies to the statements after it.
        Assertions.assertNotNull(starRocksAssert.getTable("utask_other", "ut_script_v2"));
        Assertions.assertEquals(Constants.TaskState.PAUSE, task.getState());
    }

    @Test
    public void testScriptStopsAtFirstFailedStatement() throws Exception {
        run("CREATE TASK ut_fail AS $$\n" +
                "  CREATE TABLE ut_fail_a (k int) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num' = '1');\n" +
                "  CREATE VIEW ut_fail_v AS SELECT k FROM ut_fail_missing;\n" +
                "  CREATE TABLE ut_fail_c (k int) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num' = '1');\n" +
                "$$");
        Task task = getTask("ut_fail");
        Exception e = Assertions.assertThrows(Exception.class,
                () -> GlobalStateMgr.getCurrentState().getTaskManager().executeTaskSync(task));
        Assertions.assertTrue(e.getMessage().contains("Statement 2 of 3 failed"), e.getMessage());
        Assertions.assertNotNull(starRocksAssert.getTable("utask", "ut_fail_a"));
        Assertions.assertNull(GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("utask", "ut_fail_c"),
                "statements after the failed one must not run");
    }

    @Test
    public void testExecuteTaskStatement() throws Exception {
        run("CREATE TASK ut_exec AS SELECT 1");
        ShowResultSet result = run("EXECUTE TASK ut_exec");
        Assertions.assertEquals("ut_exec", result.getResultRows().get(0).get(0));
        Assertions.assertEquals("SUBMITTED", result.getResultRows().get(0).get(2));
        Assertions.assertThrows(Exception.class, () -> run("EXECUTE TASK ut_exec_missing"));
    }

    @Test
    public void testInvalidTasks() {
        String[] bad = {
                "CREATE TASK ut_bad AS $$ $$",
                "CREATE TASK ut_bad AS $$ SELEC 1 $$",
                "CREATE TASK ut_bad NOT_AN_OPTION = 1 AS SELECT 1",
                "CREATE TASK ut_bad SCHEDULE = 'every day' AS SELECT 1",
                "CREATE TASK ut_bad SCHEDULE = '1 SECOND' AS SELECT 1",
                "CREATE TASK ut_bad SCHEDULE = 'USING CRON 0 0 * * UTC' AS SELECT 1",
                "CREATE TASK ut_bad SCHEDULE = 'USING CRON 0 0 1 * 1 UTC' AS SELECT 1",
                "CREATE TASK ut_bad SCHEDULE = 'USING CRON 0 0 * * * Mars/Base' AS SELECT 1",
                "CREATE TASK ut_bad USER_TASK_TIMEOUT_MS = -1 AS SELECT 1",
                "CREATE TASK ut_bad COMMENT = 'a' COMMENT = 'b' AS SELECT 1",
                "CREATE OR REPLACE TASK IF NOT EXISTS ut_bad AS SELECT 1",
                "CREATE TASK ut_bad AS $$ EXECUTE AS root WITH NO REVERT $$",
        };
        for (String sql : bad) {
            Assertions.assertThrows(Exception.class, () -> run(sql), sql);
        }
        Assertions.assertNull(getTask("ut_bad"));
    }

    @Test
    public void testUnixCronToQuartz() {
        ZoneId utc = ZoneId.of("UTC");
        Assertions.assertEquals("0 30 2 * * ?", UserTaskSpec.toQuartzCron("30 2 * * *", utc).getCronExpression());
        // Unix day of week 1-5 (Monday to Friday) is 2-6 in Quartz; 0 and 7 are both Sunday (1).
        Assertions.assertEquals("0 0 9 ? * 2-6", UserTaskSpec.toQuartzCron("0 9 * * 1-5", utc).getCronExpression());
        Assertions.assertEquals("0 0 9 ? * 1,1", UserTaskSpec.toQuartzCron("0 9 * * 0,7", utc).getCronExpression());
        Assertions.assertEquals("0 0 9 ? * MON-FRI",
                UserTaskSpec.toQuartzCron("0 9 * * MON-FRI", utc).getCronExpression());
        Assertions.assertEquals("0 */15 * 1 * ?", UserTaskSpec.toQuartzCron("*/15 * 1 * *", utc).getCronExpression());
        Assertions.assertThrows(Exception.class, () -> UserTaskSpec.toQuartzCron("0 9 * * 5-7", utc));

        // 02:30 every day in Jakarta (UTC+7) is 19:30 UTC the day before.
        TaskSchedule schedule = TaskSchedule.ofCron("30 2 * * *", "Asia/Jakarta");
        long after = ZonedDateTime.of(2026, 11, 21, 0, 0, 0, 0, utc).toInstant().toEpochMilli();
        long next = UserTaskSpec.nextCronFireTimeMs(schedule, after);
        Assertions.assertEquals(ZonedDateTime.of(2026, 11, 21, 19, 30, 0, 0, utc).toInstant().toEpochMilli(), next);
    }
}
