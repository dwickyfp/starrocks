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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.TaskName;
import com.starrocks.sql.ast.task.AlterTaskOptionsStmt;
import com.starrocks.sql.ast.task.CreateTaskStmt;
import com.starrocks.sql.ast.task.DescTaskStmt;
import com.starrocks.sql.ast.task.ExecuteTaskStmt;
import com.starrocks.sql.ast.task.ShowTasksStmt;
import com.starrocks.type.TypeFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Executes the Snowflake style task statements: CREATE TASK, ALTER TASK SET / UNSET / MODIFY AS, EXECUTE TASK,
 * SHOW TASKS and DESC TASK. SUSPEND, RESUME and DROP use the existing ALTER TASK and DROP TASK paths.
 */
public final class UserTaskExecutor {
    public static final ShowResultSetMetaData TASK_META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("CREATED_ON", TypeFactory.createVarcharType(32)))
            .addColumn(new Column("NAME", TypeFactory.createVarcharType(256)))
            .addColumn(new Column("DATABASE_NAME", TypeFactory.createVarcharType(256)))
            .addColumn(new Column("OWNER", TypeFactory.createVarcharType(256)))
            .addColumn(new Column("COMMENT", TypeFactory.createVarcharType(1024)))
            .addColumn(new Column("WAREHOUSE", TypeFactory.createVarcharType(256)))
            .addColumn(new Column("SCHEDULE", TypeFactory.createVarcharType(256)))
            .addColumn(new Column("STATE", TypeFactory.createVarcharType(32)))
            .addColumn(new Column("DEFINITION", TypeFactory.createVarcharType(65535)))
            .addColumn(new Column("SOURCE", TypeFactory.createVarcharType(32)))
            .addColumn(new Column("LAST_SCHEDULED_ON", TypeFactory.createVarcharType(32)))
            .addColumn(new Column("NEXT_SCHEDULED_ON", TypeFactory.createVarcharType(32)))
            .addColumn(new Column("OPTIONS", TypeFactory.createVarcharType(65535)))
            .build();

    public static final ShowResultSetMetaData EXECUTE_META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("TaskName", TypeFactory.createVarcharType(256)))
            .addColumn(new Column("QueryId", TypeFactory.createVarcharType(64)))
            .addColumn(new Column("Status", TypeFactory.createVarcharType(16)))
            .build();

    private UserTaskExecutor() {
    }

    public static void createTask(CreateTaskStmt stmt, ConnectContext ctx) throws DdlException {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        String name = stmt.getTaskName().getName();
        Task existing = taskManager.getTask(name);
        if (existing != null && stmt.isIfNotExists()) {
            return;
        }
        if (existing != null && !stmt.isOrReplace()) {
            throw new DdlException("Task [" + name + "] already exists");
        }
        if (existing != null && !existing.isUserSqlTask()) {
            throw new DdlException("Task [" + name + "] was not created by CREATE TASK and cannot be replaced");
        }

        Task task = new Task(name);
        task.setSource(Constants.TaskSource.USER_SQL);
        task.setCatalogName(ctx.getCurrentCatalog());
        task.setDbName(resolveDbName(stmt.getTaskName(), ctx));
        task.setDefinition(stmt.getBody());
        task.setType(Constants.TaskType.MANUAL);
        task.setState(Constants.TaskState.PAUSE);
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_WAREHOUSE, GlobalStateMgr.getCurrentState().getWarehouseMgr()
                .getWarehouse(ctx.getCurrentWarehouseId()).getName());
        task.setProperties(properties);
        UserTaskSpec.applyOptions(task, stmt.getOptions());
        UserTaskSpec.parseBody(task.getDefinition(), ctx.getSessionVariable());
        task.setCreateUser(ctx.getCurrentUserIdentity().getUser());
        task.setUserIdentity(ctx.getCurrentUserIdentity());

        if (existing == null) {
            taskManager.createTask(task);
            return;
        }
        // CREATE OR REPLACE keeps the task (and its run history) but leaves it suspended, as Snowflake does.
        if (existing.getState() == Constants.TaskState.ACTIVE) {
            taskManager.suspendTask(existing, false);
        }
        taskManager.replaceUserTask(existing, task);
    }

    public static void alterTask(AlterTaskOptionsStmt stmt, ConnectContext ctx) throws DdlException {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        String name = stmt.getTaskName().getName();
        Task current = taskManager.getTask(name);
        if (current == null) {
            if (stmt.isIfExists()) {
                return;
            }
            throw new SemanticException("Task " + name + " is not exist");
        }
        if (!current.isUserSqlTask()) {
            throw new DdlException("Task [" + name + "] was not created by CREATE TASK, use ALTER TASK " + name
                    + " SET (\"key\" = \"value\") to change its properties");
        }
        Task changed = new Task(name);
        changed.setSource(Constants.TaskSource.USER_SQL);
        changed.applyUserTaskDefinition(current);
        switch (stmt.getAction()) {
            case SET:
                UserTaskSpec.applyOptions(changed, stmt.getOptions());
                break;
            case UNSET:
                UserTaskSpec.unsetOptions(changed, stmt.getUnsetOptions());
                break;
            case MODIFY_AS:
                UserTaskSpec.parseBody(stmt.getBody(), ctx.getSessionVariable());
                changed.setDefinition(stmt.getBody());
                break;
            default:
                throw new SemanticException("Unsupported alter task action: " + stmt.getAction());
        }
        taskManager.replaceUserTask(current, changed);
    }

    public static ShowResultSet executeTask(ExecuteTaskStmt stmt) {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        String name = stmt.getTaskName().getName();
        if (taskManager.getTask(name) == null) {
            throw new SemanticException("Task " + name + " is not exist");
        }
        SubmitResult result = taskManager.executeTask(name);
        List<String> row = Lists.newArrayList(name, Strings.nullToEmpty(result.getQueryId()),
                result.getStatus().toString());
        return new ShowResultSet(EXECUTE_META_DATA, ImmutableList.of(row));
    }

    public static ShowResultSet showTasks(ShowTasksStmt stmt, ConnectContext ctx) {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        PatternMatcher matcher = stmt.getPattern() == null ? null :
                PatternMatcher.createMysqlPattern(stmt.getPattern(), false);
        List<List<String>> rows = Lists.newArrayList();
        taskManager.filterTasks(null).stream()
                .filter(task -> stmt.getDbName() == null || stmt.getDbName().equalsIgnoreCase(task.getDbName()))
                .filter(task -> matcher == null || matcher.match(task.getName()))
                .filter(task -> canSee(task, ctx))
                .sorted(Comparator.comparing(Task::getName))
                .forEach(task -> rows.add(toRow(task)));
        return new ShowResultSet(TASK_META_DATA, rows);
    }

    public static ShowResultSet descTask(DescTaskStmt stmt, ConnectContext ctx) {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        String name = stmt.getTaskName().getName();
        Task task = taskManager.getTask(name);
        if (task == null || !canSee(task, ctx)) {
            throw new SemanticException("Task " + name + " is not exist");
        }
        return new ShowResultSet(TASK_META_DATA, ImmutableList.of(toRow(task)));
    }

    /**
     * A user sees the tasks of the databases it has any privilege on, like information_schema.tasks, and its own.
     */
    private static boolean canSee(Task task, ConnectContext ctx) {
        UserIdentity current = ctx.getCurrentUserIdentity();
        if (current != null && current.equals(task.getUserIdentity())) {
            return true;
        }
        if (task.getDbName() == null) {
            return false;
        }
        try {
            Authorizer.checkAnyActionOnOrInDb(ctx, task.getCatalogName(), task.getDbName());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static List<String> toRow(Task task) {
        List<String> row = Lists.newArrayList();
        row.add(TimeUtils.longToTimeString(task.getCreateTime()));
        row.add(task.getName());
        row.add(Strings.nullToEmpty(task.getDbName()));
        row.add(task.getUserIdentity() != null ? task.getUserIdentity().toString() : Strings.nullToEmpty(task.getCreateUser()));
        row.add(Strings.nullToEmpty(task.getTaskOption(UserTaskSpec.COMMENT)));
        row.add(task.getProperties() == null ? "" :
                Strings.nullToEmpty(task.getProperties().get(PropertyAnalyzer.PROPERTIES_WAREHOUSE)));
        row.add(scheduleString(task));
        row.add(stateString(task));
        row.add(Strings.nullToEmpty(task.getDefinition()));
        row.add(task.getSource() == null ? "" : task.getSource().name());
        row.add(epochSecondsToString(task.getLastScheduleTime()));
        row.add(task.getType() == Constants.TaskType.PERIODICAL && task.getState() == Constants.TaskState.ACTIVE
                ? epochSecondsToString(task.getNextScheduleTime()) : "");
        Map<String, String> options = new HashMap<>(task.getTaskOptions());
        options.remove(UserTaskSpec.COMMENT);
        options.remove(UserTaskSpec.SCHEDULE);
        if (task.getProperties() != null) {
            task.getProperties().forEach((key, value) -> {
                if (!key.equals(PropertyAnalyzer.PROPERTIES_WAREHOUSE)) {
                    options.put(key, value);
                }
            });
        }
        row.add(options.isEmpty() ? "" : new TreeMap<>(options).toString());
        return row;
    }

    private static String scheduleString(Task task) {
        String schedule = task.getTaskOption(UserTaskSpec.SCHEDULE);
        if (schedule != null) {
            return schedule;
        }
        return task.getSchedule() == null ? "" : task.getSchedule().toString();
    }

    private static String stateString(Task task) {
        // Snowflake names: a task runs on its schedule when started, and only by EXECUTE TASK when suspended.
        if (task.getState() == Constants.TaskState.ACTIVE) {
            return "STARTED";
        } else if (task.getState() == Constants.TaskState.PAUSE) {
            return "SUSPENDED";
        }
        return task.getState() == null ? "" : task.getState().name();
    }

    private static String epochSecondsToString(long epochSeconds) {
        return epochSeconds > 0 ? TimeUtils.longToTimeString(epochSeconds * 1000) : "";
    }

    private static String resolveDbName(TaskName taskName, ConnectContext ctx) {
        String dbName = taskName.getDbName();
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = ctx.getDatabase();
        }
        if (Strings.isNullOrEmpty(dbName)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
        }
        if (GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(ctx, ctx.getCurrentCatalog(), dbName) == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        return dbName;
    }
}
