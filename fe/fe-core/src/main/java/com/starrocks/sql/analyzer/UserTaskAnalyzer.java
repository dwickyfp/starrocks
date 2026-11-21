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

package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.UserTaskSpec;
import com.starrocks.sql.ast.TaskName;
import com.starrocks.sql.ast.task.AlterTaskOptionsStmt;
import com.starrocks.sql.ast.task.CreateTaskStmt;

/**
 * Checks the Snowflake style task statements before they run. Names in the task body are not resolved, since a
 * statement of a script may use an object an earlier statement creates; they are checked when the task runs.
 */
public class UserTaskAnalyzer {

    public static void analyze(CreateTaskStmt stmt, ConnectContext context) {
        checkTaskName(stmt.getTaskName());
        if (Strings.isNullOrEmpty(stmt.getTaskName().getDbName()) && Strings.isNullOrEmpty(context.getDatabase())) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
        }
        // Apply the options to a scratch task only to validate them.
        Task scratch = new Task(stmt.getTaskName().getName());
        scratch.setSource(Constants.TaskSource.USER_SQL);
        UserTaskSpec.applyOptions(scratch, stmt.getOptions());
        UserTaskSpec.parseBody(stmt.getBody(), context.getSessionVariable());
    }

    public static void analyze(AlterTaskOptionsStmt stmt, ConnectContext context) {
        checkTaskName(stmt.getTaskName());
        switch (stmt.getAction()) {
            case SET:
                Task scratch = new Task(stmt.getTaskName().getName());
                scratch.setSource(Constants.TaskSource.USER_SQL);
                UserTaskSpec.applyOptions(scratch, stmt.getOptions());
                break;
            case UNSET:
                if (stmt.getUnsetOptions().isEmpty()) {
                    throw new SemanticException("ALTER TASK UNSET needs at least one option");
                }
                break;
            case MODIFY_AS:
                UserTaskSpec.parseBody(stmt.getBody(), context.getSessionVariable());
                break;
            default:
                throw new SemanticException("Unsupported alter task action: " + stmt.getAction());
        }
    }

    public static void checkTaskName(TaskName taskName) {
        if (taskName == null || Strings.isNullOrEmpty(taskName.getName())) {
            throw new SemanticException("Task name is empty");
        }
    }
}
