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

package com.starrocks.sql.ast.task;

import com.google.common.collect.ImmutableMap;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.TaskName;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

/**
 * Snowflake style task: {@code CREATE [OR REPLACE] TASK [IF NOT EXISTS] name [option = value ...] AS body}.
 * The body is kept as SQL text: one statement, or a script of statements separated by ';'.
 */
public class CreateTaskStmt extends DdlStmt {
    private final TaskName taskName;
    private final boolean orReplace;
    private final boolean ifNotExists;
    // option name (upper case) -> value, in the order written
    private final Map<String, String> options;
    private final String body;

    public CreateTaskStmt(TaskName taskName, boolean orReplace, boolean ifNotExists, Map<String, String> options,
                          String body, NodePosition pos) {
        super(pos);
        this.taskName = taskName;
        this.orReplace = orReplace;
        this.ifNotExists = ifNotExists;
        this.options = ImmutableMap.copyOf(options);
        this.body = body;
    }

    public TaskName getTaskName() {
        return taskName;
    }

    public boolean isOrReplace() {
        return orReplace;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public String getBody() {
        return body;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitCreateTaskStatement(this, context);
    }
}
