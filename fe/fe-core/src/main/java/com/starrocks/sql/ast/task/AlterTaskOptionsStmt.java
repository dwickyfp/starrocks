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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.TaskName;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

/**
 * Snowflake style task changes: {@code ALTER TASK name SET option = value ...},
 * {@code ALTER TASK name UNSET option, ...} and {@code ALTER TASK name MODIFY AS body}.
 */
public class AlterTaskOptionsStmt extends DdlStmt {
    public enum Action {
        SET,
        UNSET,
        MODIFY_AS
    }

    private final TaskName taskName;
    private final boolean ifExists;
    private final Action action;
    private final Map<String, String> options;
    private final List<String> unsetOptions;
    private final String body;

    public AlterTaskOptionsStmt(TaskName taskName, boolean ifExists, Action action, Map<String, String> options,
                                List<String> unsetOptions, String body, NodePosition pos) {
        super(pos);
        this.taskName = taskName;
        this.ifExists = ifExists;
        this.action = action;
        this.options = options == null ? ImmutableMap.of() : ImmutableMap.copyOf(options);
        this.unsetOptions = unsetOptions == null ? ImmutableList.of() : ImmutableList.copyOf(unsetOptions);
        this.body = body;
    }

    public TaskName getTaskName() {
        return taskName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public Action getAction() {
        return action;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public List<String> getUnsetOptions() {
        return unsetOptions;
    }

    public String getBody() {
        return body;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitAlterTaskOptionsStatement(this, context);
    }
}
