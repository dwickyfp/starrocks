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
package com.starrocks.sql.ast.stream;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

/**
 * {@code CREATE [OR REPLACE] STREAM [IF NOT EXISTS] name ON TABLE t [AT | BEFORE (...)] [APPEND_ONLY = TRUE]
 * [SHOW_INITIAL_ROWS = TRUE] [COMMENT '...']}.
 */
public class CreateTableStreamStmt extends DdlStmt {
    private final QualifiedName name;
    private final QualifiedName tableName;
    private final boolean orReplace;
    private final boolean ifNotExists;
    // the point in time of AT / BEFORE, or null
    private final Expr offsetPoint;
    // option name in upper case -> value
    private final Map<String, String> options;
    private final String comment;

    public CreateTableStreamStmt(QualifiedName name, QualifiedName tableName, boolean orReplace, boolean ifNotExists,
                                 Expr offsetPoint, Map<String, String> options, String comment, NodePosition pos) {
        super(pos);
        this.name = name;
        this.tableName = tableName;
        this.orReplace = orReplace;
        this.ifNotExists = ifNotExists;
        this.offsetPoint = offsetPoint;
        this.options = options;
        this.comment = comment;
    }

    public QualifiedName getName() {
        return name;
    }

    public QualifiedName getTableName() {
        return tableName;
    }

    public boolean isOrReplace() {
        return orReplace;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public Expr getOffsetPoint() {
        return offsetPoint;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitCreateTableStreamStatement(this, context);
    }
}
