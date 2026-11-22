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

package com.starrocks.sql.ast.policy;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

/**
 * The statements that change masking and row access policies and attach them to tables:
 * <ul>
 *     <li>{@code CREATE [OR REPLACE] MASKING | ROW ACCESS POLICY [IF NOT EXISTS] name AS (arg type, ...)
 *     RETURNS type -> body [COMMENT '...']}</li>
 *     <li>{@code ALTER MASKING | ROW ACCESS POLICY [IF EXISTS] name SET BODY -> body | SET COMMENT = '...' |
 *     RENAME TO name}</li>
 *     <li>{@code DROP MASKING | ROW ACCESS POLICY [IF EXISTS] name}</li>
 *     <li>{@code ALTER TABLE t MODIFY COLUMN c SET MASKING POLICY p [USING (c, ...)] [FORCE]}, {@code ... UNSET
 *     MASKING POLICY}</li>
 *     <li>{@code ALTER TABLE t ADD ROW ACCESS POLICY p ON (c, ...)}, {@code ALTER TABLE t DROP ROW ACCESS POLICY p},
 *     {@code ALTER TABLE t DROP ALL ROW ACCESS POLICIES}</li>
 * </ul>
 */
public class SecurityPolicyStmt extends DdlStmt {
    public enum Kind {
        MASKING("MASKING POLICY"),
        ROW_ACCESS("ROW ACCESS POLICY");

        private final String sql;

        Kind(String sql) {
            this.sql = sql;
        }

        /** {@code MASKING POLICY} or {@code ROW ACCESS POLICY}. */
        public String sql() {
            return sql;
        }
    }

    public enum Action {
        CREATE, SET_BODY, SET_COMMENT, RENAME, DROP, SET_MASKING, UNSET_MASKING, ADD_ROW_ACCESS, DROP_ROW_ACCESS,
        DROP_ALL_ROW_ACCESS
    }

    /** An argument of a policy: its name and the SQL of its type. */
    public record Arg(String name, String type) {
    }

    private final Action action;
    private final Kind kind;
    // the policy, or null for DROP ALL ROW ACCESS POLICIES and UNSET MASKING POLICY
    private final QualifiedName name;
    // OR REPLACE
    private final boolean orReplace;
    // IF NOT EXISTS, IF EXISTS
    private final boolean ifExists;
    private final List<Arg> args;
    private final String returnType;
    // the SQL text of the body
    private final String body;
    private final String comment;
    private final String newName;
    // the table, column and columns of ALTER TABLE
    private final QualifiedName table;
    private final String column;
    private final List<String> columns;
    private final boolean force;

    public SecurityPolicyStmt(Action action, Kind kind, QualifiedName name, boolean orReplace, boolean ifExists,
                              List<Arg> args, String returnType, String body, String comment, String newName,
                              QualifiedName table, String column, List<String> columns, boolean force,
                              NodePosition pos) {
        super(pos);
        this.action = action;
        this.kind = kind;
        this.name = name;
        this.orReplace = orReplace;
        this.ifExists = ifExists;
        this.args = args;
        this.returnType = returnType;
        this.body = body;
        this.comment = comment;
        this.newName = newName;
        this.table = table;
        this.column = column;
        this.columns = columns;
        this.force = force;
    }

    public Action getAction() {
        return action;
    }

    public Kind getKind() {
        return kind;
    }

    public QualifiedName getName() {
        return name;
    }

    public boolean isOrReplace() {
        return orReplace;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public List<Arg> getArgs() {
        return args;
    }

    public String getReturnType() {
        return returnType;
    }

    public String getBody() {
        return body;
    }

    public String getComment() {
        return comment;
    }

    public String getNewName() {
        return newName;
    }

    public QualifiedName getTable() {
        return table;
    }

    public String getColumn() {
        return column;
    }

    public List<String> getColumns() {
        return columns;
    }

    public boolean isForce() {
        return force;
    }

    /** Whether the statement attaches or detaches a policy (ALTER TABLE), not changes one. */
    public boolean isAttachment() {
        return table != null;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitSecurityPolicyStatement(this, context);
    }
}
