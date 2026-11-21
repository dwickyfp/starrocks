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

package com.starrocks.sql.ast.procedure;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

/**
 * {@code CREATE [OR REPLACE] PROCEDURE [IF NOT EXISTS] name(arg type [DEFAULT expr], ...) RETURNS type
 * LANGUAGE SQL [EXECUTE AS OWNER | CALLER] [COMMENT '...'] AS $$ body $$}.
 */
public class CreateProcedureStmt extends DdlStmt {

    /** One declared argument. The type and the default are kept as SQL text. */
    public static class Parameter {
        private final String name;
        private final String typeSql;
        private final String defaultSql;

        public Parameter(String name, String typeSql, String defaultSql) {
            this.name = name;
            this.typeSql = typeSql;
            this.defaultSql = defaultSql;
        }

        public String getName() {
            return name;
        }

        public String getTypeSql() {
            return typeSql;
        }

        public String getDefaultSql() {
            return defaultSql;
        }
    }

    private final QualifiedName name;
    private final boolean orReplace;
    private final boolean ifNotExists;
    private final List<Parameter> parameters;
    private final String returnTypeSql;
    private final boolean returnNotNull;
    private final String language;
    private final boolean executeAsCaller;
    private final String comment;
    private final String body;

    public CreateProcedureStmt(QualifiedName name, boolean orReplace, boolean ifNotExists, List<Parameter> parameters,
                               String returnTypeSql, boolean returnNotNull, String language, boolean executeAsCaller,
                               String comment, String body, NodePosition pos) {
        super(pos);
        this.name = name;
        this.orReplace = orReplace;
        this.ifNotExists = ifNotExists;
        this.parameters = parameters;
        this.returnTypeSql = returnTypeSql;
        this.returnNotNull = returnNotNull;
        this.language = language;
        this.executeAsCaller = executeAsCaller;
        this.comment = comment;
        this.body = body;
    }

    public QualifiedName getName() {
        return name;
    }

    public boolean isOrReplace() {
        return orReplace;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public String getReturnTypeSql() {
        return returnTypeSql;
    }

    public boolean isReturnNotNull() {
        return returnNotNull;
    }

    public String getLanguage() {
        return language;
    }

    public boolean isExecuteAsCaller() {
        return executeAsCaller;
    }

    public String getComment() {
        return comment;
    }

    public String getBody() {
        return body;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitCreateProcedureStatement(this, context);
    }
}
