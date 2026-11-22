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

package com.starrocks.sql.ast;

import com.google.common.collect.ImmutableList;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

/**
 * Snowflake's multi-table insert, {@code INSERT ALL | FIRST ... <query>}. It keeps the SQL text of its parts and
 * runs as one {@code INSERT INTO ... SELECT} per target table, see {@code InsertMultiTableExecutor}.
 */
public class InsertMultiTableStmt extends StatementBase {
    /** One {@code INTO table [(columns)] [VALUES (...)]}; a null value stands for DEFAULT. */
    public static final class Into {
        private final List<String> tableNameParts;
        private final String tableSql;
        private final List<String> columns;
        private final List<String> valueSqls;

        public Into(List<String> tableNameParts, String tableSql, List<String> columns, List<String> valueSqls) {
            this.tableNameParts = ImmutableList.copyOf(tableNameParts);
            this.tableSql = tableSql;
            this.columns = columns;
            this.valueSqls = valueSqls;
        }

        public List<String> getTableNameParts() {
            return tableNameParts;
        }

        public String getTableSql() {
            return tableSql;
        }

        /** The column names, or null to write the columns of the table in order. */
        public List<String> getColumns() {
            return columns;
        }

        /** The value expressions, null entries for DEFAULT, or null to write the columns of the query. */
        public List<String> getValueSqls() {
            return valueSqls;
        }
    }

    /** {@code WHEN condition THEN INTO ...}, {@code ELSE INTO ...} (null condition), or the INTO list of INSERT ALL. */
    public static final class Branch {
        private final String conditionSql;
        private final boolean isElse;
        private final List<Into> intos;

        public Branch(String conditionSql, boolean isElse, List<Into> intos) {
            this.conditionSql = conditionSql;
            this.isElse = isElse;
            this.intos = ImmutableList.copyOf(intos);
        }

        public String getConditionSql() {
            return conditionSql;
        }

        public boolean isElse() {
            return isElse;
        }

        public List<Into> getIntos() {
            return intos;
        }
    }

    private final boolean first;
    private final List<Branch> branches;
    private final String querySql;

    public InsertMultiTableStmt(boolean first, List<Branch> branches, String querySql, NodePosition pos) {
        super(pos);
        this.first = first;
        this.branches = ImmutableList.copyOf(branches);
        this.querySql = querySql;
    }

    /** INSERT FIRST: a row goes to the INTO clauses of the first WHEN clause it satisfies only. */
    public boolean isFirst() {
        return first;
    }

    public List<Branch> getBranches() {
        return branches;
    }

    public String getQuerySql() {
        return querySql;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitInsertMultiTableStatement(this, context);
    }
}
