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

import com.starrocks.sql.parser.NodePosition;

/**
 * {@code RESTORE TABLE t TO TIMESTAMP AS OF expr} (or {@code TO AT(...)} / {@code TO BEFORE(...)}): writes the rows the
 * table had at that point as a new version of the table. Keeps the SQL text of the point, which the statements it
 * runs read the table with.
 */
public class RestoreTableStmt extends StatementBase {
    private final TableRef tableRef;
    /** the time travel clause that reads the table at the point, such as {@code FOR TIMESTAMP AS OF '...'} */
    private final String timeTravelSql;

    public RestoreTableStmt(TableRef tableRef, String timeTravelSql, NodePosition pos) {
        super(pos);
        this.tableRef = tableRef;
        this.timeTravelSql = timeTravelSql;
    }

    public TableRef getTableRef() {
        return tableRef;
    }

    public String getTimeTravelSql() {
        return timeTravelSql;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitRestoreTableStatement(this, context);
    }
}
