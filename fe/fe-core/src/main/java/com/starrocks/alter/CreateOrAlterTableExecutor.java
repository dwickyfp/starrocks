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

package com.starrocks.alter;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * CREATE OR ALTER TABLE, as in Snowflake: creates the table, or changes the table that exists to the definition --
 * the columns it adds and drops, the comments of its columns and its comment -- with the ALTER TABLE statements of
 * fast schema evolution, which keep its data. What it cannot change so is an error, and the table is left as it is:
 * the statements are planned and checked before the first one runs.
 */
public final class CreateOrAlterTableExecutor {
    private CreateOrAlterTableExecutor() {
    }

    public static void execute(CreateTableStmt stmt, ConnectContext context) throws StarRocksException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(stmt.getDbName());
        Table table = db == null ? null
                : GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), stmt.getTableName());
        if (table == null) {
            context.getGlobalStateMgr().getMetadataMgr().createTable(context, stmt);
            return;
        }
        List<String> statements = plan(stmt, table);
        List<StatementBase> parsed = new ArrayList<>();
        for (String sql : statements) {
            parsed.add(SqlParser.parse(sql, context.getSessionVariable()).get(0));
        }
        // the privileges of every statement before any runs
        for (StatementBase alter : parsed) {
            Authorizer.check(alter, context);
        }
        for (StatementBase alter : parsed) {
            Analyzer.analyze(alter, context);
            try {
                DDLStmtExecutor.execute(alter, context);
            } catch (StarRocksException | RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new DdlException(e.getMessage(), e);
            }
        }
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private static String literal(String text) {
        return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }

    private static List<Column> visible(List<Column> columns) {
        return columns.stream().filter(c -> !c.isHidden()).collect(Collectors.toList());
    }

    private static String key(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    /** The ALTER TABLE statements that change the table to the definition of {@code stmt}, analyzed. */
    static List<String> plan(CreateTableStmt stmt, Table table) {
        if (!(table instanceof OlapTable olap) || table.isMaterializedView() || !table.isNativeTable()) {
            throw new SemanticException("CREATE OR ALTER TABLE can change a native table only, and %s is a %s",
                    table.getName(), table.getType());
        }
        String name = quote(stmt.getDbName()) + "." + quote(table.getName());
        List<Column> current = visible(olap.getBaseSchema());
        List<Column> wanted = visible(stmt.getColumns());
        Map<String, String> sqls = new HashMap<>();
        for (int i = 0; i < stmt.getColumnDefs().size(); i++) {
            sqls.put(key(stmt.getColumnDefs().get(i).getName()), stmt.getColumnSqls().get(i));
        }
        Map<String, Column> currentByName = new HashMap<>();
        current.forEach(c -> currentByName.put(key(c.getName()), c));
        Map<String, Integer> wantedIndex = new HashMap<>();
        for (int i = 0; i < wanted.size(); i++) {
            wantedIndex.put(key(wanted.get(i).getName()), i);
        }

        if (stmt.getKeysDesc() != null && stmt.getKeysDesc().getKeysType() != olap.getKeysType()) {
            throw new SemanticException("CREATE OR ALTER TABLE cannot change table %s from %s to %s",
                    table.getName(), olap.getKeysType().toSql(), stmt.getKeysDesc().getKeysType().toSql());
        }
        List<String> currentKeys = current.stream().filter(Column::isKey).map(c -> key(c.getName())).toList();
        List<String> wantedKeys = wanted.stream().filter(Column::isKey).map(c -> key(c.getName())).toList();
        if (!currentKeys.equals(wantedKeys)) {
            throw new SemanticException("CREATE OR ALTER TABLE cannot change the key columns of table %s: they are %s",
                    table.getName(), current.stream().filter(Column::isKey).map(Column::getName).toList());
        }

        List<String> statements = new ArrayList<>();
        // the columns of the table that stay, in their order
        int last = -1;
        for (Column column : current) {
            Integer index = wantedIndex.get(key(column.getName()));
            if (index == null) {
                continue;
            }
            if (index < last) {
                throw new SemanticException("CREATE OR ALTER TABLE cannot reorder the columns of table %s: column %s "
                        + "must stay after the columns it follows", table.getName(), column.getName());
            }
            last = index;
            Column target = wanted.get(index);
            if (!column.equalsIgnoreComment(target)) {
                throw new SemanticException("CREATE OR ALTER TABLE cannot change column %s of table %s from %s to %s: "
                        + "it can add and drop columns and change comments; use ALTER TABLE MODIFY COLUMN",
                        column.getName(), table.getName(), describe(column), describe(target));
            }
        }
        for (Column column : current) {
            if (!wantedIndex.containsKey(key(column.getName()))) {
                statements.add("ALTER TABLE " + name + " DROP COLUMN " + quote(column.getName()));
            }
        }
        String previous = null;
        for (int i = 0; i < wanted.size(); i++) {
            Column column = wanted.get(i);
            Column existing = currentByName.get(key(column.getName()));
            if (existing == null) {
                statements.add("ALTER TABLE " + name + " ADD COLUMN " + sqls.get(key(column.getName()))
                        + (previous == null ? " FIRST" : " AFTER " + quote(previous)));
            } else if (!Objects.equals(nullToEmpty(existing.getComment()), nullToEmpty(column.getComment()))) {
                statements.add("ALTER TABLE " + name + " MODIFY COLUMN " + quote(existing.getName()) + " COMMENT "
                        + literal(nullToEmpty(column.getComment())));
            }
            previous = column.getName();
        }
        String comment = nullToEmpty(stmt.getComment());
        if (!comment.equals(nullToEmpty(table.getComment()))) {
            statements.add("ALTER TABLE " + name + " COMMENT = " + literal(comment));
        }
        return statements;
    }

    private static String nullToEmpty(String text) {
        return text == null ? "" : text;
    }

    private static String describe(Column column) {
        StringBuilder text = new StringBuilder(column.getType().toSql());
        if (column.getAggregationType() != null && !column.isAggregationTypeImplicit()) {
            text.append(' ').append(column.getAggregationType().toString());
        }
        text.append(column.isAllowNull() ? " NULL" : " NOT NULL");
        if (column.getDefaultValue() != null) {
            text.append(" DEFAULT ").append(literal(column.getDefaultValue()));
        } else if (column.getDefaultExpr() != null) {
            text.append(" DEFAULT ").append(column.getDefaultExpr().getExpr());
        }
        if (column.isGeneratedColumn()) {
            text.append(" AS ...");
        }
        return text.toString();
    }
}
