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

package com.starrocks.qe;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.procedure.StarRocksScriptSession;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.MergeIntoOlapAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.InsertMultiTableStmt;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.ast.txn.BeginStmt;
import com.starrocks.sql.ast.txn.CommitStmt;
import com.starrocks.sql.ast.txn.RollbackStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.transaction.TransactionStmtExecutor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Runs {@code INSERT ALL | FIRST} as one {@code INSERT INTO t SELECT ... UNION ALL ...} per target table, in one
 * transaction: the caller's explicit transaction, or one of its own when it writes more than one table. A table
 * is written once only, as a transaction can load a table once.
 */
public class InsertMultiTableExecutor {
    private static final String SOURCE = "`__insert_source`";

    private InsertMultiTableExecutor() {
    }

    public static void execute(InsertMultiTableStmt stmt, ConnectContext caller) {
        List<Target> targets = targets(stmt, caller);
        List<String> inserts = targets.stream().map(t -> toSql(stmt, t)).collect(Collectors.toList());

        ConnectContext ctx = SimpleExecutor.createCallerQueryContext(caller,
                Math.max(1, SimpleExecutor.remainingQueryTimeoutS(caller)));
        ctx.setCurrentCatalog(caller.getCurrentCatalog());
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        // The statements run with the caller's settings: strict mode, filter ratio, timeouts and so on.
        ctx.setSessionVariable(caller.getSessionVariable().clone());
        ctx.getSessionVariable().setEnableShortCircuit(false);
        long callerTxnId = caller.getTxnId();
        ctx.setTxnId(callerTxnId);
        boolean ownTxn = callerTxnId == 0 && inserts.size() > 1;
        StarRocksScriptSession session = new StarRocksScriptSession(ctx, "INSERT ALL", 1);

        long rows = 0;
        int done = 0;
        List<String> counts = new ArrayList<>();
        try {
            ctx.setThreadLocalInfo();
            if (ownTxn) {
                TransactionStmtExecutor.beginStmt(ctx, new BeginStmt(NodePosition.ZERO));
                checkState(ctx);
            }
            for (int i = 0; i < inserts.size(); i++) {
                long n = session.execute(inserts.get(i));
                rows += n;
                done++;
                counts.add(targets.get(i).into.getTableSql() + ": " + n);
            }
            if (ownTxn) {
                ctx.getState().reset();
                TransactionStmtExecutor.commitStmt(ctx, new CommitStmt(NodePosition.ZERO));
                checkState(ctx);
            }
        } catch (RuntimeException e) {
            String message = e instanceof SemanticException se ? se.getDetailMsg()
                    : e.getMessage() == null ? e.toString() : e.getMessage();
            if (ownTxn && ctx.getTxnId() != 0) {
                rollback(ctx);
            } else if (callerTxnId != 0 && done > 0) {
                // Part of the rows is loaded in the caller's transaction already and cannot be taken back alone.
                rollback(ctx);
                caller.setTxnId(0);
                message += ". The transaction was rolled back";
            }
            throw new SemanticException(message);
        } finally {
            caller.setThreadLocalInfo();
        }
        caller.getState().setOk(rows, 0, "{" + String.join(", ", counts) + "}");
    }

    private static void checkState(ConnectContext ctx) {
        if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            throw new IllegalStateException(ctx.getState().getErrorMessage());
        }
    }

    private static void rollback(ConnectContext ctx) {
        try {
            TransactionStmtExecutor.rollbackStmt(ctx, new RollbackStmt(NodePosition.ZERO));
        } catch (RuntimeException ignored) {
            // the original error is reported
        }
        ctx.setTxnId(0);
    }

    /**
     * The INTO clauses of one target table, each with the branch it belongs to and its values for the columns of
     * the table's INSERT.
     */
    @VisibleForTesting
    public static final class Target {
        final InsertMultiTableStmt.Into into;
        /** null: every column, as no INTO clause of the table lists its columns */
        List<String> columns;
        final List<Integer> branches = new ArrayList<>();
        final List<InsertMultiTableStmt.Into> intos = new ArrayList<>();
        /** per INTO clause, one value for each of the columns; null: the columns of the query */
        final List<List<String>> values = new ArrayList<>();

        Target(InsertMultiTableStmt.Into into) {
            this.into = into;
        }
    }

    @VisibleForTesting
    public static List<Target> targets(InsertMultiTableStmt stmt, ConnectContext ctx) {
        Map<String, Target> targets = new LinkedHashMap<>();
        Map<Target, List<List<String>>> columnsOfInto = new LinkedHashMap<>();
        for (int b = 0; b < stmt.getBranches().size(); b++) {
            for (InsertMultiTableStmt.Into into : stmt.getBranches().get(b).getIntos()) {
                List<String> columns = into.getColumns();
                List<String> values = into.getValueSqls();
                if (values != null && values.contains(null)) {
                    if (columns == null) {
                        throw new SemanticException("INTO %s: VALUES with DEFAULT needs a column list",
                                into.getTableSql());
                    }
                    List<String> keptColumns = new ArrayList<>();
                    List<String> keptValues = new ArrayList<>();
                    for (int i = 0; i < values.size(); i++) {
                        if (values.get(i) != null) {
                            keptColumns.add(columns.get(i));
                            keptValues.add(values.get(i));
                        }
                    }
                    if (keptColumns.isEmpty()) {
                        throw new SemanticException("INTO %s: at least one value must not be DEFAULT",
                                into.getTableSql());
                    }
                    columns = keptColumns;
                    values = keptValues;
                }
                Target target = targets.computeIfAbsent(tableKey(into.getTableNameParts(), ctx), k -> new Target(into));
                target.branches.add(b);
                target.intos.add(into);
                target.values.add(values);
                columnsOfInto.computeIfAbsent(target, t -> new ArrayList<>()).add(columns);
            }
        }
        for (Map.Entry<Target, List<List<String>>> entry : columnsOfInto.entrySet()) {
            alignColumns(entry.getKey(), entry.getValue(), ctx);
        }
        return new ArrayList<>(targets.values());
    }

    /**
     * A table is written by one INSERT, which writes every column that an INTO clause of the table lists. An INTO
     * clause that leaves out one of them writes its default value, like an INSERT that leaves it out.
     */
    private static void alignColumns(Target target, List<List<String>> columnsOfInto, ConnectContext ctx) {
        String table = target.into.getTableSql();
        long listed = columnsOfInto.stream().filter(c -> c != null).count();
        if (listed == 0) {
            return;
        }
        if (listed != columnsOfInto.size()) {
            throw new SemanticException("Every INTO %s must list its columns, or none of them, "
                    + "as the table is written by one INSERT", table);
        }
        Map<String, String> union = new LinkedHashMap<>();
        for (List<String> columns : columnsOfInto) {
            for (String column : columns) {
                union.putIfAbsent(column.toLowerCase(Locale.ROOT), column);
            }
        }
        target.columns = new ArrayList<>(union.values());
        Table resolved = null;
        for (int i = 0; i < columnsOfInto.size(); i++) {
            List<String> lowered = columnsOfInto.get(i).stream().map(c -> c.toLowerCase(Locale.ROOT))
                    .collect(Collectors.toList());
            if (lowered.stream().distinct().count() != lowered.size()) {
                throw new SemanticException("INTO %s lists a column twice", table);
            }
            List<String> values = target.values.get(i);
            if (values == null) {
                if (!lowered.equals(new ArrayList<>(union.keySet()))) {
                    throw new SemanticException("Every INTO %s without VALUES must list the same columns in the same "
                            + "order as the other INTO clauses of the table", table);
                }
                continue;
            }
            List<String> aligned = new ArrayList<>();
            for (String column : union.keySet()) {
                int at = lowered.indexOf(column);
                if (at >= 0) {
                    aligned.add(values.get(at));
                    continue;
                }
                if (resolved == null) {
                    resolved = resolveTable(target.into, ctx);
                }
                aligned.add(defaultSql(resolved, union.get(column), table));
            }
            target.values.set(i, aligned);
        }
    }

    private static Table resolveTable(InsertMultiTableStmt.Into into, ConnectContext ctx) {
        List<String> parts = into.getTableNameParts();
        String catalog = parts.size() == 3 ? parts.get(0) : ctx.getCurrentCatalog();
        String db = parts.size() >= 2 ? parts.get(parts.size() - 2) : ctx.getDatabase();
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(ctx, catalog, db, parts.get(parts.size() - 1));
        if (table == null) {
            throw new SemanticException("Table %s is not found", into.getTableSql());
        }
        return table;
    }

    private static String defaultSql(Table table, String columnName, String tableSql) {
        Column column = table.getColumn(columnName);
        if (column == null) {
            throw new SemanticException("Unknown column '%s' in '%s'", columnName, tableSql);
        }
        if (column.isAutoIncrement()) {
            throw new SemanticException("Every INTO %s must give a value for the AUTO_INCREMENT column '%s', "
                    + "as one of them does", tableSql, column.getName());
        }
        return ExprToSql.toSql(MergeIntoOlapAnalyzer.defaultValue(column, "INTO " + tableSql));
    }

    private static String tableKey(List<String> parts, ConnectContext ctx) {
        String catalog = parts.size() == 3 ? parts.get(0) : ctx.getCurrentCatalog();
        String db = parts.size() >= 2 ? parts.get(parts.size() - 2) : ctx.getDatabase();
        return (catalog + "\u0000" + db).toLowerCase(Locale.ROOT) + "\u0000" + parts.get(parts.size() - 1);
    }

    /**
     * {@code INSERT INTO t (columns) WITH source AS (query) SELECT values FROM source WHERE predicate UNION ALL ...},
     * one SELECT per INTO clause.
     */
    @VisibleForTesting
    static String toSql(InsertMultiTableStmt stmt, Target target) {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(target.into.getTableSql());
        if (target.columns != null) {
            sql.append(" (").append(target.columns.stream().map(InsertMultiTableExecutor::quote)
                    .collect(Collectors.joining(", "))).append(")");
        }
        sql.append(" WITH ").append(SOURCE).append(" AS (SELECT * FROM (").append(stmt.getQuerySql())
                .append(") `__insert_query`) ");
        for (int i = 0; i < target.branches.size(); i++) {
            if (i > 0) {
                sql.append(" UNION ALL ");
            }
            List<String> values = target.values.get(i);
            sql.append("SELECT ").append(values == null ? "*" : String.join(", ", values)).append(" FROM ")
                    .append(SOURCE);
            String predicate = predicate(stmt, target.branches.get(i));
            if (predicate != null) {
                sql.append(" WHERE ").append(predicate);
            }
        }
        return sql.toString();
    }

    /** The rows of a branch: its condition, and for INSERT FIRST and ELSE, no condition before it holds. */
    private static String predicate(InsertMultiTableStmt stmt, int branch) {
        List<InsertMultiTableStmt.Branch> branches = stmt.getBranches();
        InsertMultiTableStmt.Branch current = branches.get(branch);
        List<String> parts = new ArrayList<>();
        if (current.getConditionSql() != null) {
            parts.add("coalesce((" + current.getConditionSql() + "), false)");
        }
        if (stmt.isFirst() || current.isElse()) {
            for (int i = 0; i < branch; i++) {
                parts.add("NOT coalesce((" + branches.get(i).getConditionSql() + "), false)");
            }
        }
        return parts.isEmpty() ? null : String.join(" AND ", parts);
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }
}
