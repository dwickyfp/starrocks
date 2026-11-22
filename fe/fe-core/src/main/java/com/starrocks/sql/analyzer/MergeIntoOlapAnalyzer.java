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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TableName;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.load.Load;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.ColumnAssignment;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.MergeIntoStmt;
import com.starrocks.sql.ast.MergeWhenClause;
import com.starrocks.sql.ast.MergeWhenMatchedDeleteClause;
import com.starrocks.sql.ast.MergeWhenMatchedUpdateClause;
import com.starrocks.sql.ast.MergeWhenNotMatchedInsertClause;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.AnalyticExpr;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.CaseExpr;
import com.starrocks.sql.ast.expression.CaseWhenClause;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.DefaultValueExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.FunctionParams;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.IsNullPredicate;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.type.IntegerType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * MERGE INTO a Primary Key table. The statement becomes one query whose rows are written by an upsert or a
 * delete, chosen by the load operation column {@code __op} of the table sink:
 * <pre>
 *   SELECT c1, ..., __merge_action AS __op                     -- generated columns are computed here
 *   FROM (SELECT *, count(*) OVER (PARTITION BY keys) AS __merge_rows
 *         FROM (SELECT CASE WHEN p1 THEN v1(c1) ... ELSE t.c1 END AS c1, ...,
 *                      CASE WHEN p1 THEN a1 ... END AS __merge_action
 *               FROM source LEFT | FULL OUTER JOIN target t ON cond) __MERGE__
 *         WHERE __merge_action IS NOT NULL) __MERGE__
 *   WHERE assert_true(__merge_rows = 1, ...)                    -- error_on_nondeterministic_merge
 * </pre>
 * where {@code p_i} is "the row is of the kind of WHEN clause i, and its condition holds". The source is read
 * once and joined with the target once, the first WHEN clause that applies to a row decides its action, and a
 * row that no clause applies to is left alone. A delete outputs the current values of the target row.
 *
 * <p>A FULL OUTER JOIN is used only when a WHEN NOT MATCHED BY SOURCE clause needs the target rows that no
 * source row matches; the source then gets a marker column that tells a source row from a missing one.
 */
final class MergeIntoOlapAnalyzer {
    static final String ACTION_COLUMN = "__merge_action";
    private static final String ROWS_COLUMN = "__merge_rows";
    private static final String SOURCE_ROW_COLUMN = "__merge_source_row";
    private static final String SUBQUERY_ALIAS = "__MERGE__";
    private static final int UPSERT = 0;
    private static final int DELETE = 1;

    private MergeIntoOlapAnalyzer() {
    }

    static void analyze(MergeIntoStmt stmt, OlapTable table, TableName tableName, ConnectContext session) {
        if (table instanceof MaterializedView) {
            throw new SemanticException("The data of '%s' cannot be modified because '%s' is a materialized view, "
                    + "and the data of materialized view must be consistent with the base table.",
                    tableName.getTbl(), tableName.getTbl());
        }
        if (table.getKeysType() != KeysType.PRIMARY_KEYS) {
            throw new SemanticException("MERGE INTO is only supported for Primary Key tables and Iceberg tables, "
                    + "but '%s' is a %s table", tableName.getTbl(), table.getKeysType().toSql());
        }
        if (stmt.getTargetBranch() != null) {
            throw new SemanticException("Only an Iceberg table can be written on a branch");
        }
        if (table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE) {
            throw new SemanticException("MERGE INTO '%s' is not supported while its schema is being changed",
                    tableName.getTbl());
        }
        List<MergeWhenClause> clauses = stmt.getWhenClauses();
        if (clauses.isEmpty()) {
            throw new SemanticException("MERGE INTO requires at least one WHEN clause");
        }
        validateClauseOrder(clauses);

        boolean bySource = clauses.stream().anyMatch(MergeWhenClause::isNotMatchedBySource);
        TableName target = stmt.getTargetAlias() != null
                ? new TableName(null, null, stmt.getTargetAlias())
                : new TableName(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
        // The source is named only by UPDATE SET *, INSERT * and the marker of NOT MATCHED BY SOURCE.
        boolean namesSource = bySource || clauses.stream().anyMatch(c -> c instanceof MergeWhenMatchedUpdateClause u
                && u.isStar() || c instanceof MergeWhenNotMatchedInsertClause i && i.isStar());
        String sourceName = namesSource ? MergeIntoAnalyzer.sourceQualifier(stmt) : null;
        TableName source = new TableName(null, null, sourceName);
        List<Column> columns = table.getBaseSchema();
        List<Column> keys = columns.stream().filter(Column::isKey).toList();

        List<Map<String, Expr>> values = new ArrayList<>();
        for (MergeWhenClause clause : clauses) {
            values.add(clauseValues(clause, table, tableName, target, source));
        }

        SelectList inner = new SelectList();
        for (Column column : columns) {
            if (!column.isGeneratedColumn()) {
                inner.addItem(new SelectListItem(columnCase(clauses, values, column, target, source, keys, bySource),
                        column.getName()));
            }
        }
        inner.addItem(new SelectListItem(actionCase(clauses, target, source, keys, bySource), ACTION_COLUMN));

        Relation sourceRelation = bySource ? markSourceRows(stmt.getSourceRelation(), sourceName)
                : stmt.getSourceRelation();
        TableRelation targetRelation = new TableRelation(tableName);
        targetRelation.setTable(table);
        if (stmt.getTargetAlias() != null) {
            targetRelation.setAlias(new TableName(null, null, stmt.getTargetAlias()));
        }
        JoinRelation join = new JoinRelation(bySource ? JoinOperator.FULL_OUTER_JOIN : JoinOperator.LEFT_OUTER_JOIN,
                sourceRelation, targetRelation, stmt.getMergeCondition(), false);

        // The rows that are written, with the number of them that have the same key.
        boolean checkKeys = session.getSessionVariable().isErrorOnNondeterministicMerge();
        SelectList middle = new SelectList();
        middle.addItem(new SelectListItem(null));
        if (checkKeys) {
            List<Expr> partition = new ArrayList<>();
            for (Column key : keys) {
                partition.add(new SlotRef(null, key.getName()));
            }
            FunctionCallExpr count = new FunctionCallExpr("count", FunctionParams.createStarParam());
            middle.addItem(new SelectListItem(new AnalyticExpr(count, partition, null, null, null), ROWS_COLUMN));
        }
        Expr written = new IsNullPredicate(new SlotRef(null, ACTION_COLUMN), true);
        SelectRelation middleSelect = new SelectRelation(middle,
                subquery(new SelectRelation(inner, join, null, null, null)), written, null, null);

        SelectList outer = new SelectList();
        for (Column column : columns) {
            Expr expr = column.isGeneratedColumn() ? column.getGeneratedColumnExpr(table.getIdToColumn())
                    : new SlotRef(null, column.getName());
            outer.addItem(new SelectListItem(expr, column.getName()));
        }
        outer.addItem(new SelectListItem(new SlotRef(null, ACTION_COLUMN), Load.LOAD_OP_COLUMN));
        Expr check = null;
        if (checkKeys) {
            Expr single = new BinaryPredicate(BinaryType.EQ, new SlotRef(null, ROWS_COLUMN), new IntLiteral(1));
            check = new FunctionCallExpr("assert_true", Lists.newArrayList(single, new StringLiteral(
                    "Duplicate row detected during MERGE INTO: more than one source row matches the same target "
                            + "row, or more than one row with the same key is inserted. Set "
                            + "error_on_nondeterministic_merge to false to write one of them")));
        }
        SelectRelation outerSelect = new SelectRelation(outer, subquery(middleSelect), check, null, null);

        QueryStatement queryStatement = new QueryStatement(outerSelect);
        queryStatement.setIsExplain(stmt.isExplain(), stmt.getExplainLevel());
        new QueryAnalyzer(session).analyze(queryStatement);

        List<Expr> outputs = Lists.newArrayList(outerSelect.getOutputExpression());
        for (int i = 0; i < columns.size(); i++) {
            outputs.set(i, TypeManager.addCastExpr(outputs.get(i), columns.get(i).getType()));
        }
        outerSelect.setOutputExpr(outputs);

        List<String> names = new ArrayList<>();
        for (Column column : columns) {
            names.add(column.getName());
        }
        names.add(Load.LOAD_OP_COLUMN);
        outerSelect.setColumnOutputNames(names);

        stmt.setTable(table);
        stmt.setQueryStatement(queryStatement);
        stmt.setOutputColumnNames(names);
        stmt.setRoutingExpr(null);
    }

    /**
     * The value each written column gets from one WHEN clause, by lower-case column name. Generated columns are
     * left out: they are computed from the others.
     */
    private static Map<String, Expr> clauseValues(MergeWhenClause clause, OlapTable table, TableName tableName,
                                                  TableName target, TableName source) {
        Map<String, Expr> values = new LinkedHashMap<>();
        if (clause instanceof MergeWhenMatchedDeleteClause) {
            return values;
        }
        if (clause instanceof MergeWhenMatchedUpdateClause update) {
            if (update.isStar()) {
                for (Column column : table.getBaseSchema()) {
                    if (!column.isKey() && !column.isGeneratedColumn()) {
                        values.put(column.getName().toLowerCase(), new SlotRef(source, column.getName()));
                    }
                }
                return values;
            }
            // The last assignment of a column wins, as in UPDATE.
            for (ColumnAssignment assignment : update.getAssignments()) {
                Column column = table.getColumn(assignment.getColumn());
                if (column == null) {
                    throw new SemanticException("table '%s' does not have column '%s'", tableName.getTbl(),
                            assignment.getColumn());
                }
                if (column.isKey()) {
                    throw new SemanticException("primary key column cannot be updated: " + column.getName());
                }
                if (column.isGeneratedColumn()) {
                    throw new SemanticException("generated column cannot be updated: " + column.getName());
                }
                Expr expr = assignment.getExpr() instanceof DefaultValueExpr ? defaultValue(column)
                        : assignment.getExpr();
                values.put(column.getName().toLowerCase(), expr);
            }
            return values;
        }

        MergeWhenNotMatchedInsertClause insert = (MergeWhenNotMatchedInsertClause) clause;
        List<Column> writable = table.getBaseSchema().stream().filter(c -> !c.isGeneratedColumn()).toList();
        if (insert.isStar()) {
            for (Column column : writable) {
                values.put(column.getName().toLowerCase(), new SlotRef(source, column.getName()));
            }
            return values;
        }
        List<String> names = insert.getTargetColumnNames();
        List<Expr> exprs = insert.getValues();
        if (names == null) {
            if (exprs.size() != writable.size()) {
                throw new SemanticException("MERGE INTO INSERT VALUES count %d does not match target table column "
                        + "count %d", exprs.size(), writable.size());
            }
            names = writable.stream().map(Column::getName).toList();
        } else if (names.size() != exprs.size()) {
            throw new SemanticException("MERGE INTO INSERT column count %d does not match value count %d",
                    names.size(), exprs.size());
        }
        Set<String> seen = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < names.size(); i++) {
            Column column = table.getColumn(names.get(i));
            if (column == null) {
                throw new SemanticException("table '%s' does not have column '%s'", tableName.getTbl(), names.get(i));
            }
            if (column.isGeneratedColumn()) {
                throw new SemanticException("generated column '%s' cannot be inserted", column.getName());
            }
            if (!seen.add(column.getName())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, column.getName());
            }
            Expr expr = exprs.get(i) instanceof DefaultValueExpr ? defaultValue(column) : exprs.get(i);
            values.put(column.getName().toLowerCase(), expr);
        }
        for (Column column : writable) {
            if (!seen.contains(column.getName())) {
                if (column.isKey() && !column.isAutoIncrement()) {
                    throw new SemanticException("MERGE INTO INSERT must give a value for the key column '%s'",
                            column.getName());
                }
                values.put(column.getName().toLowerCase(), defaultValue(column));
            }
        }
        return values;
    }

    /**
     * The value of a column that is not given, like an INSERT that leaves it out. An AUTO_INCREMENT column gets
     * NULL, which the table sink fills with a new ID.
     */
    private static Expr defaultValue(Column column) {
        Column.DefaultValueType type = column.getDefaultValueType();
        if (column.isAutoIncrement() || type == Column.DefaultValueType.NULL) {
            if (!column.isAllowNull() && !column.isAutoIncrement()) {
                throw new SemanticException("Column '%s' is not nullable and has no default value; "
                        + "MERGE INTO INSERT must give a value for it", column.getName());
            }
            return new NullLiteral();
        }
        if (type == Column.DefaultValueType.VARY
                || (column.getDefaultExpr() != null && column.getDefaultExpr().getExprObject() != null)) {
            Expr expr = column.getDefaultExpr().obtainExpr();
            return expr.getType().equals(column.getType()) ? expr : new CastExpr(column.getType(), expr);
        }
        return new StringLiteral(column.calculatedDefaultValue());
    }

    /**
     * {@code CASE WHEN p1 THEN v1 ... ELSE t.c END}, or just {@code t.c} when no clause changes the column. A
     * clause that keeps the column still gets a WHEN, so that a later clause does not take its rows, unless no
     * later clause changes the column.
     */
    private static Expr columnCase(List<MergeWhenClause> clauses, List<Map<String, Expr>> values, Column column,
                                   TableName target, TableName source, List<Column> keys, boolean bySource) {
        int last = -1;
        for (int i = 0; i < clauses.size(); i++) {
            if (values.get(i).containsKey(column.getName().toLowerCase())) {
                last = i;
            }
        }
        if (last < 0) {
            return new SlotRef(target, column.getName());
        }
        List<CaseWhenClause> whens = new ArrayList<>();
        for (int i = 0; i <= last; i++) {
            Expr value = values.get(i).get(column.getName().toLowerCase());
            // A value of another type is cast to the type of the column, so that the CASE has that type and the
            // current value of the column is not cast to a common type and back.
            Expr then = value == null ? new SlotRef(target, column.getName())
                    : new CastExpr(column.getType(), value.clone());
            whens.add(new CaseWhenClause(clausePredicate(clauses.get(i), target, source, keys, bySource), then));
        }
        return new CaseExpr(null, whens, new SlotRef(target, column.getName()));
    }

    /**
     * {@code CASE WHEN p1 THEN a1 ... END}: 0 to upsert the row, 1 to delete it, NULL to leave it alone.
     */
    private static Expr actionCase(List<MergeWhenClause> clauses, TableName target, TableName source,
                                   List<Column> keys, boolean bySource) {
        List<CaseWhenClause> whens = new ArrayList<>();
        for (MergeWhenClause clause : clauses) {
            int action = clause instanceof MergeWhenMatchedDeleteClause ? DELETE : UPSERT;
            whens.add(new CaseWhenClause(clausePredicate(clause, target, source, keys, bySource),
                    new IntLiteral(action, IntegerType.TINYINT)));
        }
        return new CaseExpr(null, whens, new NullLiteral());
    }

    /**
     * True for a row of the kind of the clause whose condition holds. A new tree on every call, since the
     * analyzer changes expressions in place.
     */
    private static Expr clausePredicate(MergeWhenClause clause, TableName target, TableName source,
                                        List<Column> keys, boolean bySource) {
        // The key of a Primary Key table is never NULL, so a NULL key tells a missing target row.
        Expr targetRow = new IsNullPredicate(new SlotRef(target, keys.get(0).getName()), true);
        Expr kind;
        if (clause.isNotMatchedBySource()) {
            kind = new IsNullPredicate(new SlotRef(source, SOURCE_ROW_COLUMN), false);
        } else if (clause.isMatched()) {
            kind = bySource ? new CompoundPredicate(CompoundPredicate.Operator.AND, targetRow,
                    new IsNullPredicate(new SlotRef(source, SOURCE_ROW_COLUMN), true)) : targetRow;
        } else {
            kind = new IsNullPredicate(new SlotRef(target, keys.get(0).getName()), false);
        }
        Expr condition = clause.getOptionalCondition();
        return condition == null ? kind
                : new CompoundPredicate(CompoundPredicate.Operator.AND, kind, condition.clone());
    }

    /**
     * {@code (SELECT *, TRUE AS __merge_source_row FROM source) source}, so that a FULL OUTER JOIN can tell a
     * target row no source row matches.
     */
    private static Relation markSourceRows(Relation source, String name) {
        SelectList items = new SelectList();
        items.addItem(new SelectListItem(null));
        items.addItem(new SelectListItem(new BoolLiteral(true), SOURCE_ROW_COLUMN));
        SubqueryRelation marked = new SubqueryRelation(new QueryStatement(
                new SelectRelation(items, source, null, null, null)));
        marked.setAlias(new TableName(null, name));
        return marked;
    }

    private static SubqueryRelation subquery(SelectRelation select) {
        SubqueryRelation subquery = new SubqueryRelation(new QueryStatement(select));
        subquery.setAlias(new TableName(null, SUBQUERY_ALIAS));
        return subquery;
    }

    /**
     * An unconditional clause must be the last one of its kind, since it takes every row of that kind.
     */
    private static void validateClauseOrder(List<MergeWhenClause> clauses) {
        boolean matched = false;
        boolean notMatched = false;
        boolean bySource = false;
        for (MergeWhenClause clause : clauses) {
            boolean unconditional = clause.getOptionalCondition() == null;
            if (clause.isNotMatchedBySource()) {
                if (bySource) {
                    throw new SemanticException("Unconditional WHEN NOT MATCHED BY SOURCE clause must be the last "
                            + "NOT MATCHED BY SOURCE clause");
                }
                bySource = unconditional;
            } else if (clause.isMatched()) {
                if (matched) {
                    throw new SemanticException("Unconditional WHEN MATCHED clause must be the last MATCHED clause");
                }
                matched = unconditional;
            } else {
                if (notMatched) {
                    throw new SemanticException(
                            "Unconditional WHEN NOT MATCHED clause must be the last NOT MATCHED clause");
                }
                notMatched = unconditional;
            }
        }
    }
}
