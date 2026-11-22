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

package com.starrocks.sql.parser;

import com.starrocks.catalog.TableName;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.expression.AnalyticExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.Subquery;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rewrites {@code SELECT items FROM ... [WHERE] [GROUP BY] [HAVING] QUALIFY cond} into
 * <pre>
 *   SELECT [DISTINCT] * EXCLUDE (__qualify_0, ...)
 *   FROM (SELECT items, w0 AS __qualify_0, ... FROM ... [WHERE] [GROUP BY] [HAVING]) __QUALIFY__TABLE
 *   WHERE cond'
 * </pre>
 * where every window function of {@code cond} is computed by the inner query and {@code cond'} refers to it.
 * A window function that is also a named select item is not computed twice. {@code cond} can refer to the
 * select items by name, and to columns of the FROM clause that are not selected. As in Snowflake, QUALIFY is
 * evaluated after the window functions and before DISTINCT,
 * so a DISTINCT select moves to the outer query. A filter on a ranking function keeps its top-N optimization,
 * since the predicate is pushed down to the window.
 */
final class QualifyRewriter {
    static final String TABLE_NAME = "__QUALIFY__TABLE";
    static final String COLUMN_PREFIX = "__qualify_";

    private QualifyRewriter() {
    }

    static SelectRelation rewrite(SelectRelation select, Expr qualify, NodePosition pos) {
        TableName tableName = new TableName(null, TABLE_NAME);
        SelectList selectList = select.getSelectList();
        List<SelectListItem> innerItems = new ArrayList<>(selectList.getItems());

        List<AnalyticExpr> windows = new ArrayList<>();
        qualify.collect(AnalyticExpr.class, windows);
        Map<Expr, SlotRef> replacements = new IdentityHashMap<>();
        List<String> hidden = new ArrayList<>();
        for (AnalyticExpr window : windows) {
            String name = namedSelectItem(innerItems, window);
            if (name == null) {
                name = COLUMN_PREFIX + hidden.size();
                hidden.add(name);
                innerItems.add(new SelectListItem(window, name, window.getPos()));
            }
            replacements.put(window, new SlotRef(tableName, name));
        }
        Set<String> outputNames = outputNames(selectList.getItems());
        Expr predicate = replace(qualify, replacements, outputNames, tableName, innerItems, hidden);

        boolean distinct = selectList.isDistinct();
        SelectList innerList = new SelectList(innerItems, false);
        innerList.setHintNodes(selectList.getHintNodes());
        SelectRelation inner = new SelectRelation(innerList, select.getRelation(), select.getWhereClause(),
                select.getGroupByClause(), select.getHavingClause(), select.getPos());
        inner.setOrderBy(new ArrayList<>());

        SubqueryRelation subquery = new SubqueryRelation(new QueryStatement(inner));
        subquery.setAlias(tableName);

        List<SelectListItem> outerItems = new ArrayList<>();
        outerItems.add(new SelectListItem(null, pos, hidden));
        SelectList outerList = new SelectList(outerItems, distinct);
        outerList.setHintNodes(selectList.getHintNodes());
        return new SelectRelation(outerList, subquery, predicate, null, null, pos);
    }

    /**
     * Returns the name of a select item that is the given window function with an alias, or null.
     */
    private static String namedSelectItem(List<SelectListItem> items, AnalyticExpr window) {
        for (SelectListItem item : items) {
            if (!item.isStar() && item.getAlias() != null && window.equals(item.getExpr())) {
                return item.getAlias();
            }
        }
        return null;
    }

    /**
     * Returns the lower-case output names of the select items, or null when a star item makes them unknown.
     */
    private static Set<String> outputNames(List<SelectListItem> items) {
        Set<String> names = new HashSet<>();
        for (SelectListItem item : items) {
            if (item.isStar()) {
                return null;
            }
            if (item.getAlias() != null) {
                names.add(item.getAlias().toLowerCase());
            } else if (item.getExpr() instanceof SlotRef) {
                names.add(((SlotRef) item.getExpr()).getColumnName().toLowerCase());
            }
        }
        return names;
    }

    /**
     * Replaces the window functions by references to the inner query. A column reference outside a window
     * function that names a select item refers to that item. Any other column reference is computed by the inner
     * query as a hidden column, so QUALIFY can use a column that is not selected. Subqueries keep their own scope.
     */
    private static Expr replace(Expr expr, Map<Expr, SlotRef> replacements, Set<String> outputNames,
                                TableName tableName, List<SelectListItem> innerItems, List<String> hidden) {
        SlotRef replacement = replacements.get(expr);
        if (replacement != null) {
            return replacement;
        }
        if (expr instanceof Subquery) {
            return expr;
        }
        if (expr instanceof SlotRef) {
            SlotRef slot = (SlotRef) expr;
            if (outputNames != null && !outputNames.contains(slot.getColumnName().toLowerCase())) {
                String name = COLUMN_PREFIX + hidden.size();
                hidden.add(name);
                innerItems.add(new SelectListItem(slot, name, slot.getPos()));
                return new SlotRef(tableName, name);
            }
            if (slot.getTblNameWithoutAnalyzed() != null) {
                return new SlotRef(null, slot.getColumnName());
            }
            return expr;
        }
        for (int i = 0; i < expr.getChildren().size(); i++) {
            Expr child = expr.getChild(i);
            Expr newChild = replace(child, replacements, outputNames, tableName, innerItems, hidden);
            if (newChild != child) {
                expr.setChild(i, newChild);
            }
        }
        return expr;
    }
}
