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
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IsNullPredicate;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Rewrites {@code FROM r UNPIVOT [INCLUDE | EXCLUDE NULLS] ((v1, ...) FOR name IN ((a1, ...) [AS 'x'], ...)) [alias]}
 * into
 * <pre>
 *   (SELECT * EXCLUDE (a1, ..., b1, ..., __unpivot_name, __unpivot_v0, ...),
 *           __unpivot_name AS name, __unpivot_v0 AS v1, ...
 *    FROM r, unnest(['x', ...], [a1, b1, ...], ...) __UNPIVOT__(__unpivot_name, __unpivot_v0, ...)
 *    [WHERE __unpivot_v0 IS NOT NULL OR ...]) alias
 * </pre>
 * so each row of {@code r} is read once and becomes one row per IN value, like the UNPIVOT of Snowflake and
 * Databricks. The result has the columns of {@code r} that are not unpivoted, then the name column, then the value
 * columns. By default (EXCLUDE NULLS) a row whose values are all NULL is left out. The values of one value column
 * must have a common type.
 */
final class UnpivotRewriter {
    private static final String FUNCTION_ALIAS = "__UNPIVOT__";
    private static final String NAME_COLUMN = "__unpivot_name";
    private static final String VALUE_PREFIX = "__unpivot_v";

    /**
     * One IN value: the columns it reads, one per value column, and the label it gets in the name column.
     */
    record Value(List<String> columns, String label) {
    }

    private UnpivotRewriter() {
    }

    static Relation rewrite(Relation from, List<String> valueColumns, String nameColumn, List<Value> values,
                            boolean includeNulls, String alias, NodePosition pos) {
        Set<String> outputNames = new HashSet<>();
        for (String column : valueColumns) {
            if (!outputNames.add(column.toLowerCase())) {
                throw new ParsingException("UNPIVOT value column '" + column + "' is given more than once", pos);
            }
        }
        if (!outputNames.add(nameColumn.toLowerCase())) {
            throw new ParsingException("UNPIVOT name column '" + nameColumn + "' is also a value column", pos);
        }

        List<String> excluded = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        List<Expr> labels = new ArrayList<>();
        List<List<Expr>> valueArrays = new ArrayList<>();
        for (int i = 0; i < valueColumns.size(); i++) {
            valueArrays.add(new ArrayList<>());
        }
        Set<String> seenLabels = new HashSet<>();
        for (Value value : values) {
            if (value.columns().size() != valueColumns.size()) {
                throw new ParsingException(String.format("UNPIVOT value %s has %d columns, but %d value columns are "
                        + "given", value.columns(), value.columns().size(), valueColumns.size()), pos);
            }
            if (!seenLabels.add(value.label())) {
                throw new ParsingException("UNPIVOT name '" + value.label() + "' is given more than once", pos);
            }
            labels.add(new StringLiteral(value.label(), pos));
            for (int i = 0; i < value.columns().size(); i++) {
                String column = value.columns().get(i);
                valueArrays.get(i).add(new SlotRef(null, column));
                if (seen.add(column.toLowerCase())) {
                    excluded.add(column);
                }
            }
        }

        List<Expr> unnestArgs = new ArrayList<>();
        unnestArgs.add(new ArrayExpr(null, labels, pos));
        List<String> functionColumns = new ArrayList<>();
        functionColumns.add(NAME_COLUMN);
        for (int i = 0; i < valueArrays.size(); i++) {
            unnestArgs.add(new ArrayExpr(null, valueArrays.get(i), pos));
            functionColumns.add(VALUE_PREFIX + i);
        }
        TableFunctionRelation unnest = new TableFunctionRelation(new FunctionCallExpr("unnest", unnestArgs, pos));
        unnest.setAlias(new TableName(null, FUNCTION_ALIAS));
        unnest.setColumnOutputNames(functionColumns);
        excluded.addAll(functionColumns);

        TableName functionTable = new TableName(null, FUNCTION_ALIAS);
        List<SelectListItem> items = new ArrayList<>();
        items.add(new SelectListItem(null, pos, excluded));
        items.add(new SelectListItem(new SlotRef(functionTable, NAME_COLUMN), nameColumn, pos));
        Expr notAllNull = null;
        for (int i = 0; i < valueColumns.size(); i++) {
            items.add(new SelectListItem(new SlotRef(functionTable, VALUE_PREFIX + i), valueColumns.get(i), pos));
            Expr notNull = new IsNullPredicate(new SlotRef(functionTable, VALUE_PREFIX + i), true, pos);
            notAllNull = notAllNull == null ? notNull
                    : new CompoundPredicate(CompoundPredicate.Operator.OR, notAllNull, notNull, pos);
        }

        Relation join = new JoinRelation(null, from, unnest, null, false);
        SelectRelation select = new SelectRelation(new SelectList(items, false), join,
                includeNulls ? null : notAllNull, null, null, pos);
        SubqueryRelation subquery = new SubqueryRelation(new QueryStatement(select));
        subquery.setAlias(new TableName(null, alias != null ? alias : FUNCTION_ALIAS + "TABLE"));
        return subquery;
    }
}
