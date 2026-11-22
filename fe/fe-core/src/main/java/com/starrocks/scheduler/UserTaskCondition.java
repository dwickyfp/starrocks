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


package com.starrocks.scheduler;

import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.InformationFunction;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.tablestream.TableStreamMgr;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * The WHEN condition of a Snowflake style task: {@code SYSTEM$PREDECESSORS_STATE('<state>')}, which only a child
 * task can have, and conditions on streams, {@code SYSTEM$STREAM_HAS_DATA('<stream>')} combined with AND, OR and
 * NOT, which any task can have. The state is a top level conjunct: {@code SYSTEM$PREDECESSORS_STATE('ALL_DONE') AND
 * SYSTEM$STREAM_HAS_DATA('s')}.
 *
 * <p>The condition is kept as text in the WHEN option of the task: the state alone is its name, as before conditions
 * on streams, and any other condition the SQL text {@link #toString()} gives.
 */
public final class UserTaskCondition {
    private static final String PREDECESSORS_STATE = "SYSTEM$PREDECESSORS_STATE";
    private static final Pattern STATE_ONLY = Pattern.compile("[A-Z_]+");

    /** A condition on streams. */
    public sealed interface Streams permits HasData, And, Or, Not {
        boolean test(Predicate<String> hasData);
    }

    public record HasData(String stream) implements Streams {
        @Override
        public boolean test(Predicate<String> hasData) {
            return hasData.test(stream);
        }

        @Override
        public String toString() {
            return TableStreamMgr.HAS_DATA + "('" + stream.replace("'", "''") + "')";
        }
    }

    public record And(Streams left, Streams right) implements Streams {
        @Override
        public boolean test(Predicate<String> hasData) {
            return left.test(hasData) && right.test(hasData);
        }

        @Override
        public String toString() {
            return "(" + left + " AND " + right + ")";
        }
    }

    public record Or(Streams left, Streams right) implements Streams {
        @Override
        public boolean test(Predicate<String> hasData) {
            return left.test(hasData) || right.test(hasData);
        }

        @Override
        public String toString() {
            return "(" + left + " OR " + right + ")";
        }
    }

    public record Not(Streams operand) implements Streams {
        @Override
        public boolean test(Predicate<String> hasData) {
            return !operand.test(hasData);
        }

        @Override
        public String toString() {
            return "NOT " + operand;
        }
    }

    // null when the condition does not name one
    private final UserTaskSpec.PredecessorsState state;
    // null when the condition has none
    private final Streams streams;

    private UserTaskCondition(UserTaskSpec.PredecessorsState state, Streams streams) {
        this.state = state;
        this.streams = streams;
    }

    public UserTaskSpec.PredecessorsState getState() {
        return state;
    }

    public Streams getStreams() {
        return streams;
    }

    /** The streams the condition reads, in the order written. */
    public List<String> streamNames() {
        List<String> names = new ArrayList<>();
        collect(streams, names);
        return names;
    }

    private static void collect(Streams node, List<String> names) {
        if (node instanceof HasData has) {
            names.add(has.stream());
        } else if (node instanceof And and) {
            collect(and.left(), names);
            collect(and.right(), names);
        } else if (node instanceof Or or) {
            collect(or.left(), names);
            collect(or.right(), names);
        } else if (node instanceof Not not) {
            collect(not.operand(), names);
        }
    }

    /** Whether the conditions on streams hold; true without any. */
    public boolean testStreams(Predicate<String> hasData) {
        return streams == null || streams.test(hasData);
    }

    /**
     * Why the conditions on streams of the WHEN clause of the task stop a run of it now, or null when they hold or
     * there are none. They are evaluated on the FE from the metadata of the streams: no data is read.
     */
    public static String blockingReason(Task task) {
        UserTaskCondition when = UserTaskSpec.getWhen(task);
        if (when == null || when.streams == null) {
            return null;
        }
        try {
            return when.testStreams(stream -> TableStreamMgr.hasData(stream, task.getDbName())) ? null
                    : strip(when.streams.toString()) + " is false";
        } catch (SemanticException e) {
            return "its WHEN condition cannot be evaluated: " + e.getDetailMsg();
        } catch (Exception e) {
            return "its WHEN condition cannot be evaluated: " + e.getMessage();
        }
    }

    /** The text the WHEN option keeps: the state alone is its name. */
    public String toOption() {
        if (streams == null) {
            return state == null ? null : state.name();
        }
        return toString();
    }

    @Override
    public String toString() {
        String streamsSql = streams == null ? null : strip(streams.toString());
        if (state == null) {
            return streamsSql;
        }
        String stateSql = PREDECESSORS_STATE + "('" + state.name() + "')";
        return streamsSql == null ? stateSql : stateSql + " AND " + (streams instanceof Or ? "(" + streamsSql + ")"
                : streamsSql);
    }

    private static String strip(String sql) {
        return sql.startsWith("(") && sql.endsWith(")") && balanced(sql.substring(1, sql.length() - 1))
                ? sql.substring(1, sql.length() - 1) : sql;
    }

    private static boolean balanced(String sql) {
        int depth = 0;
        boolean quoted = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'') {
                quoted = !quoted;
            } else if (!quoted && c == '(') {
                depth++;
            } else if (!quoted && c == ')' && --depth < 0) {
                return false;
            }
        }
        return depth == 0;
    }

    /** The condition of a WHEN clause, as the parser builds it. */
    public static UserTaskCondition fromExpr(Expr expr) {
        List<Expr> conjuncts = new ArrayList<>();
        flattenAnd(expr, conjuncts);
        UserTaskSpec.PredecessorsState state = null;
        Streams streams = null;
        for (Expr conjunct : conjuncts) {
            String value = predecessorsState(conjunct);
            if (value != null) {
                if (state != null) {
                    throw new SemanticException("A WHEN condition can name " + PREDECESSORS_STATE + " once");
                }
                state = UserTaskSpec.PredecessorsState.parse(value);
                continue;
            }
            Streams node = streams(conjunct);
            streams = streams == null ? node : new And(streams, node);
        }
        return new UserTaskCondition(state, streams);
    }

    /** The condition a WHEN option keeps. */
    public static UserTaskCondition parse(String option) {
        if (option == null) {
            return null;
        }
        String text = option.trim();
        if (STATE_ONLY.matcher(text.toUpperCase(Locale.ROOT)).matches()) {
            return new UserTaskCondition(UserTaskSpec.PredecessorsState.parse(text), null);
        }
        return fromExpr(SqlParser.parseSqlToExpr(text, 0));
    }

    private static void flattenAnd(Expr expr, List<Expr> conjuncts) {
        if (expr instanceof CompoundPredicate compound && compound.getOp() == CompoundPredicate.Operator.AND) {
            flattenAnd(compound.getChild(0), conjuncts);
            flattenAnd(compound.getChild(1), conjuncts);
        } else {
            conjuncts.add(expr);
        }
    }

    private static String predecessorsState(Expr expr) {
        if (expr instanceof FunctionCallExpr call && call.getFunctionName().equalsIgnoreCase(PREDECESSORS_STATE)) {
            if (call.getChildren().size() != 1 || !(call.getChild(0) instanceof StringLiteral literal)) {
                throw new SemanticException(PREDECESSORS_STATE + " takes one state as a string literal");
            }
            return literal.getValue();
        }
        return null;
    }

    private static Streams streams(Expr expr) {
        if (expr instanceof InformationFunction function
                && function.getFuncType().equalsIgnoreCase(TableStreamMgr.HAS_DATA)) {
            return new HasData(function.getStrValue().trim());
        }
        if (expr instanceof CompoundPredicate compound) {
            switch (compound.getOp()) {
                case AND:
                    return new And(streams(compound.getChild(0)), streams(compound.getChild(1)));
                case OR:
                    return new Or(streams(compound.getChild(0)), streams(compound.getChild(1)));
                case NOT:
                    return new Not(streams(compound.getChild(0)));
                default:
                    break;
            }
        }
        if (predecessorsState(expr) != null) {
            throw new SemanticException(PREDECESSORS_STATE + " must be a condition of its own in WHEN, combined with "
                    + "the others by AND");
        }
        throw new SemanticException("Unsupported task condition: WHEN takes " + PREDECESSORS_STATE + "('<state>') "
                + "and " + TableStreamMgr.HAS_DATA + "('<stream>') conditions combined with AND, OR and NOT");
    }
}
