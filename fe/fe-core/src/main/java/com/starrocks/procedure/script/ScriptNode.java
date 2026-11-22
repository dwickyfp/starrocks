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

package com.starrocks.procedure.script;

import java.util.List;

/**
 * The parsed form of a Snowflake Scripting block. Expressions and SQL statements are kept as source text
 * ({@link Text}); the interpreter binds variables into them when they run.
 */
public abstract class ScriptNode {
    private final int line;

    protected ScriptNode(int line) {
        this.line = line;
    }

    public int getLine() {
        return line;
    }

    /** Source text of an expression or a statement, with the tokens it was cut from. */
    public static final class Text {
        private final String source;
        private final String sql;
        private final List<ScriptToken> tokens;
        private final int line;

        public Text(String source, List<ScriptToken> tokens) {
            this.source = source;
            this.tokens = tokens;
            this.sql = source.substring(tokens.get(0).getStart(), tokens.get(tokens.size() - 1).getEnd());
            this.line = tokens.get(0).getLine();
        }

        /** The whole script the tokens point into. */
        public String getSource() {
            return source;
        }

        public String getSql() {
            return sql;
        }

        public List<ScriptToken> getTokens() {
            return tokens;
        }

        public int getLine() {
            return line;
        }

        @Override
        public String toString() {
            return sql;
        }
    }

    // ------------------------------------------------ declarations ------------------------------------------------

    /** {@code name [type] [DEFAULT | := expr]}; the type is null when not written. */
    public static final class VariableDecl extends ScriptNode {
        public final String name;
        public final String typeSql;
        public final Text defaultValue;

        public VariableDecl(int line, String name, String typeSql, Text defaultValue) {
            super(line);
            this.name = name;
            this.typeSql = typeSql;
            this.defaultValue = defaultValue;
        }
    }

    /** {@code name CURSOR FOR query}. */
    public static final class CursorDecl extends ScriptNode {
        public final String name;
        public final Text query;

        public CursorDecl(int line, String name, Text query) {
            super(line);
            this.name = name;
            this.query = query;
        }
    }

    /** {@code name EXCEPTION [(code, 'message')]}. */
    public static final class ExceptionDecl extends ScriptNode {
        public final String name;
        public final Integer code;
        public final String message;

        public ExceptionDecl(int line, String name, Integer code, String message) {
            super(line);
            this.name = name;
            this.code = code;
            this.message = message;
        }
    }

    // ------------------------------------------------ statements --------------------------------------------------

    /** {@code [DECLARE ...] BEGIN ... [EXCEPTION WHEN ... THEN ...] END}. */
    public static final class Block extends ScriptNode {
        public final List<ScriptNode> declarations;
        public final List<ScriptNode> statements;
        public final List<Handler> handlers;

        public Block(int line, List<ScriptNode> declarations, List<ScriptNode> statements, List<Handler> handlers) {
            super(line);
            this.declarations = declarations;
            this.statements = statements;
            this.handlers = handlers;
        }
    }

    /** {@code WHEN name [OR name ...] THEN statements}; OTHER matches everything. */
    public static final class Handler {
        public final List<String> exceptionNames;
        public final List<ScriptNode> statements;

        public Handler(List<String> exceptionNames, List<ScriptNode> statements) {
            this.exceptionNames = exceptionNames;
            this.statements = statements;
        }
    }

    /** {@code LET name [type] := expr} declares; {@code name := expr} assigns an existing variable. */
    public static final class Assign extends ScriptNode {
        public final String name;
        public final String typeSql;
        public final Text value;
        public final boolean declare;

        public Assign(int line, String name, String typeSql, Text value, boolean declare) {
            super(line);
            this.name = name;
            this.typeSql = typeSql;
            this.value = value;
            this.declare = declare;
        }
    }

    public static final class If extends ScriptNode {
        public final List<Text> conditions;
        public final List<List<ScriptNode>> branches;
        public final List<ScriptNode> elseBranch;

        public If(int line, List<Text> conditions, List<List<ScriptNode>> branches, List<ScriptNode> elseBranch) {
            super(line);
            this.conditions = conditions;
            this.branches = branches;
            this.elseBranch = elseBranch;
        }
    }

    /** {@code CASE [operand] WHEN v THEN ... [ELSE ...] END [CASE]}; without an operand each WHEN is a condition. */
    public static final class Case extends ScriptNode {
        public final Text operand;
        public final List<Text> whens;
        public final List<List<ScriptNode>> branches;
        public final List<ScriptNode> elseBranch;

        public Case(int line, Text operand, List<Text> whens, List<List<ScriptNode>> branches,
                    List<ScriptNode> elseBranch) {
            super(line);
            this.operand = operand;
            this.whens = whens;
            this.branches = branches;
            this.elseBranch = elseBranch;
        }
    }

    /** Base of the loops, which may carry a label for BREAK and CONTINUE. */
    public abstract static class Loop extends ScriptNode {
        public final String label;
        public final List<ScriptNode> body;

        protected Loop(int line, String label, List<ScriptNode> body) {
            super(line);
            this.label = label;
            this.body = body;
        }
    }

    /** {@code FOR i IN [REVERSE] from TO to DO ... END FOR}. */
    public static final class ForRange extends Loop {
        public final String variable;
        public final boolean reverse;
        public final Text from;
        public final Text to;

        public ForRange(int line, String label, List<ScriptNode> body, String variable, boolean reverse, Text from,
                        Text to) {
            super(line, label, body);
            this.variable = variable;
            this.reverse = reverse;
            this.from = from;
            this.to = to;
        }
    }

    /** {@code FOR row IN cursor DO ... END FOR}; the row's columns are read as {@code row.column}. */
    public static final class ForCursor extends Loop {
        public final String variable;
        public final String cursor;

        public ForCursor(int line, String label, List<ScriptNode> body, String variable, String cursor) {
            super(line, label, body);
            this.variable = variable;
            this.cursor = cursor;
        }
    }

    public static final class While extends Loop {
        public final Text condition;

        public While(int line, String label, List<ScriptNode> body, Text condition) {
            super(line, label, body);
            this.condition = condition;
        }
    }

    public static final class Repeat extends Loop {
        public final Text until;

        public Repeat(int line, String label, List<ScriptNode> body, Text until) {
            super(line, label, body);
            this.until = until;
        }
    }

    public static final class InfiniteLoop extends Loop {
        public InfiniteLoop(int line, String label, List<ScriptNode> body) {
            super(line, label, body);
        }
    }

    /** {@code BREAK | EXIT [label]} or, with {@code isContinue}, {@code CONTINUE | ITERATE [label]}. */
    public static final class Jump extends ScriptNode {
        public final boolean isContinue;
        public final String label;

        public Jump(int line, boolean isContinue, String label) {
            super(line);
            this.isContinue = isContinue;
            this.label = label;
        }
    }

    public static final class Return extends ScriptNode {
        public final Text value;

        public Return(int line, Text value) {
            super(line);
            this.value = value;
        }
    }

    /** {@code RAISE name}, or a bare {@code RAISE} in a handler to raise the caught exception again. */
    public static final class Raise extends ScriptNode {
        public final String exceptionName;

        public Raise(int line, String exceptionName) {
            super(line);
            this.exceptionName = exceptionName;
        }
    }

    public static final class Null extends ScriptNode {
        public Null(int line) {
            super(line);
        }
    }

    /** {@code OPEN c}, {@code CLOSE c}, {@code FETCH c INTO a, b}. */
    public static final class CursorOp extends ScriptNode {
        public enum Op { OPEN, CLOSE, FETCH }

        public final Op op;
        public final String cursor;
        public final List<String> into;

        public CursorOp(int line, Op op, String cursor, List<String> into) {
            super(line);
            this.op = op;
            this.cursor = cursor;
            this.into = into;
        }
    }

    /** {@code CALL name(args) [INTO :v]}: a nested stored procedure call. */
    public static final class Call extends ScriptNode {
        public final List<String> nameParts;
        public final List<Text> arguments;
        public final String into;

        public Call(int line, List<String> nameParts, List<Text> arguments, String into) {
            super(line);
            this.nameParts = nameParts;
            this.arguments = arguments;
            this.into = into;
        }
    }

    /**
     * {@code EXECUTE IMMEDIATE expr [USING (a, b)]}: the expression's value is a statement or a block to run; its
     * {@code ?} placeholders take the values of the USING variables in order.
     */
    public static final class ExecuteImmediate extends ScriptNode {
        public final Text statement;
        public final List<String> using;

        public ExecuteImmediate(int line, Text statement, List<String> using) {
            super(line);
            this.statement = statement;
            this.using = using;
        }
    }

    /**
     * {@code name RESULTSET [DEFAULT | := source]} in DECLARE, or {@code LET name RESULTSET := source}. The source,
     * null when not given, is {@code (SELECT ...)}, {@code (EXECUTE IMMEDIATE ...)} or {@code (CALL p(...))} of a
     * RETURNS TABLE procedure; it runs when assigned.
     */
    public static final class ResultSetDecl extends ScriptNode {
        public final String name;
        public final Text source;

        public ResultSetDecl(int line, String name, Text source) {
            super(line);
            this.name = name;
            this.source = source;
        }
    }

    /** {@code RETURN TABLE(rs)} with a RESULTSET variable, or {@code RETURN TABLE(SELECT ...)}. */
    public static final class ReturnTable extends ScriptNode {
        public final Text source;

        public ReturnTable(int line, Text source) {
            super(line);
            this.source = source;
        }
    }

    /** A SQL statement; {@code SELECT ... INTO :a, :b} keeps the variables in {@code into}. */
    public static final class Sql extends ScriptNode {
        public final Text statement;
        public final List<String> into;

        public Sql(int line, Text statement, List<String> into) {
            super(line);
            this.statement = statement;
            this.into = into;
        }
    }
}
