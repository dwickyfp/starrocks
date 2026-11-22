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

import com.starrocks.procedure.script.ScriptNode.Text;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Runs a parsed Snowflake Scripting block against a {@link ScriptSession}.
 *
 * <p>Variables live in nested scopes: a block, a loop and an exception handler each open one. In SQL statements
 * a variable is written {@code :name} (or {@code :row.column}); in expressions (LET, IF, WHILE, RETURN, ...) it
 * can also be written bare, except inside a parenthesized SELECT. {@code IDENTIFIER(:v)} binds a variable as a
 * table or column name.
 *
 * <p>SQLROWCOUNT, SQLFOUND and SQLNOTFOUND describe the last SQL statement; SQLCODE, SQLERRM and SQLSTATE the
 * exception an EXCEPTION handler caught.
 */
public final class ScriptInterpreter {
    /** Statements and loop iterations one call may run, so a runaway loop ends with an error. */
    public static final long MAX_STEPS = 10_000_000L;

    private static final String[] BUILTINS = {"SQLROWCOUNT", "SQLFOUND", "SQLNOTFOUND"};

    private final ScriptSession session;
    private final Deque<ScriptException> caught = new ArrayDeque<>();
    private long steps = 0;

    public ScriptInterpreter(ScriptSession session) {
        this.session = session;
    }

    // ------------------------------------------------ scopes ------------------------------------------------------

    private static final class Variable {
        final String type;
        ScriptValue value;

        Variable(String type, ScriptValue value) {
            this.type = type;
            this.value = value;
        }
    }

    /** A cursor loop's row: its columns by lower-case name. */
    private static final class Row {
        final Map<String, ScriptValue> columns = new LinkedHashMap<>();
    }

    private static final class Cursor {
        final Text query;
        QueryResult result;
        int next;

        Cursor(Text query) {
            this.query = query;
        }
    }

    private static final class ExceptionDef {
        final String name;
        final int code;
        final String message;

        ExceptionDef(String name, int code, String message) {
            this.name = name;
            this.code = code;
            this.message = message;
        }
    }

    private static final class Scope {
        final Scope parent;
        final Map<String, Object> entries = new HashMap<>();

        Scope(Scope parent) {
            this.parent = parent;
        }

        Object find(String name) {
            String key = name.toLowerCase(Locale.ROOT);
            for (Scope s = this; s != null; s = s.parent) {
                Object entry = s.entries.get(key);
                if (entry != null) {
                    return entry;
                }
            }
            return null;
        }

        void put(String name, Object entry) {
            entries.put(name.toLowerCase(Locale.ROOT), entry);
        }
    }

    // ------------------------------------------------ control flow signals ----------------------------------------

    private static class Signal extends RuntimeException {
        Signal() {
            super(null, null, false, false);
        }
    }

    private static final class ReturnSignal extends Signal {
        final ScriptValue value;

        ReturnSignal(ScriptValue value) {
            this.value = value;
        }
    }

    private static final class JumpSignal extends Signal {
        final boolean isContinue;
        final String label;
        final int line;

        JumpSignal(boolean isContinue, String label, int line) {
            this.isContinue = isContinue;
            this.label = label;
            this.line = line;
        }
    }

    // ------------------------------------------------ entry -------------------------------------------------------

    /**
     * Runs {@code block} with {@code parameters} (name to typed value, in order) as variables and returns the value
     * of its RETURN, or NULL when it ends without one.
     */
    public ScriptValue run(ScriptNode.Block block, Map<String, ScriptValue> parameters) {
        Scope root = new Scope(null);
        for (String builtin : BUILTINS) {
            root.put(builtin, new Variable(null, ScriptValue.nullOf(null)));
        }
        parameters.forEach((name, value) -> root.put(name, new Variable(value.getType(), value)));
        try {
            runBlock(block, root);
            return ScriptValue.nullOf(null);
        } catch (ReturnSignal r) {
            return r.value;
        } catch (JumpSignal j) {
            throw new ScriptException(ScriptException.STATEMENT_ERROR, ScriptException.STATEMENT_ERROR_CODE,
                    (j.isContinue ? "CONTINUE" : "BREAK") + " outside of a loop"
                            + (j.label == null ? "" : " labeled " + j.label), j.line);
        } finally {
            session.rollbackOpenTransaction();
        }
    }

    private void runBlock(ScriptNode.Block block, Scope outer) {
        Scope scope = new Scope(outer);
        for (ScriptNode declaration : block.declarations) {
            declare(declaration, scope);
        }
        try {
            runStatements(block.statements, scope);
        } catch (ScriptException e) {
            ScriptNode.Handler handler = findHandler(block, e);
            if (handler == null) {
                throw e;
            }
            Scope handlerScope = new Scope(scope);
            handlerScope.put("SQLCODE", new Variable("INT", new ScriptValue("INT", String.valueOf(e.getSqlCode()))));
            handlerScope.put("SQLERRM", new Variable("VARCHAR", new ScriptValue("VARCHAR", e.getMessage())));
            boolean builtin = e.getExceptionName().equals(ScriptException.STATEMENT_ERROR)
                    || e.getExceptionName().equals(ScriptException.EXPRESSION_ERROR);
            handlerScope.put("SQLSTATE", new Variable("VARCHAR", new ScriptValue("VARCHAR",
                    builtin ? "42000" : "P0001")));
            caught.push(e);
            try {
                runStatements(handler.statements, handlerScope);
            } finally {
                caught.pop();
            }
        }
    }

    private ScriptNode.Handler findHandler(ScriptNode.Block block, ScriptException e) {
        for (ScriptNode.Handler handler : block.handlers) {
            for (String name : handler.exceptionNames) {
                if (name.equalsIgnoreCase("OTHER") || name.equalsIgnoreCase(e.getExceptionName())) {
                    return handler;
                }
            }
        }
        return null;
    }

    private void declare(ScriptNode declaration, Scope scope) {
        step(declaration);
        if (declaration instanceof ScriptNode.VariableDecl v) {
            ScriptValue value = v.defaultValue == null ? ScriptValue.nullOf(v.typeSql)
                    : evaluate(v.defaultValue, v.typeSql, scope);
            scope.put(v.name, new Variable(v.typeSql != null ? v.typeSql : value.getType(), value));
        } else if (declaration instanceof ScriptNode.CursorDecl c) {
            scope.put(c.name, new Cursor(c.query));
        } else if (declaration instanceof ScriptNode.ExceptionDecl e) {
            int code = e.code == null ? ScriptException.USER_EXCEPTION_CODE : e.code;
            String message = e.message == null ? e.name : e.message;
            scope.put(e.name, new ExceptionDef(e.name, code, message));
        }
    }

    private void runStatements(List<ScriptNode> statements, Scope scope) {
        for (ScriptNode statement : statements) {
            runStatement(statement, scope);
        }
    }

    private void runStatement(ScriptNode node, Scope scope) {
        step(node);
        if (node instanceof ScriptNode.Block b) {
            runBlock(b, scope);
        } else if (node instanceof ScriptNode.Assign a) {
            assign(a, scope);
        } else if (node instanceof ScriptNode.CursorDecl c) {
            scope.put(c.name, new Cursor(c.query));
        } else if (node instanceof ScriptNode.If i) {
            for (int k = 0; k < i.conditions.size(); k++) {
                if (evaluate(i.conditions.get(k), "BOOLEAN", scope).isTrue()) {
                    runStatements(i.branches.get(k), new Scope(scope));
                    return;
                }
            }
            if (i.elseBranch != null) {
                runStatements(i.elseBranch, new Scope(scope));
            }
        } else if (node instanceof ScriptNode.Case c) {
            runCase(c, scope);
        } else if (node instanceof ScriptNode.Loop loop) {
            runLoop(loop, scope);
        } else if (node instanceof ScriptNode.Jump j) {
            throw new JumpSignal(j.isContinue, j.label, j.getLine());
        } else if (node instanceof ScriptNode.Return r) {
            throw new ReturnSignal(r.value == null ? ScriptValue.nullOf(null) : evaluate(r.value, null, scope));
        } else if (node instanceof ScriptNode.Raise r) {
            raise(r, scope);
        } else if (node instanceof ScriptNode.CursorOp op) {
            cursorOp(op, scope);
        } else if (node instanceof ScriptNode.Call c) {
            List<ScriptValue> arguments = new ArrayList<>();
            for (Text argument : c.arguments) {
                arguments.add(evaluate(argument, null, scope));
            }
            ScriptValue result = session.call(c.nameParts, arguments);
            if (c.into != null) {
                setVariable(c.into, result, scope, c.getLine());
            }
        } else if (node instanceof ScriptNode.ExecuteImmediate e) {
            ScriptValue statement = evaluate(e.statement, "VARCHAR", scope);
            if (statement.isNull()) {
                throw expressionError("EXECUTE IMMEDIATE got NULL", e.getLine());
            }
            ScriptNode.Block nested = ScriptParser.parse(statement.getText());
            // The nested script has its own variables; only its effects are visible here, not its RETURN.
            try {
                runBlock(nested, rootOf(scope));
            } catch (ReturnSignal ignored) {
                // the value of the nested block is not used
            } catch (JumpSignal j) {
                throw statementError("BREAK or CONTINUE outside of a loop in EXECUTE IMMEDIATE", e.getLine());
            }
        } else if (node instanceof ScriptNode.Sql s) {
            runSql(s, scope);
        }
        // ScriptNode.Null does nothing
    }

    private static Scope rootOf(Scope scope) {
        Scope s = scope;
        while (s.parent != null) {
            s = s.parent;
        }
        Scope isolated = new Scope(null);
        for (String builtin : BUILTINS) {
            isolated.entries.put(builtin.toLowerCase(Locale.ROOT), s.entries.get(builtin.toLowerCase(Locale.ROOT)));
        }
        return isolated;
    }

    private void assign(ScriptNode.Assign a, Scope scope) {
        if (a.declare) {
            Object existing = scope.entries.get(a.name.toLowerCase(Locale.ROOT));
            String type = a.typeSql;
            if (type == null && existing instanceof Variable v) {
                type = v.type;
            }
            ScriptValue value = evaluate(a.value, type, scope);
            scope.put(a.name, new Variable(type != null ? type : value.getType(), value));
            return;
        }
        Object entry = scope.find(a.name);
        if (!(entry instanceof Variable v)) {
            throw expressionError("Undeclared variable '" + a.name + "'", a.getLine());
        }
        v.value = evaluate(a.value, v.type, scope);
    }

    private void setVariable(String name, ScriptValue value, Scope scope, int line) {
        Object entry = scope.find(name);
        if (!(entry instanceof Variable v)) {
            throw expressionError("Undeclared variable '" + name + "'", line);
        }
        v.value = castTo(value, v.type, line);
    }

    /** {@code value} as {@code type}; a value of the same type is returned as is. */
    private ScriptValue castTo(ScriptValue value, String type, int line) {
        if (type == null) {
            return value;
        }
        if (value.isNull()) {
            return ScriptValue.nullOf(type);
        }
        if (type.equalsIgnoreCase(value.getType())) {
            return value;
        }
        return evaluateSql("CAST(" + value.toSqlLiteral() + " AS " + type + ")", type, line);
    }

    private void runCase(ScriptNode.Case c, Scope scope) {
        String operand = c.operand == null ? null : evaluate(c.operand, null, scope).toSqlLiteral();
        for (int k = 0; k < c.whens.size(); k++) {
            Text when = c.whens.get(k);
            boolean match;
            if (operand == null) {
                match = evaluate(when, "BOOLEAN", scope).isTrue();
            } else {
                String sql = "CAST((" + operand + ") = (" + bind(when, scope, true) + ") AS BOOLEAN)";
                match = evaluateSql(sql, "BOOLEAN", when.getLine()).isTrue();
            }
            if (match) {
                runStatements(c.branches.get(k), new Scope(scope));
                return;
            }
        }
        if (c.elseBranch != null) {
            runStatements(c.elseBranch, new Scope(scope));
        }
    }

    // ------------------------------------------------ loops -------------------------------------------------------

    private void runLoop(ScriptNode.Loop loop, Scope scope) {
        if (loop instanceof ScriptNode.ForRange f) {
            long from = toLong(evaluate(f.from, "BIGINT", scope), f.getLine());
            long to = toLong(evaluate(f.to, "BIGINT", scope), f.getLine());
            long first = f.reverse ? to : from;
            long last = f.reverse ? from : to;
            long stepBy = f.reverse ? -1 : 1;
            for (long i = first; f.reverse ? i >= last : i <= last; i += stepBy) {
                Scope body = new Scope(scope);
                body.put(f.variable, new Variable("BIGINT", new ScriptValue("BIGINT", String.valueOf(i))));
                if (!iterate(loop, body)) {
                    return;
                }
            }
        } else if (loop instanceof ScriptNode.ForCursor f) {
            Cursor cursor = findCursor(f.cursor, scope, f.getLine());
            open(cursor, scope);
            try {
                while (cursor.next < cursor.result.getRows().size()) {
                    Row row = new Row();
                    for (int col = 0; col < cursor.result.getColumns().size(); col++) {
                        row.columns.put(cursor.result.getColumns().get(col).toLowerCase(Locale.ROOT),
                                cursor.result.value(cursor.next, col));
                    }
                    cursor.next++;
                    Scope body = new Scope(scope);
                    body.put(f.variable, row);
                    if (!iterate(loop, body)) {
                        return;
                    }
                }
            } finally {
                cursor.result = null;
            }
        } else if (loop instanceof ScriptNode.While w) {
            while (evaluate(w.condition, "BOOLEAN", scope).isTrue()) {
                step(w);
                if (!iterate(loop, new Scope(scope))) {
                    return;
                }
            }
        } else if (loop instanceof ScriptNode.Repeat r) {
            do {
                step(r);
                if (!iterate(loop, new Scope(scope))) {
                    return;
                }
            } while (!evaluate(r.until, "BOOLEAN", scope).isTrue());
        } else {
            while (true) {
                step(loop);
                if (!iterate(loop, new Scope(scope))) {
                    return;
                }
            }
        }
    }

    /** Runs one iteration; false when the loop must stop. */
    private boolean iterate(ScriptNode.Loop loop, Scope body) {
        try {
            runStatements(loop.body, body);
            return true;
        } catch (JumpSignal j) {
            if (j.label != null && !j.label.equalsIgnoreCase(loop.label)) {
                throw j;
            }
            return j.isContinue;
        }
    }

    private long toLong(ScriptValue value, int line) {
        if (value.isNull()) {
            throw expressionError("FOR loop bound is NULL", line);
        }
        try {
            return Long.parseLong(value.getText());
        } catch (NumberFormatException e) {
            throw expressionError("FOR loop bound is not an integer: " + value.getText(), line);
        }
    }

    // ------------------------------------------------ cursors and exceptions --------------------------------------

    private Cursor findCursor(String name, Scope scope, int line) {
        Object entry = scope.find(name);
        if (!(entry instanceof Cursor c)) {
            throw expressionError("Undeclared cursor '" + name + "'", line);
        }
        return c;
    }

    private void open(Cursor cursor, Scope scope) {
        cursor.result = runQuery(bind(cursor.query, scope, false), cursor.query.getLine());
        cursor.next = 0;
    }

    private void cursorOp(ScriptNode.CursorOp op, Scope scope) {
        Cursor cursor = findCursor(op.cursor, scope, op.getLine());
        switch (op.op) {
            case OPEN:
                open(cursor, scope);
                break;
            case CLOSE:
                cursor.result = null;
                break;
            case FETCH:
                if (cursor.result == null) {
                    throw statementError("Cursor '" + op.cursor + "' is not open", op.getLine());
                }
                boolean hasRow = cursor.next < cursor.result.getRows().size();
                for (int i = 0; i < op.into.size(); i++) {
                    ScriptValue value = hasRow && i < cursor.result.getColumns().size()
                            ? cursor.result.value(cursor.next, i) : ScriptValue.nullOf(null);
                    setVariable(op.into.get(i), value, scope, op.getLine());
                }
                if (hasRow) {
                    cursor.next++;
                }
                setRowCount(hasRow ? 1 : 0, scope);
                break;
            default:
                break;
        }
    }

    private void raise(ScriptNode.Raise r, Scope scope) {
        if (r.exceptionName == null) {
            if (caught.isEmpty()) {
                throw statementError("RAISE without an exception outside an exception handler", r.getLine());
            }
            throw caught.peek();
        }
        Object entry = scope.find(r.exceptionName);
        if (!(entry instanceof ExceptionDef e)) {
            throw expressionError("Undeclared exception '" + r.exceptionName + "'", r.getLine());
        }
        throw new ScriptException(e.name, e.code, e.message, r.getLine());
    }

    // ------------------------------------------------ SQL ---------------------------------------------------------

    /**
     * Snowflake starts a transaction with {@code BEGIN [WORK | TRANSACTION] [NAME n]} or
     * {@code START TRANSACTION [NAME n]}; StarRocks takes {@code BEGIN} and has no transaction names.
     * Returns the StarRocks statement, or null when the tokens are not such a statement.
     */
    static String beginTransaction(List<ScriptToken> tokens) {
        int at;
        if (!tokens.isEmpty() && tokens.get(0).is("BEGIN")) {
            at = 1;
            if (at < tokens.size() && (tokens.get(at).is("WORK") || tokens.get(at).is("TRANSACTION"))) {
                at++;
            }
        } else if (tokens.size() >= 2 && tokens.get(0).is("START") && tokens.get(1).is("TRANSACTION")) {
            at = 2;
        } else {
            return null;
        }
        if (at + 1 < tokens.size() && tokens.get(at).is("NAME")) {
            at += 2;
        }
        return at == tokens.size() ? "BEGIN" : null;
    }

    private void runSql(ScriptNode.Sql s, Scope scope) {
        String begin = beginTransaction(s.statement.getTokens());
        String sql = begin != null ? begin : bind(s.statement, scope, false);
        if (s.into == null) {
            long rows;
            try {
                rows = session.execute(sql);
            } catch (ScriptException e) {
                throw e;
            } catch (RuntimeException e) {
                throw statementError(message(e), s.getLine());
            }
            setRowCount(rows, scope);
            return;
        }
        QueryResult result = runQuery(sql, s.getLine());
        if (result.getRows().size() > 1) {
            throw statementError("SELECT ... INTO returned " + result.getRows().size() + " rows, expected one",
                    s.getLine());
        }
        if (result.getColumns().size() != s.into.size()) {
            throw statementError("SELECT ... INTO has " + s.into.size() + " variables for "
                    + result.getColumns().size() + " columns", s.getLine());
        }
        boolean found = !result.getRows().isEmpty();
        for (int i = 0; i < s.into.size(); i++) {
            setVariable(s.into.get(i), found ? result.value(0, i) : ScriptValue.nullOf(null), scope, s.getLine());
        }
        setRowCount(found ? 1 : 0, scope);
    }

    private QueryResult runQuery(String sql, int line) {
        try {
            return session.query(sql);
        } catch (ScriptException e) {
            throw e;
        } catch (RuntimeException e) {
            throw statementError(message(e), line);
        }
    }

    private void setRowCount(long rows, Scope scope) {
        Scope root = scope;
        while (root.parent != null) {
            root = root.parent;
        }
        set(root, "SQLROWCOUNT", new ScriptValue("BIGINT", String.valueOf(rows)));
        set(root, "SQLFOUND", new ScriptValue("BOOLEAN", rows > 0 ? "true" : "false"));
        set(root, "SQLNOTFOUND", new ScriptValue("BOOLEAN", rows > 0 ? "false" : "true"));
    }

    private static void set(Scope root, String name, ScriptValue value) {
        Object entry = root.entries.get(name.toLowerCase(Locale.ROOT));
        if (entry instanceof Variable v) {
            v.value = value;
        }
    }

    // ------------------------------------------------ expressions -------------------------------------------------

    private ScriptValue evaluate(Text expression, String type, Scope scope) {
        return evaluateSql(bind(expression, scope, true), type, expression.getLine());
    }

    /** Evaluates bound SQL, cast to {@code type} when it is not null. */
    private ScriptValue evaluateSql(String sql, String type, int line) {
        String expression = type == null ? sql : "CAST((" + sql + ") AS " + type + ")";
        ScriptValue value;
        try {
            value = session.evaluate(expression);
        } catch (ScriptException e) {
            throw e;
        } catch (RuntimeException e) {
            throw expressionError(message(e), line);
        }
        if (type != null && !type.equalsIgnoreCase(value.getType())) {
            // keep the declared type, for example DECIMAL(10, 2) instead of the type the cast reported
            value = new ScriptValue(type, value.getText());
        }
        return value;
    }

    /**
     * The SQL of {@code text} with its variables replaced by literals. {@code bareNames} also binds variables written
     * without ':' (in expressions), except inside a parenthesized SELECT.
     */
    String bind(Text text, Scope scope, boolean bareNames) {
        List<ScriptToken> tokens = text.getTokens();
        String source = text.getSource();
        StringBuilder sql = new StringBuilder();
        Deque<Boolean> queryParens = new ArrayDeque<>();
        ScriptToken previous = null;
        int i = 0;
        while (i < tokens.size()) {
            ScriptToken t = tokens.get(i);
            if (previous != null) {
                String gap = source.substring(Math.min(previous.getEnd(), t.getStart()), t.getStart());
                sql.append(gap.isBlank() ? gap : " ");
            }
            ScriptToken next = i + 1 < tokens.size() ? tokens.get(i + 1) : null;
            boolean inQuery = !queryParens.isEmpty() && queryParens.peek();
            if (t.isSymbol("(")) {
                queryParens.push(inQuery || next != null && (next.is("SELECT") || next.is("WITH")));
            } else if (t.isSymbol(")") && !queryParens.isEmpty()) {
                queryParens.pop();
            }

            if (t.is("IDENTIFIER") && next != null && next.isSymbol("(")) {
                int close = indexOf(tokens, ")", i + 2);
                if (close > 0) {
                    sql.append(identifier(tokens.subList(i + 2, close), scope, t.getLine()));
                    previous = tokens.get(close);
                    i = close + 1;
                    continue;
                }
            }
            if (t.isSymbol(":") && next != null && isName(next)
                    && (previous == null || previous.getKind() != ScriptToken.Kind.NUMBER)) {
                int used = bindName(tokens, i + 1, scope, sql, t.getLine());
                previous = tokens.get(i + used);
                i += used + 1;
                continue;
            }
            if (bareNames && !inQuery && t.getKind() == ScriptToken.Kind.WORD
                    && (previous == null || !previous.isSymbol("."))) {
                Object entry = scope.find(t.getText());
                boolean isVariable = entry instanceof Variable && (next == null || !next.isSymbol("("));
                if (isVariable || entry instanceof Row) {
                    int used = bindName(tokens, i, scope, sql, t.getLine());
                    previous = tokens.get(i + used - 1);
                    i += used;
                    continue;
                }
            }
            sql.append(source, t.getStart(), t.getEnd());
            previous = t;
            i++;
        }
        return sql.toString();
    }

    private static boolean isName(ScriptToken t) {
        return t.getKind() == ScriptToken.Kind.WORD || t.getKind() == ScriptToken.Kind.QUOTED_IDENT;
    }

    private static int indexOf(List<ScriptToken> tokens, String symbol, int from) {
        for (int k = from; k < tokens.size(); k++) {
            if (tokens.get(k).isSymbol(symbol)) {
                return k;
            }
        }
        return -1;
    }

    /** Appends the value of the variable (or row column) at {@code at}; returns how many tokens it used. */
    private int bindName(List<ScriptToken> tokens, int at, Scope scope, StringBuilder sql, int line) {
        String name = tokens.get(at).getText();
        Object entry = scope.find(name);
        if (entry instanceof Variable v) {
            sql.append(v.value.toSqlLiteral());
            return 1;
        }
        if (entry instanceof Row row) {
            if (at + 2 >= tokens.size() || !tokens.get(at + 1).isSymbol(".")) {
                throw expressionError("Row variable '" + name + "' needs a column, as " + name + ".column", line);
            }
            String column = tokens.get(at + 2).getText();
            ScriptValue value = row.columns.get(column.toLowerCase(Locale.ROOT));
            if (value == null) {
                throw expressionError("Row '" + name + "' has no column '" + column + "'", line);
            }
            sql.append(value.toSqlLiteral());
            return 3;
        }
        throw expressionError("Undeclared variable '" + name + "'", line);
    }

    /** {@code IDENTIFIER(:v)} or {@code IDENTIFIER('name')}: the name, quoted part by part. */
    private String identifier(List<ScriptToken> inner, Scope scope, int line) {
        String name;
        if (inner.size() == 1 && inner.get(0).getKind() == ScriptToken.Kind.STRING) {
            name = inner.get(0).getText();
        } else if (inner.size() == 2 && inner.get(0).isSymbol(":") && scope.find(inner.get(1).getText())
                instanceof Variable v) {
            name = v.value.getText();
        } else if (inner.size() == 1 && scope.find(inner.get(0).getText()) instanceof Variable v) {
            name = v.value.getText();
        } else {
            throw expressionError("IDENTIFIER() takes a string or a variable", line);
        }
        if (name == null || name.isEmpty()) {
            throw expressionError("IDENTIFIER() got an empty name", line);
        }
        StringBuilder quoted = new StringBuilder();
        for (String part : name.split("\\.")) {
            if (quoted.length() > 0) {
                quoted.append('.');
            }
            quoted.append('`').append(part.replace("`", "``")).append('`');
        }
        return quoted.toString();
    }

    // ------------------------------------------------ errors and limits -------------------------------------------

    private void step(ScriptNode node) {
        if (++steps > MAX_STEPS) {
            throw statementError("The script ran more than " + MAX_STEPS + " statements and loop iterations",
                    node.getLine());
        }
        session.checkAlive();
    }

    private static String message(RuntimeException e) {
        Throwable t = e;
        while (t.getCause() != null && (t.getMessage() == null || t.getMessage().isEmpty())) {
            t = t.getCause();
        }
        return t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
    }

    private static ScriptException expressionError(String message, int line) {
        return new ScriptException(ScriptException.EXPRESSION_ERROR, ScriptException.EXPRESSION_ERROR_CODE, message,
                line);
    }

    private static ScriptException statementError(String message, int line) {
        return new ScriptException(ScriptException.STATEMENT_ERROR, ScriptException.STATEMENT_ERROR_CODE, message,
                line);
    }
}
