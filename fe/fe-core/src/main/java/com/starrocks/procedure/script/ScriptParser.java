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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Parses the Snowflake Scripting subset that stored procedures and EXECUTE IMMEDIATE run:
 *
 * <pre>
 * [DECLARE { var [type] [DEFAULT expr]; | c CURSOR FOR query; | e EXCEPTION [(code, 'message')]; } ...]
 * BEGIN
 *   statements
 * [EXCEPTION WHEN name [OR name ...] THEN statements ... [WHEN OTHER THEN statements]]
 * END;
 * </pre>
 *
 * <p>Statements are LET, {@code :=}, IF, CASE, FOR (range or cursor), WHILE, REPEAT, LOOP, BREAK, CONTINUE, RETURN,
 * RAISE, NULL, OPEN / FETCH / CLOSE, CALL, EXECUTE IMMEDIATE, nested blocks, and any SQL statement up to its
 * {@code ;}. A script without DECLARE or BEGIN is read as a list of statements.
 */
public final class ScriptParser {
    private static final Set<String> LOOP_ENDS = new HashSet<>(Arrays.asList("FOR", "LOOP", "WHILE", "REPEAT"));

    private final String source;
    private final List<ScriptToken> tokens;
    private int pos = 0;

    private ScriptParser(String source) {
        this.source = source;
        this.tokens = ScriptLexer.tokenize(source);
    }

    public static ScriptNode.Block parse(String source) {
        return new ScriptParser(source).parseScript();
    }

    private ScriptNode.Block parseScript() {
        ScriptNode.Block block;
        if (peek().is("DECLARE") || isBlockBegin()) {
            block = parseBlock();
            acceptSymbol(";");
        } else {
            int line = peek().getLine();
            block = new ScriptNode.Block(line, new ArrayList<>(), parseStatements(), new ArrayList<>());
        }
        while (acceptSymbol(";")) {
            // stray separators
        }
        if (peek().getKind() != ScriptToken.Kind.EOF) {
            throw error("Unexpected " + peek() + " after the end of the block");
        }
        return block;
    }

    // ------------------------------------------------ blocks ------------------------------------------------------

    private boolean isBlockBegin() {
        if (!peek().is("BEGIN")) {
            return false;
        }
        ScriptToken next = peek(1);
        // BEGIN TRANSACTION, BEGIN WORK and a bare BEGIN; start a transaction
        return !(next.is("TRANSACTION") || next.is("WORK") || next.isSymbol(";")
                || next.getKind() == ScriptToken.Kind.EOF);
    }

    private ScriptNode.Block parseBlock() {
        int line = peek().getLine();
        List<ScriptNode> declarations = new ArrayList<>();
        if (accept("DECLARE")) {
            while (!peek().is("BEGIN")) {
                if (peek().getKind() == ScriptToken.Kind.EOF) {
                    throw error("Expected BEGIN after DECLARE");
                }
                declarations.add(parseDeclaration());
            }
        }
        expect("BEGIN");
        List<ScriptNode> statements = parseStatements("EXCEPTION", "END");
        List<ScriptNode.Handler> handlers = new ArrayList<>();
        if (accept("EXCEPTION")) {
            if (!peek().is("WHEN")) {
                throw error("Expected WHEN after EXCEPTION");
            }
            while (accept("WHEN")) {
                List<String> names = new ArrayList<>();
                names.add(name());
                while (accept("OR")) {
                    names.add(name());
                }
                expect("THEN");
                handlers.add(new ScriptNode.Handler(names, parseStatements("WHEN", "END")));
            }
        }
        expect("END");
        return new ScriptNode.Block(line, declarations, statements, handlers);
    }

    private ScriptNode parseDeclaration() {
        int line = peek().getLine();
        String name = name();
        if (accept("CURSOR")) {
            expect("FOR");
            Text query = collect(Set.of(), true);
            expectSymbol(";");
            return new ScriptNode.CursorDecl(line, name, query);
        }
        if (accept("EXCEPTION")) {
            Integer code = null;
            String message = null;
            if (acceptSymbol("(")) {
                boolean negative = acceptSymbol("-");
                ScriptToken number = next();
                if (number.getKind() != ScriptToken.Kind.NUMBER) {
                    throw error("Expected the exception number, got " + number);
                }
                code = Integer.parseInt(number.getText()) * (negative ? -1 : 1);
                expectSymbol(",");
                ScriptToken text = next();
                if (text.getKind() != ScriptToken.Kind.STRING) {
                    throw error("Expected the exception message, got " + text);
                }
                message = text.getText();
                expectSymbol(")");
            }
            expectSymbol(";");
            return new ScriptNode.ExceptionDecl(line, name, code, message);
        }
        if (peek().is("RESULTSET")) {
            throw error("RESULTSET variables are not supported, use a CURSOR");
        }
        String typeSql = typeUntilValue();
        Text value = null;
        if (accept("DEFAULT") || acceptSymbol(":=")) {
            value = collect(Set.of(), true);
        }
        expectSymbol(";");
        return new ScriptNode.VariableDecl(line, name, typeSql, value);
    }

    /** The type written before DEFAULT, := or ;, or null when there is none. */
    private String typeUntilValue() {
        int start = pos;
        int depth = 0;
        while (true) {
            ScriptToken t = peek();
            if (t.getKind() == ScriptToken.Kind.EOF) {
                throw error("Unexpected end of script in a declaration");
            }
            if (depth == 0 && (t.is("DEFAULT") || t.isSymbol(":=") || t.isSymbol(";"))) {
                break;
            }
            if (t.isSymbol("(")) {
                depth++;
            } else if (t.isSymbol(")")) {
                depth--;
            }
            pos++;
        }
        if (pos == start) {
            return null;
        }
        return source.substring(tokens.get(start).getStart(), tokens.get(pos - 1).getEnd());
    }

    // ------------------------------------------------ statements --------------------------------------------------

    private List<ScriptNode> parseStatements(String... stopWords) {
        List<ScriptNode> statements = new ArrayList<>();
        while (true) {
            while (acceptSymbol(";")) {
                // empty statement
            }
            ScriptToken t = peek();
            if (t.getKind() == ScriptToken.Kind.EOF) {
                if (stopWords.length > 0) {
                    throw error("Unexpected end of script, expected " + String.join(" or ", stopWords));
                }
                return statements;
            }
            for (String stop : stopWords) {
                if (t.is(stop)) {
                    return statements;
                }
            }
            statements.add(parseStatement());
        }
    }

    private ScriptNode parseStatement() {
        ScriptToken t = peek();
        int line = t.getLine();
        if (t.is("DECLARE") || isBlockBegin()) {
            ScriptNode.Block block = parseBlock();
            endStatement();
            return block;
        }
        if (t.is("LET")) {
            pos++;
            String name = name();
            if (accept("CURSOR")) {
                expect("FOR");
                Text query = collect(Set.of(), true);
                endStatement();
                return new ScriptNode.CursorDecl(line, name, query);
            }
            if (peek().is("RESULTSET")) {
                throw error("RESULTSET variables are not supported, use a CURSOR");
            }
            String typeSql = typeUntilValue();
            if (!accept("DEFAULT") && !acceptSymbol(":=")) {
                throw error("Expected := or DEFAULT in LET " + name);
            }
            Text value = collect(Set.of(), true);
            endStatement();
            return new ScriptNode.Assign(line, name, typeSql, value, true);
        }
        if ((t.getKind() == ScriptToken.Kind.WORD || t.getKind() == ScriptToken.Kind.QUOTED_IDENT)
                && peek(1).isSymbol(":=")) {
            String name = name();
            pos++;
            Text value = collect(Set.of(), true);
            endStatement();
            return new ScriptNode.Assign(line, name, null, value, false);
        }
        if (t.is("IF")) {
            return parseIf();
        }
        if (t.is("CASE")) {
            return parseCase();
        }
        if (t.is("FOR")) {
            return parseFor();
        }
        if (t.is("WHILE")) {
            pos++;
            Text condition = collect(Set.of("DO", "LOOP"), false);
            next();
            List<ScriptNode> body = parseStatements("END");
            return new ScriptNode.While(line, endLoop(), body, condition);
        }
        if (t.is("REPEAT")) {
            pos++;
            List<ScriptNode> body = parseStatements("UNTIL");
            expect("UNTIL");
            Text until = collect(Set.of("END"), false);
            return new ScriptNode.Repeat(line, endLoop(), body, until);
        }
        if (t.is("LOOP")) {
            pos++;
            List<ScriptNode> body = parseStatements("END");
            return new ScriptNode.InfiniteLoop(line, endLoop(), body);
        }
        if (t.is("BREAK") || t.is("EXIT") || t.is("CONTINUE") || t.is("ITERATE")) {
            pos++;
            boolean isContinue = t.is("CONTINUE") || t.is("ITERATE");
            String label = isStatementEnd() ? null : name();
            endStatement();
            return new ScriptNode.Jump(line, isContinue, label);
        }
        if (t.is("RETURN")) {
            pos++;
            Text value = isStatementEnd() ? null : collect(Set.of(), true);
            endStatement();
            return new ScriptNode.Return(line, value);
        }
        if (t.is("RAISE")) {
            pos++;
            String name = isStatementEnd() ? null : name();
            endStatement();
            return new ScriptNode.Raise(line, name);
        }
        if (t.is("NULL") && (peek(1).isSymbol(";") || peek(1).getKind() == ScriptToken.Kind.EOF)) {
            pos++;
            endStatement();
            return new ScriptNode.Null(line);
        }
        if (t.is("OPEN") || t.is("CLOSE")) {
            pos++;
            String cursor = name();
            endStatement();
            return new ScriptNode.CursorOp(line, t.is("OPEN") ? ScriptNode.CursorOp.Op.OPEN
                    : ScriptNode.CursorOp.Op.CLOSE, cursor, null);
        }
        if (t.is("FETCH")) {
            pos++;
            String cursor = name();
            expect("INTO");
            List<String> into = variableList();
            endStatement();
            return new ScriptNode.CursorOp(line, ScriptNode.CursorOp.Op.FETCH, cursor, into);
        }
        if (t.is("CALL")) {
            return parseCall();
        }
        if (t.is("EXECUTE") && peek(1).is("IMMEDIATE")) {
            pos += 2;
            Text statement = collect(Set.of("USING"), true);
            if (peek().is("USING")) {
                throw error("EXECUTE IMMEDIATE ... USING is not supported, bind variables with :name");
            }
            endStatement();
            return new ScriptNode.ExecuteImmediate(line, statement);
        }
        return parseSql();
    }

    private ScriptNode parseIf() {
        int line = next().getLine();
        List<Text> conditions = new ArrayList<>();
        List<List<ScriptNode>> branches = new ArrayList<>();
        conditions.add(collect(Set.of("THEN"), false));
        expect("THEN");
        branches.add(parseStatements("ELSEIF", "ELSE", "END"));
        List<ScriptNode> elseBranch = null;
        while (true) {
            if (accept("ELSEIF")) {
                conditions.add(collect(Set.of("THEN"), false));
                expect("THEN");
                branches.add(parseStatements("ELSEIF", "ELSE", "END"));
            } else if (accept("ELSE")) {
                elseBranch = parseStatements("END");
            } else {
                break;
            }
        }
        expect("END");
        expect("IF");
        endStatement();
        return new ScriptNode.If(line, conditions, branches, elseBranch);
    }

    private ScriptNode parseCase() {
        int line = next().getLine();
        Text operand = peek().is("WHEN") ? null : collect(Set.of("WHEN"), false);
        List<Text> whens = new ArrayList<>();
        List<List<ScriptNode>> branches = new ArrayList<>();
        if (!peek().is("WHEN")) {
            throw error("Expected WHEN in CASE");
        }
        while (accept("WHEN")) {
            whens.add(collect(Set.of("THEN"), false));
            expect("THEN");
            branches.add(parseStatements("WHEN", "ELSE", "END"));
        }
        List<ScriptNode> elseBranch = null;
        if (accept("ELSE")) {
            elseBranch = parseStatements("END");
        }
        expect("END");
        accept("CASE");
        endStatement();
        return new ScriptNode.Case(line, operand, whens, branches, elseBranch);
    }

    private ScriptNode parseFor() {
        int line = next().getLine();
        String variable = name();
        expect("IN");
        boolean reverse = accept("REVERSE");
        Text first = collect(Set.of("TO", "DO", "LOOP"), false);
        if (accept("TO")) {
            Text to = collect(Set.of("DO", "LOOP"), false);
            next();
            List<ScriptNode> body = parseStatements("END");
            return new ScriptNode.ForRange(line, endLoop(), body, variable, reverse, first, to);
        }
        next();
        if (first.getTokens().size() != 1 || first.getTokens().get(0).getKind() == ScriptToken.Kind.STRING
                || first.getTokens().get(0).getKind() == ScriptToken.Kind.NUMBER) {
            throw error("FOR ... IN expects a cursor name or 'from TO to', got " + first.getSql());
        }
        if (reverse) {
            throw error("REVERSE applies only to FOR ... IN from TO to");
        }
        List<ScriptNode> body = parseStatements("END");
        return new ScriptNode.ForCursor(line, endLoop(), body, variable, first.getTokens().get(0).getText());
    }

    /** {@code END FOR | LOOP | WHILE | REPEAT [label] ;}; returns the label or null. */
    private String endLoop() {
        expect("END");
        ScriptToken kind = next();
        if (kind.getKind() != ScriptToken.Kind.WORD || !LOOP_ENDS.contains(kind.getText().toUpperCase())) {
            throw error("Expected END FOR, END LOOP, END WHILE or END REPEAT, got END " + kind);
        }
        String label = isStatementEnd() ? null : name();
        endStatement();
        return label;
    }

    private ScriptNode parseCall() {
        int line = next().getLine();
        List<String> parts = new ArrayList<>();
        parts.add(name());
        while (acceptSymbol(".")) {
            parts.add(name());
        }
        expectSymbol("(");
        List<Text> arguments = new ArrayList<>();
        if (!acceptSymbol(")")) {
            while (true) {
                arguments.add(collect(Set.of(), false, ",", ")"));
                if (acceptSymbol(")")) {
                    break;
                }
                expectSymbol(",");
            }
        }
        String into = null;
        if (accept("INTO")) {
            List<String> targets = variableList();
            if (targets.size() != 1) {
                throw error("CALL ... INTO takes one variable");
            }
            into = targets.get(0);
        }
        endStatement();
        return new ScriptNode.Call(line, parts, arguments, into);
    }

    private ScriptNode parseSql() {
        int line = peek().getLine();
        Text statement = collect(Set.of("END"), true);
        endStatement();
        List<ScriptToken> stmtTokens = statement.getTokens();
        ScriptToken first = stmtTokens.get(0);
        if (!first.is("SELECT") && !first.is("WITH")) {
            return new ScriptNode.Sql(line, statement, null);
        }
        // SELECT ... INTO :a, :b ... : take the targets out of the statement
        int depth = 0;
        for (int i = 0; i < stmtTokens.size(); i++) {
            ScriptToken t = stmtTokens.get(i);
            if (t.isSymbol("(")) {
                depth++;
            } else if (t.isSymbol(")")) {
                depth--;
            } else if (depth == 0 && t.is("INTO") && i + 1 < stmtTokens.size() && stmtTokens.get(i + 1).isSymbol(":")) {
                List<String> into = new ArrayList<>();
                int j = i + 1;
                while (j + 1 < stmtTokens.size() && stmtTokens.get(j).isSymbol(":")) {
                    into.add(identifierText(stmtTokens.get(j + 1)));
                    j += 2;
                    if (j < stmtTokens.size() && stmtTokens.get(j).isSymbol(",")) {
                        j++;
                    } else {
                        break;
                    }
                }
                List<ScriptToken> kept = new ArrayList<>(stmtTokens.subList(0, i));
                kept.addAll(stmtTokens.subList(j, stmtTokens.size()));
                return new ScriptNode.Sql(line, new Text(source, kept), into);
            }
        }
        return new ScriptNode.Sql(line, statement, null);
    }

    // ------------------------------------------------ helpers -----------------------------------------------------

    /**
     * Collects tokens up to one of {@code stopWords} (or {@code ;} when {@code stopAtSemicolon}) outside parentheses
     * and CASE ... END, without consuming the stop token.
     */
    private Text collect(Set<String> stopWords, boolean stopAtSemicolon, String... stopSymbols) {
        int start = pos;
        int depth = 0;
        int caseDepth = 0;
        while (true) {
            ScriptToken t = peek();
            if (t.getKind() == ScriptToken.Kind.EOF) {
                break;
            }
            if (depth == 0 && caseDepth == 0) {
                if (stopAtSemicolon && t.isSymbol(";")) {
                    break;
                }
                if (t.getKind() == ScriptToken.Kind.WORD && stopWords.contains(t.getText().toUpperCase())) {
                    break;
                }
                boolean stop = false;
                for (String symbol : stopSymbols) {
                    stop |= t.isSymbol(symbol);
                }
                if (stop) {
                    break;
                }
            }
            if (t.isSymbol(";") && depth == 0 && caseDepth == 0) {
                throw error("Unexpected ';'");
            }
            if (t.isSymbol("(")) {
                depth++;
            } else if (t.isSymbol(")")) {
                if (depth == 0) {
                    throw error("Unbalanced ')'");
                }
                depth--;
            } else if (t.is("CASE")) {
                caseDepth++;
            } else if (t.is("END") && caseDepth > 0) {
                caseDepth--;
            }
            pos++;
        }
        if (pos == start) {
            throw error("Expected an expression or a statement, got " + peek());
        }
        return new Text(source, new ArrayList<>(tokens.subList(start, pos)));
    }

    private List<String> variableList() {
        List<String> names = new ArrayList<>();
        do {
            acceptSymbol(":");
            names.add(name());
        } while (acceptSymbol(","));
        return names;
    }

    private String name() {
        ScriptToken t = next();
        if (t.getKind() != ScriptToken.Kind.WORD && t.getKind() != ScriptToken.Kind.QUOTED_IDENT) {
            throw error("Expected a name, got " + t, t);
        }
        return t.getText();
    }

    private static String identifierText(ScriptToken t) {
        return t.getText();
    }

    private boolean isStatementEnd() {
        return peek().isSymbol(";") || peek().getKind() == ScriptToken.Kind.EOF;
    }

    private void endStatement() {
        if (!acceptSymbol(";") && peek().getKind() != ScriptToken.Kind.EOF && !peek().is("END")) {
            throw error("Expected ';', got " + peek());
        }
    }

    private ScriptToken peek() {
        return tokens.get(Math.min(pos, tokens.size() - 1));
    }

    private ScriptToken peek(int ahead) {
        return tokens.get(Math.min(pos + ahead, tokens.size() - 1));
    }

    private ScriptToken next() {
        ScriptToken t = peek();
        if (pos < tokens.size() - 1) {
            pos++;
        }
        return t;
    }

    private boolean accept(String keyword) {
        if (peek().is(keyword)) {
            pos++;
            return true;
        }
        return false;
    }

    private boolean acceptSymbol(String symbol) {
        if (peek().isSymbol(symbol)) {
            pos++;
            return true;
        }
        return false;
    }

    private void expect(String keyword) {
        if (!accept(keyword)) {
            throw error("Expected " + keyword + ", got " + peek());
        }
    }

    private void expectSymbol(String symbol) {
        if (!acceptSymbol(symbol)) {
            throw error("Expected '" + symbol + "', got " + peek());
        }
    }

    private ScriptException error(String message) {
        return error(message, peek());
    }

    private static ScriptException error(String message, ScriptToken at) {
        return new ScriptException(ScriptException.STATEMENT_ERROR, ScriptException.STATEMENT_ERROR_CODE,
                "Syntax error: " + message, at.getLine());
    }
}
