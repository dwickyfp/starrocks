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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ScriptParserTest {

    @Test
    public void testBlockStructure() {
        ScriptNode.Block block = ScriptParser.parse(
                "DECLARE\n" +
                "  a INT DEFAULT 1;\n" +
                "  b DECIMAL(10, 2);\n" +
                "  c CURSOR FOR SELECT id FROM t WHERE x = :a;\n" +
                "  e EXCEPTION (-20001, 'bad');\n" +
                "BEGIN\n" +
                "  LET d VARCHAR := 'x;y';\n" +
                "  a := a + 1;\n" +
                "  INSERT INTO t VALUES (:a, CASE WHEN :a > 1 THEN 'big' ELSE 'small' END);\n" +
                "  SELECT count(*), max(id) INTO :a, :b FROM t;\n" +
                "  CALL db.p(:a, 'x') INTO :d;\n" +
                "  EXECUTE IMMEDIATE 'SELECT 1';\n" +
                "  BEGIN TRANSACTION;\n" +
                "  COMMIT;\n" +
                "EXCEPTION\n" +
                "  WHEN e OR STATEMENT_ERROR THEN RETURN 1;\n" +
                "  WHEN OTHER THEN RAISE;\n" +
                "END;");
        Assertions.assertEquals(4, block.declarations.size());
        ScriptNode.VariableDecl b = (ScriptNode.VariableDecl) block.declarations.get(1);
        Assertions.assertEquals("DECIMAL(10, 2)", b.typeSql);
        Assertions.assertNull(b.defaultValue);
        ScriptNode.ExceptionDecl e = (ScriptNode.ExceptionDecl) block.declarations.get(3);
        Assertions.assertEquals(-20001, e.code);
        Assertions.assertEquals("bad", e.message);

        List<ScriptNode> statements = block.statements;
        Assertions.assertEquals(8, statements.size());
        ScriptNode.Assign let = (ScriptNode.Assign) statements.get(0);
        Assertions.assertTrue(let.declare);
        Assertions.assertEquals("'x;y'", let.value.getSql(), "a ';' inside a string does not end the statement");
        Assertions.assertFalse(((ScriptNode.Assign) statements.get(1)).declare);
        ScriptNode.Sql insert = (ScriptNode.Sql) statements.get(2);
        Assertions.assertTrue(insert.statement.getSql().endsWith("END)"));
        ScriptNode.Sql select = (ScriptNode.Sql) statements.get(3);
        Assertions.assertEquals(List.of("a", "b"), select.into);
        Assertions.assertFalse(select.statement.getTokens().stream().anyMatch(t -> t.is("INTO")));
        ScriptNode.Call call = (ScriptNode.Call) statements.get(4);
        Assertions.assertEquals(List.of("db", "p"), call.nameParts);
        Assertions.assertEquals(2, call.arguments.size());
        Assertions.assertEquals("d", call.into);
        Assertions.assertTrue(statements.get(5) instanceof ScriptNode.ExecuteImmediate);
        Assertions.assertEquals("BEGIN TRANSACTION", ((ScriptNode.Sql) statements.get(6)).statement.getSql());
        Assertions.assertEquals(2, block.handlers.size());
        Assertions.assertEquals(List.of("e", "STATEMENT_ERROR"), block.handlers.get(0).exceptionNames);
    }

    @Test
    public void testLoopsAndLabels() {
        ScriptNode.Block block = ScriptParser.parse(
                "BEGIN\n" +
                "  FOR i IN REVERSE 1 TO 10 DO\n" +
                "    FOR r IN c LOOP BREAK outer_loop; END LOOP;\n" +
                "  END FOR outer_loop;\n" +
                "  WHILE (x < 3) LOOP x := x + 1; END LOOP;\n" +
                "  REPEAT x := x - 1; UNTIL (x = 0) END REPEAT;\n" +
                "  LOOP EXIT; END LOOP;\n" +
                "END");
        ScriptNode.ForRange range = (ScriptNode.ForRange) block.statements.get(0);
        Assertions.assertTrue(range.reverse);
        Assertions.assertEquals("outer_loop", range.label);
        ScriptNode.ForCursor cursor = (ScriptNode.ForCursor) range.body.get(0);
        Assertions.assertEquals("c", cursor.cursor);
        Assertions.assertEquals("outer_loop", ((ScriptNode.Jump) cursor.body.get(0)).label);
        Assertions.assertTrue(block.statements.get(1) instanceof ScriptNode.While);
        Assertions.assertEquals("(x = 0)", ((ScriptNode.Repeat) block.statements.get(2)).until.getSql());
        Assertions.assertTrue(block.statements.get(3) instanceof ScriptNode.InfiniteLoop);
    }

    @Test
    public void testStatementListWithoutBlock() {
        ScriptNode.Block block = ScriptParser.parse("INSERT INTO t VALUES (1); -- comment; here\n"
                + "/* block; comment */ UPDATE t SET v = 2");
        Assertions.assertEquals(2, block.statements.size());
        Assertions.assertTrue(((ScriptNode.Sql) block.statements.get(1)).statement.getSql().startsWith("UPDATE"));
    }

    @Test
    public void testSyntaxErrorsReportTheLine() {
        ScriptException e = Assertions.assertThrows(ScriptException.class, () -> ScriptParser.parse(
                "BEGIN\n  IF (1 = 1) THEN\n    RETURN 1;\n  END WHILE;\nEND"));
        Assertions.assertEquals(4, e.getLine());
        Assertions.assertTrue(e.getMessage().contains("Expected IF"), e.getMessage());
        Assertions.assertThrows(ScriptException.class, () -> ScriptParser.parse("BEGIN SELECT 'x; END"));
        Assertions.assertThrows(ScriptException.class, () -> ScriptParser.parse("DECLARE x RESULTSET; BEGIN END"));
        Assertions.assertThrows(ScriptException.class, () -> ScriptParser.parse("BEGIN END; extra"));
        Assertions.assertThrows(ScriptException.class, () -> ScriptParser.parse(
                "BEGIN EXECUTE IMMEDIATE 'x' USING (a); END"));
    }

    @Test
    public void testValueLiterals() {
        Assertions.assertEquals("(-5)", new ScriptValue("BIGINT", "-5").toSqlLiteral());
        Assertions.assertEquals("10.50", new ScriptValue("DECIMAL(10, 2)", "10.50").toSqlLiteral());
        Assertions.assertEquals("'it\\'s'", new ScriptValue("VARCHAR", "it's").toSqlLiteral());
        Assertions.assertEquals("TRUE", new ScriptValue("BOOLEAN", "1").toSqlLiteral());
        Assertions.assertEquals("CAST('2024-01-02' AS DATE)", new ScriptValue("DATE", "2024-01-02").toSqlLiteral());
        Assertions.assertEquals("CAST(NULL AS INT)", ScriptValue.nullOf("INT").toSqlLiteral());
        Assertions.assertEquals("NULL", ScriptValue.nullOf("NULL_TYPE").toSqlLiteral());
        Assertions.assertEquals("DECIMAL(9, 2)", ScriptValue.normalizeType("DECIMAL64(9, 2)"));
    }

    private static String begin(String sql) {
        List<ScriptToken> tokens = ScriptLexer.tokenize(sql).stream()
                .filter(t -> t.getKind() != ScriptToken.Kind.EOF).toList();
        return ScriptInterpreter.beginTransaction(tokens);
    }

    @Test
    public void testSnowflakeBeginTransaction() {
        Assertions.assertEquals("BEGIN", begin("BEGIN"));
        Assertions.assertEquals("BEGIN", begin("BEGIN TRANSACTION"));
        Assertions.assertEquals("BEGIN", begin("begin work"));
        Assertions.assertEquals("BEGIN", begin("BEGIN TRANSACTION NAME t1"));
        Assertions.assertEquals("BEGIN", begin("START TRANSACTION"));
        Assertions.assertEquals("BEGIN", begin("START TRANSACTION NAME t1"));
        // left to StarRocks as written
        Assertions.assertNull(begin("START TRANSACTION WITH LABEL l1"));
        Assertions.assertNull(begin("COMMIT"));
        Assertions.assertNull(begin("INSERT INTO t VALUES (1)"));
    }
}
