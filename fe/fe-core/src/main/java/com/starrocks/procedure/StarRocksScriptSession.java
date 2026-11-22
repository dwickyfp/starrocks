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

package com.starrocks.procedure;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.starrocks.authorization.SecurityPolicyRewriteRule;
import com.starrocks.common.AuditLog;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.SqlCredentialRedactor;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.procedure.script.QueryResult;
import com.starrocks.procedure.script.ScriptException;
import com.starrocks.procedure.script.ScriptSession;
import com.starrocks.procedure.script.ScriptValue;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs a procedure's SQL in its own session. Constant expressions are folded on the FE; anything else runs as a
 * query. Every statement gets its own query id and goes to the audit log.
 */
public class StarRocksScriptSession implements ScriptSession {
    private final ConnectContext ctx;
    private final String procedureName;
    private final int depth;

    public StarRocksScriptSession(ConnectContext ctx, String procedureName, int depth) {
        this.ctx = ctx;
        this.procedureName = procedureName;
        this.depth = depth;
    }

    public ConnectContext getContext() {
        return ctx;
    }

    @Override
    public ScriptValue evaluate(String expressionSql) {
        ScriptValue folded = fold(expressionSql);
        if (folded != null) {
            return folded;
        }
        QueryResult result = query("SELECT " + expressionSql);
        if (result.getRows().size() != 1 || result.getColumns().size() != 1) {
            throw new IllegalStateException("Expression returned " + result.getRows().size() + " rows: "
                    + expressionSql);
        }
        return result.value(0, 0);
    }

    /** The value of a constant expression, computed on the FE, or null when it needs to run. */
    private ScriptValue fold(String expressionSql) {
        try {
            Expr expr = SqlParser.parseSqlToExpr(expressionSql, ctx.getSessionVariable().getSqlMode());
            Scope scope = new Scope(RelationId.anonymous(), new RelationFields());
            ExpressionAnalyzer.analyzeExpression(expr, new AnalyzeState(), scope, ctx);
            ScalarOperator op = SqlToScalarOperatorTranslator.translate(expr, new ExpressionMapping(scope),
                    new ColumnRefFactory());
            if (op instanceof ConstantOperator constant) {
                return new ScriptValue(constant.getType().toSql(), constant.isNull() ? null : constant.toString());
            }
        } catch (Exception e) {
            // not foldable here (a subquery, a non-deterministic function, an error): run it as a query
        }
        return null;
    }

    @Override
    public QueryResult query(String sql) {
        StatementBase statement = parseOne(sql);
        if (!(statement instanceof QueryStatement)) {
            throw new IllegalArgumentException("Expected a query, got: " + sql);
        }
        return runQuery(statement, sql);
    }

    private QueryResult runQuery(StatementBase statement, String sql) {
        newStatement();
        try {
            SecurityPolicyRewriteRule.markRelationsForRewrite(statement);
            ExecPlan plan = StatementPlanner.plan(statement, ctx, TResultSinkType.HTTP_PROTOCAL);
            StmtExecutor executor = StmtExecutor.newInternalExecutor(ctx, statement);
            ctx.setExecutor(executor);
            ConnectContext parent = ctx.getParent();
            if (parent != null && parent.getExecutor() != null) {
                parent.getExecutor().registerSubStmtExecutor(executor);
            }
            audit(sql);
            Pair<List<TResultBatch>, Status> result = executor.executeStmtWithExecPlan(ctx, plan);
            if (!result.second.ok()) {
                throw new IllegalStateException(result.second.getErrorMsg());
            }
            List<String> types = new ArrayList<>();
            for (Expr expr : plan.getOutputExprs()) {
                types.add(expr.getType().toSql());
            }
            return new QueryResult(new ArrayList<>(plan.getColNames()), types, decode(result.first,
                    types.size()));
        } catch (ScriptException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage() == null ? e.toString() : e.getMessage(), e);
        }
    }

    private static List<List<String>> decode(List<TResultBatch> batches, int columns) {
        List<List<String>> rows = new ArrayList<>();
        if (batches == null) {
            return rows;
        }
        for (TResultBatch batch : batches) {
            if (batch.getRows() == null) {
                continue;
            }
            for (ByteBuffer buffer : batch.getRows()) {
                String json = StandardCharsets.UTF_8.decode(buffer.duplicate()).toString();
                JsonArray data = JsonParser.parseString(json).getAsJsonObject().getAsJsonArray("data");
                List<String> row = new ArrayList<>(columns);
                for (JsonElement cell : data) {
                    if (cell == null || cell.isJsonNull()) {
                        row.add(null);
                    } else if (cell.isJsonPrimitive()) {
                        row.add(cell.getAsString());
                    } else {
                        row.add(cell.toString());
                    }
                }
                rows.add(row);
            }
        }
        return rows;
    }

    @Override
    public long execute(String sql) {
        StatementBase statement = parseOne(sql);
        if (statement instanceof QueryStatement) {
            return runQuery(statement, sql).getRows().size();
        }
        newStatement();
        SecurityPolicyRewriteRule.markRelationsForRewrite(statement);
        StmtExecutor executor = StmtExecutor.newInternalExecutor(ctx, statement);
        ctx.setExecutor(executor);
        ctx.setMultiStmt(false);
        try {
            executor.addRunningQueryDetail(statement);
            executor.execute();
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage() == null ? e.toString() : e.getMessage(), e);
        } finally {
            new ConnectProcessor(ctx).auditAfterExec(sql, executor.getParsedStmt(),
                    executor.getQueryStatisticsForAuditLog());
            executor.addFinishedQueryDetail();
        }
        QueryState state = ctx.getState();
        if (state.getStateType() == QueryState.MysqlStateType.ERR) {
            String message = state.getErrorMessage();
            state.reset();
            throw new IllegalStateException(message);
        }
        return state.getAffectedRows();
    }

    private StatementBase parseOne(String sql) {
        List<StatementBase> statements = SqlParser.parse(sql, ctx.getSessionVariable());
        if (statements.size() != 1) {
            throw new IllegalArgumentException("Expected one statement, got " + statements.size() + ": " + sql);
        }
        return statements.get(0);
    }

    private void newStatement() {
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
        ctx.getState().reset();
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(ctx.getRemoteIP())
                .setUser(ctx.getQualifiedUser())
                .setDb(ctx.getDatabase())
                .setCatalog(ctx.getCurrentCatalog());
    }

    private void audit(String sql) {
        AuditLog.getInternalAudit().info("Procedure {} | Query_id {} | {}", procedureName,
                DebugUtil.printId(ctx.getQueryId()), SqlCredentialRedactor.redact(sql));
    }

    @Override
    public void checkAlive() {
        try {
            SimpleExecutor.checkInternalQueryState(ctx);
        } catch (Exception e) {
            throw new ScriptException(ScriptException.STATEMENT_ERROR, ScriptException.STATEMENT_ERROR_CODE,
                    "Procedure " + procedureName + ": " + e.getMessage(), 0);
        }
    }

    @Override
    public ScriptValue call(List<String> nameParts, List<ScriptValue> arguments) {
        StoredProcedure procedure = resolve(nameParts);
        if (procedure.returnsTable()) {
            throw new ScriptException(ScriptException.STATEMENT_ERROR, ScriptException.STATEMENT_ERROR_CODE,
                    "Procedure " + procedure.getName() + " RETURNS TABLE: read it with LET rs RESULTSET := (CALL "
                            + procedure.getName() + "(...))", 0);
        }
        return ProcedureExecutor.invoke(procedure, literals(arguments), null, ctx, depth + 1).value;
    }

    @Override
    public QueryResult callTable(List<String> nameParts, List<ScriptValue> arguments) {
        StoredProcedure procedure = resolve(nameParts);
        if (!procedure.returnsTable()) {
            throw new ScriptException(ScriptException.STATEMENT_ERROR, ScriptException.STATEMENT_ERROR_CODE,
                    "Procedure " + procedure.getName() + " RETURNS " + procedure.getReturnDisplay()
                            + ", not a TABLE", 0);
        }
        return ProcedureExecutor.invoke(procedure, literals(arguments), null, ctx, depth + 1).table;
    }

    private StoredProcedure resolve(List<String> nameParts) {
        StoredProcedure procedure = ProcedureExecutor.resolve(nameParts, ctx);
        if (procedure == null) {
            throw new ScriptException(ScriptException.STATEMENT_ERROR, ScriptException.STATEMENT_ERROR_CODE,
                    "Procedure " + String.join(".", nameParts) + " does not exist", 0);
        }
        return procedure;
    }

    private static List<String> literals(List<ScriptValue> arguments) {
        List<String> sqls = new ArrayList<>();
        for (ScriptValue argument : arguments) {
            sqls.add(argument.toSqlLiteral());
        }
        return sqls;
    }

    @Override
    public void rollbackOpenTransaction() {
        if (ctx.getTxnId() == 0) {
            return;
        }
        try {
            execute("ROLLBACK");
        } catch (Exception e) {
            AuditLog.getInternalAudit().warn("Procedure {} failed to roll back transaction {}: {}", procedureName,
                    ctx.getTxnId(), e.getMessage());
        }
    }
}
