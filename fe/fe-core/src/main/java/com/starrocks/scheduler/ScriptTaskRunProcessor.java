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

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.authorization.SecurityPolicyRewriteRule;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.procedure.script.ScriptException;
import com.starrocks.procedure.script.ScriptLexer;
import com.starrocks.procedure.script.ScriptToken;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.qe.QueryDetail;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.CallProcedureStatement;
import com.starrocks.sql.ast.ProcedureArgument;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.parser.SqlParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;

/**
 * Runs the body of a Snowflake style task: one statement or a script of statements separated by ';'.
 *
 * <p>The statements run in order in one session, so SET, USE and temporary tables carry over to the statements
 * after them. The run stops at the first failed statement and reports which one it was. A transaction the script
 * opened and did not finish is rolled back.
 *
 * <p>{@code CALL SYSTEM$SET_RETURN_VALUE('<value>')} sets the return value of the run, shown as its extra message,
 * which the tasks after it in the task graph read with {@code SYSTEM$GET_PREDECESSOR_RETURN_VALUE(['<task>'])}:
 * the call is replaced by the value as a string literal, or NULL, before the statements are parsed.
 */
public class ScriptTaskRunProcessor extends BaseTaskRunProcessor {
    private static final Logger LOG = LogManager.getLogger(ScriptTaskRunProcessor.class);
    static final String SET_RETURN_VALUE = "SYSTEM$SET_RETURN_VALUE";
    static final String GET_PREDECESSOR_RETURN_VALUE = "SYSTEM$GET_PREDECESSOR_RETURN_VALUE";
    // Snowflake limits a task return value to 10 kB.
    static final int MAX_RETURN_VALUE_LENGTH = 10 * 1024;

    @Override
    public Constants.TaskRunState processTaskRun(TaskRunContext context) throws Exception {
        ConnectContext ctx = context.getCtx();
        ctx.setQuerySource(QueryDetail.QuerySource.TASK);
        String script = bindPredecessorReturnValues(context, context.getDefinition());
        List<StatementBase> statements = SqlParser.parse(script, ctx.getSessionVariable());
        int total = statements.size();
        Tracers.register(ctx);
        Tracers.init(ctx, "TIMER", null);
        try {
            for (int i = 0; i < total; i++) {
                TaskRun taskRun = context.getTaskRun();
                if (taskRun != null && taskRun.isKilled()) {
                    throw new StarRocksException("Task run was killed before statement " + (i + 1) + " of " + total);
                }
                if (setReturnValue(context, statements.get(i), i + 1, total)) {
                    updateProgress(context, i + 1, total);
                    continue;
                }
                executeStatement(context, statements.get(i), i > 0);
                QueryState state = ctx.getState();
                if (state.getStateType() == QueryState.MysqlStateType.ERR) {
                    String message = "Statement " + (i + 1) + " of " + total + " failed: " + state.getErrorMessage();
                    rollbackOpenTransaction(context);
                    // The run fails with this message rather than with the bare error kept in the session state.
                    state.reset();
                    throw new StarRocksException(message);
                }
                updateProgress(context, i + 1, total);
            }
            rollbackOpenTransaction(context);
            return Constants.TaskRunState.SUCCESS;
        } catch (Exception e) {
            rollbackOpenTransaction(context);
            throw e;
        } finally {
            Tracers.close();
        }
    }

    /**
     * Runs {@code CALL SYSTEM$SET_RETURN_VALUE(<literal>)}, or returns false for any other statement.
     */
    private boolean setReturnValue(TaskRunContext context, StatementBase statement, int index, int total)
            throws StarRocksException {
        if (!(statement instanceof CallProcedureStatement call)
                || !call.getQualifiedName().toString().equalsIgnoreCase(SET_RETURN_VALUE)) {
            return false;
        }
        List<ProcedureArgument> arguments = call.getArguments();
        if (arguments.size() != 1 || !(arguments.get(0).getValue() instanceof LiteralExpr literal)) {
            throw new StarRocksException("Statement " + index + " of " + total + " failed: " + SET_RETURN_VALUE
                    + " takes one literal value");
        }
        String value = literal instanceof NullLiteral ? null : literal.getStringValue();
        if (value != null && value.length() > MAX_RETURN_VALUE_LENGTH) {
            throw new StarRocksException("Statement " + index + " of " + total + " failed: the return value is longer "
                    + "than " + MAX_RETURN_VALUE_LENGTH + " characters");
        }
        TaskRunStatus status = context.getStatus();
        if (status != null) {
            status.setUserTaskReturnValue(value);
        }
        return true;
    }

    /**
     * Replaces each SYSTEM$GET_PREDECESSOR_RETURN_VALUE(['task']) in the script with the value as a literal.
     */
    @VisibleForTesting
    static String bindPredecessorReturnValues(String script, Function<String, String> lookup)
            throws StarRocksException {
        if (script == null || !script.toUpperCase(Locale.ROOT).contains(GET_PREDECESSOR_RETURN_VALUE)) {
            return script;
        }
        List<ScriptToken> tokens;
        try {
            tokens = ScriptLexer.tokenize(script);
        } catch (ScriptException e) {
            return script;
        }
        StringBuilder bound = new StringBuilder();
        int last = 0;
        for (int i = 0; i < tokens.size(); i++) {
            ScriptToken t = tokens.get(i);
            if (!t.is(GET_PREDECESSOR_RETURN_VALUE) || i + 1 >= tokens.size() || !tokens.get(i + 1).isSymbol("(")) {
                continue;
            }
            int close = i + 2;
            String predecessor = null;
            if (close < tokens.size() && tokens.get(close).getKind() == ScriptToken.Kind.STRING) {
                predecessor = tokens.get(close).getText();
                close++;
            }
            if (close >= tokens.size() || !tokens.get(close).isSymbol(")")) {
                throw new StarRocksException(GET_PREDECESSOR_RETURN_VALUE + " takes an optional task name as a "
                        + "string literal");
            }
            String value = lookup.apply(predecessor);
            bound.append(script, last, t.getStart())
                    .append(value == null ? "NULL" : "'" + value.replace("\\", "\\\\").replace("'", "''") + "'");
            last = tokens.get(close).getEnd();
            i = close;
        }
        return bound.append(script.substring(last)).toString();
    }

    private static String bindPredecessorReturnValues(TaskRunContext context, String script) throws StarRocksException {
        TaskRunStatus status = context.getStatus();
        String queryId = status == null ? null : status.getQueryId();
        String taskName = status == null ? null : status.getTaskName();
        try {
            return bindPredecessorReturnValues(script, predecessor -> GlobalStateMgr.getCurrentState().getTaskManager()
                    .getUserTaskGraphRunner().getPredecessorReturnValue(queryId, taskName, predecessor));
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new StarRocksException(e.getMessage());
        }
    }

    private void executeStatement(TaskRunContext context, StatementBase statement, boolean newQueryId) throws Exception {
        ConnectContext ctx = context.getCtx();
        if (newQueryId) {
            // Every statement gets its own query id, so the audit log and query details show each one.
            ctx.setQueryId(UUIDUtil.genUUID());
        }
        ctx.getState().reset();
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(context.getRemoteIp())
                .setUser(ctx.getQualifiedUser())
                .setDb(ctx.getDatabase())
                .setCatalog(ctx.getCurrentCatalog());
        SecurityPolicyRewriteRule.markRelationsForRewrite(statement);
        StmtExecutor executor = StmtExecutor.newInternalExecutor(ctx, statement);
        ctx.setExecutor(executor);
        ctx.setMultiStmt(false);
        ctx.setThreadLocalInfo();
        try {
            executor.addRunningQueryDetail(statement);
            executor.execute();
        } finally {
            String sql = AstToSQLBuilder.toSQLOrDefault(statement, context.getDefinition());
            new ConnectProcessor(ctx).auditAfterExec(sql, executor.getParsedStmt(),
                    executor.getQueryStatisticsForAuditLog());
            executor.addFinishedQueryDetail();
        }
    }

    private void updateProgress(TaskRunContext context, int done, int total) {
        TaskRunStatus status = context.getStatus();
        if (status != null && total > 1) {
            status.setProgress(done * 100 / total);
        }
    }

    /**
     * A script may open a transaction with BEGIN; if it ends or fails before COMMIT, roll the transaction back so it
     * does not stay open after the run.
     */
    private void rollbackOpenTransaction(TaskRunContext context) {
        ConnectContext ctx = context.getCtx();
        if (ctx.getTxnId() == 0) {
            return;
        }
        long txnId = ctx.getTxnId();
        try {
            StatementBase rollback = SqlParser.parse("ROLLBACK", ctx.getSessionVariable()).get(0);
            StmtExecutor executor = StmtExecutor.newInternalExecutor(ctx, rollback);
            ctx.setExecutor(executor);
            executor.execute();
            LOG.warn("Rolled back transaction {} left open by task run {}", txnId, context.getTaskRunId());
        } catch (Exception e) {
            LOG.warn("Failed to roll back transaction {} left open by task run {}", txnId, context.getTaskRunId(), e);
        }
    }
}
