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

import com.starrocks.authorization.SecurityPolicyRewriteRule;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.qe.QueryDetail;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Runs the body of a Snowflake style task: one statement or a script of statements separated by ';'.
 *
 * <p>The statements run in order in one session, so SET, USE and temporary tables carry over to the statements
 * after them. The run stops at the first failed statement and reports which one it was. A transaction the script
 * opened and did not finish is rolled back.
 */
public class ScriptTaskRunProcessor extends BaseTaskRunProcessor {
    private static final Logger LOG = LogManager.getLogger(ScriptTaskRunProcessor.class);

    @Override
    public Constants.TaskRunState processTaskRun(TaskRunContext context) throws Exception {
        ConnectContext ctx = context.getCtx();
        ctx.setQuerySource(QueryDetail.QuerySource.TASK);
        String script = context.getDefinition();
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
