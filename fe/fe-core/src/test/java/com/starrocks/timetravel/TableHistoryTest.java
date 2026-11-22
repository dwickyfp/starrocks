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

package com.starrocks.timetravel;

import com.starrocks.catalog.OlapTable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.TableHistoryExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DescribeHistoryStmt;
import com.starrocks.sql.ast.RestoreTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DESCRIBE HISTORY: the commits a table records; RESTORE TABLE: the statements it runs.
 */
public class TableHistoryTest {
    private static final long HOUR = 3600_000L;
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext ctx;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("th").useDatabase("th");
        starRocksAssert.withTable("CREATE TABLE hpk (k INT NOT NULL, v VARCHAR(10), g BIGINT AS (k * 2)) "
                + "PRIMARY KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', 'data_retention_time_in_days' = '1')");
        starRocksAssert.withTable("CREATE TABLE hdup (k INT, v INT) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', 'data_retention_time_in_days' = '1')");
        starRocksAssert.withTable("CREATE TABLE hplain (k INT, v INT) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) "
                + "BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        starRocksAssert.withView("CREATE VIEW hview AS SELECT * FROM hdup");
    }

    private static OlapTable table(String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("th", name);
    }

    private static StatementBase parse(String sql) {
        return SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
    }

    private static TransactionState txn(long id, TransactionState.LoadJobSourceType source, long rows,
                                        String operation) {
        TransactionState txn = new TransactionState(id, "label_" + id, null, source,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "127.0.0.1"), 60_000L);
        if (rows >= 0) {
            InsertTxnCommitAttachment attachment = new InsertTxnCommitAttachment(rows);
            attachment.setOperation(operation);
            txn.setTxnCommitAttachment(attachment);
        }
        return txn;
    }

    @Test
    public void testCommitHistory() {
        TableCommitHistory history = new TableCommitHistory();
        long now = System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            history.record(new TableCommitHistory.Commit(now - (5 - i) * HOUR, "INSERT_INTO", i, 1, 100 + i, "l" + i),
                    now - 3 * HOUR - 1, 10);
        }
        // the commits older than the retention period are dropped
        assertEquals(List.of(102L, 103L, 104L), history.commits().stream().map(TableCommitHistory.Commit::getTxnId)
                .toList());
        // a replayed commit is not recorded twice, and the times stay ordered
        history.record(new TableCommitHistory.Commit(now, "INSERT_INTO", 1, 1, 104, "l4"), 0, 10);
        history.record(new TableCommitHistory.Commit(now - 10 * HOUR, "DELETE", 1, 1, 105, "l5"), 0, 10);
        assertEquals(4, history.commits().size());
        assertEquals(history.commits().get(2).getTime(), history.commits().get(3).getTime());
        // the number of commits is bounded
        history.prune(0, 2);
        assertEquals(List.of(104L, 105L), history.commits().stream().map(TableCommitHistory.Commit::getTxnId).toList());

        TableCommitHistory copy = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(history), TableCommitHistory.class);
        assertEquals("DELETE", copy.commits().get(1).getOperation());
        assertEquals("l5", copy.commits().get(1).getLabel());
    }

    @Test
    public void testOperationAndRows() {
        assertEquals("MERGE_INTO", TimeTravelRetention.operation(
                txn(1, TransactionState.LoadJobSourceType.INSERT_STREAMING, 5, "MERGE_INTO")));
        assertEquals("INSERT_INTO", TimeTravelRetention.operation(
                txn(1, TransactionState.LoadJobSourceType.INSERT_STREAMING, 5, null)));
        assertEquals("STREAM_LOAD", TimeTravelRetention.operation(
                txn(1, TransactionState.LoadJobSourceType.BACKEND_STREAMING, -1, null)));
        assertEquals("DELETE", TimeTravelRetention.operation(
                txn(1, TransactionState.LoadJobSourceType.DELETE, -1, null)));
        assertEquals(5, TimeTravelRetention.rows(new InsertTxnCommitAttachment(5)));
        assertEquals(-1, TimeTravelRetention.rows(null));
    }

    @Test
    public void testDescribeHistory() throws Exception {
        OlapTable table = table("hdup");
        long now = System.currentTimeMillis();
        TimeTravelRetention.onTableCommitVisible(table,
                txn(201, TransactionState.LoadJobSourceType.INSERT_STREAMING, 10, "INSERT_INTO"), 1, now - 3 * HOUR);
        TimeTravelRetention.onTableCommitVisible(table,
                txn(202, TransactionState.LoadJobSourceType.BACKEND_STREAMING, -1, null), 2, now - 2 * HOUR);
        TimeTravelRetention.onTableCommitVisible(table,
                txn(203, TransactionState.LoadJobSourceType.INSERT_STREAMING, 3, "DELETE"), 1, now - HOUR);

        DescribeHistoryStmt stmt = assertInstanceOf(DescribeHistoryStmt.class, parse("describe history hdup"));
        List<List<String>> rows = TableHistoryExecutor.describe(stmt, ctx);
        assertEquals(3, rows.size());
        // newest first
        assertEquals(List.of("DELETE", "3", "1", "203", "label_203", "true"), rows.get(0).subList(1, 7));
        assertEquals("STREAM_LOAD", rows.get(1).get(1));
        assertNull(rows.get(1).get(2));
        assertEquals("2", rows.get(1).get(3));
        assertTrue(rows.get(2).get(0).matches("\\d{4}-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d\\.\\d{3}"), rows.get(2).get(0));

        rows = TableHistoryExecutor.describe(
                (DescribeHistoryStmt) parse("DESC HISTORY th.hdup LIMIT 1"), ctx);
        assertEquals(1, rows.size());
        assertEquals("203", rows.get(0).get(4));

        // a commit before the data was replaced cannot be restored
        table.markHistoryFloor(now - 90 * 60_000L);
        rows = TableHistoryExecutor.describe(stmt, ctx);
        assertEquals("true", rows.get(0).get(6));
        assertEquals("false", rows.get(1).get(6));

        // the full result shape
        List<List<String>> result = starRocksAssert.show("describe history hdup limit 1");
        assertEquals(1, result.size());
        assertEquals("label_203", result.get(0).get(5));
        // a table named history is still described as a table
        starRocksAssert.withTable("CREATE TABLE history (k INT) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1')");
        assertInstanceOf(com.starrocks.sql.ast.DescribeStmt.class, parse("desc history"));
    }

    @Test
    public void testRestoreSql() {
        RestoreTableStmt stmt = assertInstanceOf(RestoreTableStmt.class,
                parse("restore table hpk to timestamp as of '2026-01-01 00:00:00'"));
        assertEquals("FOR TIMESTAMP AS OF '2026-01-01 00:00:00'", stmt.getTimeTravelSql());
        List<String> sql = TableHistoryExecutor.restoreSql(stmt,
                TableHistoryExecutor.resolve(stmt.getTableRef(), ctx, "RESTORE TABLE"), "stage");
        assertEquals(List.of(
                "CREATE TABLE `default_catalog`.`th`.`stage` LIKE `default_catalog`.`th`.`hpk`",
                "INSERT INTO `default_catalog`.`th`.`stage` (`k`, `v`) SELECT `k`, `v` FROM `default_catalog`.`th`.`hpk` "
                        + "FOR TIMESTAMP AS OF '2026-01-01 00:00:00'",
                "INSERT INTO `default_catalog`.`th`.`hpk` REPLACE WHERE TRUE SELECT `k`, `v` FROM "
                        + "`default_catalog`.`th`.`stage`",
                "DROP TABLE IF EXISTS `default_catalog`.`th`.`stage` FORCE"), sql);
        for (String s : sql) {
            parse(s);
        }

        stmt = (RestoreTableStmt) parse("RESTORE TABLE th.hdup TO AT(OFFSET => -3600)");
        assertEquals("AT(OFFSET => -3600)", stmt.getTimeTravelSql());
        sql = TableHistoryExecutor.restoreSql(stmt,
                TableHistoryExecutor.resolve(stmt.getTableRef(), ctx, "RESTORE TABLE"), "stage");
        assertEquals("INSERT OVERWRITE `default_catalog`.`th`.`hdup` (`k`, `v`) SELECT `k`, `v` FROM "
                + "`default_catalog`.`th`.`stage`", sql.get(2));
        for (String s : sql) {
            parse(s);
        }
    }

    @Test
    public void testErrors() {
        Exception e = assertThrows(Exception.class, () -> UtFrameUtils.parseStmtWithNewParser(
                "describe history hplain", ctx));
        assertTrue(e.getMessage().contains("Time travel is not enabled for table hplain"), e.getMessage());
        e = assertThrows(Exception.class, () -> UtFrameUtils.parseStmtWithNewParser(
                "restore table hview to timestamp as of now()", ctx));
        assertTrue(e.getMessage().contains("RESTORE TABLE is only supported for native tables"), e.getMessage());
        e = assertThrows(Exception.class, () -> UtFrameUtils.parseStmtWithNewParser(
                "restore table nosuch to timestamp as of now()", ctx));
        assertTrue(e.getMessage().contains("Table nosuch is not found"), e.getMessage());
        // RESTORE TABLE is not a statement of an explicit transaction
        e = assertThrows(Exception.class, () -> {
            ctx.setTxnId(12345);
            try {
                com.starrocks.transaction.ExplicitTxnStatementValidator.validate(
                        parse("restore table hpk to timestamp as of now()"), ctx);
            } finally {
                ctx.setTxnId(0);
            }
        });
        assertTrue(e.getMessage().contains("Explicit transaction only support"), e.getMessage());
    }
}
