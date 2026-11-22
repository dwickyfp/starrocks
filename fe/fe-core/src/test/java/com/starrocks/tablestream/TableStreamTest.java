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

package com.starrocks.tablestream;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.stream.CreateTableStreamStmt;
import com.starrocks.sql.ast.stream.DescTableStreamStmt;
import com.starrocks.sql.ast.stream.DropTableStreamStmt;
import com.starrocks.sql.ast.stream.ShowTableStreamsStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.timetravel.TimeTravelRetention;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TransactionCommitFailedException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Streams: the objects, the queries that read them, their offsets and when they go stale.
 */
public class TableStreamTest {
    private static final long HOUR = 3600_000L;
    private static final String RETAIN = "PROPERTIES ('replication_num' = '1', 'data_retention_time_in_days' = '1')";
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext ctx;
    private static Database db;
    private static long base;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("ts").useDatabase("ts");
        starRocksAssert.withTable("CREATE TABLE spk (k INT NOT NULL, v VARCHAR(10), n INT) PRIMARY KEY (k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 " + RETAIN);
        starRocksAssert.withTable("CREATE TABLE sdup (k INT, v INT) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) "
                + "BUCKETS 1 " + RETAIN);
        starRocksAssert.withTable("CREATE TABLE sday (dt DATE NOT NULL, id INT NOT NULL, v INT) PRIMARY KEY (dt, id) "
                + "PARTITION BY RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2024-01-02'), "
                + "PARTITION p2 VALUES LESS THAN ('2024-01-03')) DISTRIBUTED BY HASH(id) BUCKETS 1 " + RETAIN);
        starRocksAssert.withTable("CREATE TABLE shll (k INT, h HLL HLL_UNION) AGGREGATE KEY (k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 " + RETAIN);
        starRocksAssert.withTable("CREATE TABLE splain (k INT, v INT) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) "
                + "BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("ts");
        base = System.currentTimeMillis() / 1000 * 1000;
        for (String name : List.of("spk", "sdup", "sday", "shll")) {
            for (PhysicalPartition partition : table(name).getAllPhysicalPartitions()) {
                partition.setVisibleVersion(1, base - 4 * HOUR);
                publish(table(name), partition, 2, base - 3 * HOUR);
            }
        }
    }

    @AfterEach
    public void dropStreams() {
        for (TableStream stream : TableStreamMgr.list(db)) {
            TableStreamMgr.drop(db, stream.getName());
        }
        ctx.setTxnId(0);
        ctx.clearStreamReads();
    }

    private static void publish(OlapTable table, PhysicalPartition partition, long version, long time) {
        TimeTravelRetention.onVersionVisible(table, partition, version, time);
        partition.updateVisibleVersion(version, time);
        partition.setNextVersion(version + 1);
        for (MaterializedIndex index : partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
            for (Tablet tablet : index.getTablets()) {
                for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                    replica.updateVersionInfo(version, -1, version);
                }
            }
        }
    }

    private static void publishNext(String name) {
        OlapTable table = table(name);
        for (PhysicalPartition partition : table.getAllPhysicalPartitions()) {
            publish(table, partition, partition.getVisibleVersion() + 1, System.currentTimeMillis());
        }
    }

    private static OlapTable table(String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("ts", name);
    }

    private static StatementBase parse(String sql) {
        return SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
    }

    private static void create(String sql) throws DdlException {
        TableStreamExecutor.create(assertInstanceOf(CreateTableStreamStmt.class, parse(sql)), ctx);
    }

    private static String createError(String sql) {
        return assertThrows(Exception.class, () -> create(sql)).getMessage();
    }

    private static ExecPlan plan(String sql) throws Exception {
        ctx.setThreadLocalInfo();
        ctx.startStatementStreamReads();
        return StatementPlanner.plan(parse(sql), ctx);
    }

    /** The versions the scans of {@code sql} read, sorted. */
    private static List<Long> scanVersions(String sql) throws Exception {
        List<Long> versions = new ArrayList<>();
        for (ScanNode node : plan(sql).getScanNodes()) {
            versions.addAll(((OlapScanNode) node).getScanPartitionVersions().values());
        }
        return new ArrayList<>(new TreeSet<>(versions));
    }

    private static TableStream stream(String name) {
        return TableStreamMgr.get(db, name);
    }

    private static TransactionState txn(long id, List<StreamOffsetUpdate> offsets) {
        TransactionState txn = new TransactionState(id, "label_" + id, null,
                TransactionState.LoadJobSourceType.INSERT_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "127.0.0.1"), 60_000L);
        InsertTxnCommitAttachment attachment = new InsertTxnCommitAttachment(1);
        attachment.setStreamOffsets(offsets);
        txn.setTxnCommitAttachment(attachment);
        return txn;
    }

    @Test
    public void testCreateShowDescDrop() throws Exception {
        create("CREATE STREAM s1 ON TABLE spk COMMENT 'changes'");
        create("CREATE STREAM IF NOT EXISTS s1 ON TABLE sdup");
        assertEquals("spk", stream("S1").getTableName());
        assertTrue(createError("CREATE STREAM s1 ON TABLE sdup").contains("Stream s1 already exists"));
        create("CREATE OR REPLACE STREAM s1 ON TABLE sdup APPEND_ONLY = TRUE");
        assertEquals("sdup", stream("s1").getTableName());
        assertTrue(stream("s1").isAppendOnly());
        create("CREATE STREAM ts.s2 ON TABLE ts.spk SHOW_INITIAL_ROWS = TRUE");
        assertTrue(stream("s2").getOffsetVersions().isEmpty());
        // the offset is the versions of the table now
        create("CREATE STREAM s3 ON TABLE spk");
        assertEquals(Map.of(table("spk").getAllPhysicalPartitions().iterator().next().getId(), 2L),
                stream("s3").getOffsetVersions());

        List<List<String>> rows = TableStreamExecutor.show(
                assertInstanceOf(ShowTableStreamsStmt.class, parse("SHOW STREAMS FROM ts LIKE 's%'")), ctx)
                .getResultRows();
        assertEquals(3, rows.size());
        assertEquals(List.of("s1", "ts", "root"), List.of(rows.get(0).get(1), rows.get(0).get(2),
                rows.get(0).get(3).replace("'", "").replaceAll("@.*", "")));
        assertEquals(List.of("sdup", "DELTA", "false", "APPEND_ONLY"), rows.get(0).subList(5, 9));
        assertEquals("DEFAULT", rows.get(1).get(8));
        rows = TableStreamExecutor.desc(assertInstanceOf(DescTableStreamStmt.class, parse("DESC STREAM s3")), ctx)
                .getResultRows();
        assertEquals(1, rows.size());
        assertEquals("", rows.get(0).get(10));
        assertFalse(rows.get(0).get(9).isEmpty());

        TableStreamExecutor.drop(assertInstanceOf(DropTableStreamStmt.class, parse("DROP STREAM s3")), ctx);
        assertNull(stream("s3"));
        TableStreamExecutor.drop(assertInstanceOf(DropTableStreamStmt.class, parse("DROP STREAM IF EXISTS s3")),
                ctx);
        Exception e = assertThrows(DdlException.class, () -> TableStreamExecutor.drop(
                assertInstanceOf(DropTableStreamStmt.class, parse("DROP STREAM s3")), ctx));
        assertTrue(e.getMessage().contains("Stream s3 does not exist"), e.getMessage());
    }

    @Test
    public void testCreateErrors() {
        assertTrue(createError("CREATE STREAM e ON TABLE splain").contains("time travel is not enabled"));
        assertTrue(createError("CREATE STREAM e ON TABLE nosuch").contains("Table nosuch does not exist"));
        assertTrue(createError("CREATE STREAM sdup ON TABLE spk").contains("A table named sdup exists"));
        assertTrue(createError("CREATE STREAM e ON TABLE shll").contains("use APPEND_ONLY = TRUE"));
        assertTrue(createError("CREATE STREAM e ON TABLE spk FOO = TRUE").contains("Unknown option FOO"));
        assertTrue(createError("CREATE STREAM e ON TABLE spk APPEND_ONLY = 'maybe'").contains("must be TRUE or FALSE"));
        assertTrue(createError("CREATE STREAM e ON TABLE spk APPEND_ONLY = TRUE APPEND_ONLY = FALSE")
                .contains("given twice"));
        assertTrue(createError("CREATE STREAM e ON TABLE spk AT(OFFSET => -60) SHOW_INITIAL_ROWS = TRUE")
                .contains("not both"));
        assertTrue(createError("CREATE STREAM e ON TABLE spk AT(OFFSET => -7200000)")
                .contains("beyond its data retention time"));
        assertTrue(createError("CREATE STREAM e ON TABLE test.nosuch").contains("Unknown database"));
    }

    @Test
    public void testCreateAtPointInTime() throws Exception {
        // at a point before version 2, the stream starts at version 1, so it returns version 2 as changes
        create("CREATE STREAM sat ON TABLE sdup BEFORE(TIMESTAMP => '" + format(base - 3 * HOUR) + "')");
        assertEquals(List.of(1L), new ArrayList<>(stream("sat").getOffsetVersions().values()));
        create("CREATE STREAM sat2 ON TABLE sdup AT(OFFSET => -60)");
        assertEquals(List.of(2L), new ArrayList<>(stream("sat2").getOffsetVersions().values()));
    }

    private static String format(long epochMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), TimeUtils.getTimeZone().toZoneId())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
    }

    @Test
    public void testReadScansOffsetAndNow() throws Exception {
        create("CREATE STREAM r1 ON TABLE spk");
        // nothing changed: the query reads no data
        String explain = plan("SELECT * FROM r1").getExplainString(TExplainLevel.NORMAL);
        assertTrue(explain.contains("EMPTYSET") || !explain.contains("OlapScanNode"), explain);

        publishNext("spk");
        long now = table("spk").getAllPhysicalPartitions().iterator().next().getVisibleVersion();
        assertEquals(List.of(2L, now), scanVersions("SELECT k, v, METADATA$ACTION, METADATA$ISUPDATE, "
                + "METADATA$ROW_ID FROM r1 WHERE METADATA$ACTION = 'INSERT'"));
        explain = plan("SELECT * FROM r1").getExplainString(TExplainLevel.NORMAL);
        assertTrue(explain.contains("UNION"), explain);
        assertTrue(explain.contains("LEFT OUTER JOIN") || explain.contains("RIGHT OUTER JOIN"), explain);
        // the stream reads the table now and at its offset, next to the table itself
        assertEquals(List.of(2L, now), scanVersions("SELECT * FROM r1 s JOIN spk t ON s.k = t.k"));
        // the read is noted for the commit of the statement
        assertEquals(1, ctx.getConsumedStreamOffsets() == null ? 1 : 0);
        ctx.consumeStatementStreamReads();
        List<StreamOffsetUpdate> offsets = ctx.getConsumedStreamOffsets();
        assertEquals(1, offsets.size());
        assertEquals(Map.of(table("spk").getAllPhysicalPartitions().iterator().next().getId(), now),
                offsets.get(0).getToVersions());

        Exception e = assertThrows(Exception.class, () -> plan("SELECT * FROM r1 AT(OFFSET => -60)"));
        assertTrue(e.getMessage().contains("cannot be read with time travel"), e.getMessage());
        e = assertThrows(Exception.class, () -> plan("SELECT * FROM r1 PARTITION (spk)"));
        assertTrue(e.getMessage().contains("PARTITION or TABLET clause"), e.getMessage());
    }

    @Test
    public void testChangeQueries() {
        OlapTable pk = table("spk");
        TableStream standard = new TableStream(1, db.getId(), "x", pk.getId(), "spk", false, 0, "", null, Map.of(), 0);
        long partitionId = pk.getAllPhysicalPartitions().iterator().next().getId();
        String sql = TableStreamMgr.changes(standard, pk, "`ts`.`spk`", Map.of(), Map.of(partitionId, 3L)).sql();
        assertTrue(sql.contains("'INSERT' AS `METADATA$ACTION`"), sql);
        assertTrue(sql.contains("UNION ALL SELECT `__stream_old`.`k`"), sql);
        assertTrue(sql.contains("NOT (`__stream_cur`.`v` <=> `__stream_old`.`v` AND `__stream_cur`.`n` <=> "
                + "`__stream_old`.`n`)"), sql);
        TableStream appendOnly = new TableStream(1, db.getId(), "x", pk.getId(), "spk", true, 0, "", null, Map.of(),
                0);
        sql = TableStreamMgr.changes(appendOnly, pk, "`ts`.`spk`", Map.of(), Map.of(partitionId, 3L)).sql();
        assertFalse(sql.contains("UNION"), sql);
        assertFalse(sql.contains("<=>"), sql);

        OlapTable dup = table("sdup");
        sql = TableStreamMgr.changes(new TableStream(1, db.getId(), "x", dup.getId(), "sdup", false, 0, "", null,
                Map.of(), 0), dup, "`ts`.`sdup`", Map.of(), Map.of(dup.getAllPhysicalPartitions().iterator().next()
                .getId(), 3L)).sql();
        assertTrue(sql.contains("row_number() OVER (PARTITION BY `k`, `v`)"), sql);
        assertTrue(sql.contains("count(*) AS `__stream_n`"), sql);

        // only the partitions that changed are read
        OlapTable day = table("sday");
        PhysicalPartition p2 = day.getPartition("p2").getDefaultPhysicalPartition();
        Map<Long, Long> from = TableStreamMgr.currentVersions(day);
        Map<Long, Long> to = new java.util.HashMap<>(from);
        to.put(p2.getId(), p2.getVisibleVersion() + 1);
        sql = TableStreamMgr.changes(new TableStream(1, db.getId(), "x", day.getId(), "sday", false, 0, "", null,
                from, 0), day, "`ts`.`sday`", from, to).sql();
        assertTrue(sql.contains("PARTITION (`p2`)"), sql);
        assertFalse(sql.contains("`p1`"), sql);
    }

    @Test
    public void testOffsetsMoveWithCommit() throws Exception {
        create("CREATE STREAM o1 ON TABLE sdup");
        TableStream stream = stream("o1");
        publishNext("sdup");
        plan("SELECT * FROM o1");
        ctx.consumeStatementStreamReads();
        List<StreamOffsetUpdate> offsets = ctx.getConsumedStreamOffsets();

        TransactionState first = txn(1001, offsets);
        TableStreamMgr.reserve(first);
        // another transaction that read the same offset cannot commit while the first one moves it
        TransactionState second = txn(1002, offsets);
        Exception e = assertThrows(TransactionCommitFailedException.class, () -> TableStreamMgr.reserve(second));
        assertTrue(e.getMessage().contains("was consumed by another transaction"), e.getMessage());
        TableStreamMgr.onTransactionCommitted(first);
        assertEquals(offsets.get(0).getToVersions(), stream.getOffsetVersions());
        // nor afterwards: the offset is not the one it read any more
        assertThrows(TransactionCommitFailedException.class, () -> TableStreamMgr.reserve(second));
        // replaying the commit again changes nothing
        TableStreamMgr.onTransactionCommitted(first);
        assertEquals(offsets.get(0).getToVersions(), stream.getOffsetVersions());

        // a failed commit releases the stream
        plan("SELECT * FROM o1");
        ctx.consumeStatementStreamReads();
        TransactionState third = txn(1003, ctx.getConsumedStreamOffsets());
        TableStreamMgr.reserve(third);
        TableStreamMgr.release(third);
        TableStreamMgr.reserve(txn(1004, ctx.getConsumedStreamOffsets()));
        TableStreamMgr.release(txn(1004, ctx.getConsumedStreamOffsets()));

        // a stream dropped while it was read
        TableStreamMgr.drop(db, "o1");
        e = assertThrows(TransactionCommitFailedException.class, () -> TableStreamMgr.reserve(third));
        assertTrue(e.getMessage().contains("was dropped or replaced"), e.getMessage());
    }

    @Test
    public void testExplicitTransactionRepeatsRead() throws Exception {
        create("CREATE STREAM t1 ON TABLE sdup");
        publishNext("sdup");
        ctx.setTxnId(42);
        plan("SELECT * FROM t1");
        long first = ctx.getStreamRead(stream("t1").getId()).getToVersions().values().iterator().next();
        publishNext("sdup");
        // a later read in the transaction returns the same changes
        List<Long> versions = scanVersions("SELECT * FROM t1");
        assertEquals(first, versions.get(versions.size() - 1).longValue());
        // a SELECT does not consume the stream; a DML statement does
        assertNull(ctx.getConsumedStreamOffsets());
        ctx.consumeStatementStreamReads();
        assertNotNull(ctx.getConsumedStreamOffsets());
        ctx.setTxnId(0);
        ctx.startStatementStreamReads();
        assertNull(ctx.getConsumedStreamOffsets());
    }

    @Test
    public void testStale() throws Exception {
        starRocksAssert.withTable("CREATE TABLE sstale (k INT NOT NULL, v INT) PRIMARY KEY (k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 " + RETAIN);
        OlapTable table = table("sstale");
        PhysicalPartition partition = table.getAllPhysicalPartitions().iterator().next();
        partition.setVisibleVersion(1, base - 4 * HOUR);
        publish(table, partition, 2, base - 3 * HOUR);
        create("CREATE STREAM st ON TABLE sstale");
        TableStream stream = stream("st");
        long now = System.currentTimeMillis();
        assertNull(TableStreamMgr.staleReason(stream, table, now));
        assertTrue(TableStreamMgr.staleReason(stream, table, now + 2 * 24 * HOUR).contains("older than the data "
                + "retention time"));
        assertTrue(TableStreamMgr.staleReason(stream, null, now).contains("was dropped"));
        // the data of the table was replaced after the offset (TRUNCATE, INSERT OVERWRITE, ...)
        partition.markHistoryFloor(stream.getOffsetTime() + 1);
        assertTrue(TableStreamMgr.staleReason(stream, table, now).contains("was replaced"));
        Exception e = assertThrows(Exception.class, () -> plan("SELECT * FROM st"));
        assertTrue(e.getMessage().contains("Stream st is stale"), e.getMessage());
        List<List<String>> rows = TableStreamExecutor.desc(
                assertInstanceOf(DescTableStreamStmt.class, parse("DESC STREAM st")), ctx).getResultRows();
        assertEquals("true", rows.get(0).get(7));
    }

    @Test
    public void testPersistence() {
        TableStream stream = new TableStream(7, db.getId(), "p", 9, "spk", true, 100, "root", "c", Map.of(1L, 2L),
                300);
        TableStream copy = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(stream), TableStream.class);
        assertEquals(Map.of(1L, 2L), copy.getOffsetVersions());
        assertEquals(300, copy.getOffsetTime());
        assertTrue(copy.isAppendOnly());

        InsertTxnCommitAttachment attachment = new InsertTxnCommitAttachment(3);
        attachment.setStreamOffsets(List.of(new StreamOffsetUpdate(1, 7, "p", 300, Map.of(1L, 2L), 400,
                Map.of(1L, 5L))));
        InsertTxnCommitAttachment read = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(attachment),
                InsertTxnCommitAttachment.class);
        assertEquals(Map.of(1L, 5L), read.getStreamOffsets().get(0).getToVersions());
        assertEquals(400, read.getStreamOffsets().get(0).getToTime());

        TableStreamMgr.replayCreate(copy);
        assertNotNull(stream("p"));
        TableStreamMgr.replayDrop(new DropTableStreamLog(db.getId(), "P"));
        assertNull(stream("p"));
    }
}
