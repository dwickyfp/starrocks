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

import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Time travel on native tables: retention from the table property or the global variable, the version history
 * partitions record when versions become visible, and AT / BEFORE / FOR TIMESTAMP AS OF pinning scan versions.
 */
public class TimeTravelTest {
    private static final long HOUR = 3600_000L;
    private static final DateTimeFormatter FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static StarRocksAssert starRocksAssert;
    private static long base;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("tt").useDatabase("tt");
        starRocksAssert.withTable("CREATE TABLE kept (k INT, v INT) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', 'data_retention_time_in_days' = '1')");
        starRocksAssert.withTable("CREATE TABLE plain (k INT, v INT) DUPLICATE KEY (k) DISTRIBUTED BY HASH(k) "
                + "BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        // versions 2, 3 and 4 of kept became visible 3, 2 and 1 hours ago
        base = System.currentTimeMillis() / 1000 * 1000;
        OlapTable kept = table("kept");
        PhysicalPartition partition = partition(kept);
        partition.setVisibleVersion(1, base - 4 * HOUR);
        for (int version = 2; version <= 4; version++) {
            publish(kept, partition, version, base - (5 - version) * HOUR);
        }
    }

    @AfterEach
    public void resetGlobal() {
        GlobalVariable.setDataRetentionTimeInDays(0);
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

    private static OlapTable table(String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("tt", name);
    }

    private static PhysicalPartition partition(OlapTable table) {
        return table.getAllPhysicalPartitions().iterator().next();
    }

    private static String at(long epochMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), TimeUtils.getTimeZone().toZoneId())
                .format(FORMAT);
    }

    /** The versions the scans of {@code sql} read, in scan order. */
    private static List<Long> scanVersions(String sql) throws Exception {
        // planned directly: UtFrameUtils would also plan the query as a view, and views take no time travel
        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setThreadLocalInfo();
        StatementBase stmt = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        ExecPlan plan = StatementPlanner.plan(stmt, ctx);
        List<Long> versions = new ArrayList<>();
        for (ScanNode node : plan.getScanNodes()) {
            versions.addAll(((OlapScanNode) node).getScanPartitionVersions().values());
        }
        Assertions.assertNull(starRocksAssert.getCtx().getTimeTravelScanVersions(), "pinned versions outlive the plan");
        return versions;
    }

    private static String error(String sql) {
        return Assertions.assertThrows(Exception.class, () -> scanVersions(sql)).getMessage();
    }

    @Test
    public void testPointsInTime() throws Exception {
        Assertions.assertEquals(List.of(4L), scanVersions("SELECT * FROM kept"));
        Assertions.assertEquals(List.of(2L),
                scanVersions("SELECT * FROM kept AT(TIMESTAMP => '" + at(base - 150 * 60_000L) + "')"));
        Assertions.assertEquals(List.of(3L),
                scanVersions("SELECT * FROM kept AT(TIMESTAMP => '" + at(base - 90 * 60_000L) + "')"));
        Assertions.assertEquals(List.of(3L),
                scanVersions("SELECT * FROM kept FOR TIMESTAMP AS OF '" + at(base - 2 * HOUR) + "'"));
        // BEFORE excludes the version made visible at that very moment
        Assertions.assertEquals(List.of(2L),
                scanVersions("SELECT * FROM kept BEFORE(TIMESTAMP => '" + at(base - 2 * HOUR) + "') t"));
        Assertions.assertEquals(List.of(4L), scanVersions("SELECT * FROM kept AT(OFFSET => -60 * 30)"));
        Assertions.assertEquals(List.of(2L), scanVersions("SELECT count(*) FROM kept AT(OFFSET => -10700)"));
        // before the oldest point the partition had no data yet
        Assertions.assertEquals(List.of(1L),
                scanVersions("SELECT * FROM kept AT(TIMESTAMP => '" + at(base - 5 * HOUR) + "')"));
        // the same table at one point twice is fine, and a join of kept now with kept in the past is not
        Assertions.assertEquals(2, scanVersions("SELECT * FROM kept AT(OFFSET => -10700) a JOIN "
                + "kept AT(OFFSET => -10700) b ON a.k = b.k").size());
        Assertions.assertTrue(error("SELECT * FROM kept a JOIN kept AT(OFFSET => -10700) b ON a.k = b.k")
                .contains("both now and with time travel"));
        Assertions.assertTrue(error("SELECT * FROM kept AT(OFFSET => -10700) a JOIN kept b ON a.k = b.k")
                .contains("both now and with time travel"));
        // a plain query right after one with time travel reads the current version again
        Assertions.assertEquals(List.of(4L), scanVersions("SELECT * FROM kept"));
        Assertions.assertTrue(error("SELECT * FROM kept AT(OFFSET => -10700) a JOIN "
                + "kept AT(OFFSET => -5400) b ON a.k = b.k").contains("two different points in time"));
    }

    @Test
    public void testRetentionLimits() {
        Assertions.assertTrue(error("SELECT * FROM kept AT(OFFSET => -2 * 86400)").contains("beyond its data retention"));
        Assertions.assertTrue(error("SELECT * FROM kept AT(OFFSET => 3600)").contains("in the future"));
        Assertions.assertTrue(error("SELECT * FROM plain AT(OFFSET => -60)").contains("not enabled"));
        Assertions.assertTrue(error("SELECT * FROM kept FOR VERSION AS OF 3").contains("point in time"));
    }

    @Test
    public void testGlobalVariable() throws Exception {
        String message = Assertions.assertThrows(Exception.class, () -> UtFrameUtils.parseStmtWithNewParser(
                "SET GLOBAL data_retention_time_in_days = 91", starRocksAssert.getCtx())).getMessage();
        Assertions.assertTrue(message.contains("between 0 and 90"), message);
        UtFrameUtils.parseStmtWithNewParser("SET GLOBAL data_retention_time_in_days = 7", starRocksAssert.getCtx());

        // plain follows the global variable; its history starts with the version visible when it was turned on
        GlobalVariable.setDataRetentionTimeInDays(2);
        OlapTable plain = table("plain");
        PhysicalPartition partition = partition(plain);
        partition.setVisibleVersion(1, base - 10 * HOUR);
        publish(plain, partition, 2, base - 30 * 60_000L);
        Assertions.assertEquals(List.of(1L), scanVersions("SELECT * FROM plain AT(OFFSET => -3600)"));
        Assertions.assertEquals(List.of(2L), scanVersions("SELECT * FROM plain AT(OFFSET => -60)"));
        Assertions.assertEquals(48, TimeTravelBeConfigSyncer.maxRetentionDays() * 24);
        // the table property wins over the global variable
        Assertions.assertTrue(error("SELECT * FROM kept AT(OFFSET => -36 * 3600)").contains("1 days"));
    }

    @Test
    public void testTableProperty() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String message = Assertions.assertThrows(Exception.class, () -> starRocksAssert.withTable(
                "CREATE TABLE bad (k INT) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1', "
                        + "'data_retention_time_in_days' = '91')")).getMessage();
        Assertions.assertTrue(message.contains("between 0 and 90"), message);

        starRocksAssert.withTable("CREATE TABLE altered (k INT) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1')");
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "ALTER TABLE altered SET ('data_retention_time_in_days' = '3')", ctx), ctx);
        Assertions.assertEquals(3, table("altered").getDataRetentionTimeInDays());
        String ddl = starRocksAssert.showCreateTable("SHOW CREATE TABLE altered");
        Assertions.assertTrue(ddl.contains("\"data_retention_time_in_days\" = \"3\""), ddl);
        Assertions.assertEquals(3, TimeTravelBeConfigSyncer.maxRetentionDays());
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "ALTER TABLE altered SET ('data_retention_time_in_days' = '0')", ctx), ctx);
        Assertions.assertEquals(0, table("altered").getDataRetentionTimeInDays());
        Assertions.assertTrue(error("SELECT * FROM altered AT(OFFSET => -60)").contains("not enabled"));
    }

    @Test
    public void testNoTimeTravelForViewsAndDdl() throws Exception {
        String message = Assertions.assertThrows(Exception.class, () -> starRocksAssert.withView(
                "CREATE VIEW old_kept AS SELECT * FROM kept AT(OFFSET => -60)")).getMessage();
        Assertions.assertTrue(message.contains("time travel"), message);
    }

    @Test
    public void testVersionHistory() {
        VersionHistory history = new VersionHistory();
        for (long v = 1; v <= 10; v++) {
            history.record(v, v * 100, 0, 5);
        }
        Assertions.assertEquals(5, history.size());
        Assertions.assertArrayEquals(new long[] {6, 600}, history.oldest());
        Assertions.assertEquals(-1, history.versionAt(599));
        Assertions.assertEquals(7, history.versionAt(799));
        Assertions.assertEquals(10, history.versionAt(5000));
        // replays and older versions do not move the history
        history.record(9, 2000, 0, 5);
        Assertions.assertEquals(5, history.size());
        // past the retention only the newest point before it stays, telling the version at its start
        history.record(11, 1100, 950, 100);
        Assertions.assertArrayEquals(new long[] {9, 900}, history.oldest());
        Assertions.assertEquals(9, history.versionAt(950));

        VersionHistory copy = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(history), VersionHistory.class);
        Assertions.assertEquals(history.size(), copy.size());
        Assertions.assertEquals(10, copy.versionAt(1050));
    }

    @Test
    public void testBeConfigTarget() {
        Assertions.assertEquals(TimeTravelBeConfigSyncer.BE_DEFAULT_SECONDS, TimeTravelBeConfigSyncer.targetSeconds(0));
        Assertions.assertEquals(86400 + 3600, TimeTravelBeConfigSyncer.targetSeconds(1));
        Assertions.assertEquals(90 * 86400 + 3600, TimeTravelBeConfigSyncer.targetSeconds(90));
    }

    /** The global dicts of the scans of {@code sql}, with low cardinality optimization v1 or v2. */
    private static int globalDicts(String sql, boolean v2) throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setThreadLocalInfo();
        SessionVariable saved = ctx.getSessionVariable().clone();
        boolean savedMock = FeConstants.USE_MOCK_DICT_MANAGER;
        try {
            FeConstants.USE_MOCK_DICT_MANAGER = true;
            ctx.getSessionVariable().setEnableLowCardinalityOptimize(true);
            ctx.getSessionVariable().setUseLowCardinalityOptimizeV2(v2);
            ExecPlan plan = StatementPlanner.plan(SqlParser.parse(sql, ctx.getSessionVariable()).get(0), ctx);
            int dicts = 0;
            for (PhysicalOlapScanOperator scan : Utils.extractPhysicalOlapScanOperator(plan.getPhysicalPlan())) {
                dicts += scan.getGlobalDicts().size();
            }
            return dicts;
        } finally {
            FeConstants.USE_MOCK_DICT_MANAGER = savedMock;
            ctx.setSessionVariable(saved);
        }
    }

    @Test
    public void testNoGlobalDictForThePast() throws Exception {
        // the global dict of a column is built from its current values: an older version may hold others
        starRocksAssert.withTable("CREATE TABLE named (k INT, s VARCHAR(16)) DUPLICATE KEY (k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1', "
                + "'data_retention_time_in_days' = '1')");
        OlapTable named = table("named");
        PhysicalPartition partition = partition(named);
        partition.setVisibleVersion(1, base - 4 * HOUR);
        publish(named, partition, 2, base - 3 * HOUR);
        publish(named, partition, 3, base - HOUR);
        String now = "SELECT s, count(*) FROM named GROUP BY s";
        String past = "SELECT s, count(*) FROM named AT(TIMESTAMP => '" + at(base - 2 * HOUR) + "') GROUP BY s";
        for (boolean v2 : List.of(true, false)) {
            Assertions.assertTrue(globalDicts(now, v2) > 0, "the dict rewrite runs for a current read, v2=" + v2);
            Assertions.assertEquals(0, globalDicts(past, v2), "no dict rewrite for a time travel read, v2=" + v2);
        }
    }
}
