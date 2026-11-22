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

package com.starrocks.qe;

import com.starrocks.common.AuditLog;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.ShowTablesHistoryStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * SHOW TABLES HISTORY and the query_tag session variable.
 */
public class ShowTablesHistoryTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("history_db").useDatabase("history_db");
        for (String name : List.of("kept", "dropped", "forced", "recovered")) {
            starRocksAssert.withTable("create table " + name + " (k int) duplicate key(k) "
                    + "distributed by hash(k) buckets 1 properties('replication_num' = '1')");
        }
    }

    private static List<List<String>> show(String sql) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        ShowResultSet result = ShowExecutor.execute((ShowStmt) stmt, connectContext);
        return result.getResultRows();
    }

    private static List<String> names(List<List<String>> rows) {
        return rows.stream().map(row -> row.get(0)).sorted().collect(Collectors.toList());
    }

    @Test
    public void testShowTablesHistory() throws Exception {
        starRocksAssert.ddl("drop table dropped");
        starRocksAssert.ddl("drop table forced force");
        starRocksAssert.ddl("drop table recovered");
        starRocksAssert.ddl("recover table recovered");

        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser("show tables history", connectContext);
        Assertions.assertInstanceOf(ShowTablesHistoryStmt.class, stmt);
        ShowResultSet result = ShowExecutor.execute((ShowStmt) stmt, connectContext);
        Assertions.assertEquals(List.of("Tables_in_history_db", "Table_type", "Dropped_on", "Recoverable"),
                result.getMetaData().getColumns().stream().map(c -> c.getName()).collect(Collectors.toList()));

        List<List<String>> rows = result.getResultRows();
        // a table dropped with FORCE is not in the recycle bin
        Assertions.assertEquals(List.of("dropped", "kept", "recovered"), names(rows), rows.toString());
        for (List<String> row : rows) {
            Assertions.assertEquals("BASE TABLE", row.get(1), row.toString());
            if (row.get(0).equals("dropped")) {
                Assertions.assertNotNull(row.get(2), row.toString());
                Assertions.assertEquals("true", row.get(3), row.toString());
            } else {
                Assertions.assertNull(row.get(2), row.toString());
                Assertions.assertNull(row.get(3), row.toString());
            }
        }

        Assertions.assertEquals(List.of("dropped"), names(show("show tables history like 'drop%'")));
        Assertions.assertEquals(List.of("dropped", "kept", "recovered"),
                names(show("show tables history from history_db")));
        // SHOW TABLES is unchanged
        List<List<String>> plain = show("show tables");
        Assertions.assertEquals(List.of("kept", "recovered"), names(plain));
        Assertions.assertEquals(1, plain.get(0).size());
        Assertions.assertEquals(2, show("show full tables").get(0).size());
    }

    private static String render(AuditEvent event) {
        AtomicReference<String> logged = new AtomicReference<>();
        new MockUp<AuditLog>() {
            @Mock
            public void log(String message) {
                logged.set(message);
            }
        };
        boolean jsonFormat = Config.audit_log_json_format;
        try {
            Config.audit_log_json_format = false;
            event.type = AuditEvent.EventType.AFTER_QUERY;
            new AuditLogBuilder().exec(event);
        } finally {
            Config.audit_log_json_format = jsonFormat;
        }
        return logged.get();
    }

    @Test
    public void testQueryTag() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Assertions.assertEquals("", ctx.getSessionVariable().getQueryTag());
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.executeSql("set query_tag = 'nightly-etl'");
        Assertions.assertEquals("nightly-etl", ctx.getSessionVariable().getQueryTag());

        String log = render(new AuditEvent.AuditEventBuilder().setQueryTag("nightly-etl").build());
        Assertions.assertTrue(log.contains("|QueryTag=nightly-etl"), log);
        log = render(new AuditEvent.AuditEventBuilder().setQueryTag("").build());
        Assertions.assertFalse(log.contains("QueryTag"), log);
        log = render(new AuditEvent.AuditEventBuilder().build());
        Assertions.assertFalse(log.contains("QueryTag"), log);
    }
}
