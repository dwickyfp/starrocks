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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.procedure.StarRocksScriptSession;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DescribeHistoryStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.RestoreTableStmt;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.timetravel.TableCommitHistory;
import com.starrocks.timetravel.TimeTravelRetention;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * {@code DESCRIBE HISTORY} and {@code RESTORE TABLE}, on the time travel history of a native table.
 */
public final class TableHistoryExecutor {
    private static final DateTimeFormatter FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    public static final String RESTORE_OPERATION = "RESTORE";

    private TableHistoryExecutor() {
    }

    /** The table, with its name qualified by catalog and database. */
    public record Resolved(TableName name, OlapTable table) {
    }

    public static Resolved resolve(TableRef tableRef, ConnectContext ctx, String statement) {
        TableRef normalized = AnalyzerUtils.normalizedTableRef(tableRef, ctx);
        TableName name = TableName.fromTableRef(normalized);
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(ctx, name.getCatalog(), name.getDb(), name.getTbl());
        if (table == null) {
            throw new SemanticException("Table %s is not found", name.getTbl());
        }
        if (!(table instanceof OlapTable olapTable) || !table.isNativeTable() || table.isMaterializedView()) {
            throw new SemanticException("%s is only supported for native tables, but %s is a %s", statement,
                    name.getTbl(), table.getType());
        }
        if (!TimeTravelRetention.isSupported(olapTable)) {
            throw new SemanticException("%s is not supported for table %s in shared-data mode", statement,
                    name.getTbl());
        }
        if (TimeTravelRetention.effectiveDays(olapTable) <= 0) {
            throw new SemanticException("Time travel is not enabled for table %s: set its property "
                    + "data_retention_time_in_days or the global variable data_retention_time_in_days", name.getTbl());
        }
        return new Resolved(name, olapTable);
    }

    /**
     * One row per transaction that changed the table, newest first: when it became visible, what wrote it, how
     * much, and whether time travel and RESTORE TABLE can still read the table as of then.
     */
    public static List<List<String>> describe(DescribeHistoryStmt stmt, ConnectContext ctx) {
        Resolved resolved = resolve(stmt.getTableRef(), ctx, "DESCRIBE HISTORY");
        OlapTable table = resolved.table();
        TableCommitHistory history = table.getCommitHistory();
        List<TableCommitHistory.Commit> commits = history == null ? new ArrayList<>() : history.commits();
        Collections.reverse(commits);
        if (stmt.getLimit() >= 0 && commits.size() > stmt.getLimit()) {
            commits = commits.subList(0, (int) stmt.getLimit());
        }
        long now = System.currentTimeMillis();
        long oldest = Math.max(table.getHistoryFloorTime(),
                now - TimeTravelRetention.effectiveDays(table) * TimeTravelRetention.DAY_MS);
        List<List<String>> rows = new ArrayList<>();
        for (TableCommitHistory.Commit commit : commits) {
            List<String> row = new ArrayList<>();
            row.add(format(commit.getTime()));
            row.add(commit.getOperation());
            row.add(commit.getRows() < 0 ? null : String.valueOf(commit.getRows()));
            row.add(String.valueOf(commit.getPartitions()));
            row.add(String.valueOf(commit.getTxnId()));
            row.add(commit.getLabel());
            row.add(String.valueOf(commit.getTime() >= oldest && commit.getTime() <= now));
            rows.add(row);
        }
        return rows;
    }

    private static String format(long epochMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), TimeUtils.getTimeZone().toZoneId())
                .format(FORMAT);
    }

    /** The statements RESTORE TABLE runs, in order; the last one drops the staging table. */
    public static List<String> restoreSql(RestoreTableStmt stmt, Resolved resolved, String stagingName) {
        OlapTable table = resolved.table();
        String db = quote(resolved.name().getCatalog()) + "." + quote(resolved.name().getDb()) + ".";
        String target = db + quote(table.getName());
        String staging = db + quote(stagingName);
        String columns = table.getBaseSchemaWithoutGeneratedColumn().stream().map(Column::getName)
                .map(TableHistoryExecutor::quote).collect(Collectors.joining(", "));
        List<String> sql = new ArrayList<>();
        sql.add("CREATE TABLE " + staging + " LIKE " + target);
        sql.add("INSERT INTO " + staging + " (" + columns + ") SELECT " + columns + " FROM " + target + " "
                + stmt.getTimeTravelSql());
        if (table.getKeysType() == KeysType.PRIMARY_KEYS) {
            // one new version that deletes the rows added since and writes the old rows back: time travel and
            // another RESTORE TABLE can still read the table as it was before
            sql.add("INSERT INTO " + target + " REPLACE WHERE TRUE SELECT " + columns + " FROM " + staging);
        } else {
            sql.add("INSERT OVERWRITE " + target + " (" + columns + ") SELECT " + columns + " FROM " + staging);
        }
        sql.add("DROP TABLE IF EXISTS " + staging + " FORCE");
        return sql;
    }

    /**
     * Copies the rows the table had at the point into a staging table, as a statement cannot read a table both now
     * and with time travel, then writes them into the table in one transaction, and drops the staging table.
     */
    public static void restore(RestoreTableStmt stmt, ConnectContext caller) {
        Resolved resolved = resolve(stmt.getTableRef(), caller, "RESTORE TABLE");
        String stagingName = "__restore_" + resolved.table().getName() + "_"
                + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        List<String> sql = restoreSql(stmt, resolved, stagingName);

        ConnectContext ctx = InsertMultiTableExecutor.statementContext(caller);
        StarRocksScriptSession session = new StarRocksScriptSession(ctx, "RESTORE TABLE", 1);
        long rows;
        try {
            ctx.setThreadLocalInfo();
            session.execute(sql.get(0));
            try {
                rows = session.execute(sql.get(1));
                ctx.setDmlOperationName(RESTORE_OPERATION);
                session.execute(sql.get(2));
            } finally {
                ctx.setDmlOperationName(null);
                try {
                    session.execute(sql.get(3));
                } catch (RuntimeException ignored) {
                    // the original error, if any, is reported; a staging table left behind can be dropped
                }
            }
        } catch (RuntimeException e) {
            String message = e instanceof SemanticException se ? se.getDetailMsg()
                    : e.getMessage() == null ? e.toString() : e.getMessage();
            throw new SemanticException("RESTORE TABLE %s failed, the table is unchanged: %s",
                    resolved.name().getTbl(), message);
        } finally {
            caller.setThreadLocalInfo();
        }
        caller.getState().setOk(rows, 0, "");
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }
}
