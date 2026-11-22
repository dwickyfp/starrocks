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

import com.google.common.collect.Lists;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.DdlException;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryPeriod;
import com.starrocks.sql.ast.stream.CreateTableStreamStmt;
import com.starrocks.sql.ast.stream.DescTableStreamStmt;
import com.starrocks.sql.ast.stream.DropTableStreamStmt;
import com.starrocks.sql.ast.stream.ShowTableStreamsStmt;
import com.starrocks.timetravel.TimeTravelResolver;
import com.starrocks.timetravel.TimeTravelRetention;
import com.starrocks.type.TypeFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Runs CREATE STREAM, DROP STREAM, SHOW STREAMS and DESCRIBE STREAM. */
public final class TableStreamExecutor {
    public static final String APPEND_ONLY = "APPEND_ONLY";
    public static final String SHOW_INITIAL_ROWS = "SHOW_INITIAL_ROWS";

    public static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("created_on", TypeFactory.createVarcharType(32)))
            .addColumn(new Column("name", TypeFactory.createVarcharType(256)))
            .addColumn(new Column("database_name", TypeFactory.createVarcharType(256)))
            .addColumn(new Column("owner", TypeFactory.createVarcharType(256)))
            .addColumn(new Column("comment", TypeFactory.createVarcharType(1024)))
            .addColumn(new Column("table_name", TypeFactory.createVarcharType(256)))
            .addColumn(new Column("type", TypeFactory.createVarcharType(16)))
            .addColumn(new Column("stale", TypeFactory.createVarcharType(8)))
            .addColumn(new Column("mode", TypeFactory.createVarcharType(16)))
            .addColumn(new Column("stale_after", TypeFactory.createVarcharType(32)))
            .addColumn(new Column("invalid_reason", TypeFactory.createVarcharType(1024)))
            .addColumn(new Column("offset", TypeFactory.createVarcharType(32)))
            .build();

    private TableStreamExecutor() {
    }

    public static void create(CreateTableStreamStmt stmt, ConnectContext ctx) throws DdlException {
        Database db = database(stmt.getName(), ctx);
        String name = lastPart(stmt.getName());
        Map<String, String> options = stmt.getOptions();
        for (String key : options.keySet()) {
            if (!key.equals(APPEND_ONLY) && !key.equals(SHOW_INITIAL_ROWS)) {
                throw new DdlException("Unknown option " + key + " of CREATE STREAM: use APPEND_ONLY or "
                        + "SHOW_INITIAL_ROWS");
            }
        }
        boolean appendOnly = flag(options, APPEND_ONLY);
        boolean showInitialRows = flag(options, SHOW_INITIAL_ROWS);
        if (showInitialRows && stmt.getOffsetPoint() != null) {
            throw new DdlException("CREATE STREAM takes AT / BEFORE or SHOW_INITIAL_ROWS = TRUE, not both");
        }

        Database tableDb = database(stmt.getTableName(), ctx);
        String tableName = lastPart(stmt.getTableName());
        if (tableDb.getId() != db.getId()) {
            throw new DdlException("Stream " + name + " must be in the database of its table, "
                    + tableDb.getFullName());
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        if (table == null) {
            throw new DdlException("Table " + tableName + " does not exist in database " + db.getFullName());
        }
        if (!(table instanceof OlapTable olapTable) || table instanceof MaterializedView) {
            throw new DdlException("A stream can only be created on a native table, but " + tableName + " is a "
                    + table.getType());
        }
        if (GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), name) != null) {
            throw new DdlException("A table named " + name + " exists in database " + db.getFullName()
                    + ", which a stream of that name would hide");
        }
        try {
            Authorizer.checkTableAction(ctx, new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    db.getFullName(), table.getName()), PrivilegeType.SELECT);
        } catch (AccessDeniedException e) {
            throw new DdlException("Access denied: creating a stream on table " + table.getName()
                    + " needs the SELECT privilege on it");
        }
        String unsupported = TableStreamMgr.unsupportedReason(olapTable, appendOnly);
        if (unsupported != null) {
            throw new DdlException("Cannot create stream " + name + ": " + unsupported);
        }

        long now = System.currentTimeMillis();
        Map<Long, Long> offset;
        long offsetTime;
        if (showInitialRows) {
            // every partition at its initial version: the first read returns every row
            offset = new HashMap<>();
            offsetTime = now;
        } else if (stmt.getOffsetPoint() != null) {
            TimeTravelResolver.Snapshot snapshot;
            try {
                snapshot = TimeTravelResolver.snapshot(olapTable,
                        new QueryPeriod(QueryPeriod.PeriodType.TIMESTAMP, stmt.getOffsetPoint()), ctx);
            } catch (SemanticException e) {
                throw new DdlException(e.getDetailMsg());
            }
            offset = snapshot.versions();
            offsetTime = snapshot.time();
        } else {
            offset = TableStreamMgr.currentVersions(olapTable);
            offsetTime = now;
        }
        TableStream stream = new TableStream(GlobalStateMgr.getCurrentState().getNextId(), db.getId(), name,
                table.getId(), table.getName(), appendOnly, now,
                ctx.getCurrentUserIdentity() == null ? "" : ctx.getCurrentUserIdentity().toString(),
                stmt.getComment(), offset, offsetTime);
        TableStreamMgr.create(db, stream, stmt.isOrReplace(), stmt.isIfNotExists());
    }

    public static void drop(DropTableStreamStmt stmt, ConnectContext ctx) throws DdlException {
        Database db = database(stmt.getName(), ctx);
        String name = lastPart(stmt.getName());
        if (!TableStreamMgr.drop(db, name) && !stmt.isIfExists()) {
            throw new DdlException("Stream " + name + " does not exist in database " + db.getFullName());
        }
    }

    public static ShowResultSet show(ShowTableStreamsStmt stmt, ConnectContext ctx) {
        String dbName = stmt.getDbName() != null ? stmt.getDbName() : ctx.getDatabase();
        if (dbName == null || dbName.isEmpty()) {
            throw new SemanticException("No database selected");
        }
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new SemanticException("Unknown database '" + dbName + "'");
        }
        checkDb(db, ctx);
        PatternMatcher matcher = stmt.getPattern() == null ? null
                : PatternMatcher.createMysqlPattern(stmt.getPattern(), false);
        List<List<String>> rows = new ArrayList<>();
        for (TableStream stream : TableStreamMgr.list(db)) {
            if (matcher == null || matcher.match(stream.getName())) {
                rows.add(row(db, stream));
            }
        }
        return new ShowResultSet(META_DATA, rows);
    }

    public static ShowResultSet desc(DescTableStreamStmt stmt, ConnectContext ctx) {
        Database db;
        try {
            db = database(stmt.getName(), ctx);
        } catch (DdlException e) {
            throw new SemanticException(e.getMessage());
        }
        checkDb(db, ctx);
        String name = lastPart(stmt.getName());
        TableStream stream = TableStreamMgr.get(db, name);
        if (stream == null) {
            throw new SemanticException("Stream " + name + " does not exist in database " + db.getFullName());
        }
        List<List<String>> rows = new ArrayList<>();
        rows.add(row(db, stream));
        return new ShowResultSet(META_DATA, rows);
    }

    private static List<String> row(Database db, TableStream stream) {
        OlapTable table = TableStreamMgr.tableOf(stream);
        String stale = TableStreamMgr.staleReason(stream, table, System.currentTimeMillis());
        String staleAfter = "";
        if (table != null) {
            int days = TimeTravelRetention.effectiveDays(table);
            if (days > 0) {
                staleAfter = TimeUtils.longToTimeString(stream.getOffsetTime() + days * TimeTravelRetention.DAY_MS);
            }
        }
        return Lists.newArrayList(TimeUtils.longToTimeString(stream.getCreateTime()), stream.getName(),
                db.getFullName(), stream.getOwner() == null ? "" : stream.getOwner(),
                stream.getComment() == null ? "" : stream.getComment(), stream.getTableName(), "DELTA",
                stale == null ? "false" : "true", stream.isAppendOnly() ? "APPEND_ONLY" : "DEFAULT", staleAfter,
                stale == null ? "" : stale, TimeUtils.longToTimeString(stream.getOffsetTime()));
    }

    private static void checkDb(Database db, ConnectContext ctx) {
        try {
            Authorizer.checkAnyActionOnOrInDb(ctx, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db.getFullName());
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), PrivilegeType.ANY.name(),
                    "DATABASE", db.getFullName());
        }
    }

    private static boolean flag(Map<String, String> options, String key) {
        String value = options.get(key);
        if (value == null) {
            return false;
        }
        if (Set.of("TRUE", "FALSE").contains(value)) {
            return value.equals("TRUE");
        }
        throw new SemanticException("Option %s of CREATE STREAM must be TRUE or FALSE, but is %s", key, value);
    }

    private static Database database(QualifiedName name, ConnectContext ctx) throws DdlException {
        List<String> parts = name.getParts();
        String dbName;
        if (parts.size() == 1) {
            dbName = ctx.getDatabase();
        } else if (parts.size() == 2) {
            dbName = parts.get(0);
        } else if (parts.size() == 3) {
            if (!InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME.equalsIgnoreCase(parts.get(0))) {
                throw new DdlException("Streams live in " + InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME
                        + ", not in catalog " + parts.get(0));
            }
            dbName = parts.get(1);
        } else {
            throw new DdlException("Invalid name " + name);
        }
        if (dbName == null || dbName.isEmpty()) {
            throw new DdlException("No database selected");
        }
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new DdlException("Unknown database '" + dbName + "'");
        }
        return db;
    }

    private static String lastPart(QualifiedName name) {
        List<String> parts = name.getParts();
        return parts.get(parts.size() - 1);
    }
}
