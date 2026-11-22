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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableChanges;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.timetravel.TimeTravelResolver;
import com.starrocks.timetravel.TimeTravelRetention;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TransactionCommitFailedException;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Creates, drops and reads streams, and moves their offsets. A stream lives in the map of its {@link Database},
 * persisted with the database in the image; creating and dropping one go through the edit log
 * ({@code OP_CREATE_TABLE_STREAM}, {@code OP_DROP_TABLE_STREAM}), and its offset moves with the commit of a
 * transaction that read it, which carries the new offset in its commit attachment.
 */
public final class TableStreamMgr {
    private static final Logger LOG = LogManager.getLogger(TableStreamMgr.class);
    private static final Object LOCK = new Object();

    public static final String ACTION = "METADATA$ACTION";
    public static final String IS_UPDATE = "METADATA$ISUPDATE";
    public static final String ROW_ID = "METADATA$ROW_ID";
    public static final String CURRENT_ALIAS = "__stream_cur";
    public static final String OFFSET_ALIAS = "__stream_old";
    public static final String HAS_DATA = "SYSTEM$STREAM_HAS_DATA";

    // stream id -> id of the transaction whose commit is moving its offset, on the leader
    private static final Map<Long, Long> RESERVED = new HashMap<>();

    private TableStreamMgr() {
    }

    public static TableStream get(Database db, String name) {
        return db.getTableStreams().get(TableStream.key(name));
    }

    public static List<TableStream> list(Database db) {
        List<TableStream> streams = new ArrayList<>(db.getTableStreams().values());
        streams.sort(Comparator.comparing(TableStream::getName, String.CASE_INSENSITIVE_ORDER));
        return streams;
    }

    public static void create(Database db, TableStream stream, boolean orReplace, boolean ifNotExists)
            throws DdlException {
        synchronized (LOCK) {
            TableStream existing = get(db, stream.getName());
            if (existing != null && !orReplace) {
                if (ifNotExists) {
                    return;
                }
                throw new DdlException("Stream " + existing.getName() + " already exists in database "
                        + db.getFullName());
            }
            GlobalStateMgr.getCurrentState().getEditLog().logCreateTableStream(stream,
                    wal -> replayCreate((TableStream) wal));
        }
        LOG.info("Created stream {} on table {} in database {}", stream.getName(), stream.getTableName(),
                db.getFullName());
    }

    /** Drops the stream {@code name}; returns false when there is none. */
    public static boolean drop(Database db, String name) {
        synchronized (LOCK) {
            TableStream existing = get(db, name);
            if (existing == null) {
                return false;
            }
            GlobalStateMgr.getCurrentState().getEditLog().logDropTableStream(
                    new DropTableStreamLog(db.getId(), existing.getName()), wal -> replayDrop((DropTableStreamLog) wal));
            LOG.info("Dropped stream {} from database {}", existing.getName(), db.getFullName());
            return true;
        }
    }

    public static void replayCreate(TableStream stream) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(stream.getDbId());
        if (db == null) {
            LOG.warn("Skip replaying stream {}: database {} does not exist", stream.getName(), stream.getDbId());
            return;
        }
        db.getTableStreams().put(TableStream.key(stream.getName()), stream);
    }

    public static void replayDrop(DropTableStreamLog log) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(log.getDbId());
        if (db != null) {
            db.getTableStreams().remove(TableStream.key(log.getName()));
        }
    }

    /** The table of the stream, or null when it was dropped (a table of the same name made since is another one). */
    public static OlapTable tableOf(TableStream stream) {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(stream.getDbId(),
                stream.getTableId());
        return table instanceof OlapTable olapTable ? olapTable : null;
    }

    /** The versions of the table's partitions now, which reading the stream up to now reads. */
    public static Map<Long, Long> currentVersions(OlapTable table) {
        Map<Long, Long> versions = new HashMap<>();
        for (PhysicalPartition partition : table.getAllPhysicalPartitions()) {
            versions.put(partition.getId(), partition.getVisibleVersion());
        }
        return versions;
    }

    /**
     * Why the stream cannot be read, or null when it can: the changes since its offset are no longer known once its
     * table is gone, its offset is beyond the retention period of the table, or the data of a partition was replaced.
     */
    public static String staleReason(TableStream stream, OlapTable table, long now) {
        if (table == null) {
            return "its table " + stream.getTableName() + " was dropped";
        }
        int days = TimeTravelRetention.effectiveDays(table);
        if (days <= 0) {
            return "time travel is not enabled for its table " + table.getName();
        }
        if (stream.getOffsetTime() < now - days * TimeTravelRetention.DAY_MS) {
            return "its offset is older than the data retention time of its table " + table.getName() + " ("
                    + days + " days)";
        }
        for (Map.Entry<Long, Long> entry : stream.getOffsetVersions().entrySet()) {
            if (entry.getValue() <= PhysicalPartition.PARTITION_INIT_VERSION) {
                continue;
            }
            PhysicalPartition partition = table.getPhysicalPartition(entry.getKey());
            if (partition == null) {
                return "a partition of its table " + table.getName() + " was dropped or replaced since its offset";
            }
            if (partition.getHistoryFloorTime() > stream.getOffsetTime()) {
                return "the data of its table " + table.getName() + " was replaced since its offset (TRUNCATE, "
                        + "INSERT OVERWRITE or a schema change)";
            }
        }
        return null;
    }

    /**
     * The stream a name gives, {@code stream} or {@code db.stream}, with {@code defaultDb} for an unqualified one.
     */
    public static Pair<Database, TableStream> resolve(String name, String defaultDb) {
        String[] parts = name.trim().split("\\.");
        String dbName;
        String streamName;
        if (parts.length == 1) {
            dbName = defaultDb;
            streamName = parts[0];
        } else if (parts.length == 2) {
            dbName = parts[0];
            streamName = parts[1];
        } else {
            throw new SemanticException("Invalid stream name '%s': use stream or db.stream", name);
        }
        if (dbName == null || dbName.isEmpty()) {
            throw new SemanticException("No database selected for stream %s", name);
        }
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        TableStream stream = db == null ? null : get(db, streamName);
        if (stream == null) {
            throw new SemanticException("Stream %s does not exist in database %s", streamName, dbName);
        }
        return Pair.create(db, stream);
    }

    /**
     * SYSTEM$STREAM_HAS_DATA: whether the table of the stream changed since its offset, from the versions of its
     * partitions alone, without reading data. Changes that cancel out (a row inserted and deleted again) count as
     * data, as in Snowflake. A stale stream is an error: its changes are unknown.
     */
    public static boolean hasData(String name, String defaultDb) {
        TableStream stream = resolve(name, defaultDb).second;
        OlapTable table = tableOf(stream);
        String stale = staleReason(stream, table, System.currentTimeMillis());
        if (stale != null) {
            throw new SemanticException("Stream %s is stale: %s. Recreate the stream to read the table again",
                    stream.getName(), stale);
        }
        Map<Long, Long> offset = stream.getOffsetVersions();
        for (PhysicalPartition partition : table.getAllPhysicalPartitions()) {
            if (partition.getVisibleVersion() != version(offset, partition.getId())) {
                return true;
            }
        }
        return false;
    }

    /** Why the table cannot have a stream of this kind, or null. */
    public static String unsupportedReason(OlapTable table, boolean appendOnly) {
        if (!TimeTravelRetention.isSupported(table)) {
            return "streams are not supported for table " + table.getName() + " in shared-data mode";
        }
        if (TimeTravelRetention.effectiveDays(table) <= 0) {
            return "time travel is not enabled for table " + table.getName() + ": set its property "
                    + "data_retention_time_in_days or the global variable data_retention_time_in_days";
        }
        boolean keyed = table.getKeysType() != KeysType.DUP_KEYS;
        for (Column column : columns(table)) {
            boolean compared = keyed ? column.isKey() || !appendOnly : true;
            if (compared && !column.getType().canJoinOn()) {
                return "column " + column.getName() + " of type " + column.getType().toSql() + " cannot be compared"
                        + (keyed ? ", which a standard stream does: use APPEND_ONLY = TRUE" : "");
            }
        }
        return null;
    }

    static List<Column> columns(OlapTable table) {
        return table.getBaseSchema().stream().filter(c -> !c.isHidden()).collect(Collectors.toList());
    }

    /**
     * The query a read of the stream runs: the changes of its table since its offset, up to now. A read in an
     * explicit transaction repeats the read before it in the transaction. The read is noted in the session: the DML
     * statement that reads it moves its offset when its transaction commits.
     */
    public static QueryStatement read(ConnectContext session, Database db, TableStream stream) {
        OlapTable table = tableOf(stream);
        long now = System.currentTimeMillis();
        String stale = staleReason(stream, table, now);
        if (stale != null) {
            throw new SemanticException("Stream %s is stale: %s. Recreate the stream to read the table again",
                    stream.getName(), stale);
        }
        String unsupported = unsupportedReason(table, stream.isAppendOnly());
        if (unsupported != null) {
            throw new SemanticException("Stream %s cannot be read: %s", stream.getName(), unsupported);
        }
        Map<Long, Long> from;
        long fromTime;
        synchronized (stream) {
            from = new HashMap<>(stream.getOffsetVersions());
            fromTime = stream.getOffsetTime();
        }
        StreamOffsetUpdate earlier = session.getStreamRead(stream.getId());
        Map<Long, Long> to;
        long toTime;
        if (earlier != null && earlier.getFromTime() == fromTime && earlier.getFromVersions().equals(from)) {
            to = earlier.getToVersions();
            toTime = earlier.getToTime();
        } else {
            to = currentVersions(table);
            toTime = Math.max(now, fromTime);
        }
        String qualified = quote(db.getFullName()) + "." + quote(table.getName());
        QueryStatement query = parse(session, changes(stream.isAppendOnly(), table, qualified, from, to));
        session.addStreamRead(new StreamOffsetUpdate(db.getId(), stream.getId(), stream.getName(), fromTime, from,
                toTime, to));
        return query;
    }

    /** The query of the changes, with the versions its relations scan pinned. */
    private static QueryStatement parse(ConnectContext session, Changes changes) {
        QueryStatement query = (QueryStatement) SqlParser.parse(changes.sql(), session.getSessionVariable()).get(0);
        for (TableRelation relation : AnalyzerUtils.collectTableRelations(query)) {
            String alias = relation.getAlias() == null ? null : relation.getAlias().getTbl();
            if (CURRENT_ALIAS.equals(alias)) {
                relation.setPinnedScanVersions(changes.toVersions());
            } else if (OFFSET_ALIAS.equals(alias)) {
                relation.setPinnedScanVersions(changes.fromVersions());
            }
        }
        return query;
    }

    /**
     * The query of a CHANGES clause on the table: its changes from the start point of the clause (a point in time,
     * or the offset of a stream on the table) to its end point, or now. It reads the table with time travel, as a
     * stream does, and moves no offset.
     */
    public static QueryStatement changesOf(ConnectContext session, Database db, OlapTable table, TableChanges clause) {
        String unsupported = unsupportedReason(table, clause.appendOnly());
        if (unsupported != null) {
            throw new SemanticException("CHANGES cannot read table %s: %s", table.getName(),
                    unsupported.replace("a standard stream does: use APPEND_ONLY = TRUE",
                            "INFORMATION => DEFAULT does: use INFORMATION => APPEND_ONLY"));
        }
        long now = System.currentTimeMillis();
        TimeTravelResolver.Snapshot from;
        if (clause.stream() != null) {
            TableStream stream = resolve(clause.stream(), db.getFullName()).second;
            if (stream.getTableId() != table.getId()) {
                throw new SemanticException("Stream %s is not on table %s", stream.getName(), table.getName());
            }
            String stale = staleReason(stream, table, now);
            if (stale != null) {
                throw new SemanticException("Stream %s is stale: %s", stream.getName(), stale);
            }
            synchronized (stream) {
                from = new TimeTravelResolver.Snapshot(stream.getOffsetTime(), new HashMap<>(stream.getOffsetVersions()));
            }
        } else {
            from = TimeTravelResolver.snapshot(table, clause.start(), session);
        }
        TimeTravelResolver.Snapshot to = clause.end() == null ? new TimeTravelResolver.Snapshot(now,
                currentVersions(table)) : TimeTravelResolver.snapshot(table, clause.end(), session);
        if (to.time() < from.time()) {
            throw new SemanticException("The END point of CHANGES on table %s is before its start point",
                    table.getName());
        }
        String qualified = quote(db.getFullName()) + "." + quote(table.getName());
        return parse(session, changes(clause.appendOnly(), table, qualified, from.versions(), to.versions()));
    }

    /** The changes a read of the stream returns: the SQL of the query and the versions its relations scan. */
    public record Changes(String sql, Map<Long, Long> fromVersions, Map<Long, Long> toVersions) {
    }

    /**
     * The changes of the table between the versions {@code from} and {@code to}: a query over the table that scans
     * relations named {@link #OFFSET_ALIAS} at {@code from} and relations named {@link #CURRENT_ALIAS} at
     * {@code to}. A keyed table (primary, unique or aggregate key) compares the rows of each key: a key that is new
     * is an INSERT, one that is gone a DELETE, and one whose row changed both, with METADATA$ISUPDATE. A duplicate
     * key table compares the rows as a multiset. An append-only stream returns the INSERT rows only.
     */
    public static Changes changes(boolean appendOnly, OlapTable table, String qualifiedTable,
                                  Map<Long, Long> from, Map<Long, Long> to) {
        List<String> changed = new ArrayList<>(new TreeSet<>(table.getAllPhysicalPartitions().stream()
                .filter(p -> !version(from, p.getId()).equals(version(to, p.getId())))
                .map(p -> {
                    Partition partition = table.getPartition(p.getParentId());
                    return partition == null ? null : partition.getName();
                })
                .filter(n -> n != null)
                .collect(Collectors.toSet())));
        Map<Long, Long> fromPins = new HashMap<>();
        for (PhysicalPartition partition : table.getAllPhysicalPartitions()) {
            fromPins.put(partition.getId(), version(from, partition.getId()));
        }
        List<Column> columns = columns(table);
        String cur = CURRENT_ALIAS;
        String old = OFFSET_ALIAS;
        if (changed.isEmpty()) {
            String sql = "SELECT " + list(columns, cur) + ", CAST(NULL AS VARCHAR(6)) AS " + quote(ACTION)
                    + ", CAST(NULL AS BOOLEAN) AS " + quote(IS_UPDATE) + ", CAST(NULL AS VARCHAR(32)) AS "
                    + quote(ROW_ID) + " FROM " + qualifiedTable + " AS " + quote(cur) + " WHERE FALSE";
            return new Changes(sql, fromPins, to);
        }
        String partitions = !table.getPartitionInfo().isPartitioned() ? "" : " PARTITION ("
                + changed.stream().map(TableStreamMgr::quote).collect(Collectors.joining(", ")) + ")";
        String curTable = qualifiedTable + partitions + " AS " + quote(cur);
        String oldTable = qualifiedTable + partitions + " AS " + quote(old);
        String sql;
        if (table.getKeysType() != KeysType.DUP_KEYS) {
            List<Column> keys = columns.stream().filter(Column::isKey).collect(Collectors.toList());
            List<Column> values = columns.stream().filter(c -> !c.isKey()).collect(Collectors.toList());
            String firstKey = quote(keys.get(0).getName());
            String insertSide = "SELECT " + list(columns, cur) + ", 'INSERT' AS " + quote(ACTION) + ", "
                    + (appendOnly ? "FALSE" : quote(old) + "." + firstKey + " IS NOT NULL") + " AS "
                    + quote(IS_UPDATE) + ", " + rowId(keys, cur, null) + " AS " + quote(ROW_ID)
                    + " FROM " + curTable + " LEFT JOIN " + oldTable + " ON " + join(keys, cur, old, "=")
                    + " WHERE " + quote(old) + "." + firstKey + " IS NULL"
                    + (appendOnly || values.isEmpty() ? "" : " OR NOT (" + join(values, cur, old, "<=>")
                    + ")");
            if (appendOnly) {
                sql = insertSide;
            } else {
                String deleteSide = "SELECT " + list(columns, old) + ", 'DELETE', " + quote(cur) + "." + firstKey
                        + " IS NOT NULL, " + rowId(keys, old, null) + " FROM " + oldTable + " LEFT JOIN " + curTable
                        + " ON " + join(keys, old, cur, "=") + " WHERE " + quote(cur) + "." + firstKey + " IS NULL"
                        + (values.isEmpty() ? "" : " OR NOT (" + join(values, old, cur, "<=>") + ")");
                sql = insertSide + " UNION ALL " + deleteSide;
            }
        } else {
            String insertSide = multisetSide(columns, curTable, oldTable, "INSERT");
            sql = appendOnly ? insertSide
                    : insertSide + " UNION ALL " + multisetSide(columns, oldTable, curTable, "DELETE");
        }
        return new Changes(sql, fromPins, to);
    }

    /**
     * The rows of {@code left} that {@code right} has fewer of: each row of {@code left} is numbered among its
     * equal rows, and the numbers above the count of that row in {@code right} are the extra rows.
     */
    private static String multisetSide(List<Column> columns, String left, String right, String action) {
        String numbered = "`__stream_l`";
        String counted = "`__stream_r`";
        String cols = list(columns, null);
        return "SELECT " + list(columns, "__stream_l") + ", '" + action + "' AS " + quote(ACTION) + ", FALSE AS "
                + quote(IS_UPDATE) + ", " + rowId(columns, "__stream_l", "`__stream_l`.`__stream_rn`") + " AS "
                + quote(ROW_ID)
                + " FROM (SELECT " + cols + ", row_number() OVER (PARTITION BY " + cols + ") AS `__stream_rn` FROM "
                + left + ") " + numbered
                + " LEFT JOIN (SELECT " + cols + ", count(*) AS `__stream_n` FROM " + right + " GROUP BY " + cols
                + ") " + counted + " ON " + join(columns, "__stream_l", "__stream_r", "<=>")
                + " WHERE " + numbered + ".`__stream_rn` > coalesce(" + counted + ".`__stream_n`, 0)";
    }

    private static Long version(Map<Long, Long> versions, long partitionId) {
        Long version = versions.get(partitionId);
        return version == null ? PhysicalPartition.PARTITION_INIT_VERSION : version;
    }

    private static String rowId(List<Column> columns, String alias, String extra) {
        List<String> parts = columns.stream()
                .map(c -> "coalesce(cast(" + quote(alias) + "." + quote(c.getName()) + " AS VARCHAR), '\\\\N')")
                .collect(Collectors.toList());
        if (extra != null) {
            parts.add("cast(" + extra + " AS VARCHAR)");
        }
        return "md5(concat_ws('\\\\x01', " + String.join(", ", parts) + "))";
    }

    private static String list(List<Column> columns, String alias) {
        return columns.stream().map(c -> (alias == null ? "" : quote(alias) + ".") + quote(c.getName()))
                .collect(Collectors.joining(", "));
    }

    private static String join(List<Column> columns, String left, String right, String op) {
        return columns.stream().map(c -> quote(left) + "." + quote(c.getName()) + " " + op + " " + quote(right) + "."
                + quote(c.getName())).collect(Collectors.joining(" AND "));
    }

    public static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    // ---------------------------------------- offsets -------------------------------------------------------------

    private static List<StreamOffsetUpdate> updatesOf(TransactionState txn) {
        return txn.getTxnCommitAttachment() instanceof InsertTxnCommitAttachment attachment
                ? attachment.getStreamOffsets() : null;
    }

    /**
     * Before a transaction that read streams commits, on the leader: every stream must still be at the offset it was
     * read from, and no other committing transaction may be moving it. Reserves the streams for this transaction.
     */
    public static void reserve(TransactionState txn) throws TransactionCommitFailedException {
        List<StreamOffsetUpdate> updates = updatesOf(txn);
        if (updates == null || updates.isEmpty()) {
            return;
        }
        synchronized (RESERVED) {
            for (StreamOffsetUpdate update : updates) {
                TableStream stream = streamOf(update);
                Long holder = RESERVED.get(update.getStreamId());
                if (stream == null) {
                    throw new TransactionCommitFailedException("Stream " + update.getName()
                            + " was dropped or replaced while the statement read it");
                }
                if ((holder != null && holder != txn.getTransactionId())
                        || stream.getOffsetTime() != update.getFromTime()
                        || !stream.getOffsetVersions().equals(update.getFromVersions())) {
                    throw new TransactionCommitFailedException("Stream " + update.getName() + " was consumed by "
                            + "another transaction while the statement read it; run the statement again");
                }
            }
            for (StreamOffsetUpdate update : updates) {
                RESERVED.put(update.getStreamId(), txn.getTransactionId());
            }
        }
    }

    /** Releases the streams a transaction reserved, when its commit fails. */
    public static void release(TransactionState txn) {
        List<StreamOffsetUpdate> updates = updatesOf(txn);
        if (updates == null || updates.isEmpty()) {
            return;
        }
        synchronized (RESERVED) {
            for (StreamOffsetUpdate update : updates) {
                RESERVED.remove(update.getStreamId(), txn.getTransactionId());
            }
        }
    }

    /** Moves the offsets of the streams a transaction read, when it commits: on the leader and on replay. */
    public static void onTransactionCommitted(TransactionState txn) {
        List<StreamOffsetUpdate> updates = updatesOf(txn);
        if (updates == null || updates.isEmpty()) {
            return;
        }
        synchronized (RESERVED) {
            for (StreamOffsetUpdate update : updates) {
                TableStream stream = streamOf(update);
                if (stream != null && update.getToTime() >= stream.getOffsetTime()) {
                    stream.setOffset(update.getToVersions(), update.getToTime());
                }
                RESERVED.remove(update.getStreamId(), txn.getTransactionId());
            }
        }
    }

    private static TableStream streamOf(StreamOffsetUpdate update) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(update.getDbId());
        if (db == null) {
            return null;
        }
        TableStream stream = get(db, update.getName());
        return stream == null || stream.getId() != update.getStreamId() ? null : stream;
    }
}
