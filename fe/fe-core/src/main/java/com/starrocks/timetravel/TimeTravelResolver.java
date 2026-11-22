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
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.ConnectorTableVersion;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.QueryPeriodResolver;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryPeriod;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.DateType;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Resolves time travel on a native table (AT / BEFORE / FOR TIMESTAMP AS OF) to the version each of its physical
 * partitions had at that point in time, and pins those versions on the session for the scan of the statement.
 */
public final class TimeTravelResolver {
    private static final DateTimeFormatter FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private TimeTravelResolver() {
    }

    public static void resolve(TableRelation relation, OlapTable table, ConnectContext session) {
        Snapshot snapshot = snapshot(table, relation.getQueryPeriod(), session);
        session.addTimeTravelScanVersions(table.getId(), table.getName(), snapshot.versions());
    }

    /** A point in time of a table and the versions of its physical partitions visible then. */
    public record Snapshot(long time, Map<Long, Long> versions) {
    }

    /** The snapshot of the table at the point of a time travel clause. */
    public static Snapshot snapshot(OlapTable table, QueryPeriod period, ConnectContext session) {
        String tableName = table.getName();
        if (period == null || period.getPeriodType() != QueryPeriod.PeriodType.TIMESTAMP
                || period.getStart().isPresent() || period.getEnd().isEmpty()) {
            throw new SemanticException("Time travel on table %s only reads the table as of a point in time: "
                    + "use AT(TIMESTAMP => ...), AT(OFFSET => ...), BEFORE(TIMESTAMP => ...) or FOR TIMESTAMP AS OF",
                    tableName);
        }
        if (!TimeTravelRetention.isSupported(table)) {
            throw new SemanticException("Time travel is not supported for table %s in shared-data mode", tableName);
        }
        int days = TimeTravelRetention.effectiveDays(table);
        if (days <= 0) {
            throw new SemanticException("Time travel is not enabled for table %s: set its property "
                    + "data_retention_time_in_days or the global variable data_retention_time_in_days", tableName);
        }

        long point = toEpochMillis(QueryPeriodResolver.resolve(period.getEnd(), QueryPeriod.PeriodType.TIMESTAMP,
                session), tableName);
        long now = System.currentTimeMillis();
        if (point > now) {
            throw new SemanticException("Time travel point %s on table %s is in the future", format(point), tableName);
        }
        if (point < now - days * TimeTravelRetention.DAY_MS) {
            throw new SemanticException("Time travel point %s on table %s is beyond its data retention time of %d days",
                    format(point), tableName, days);
        }

        Map<Long, Long> versions = new HashMap<>();
        for (PhysicalPartition partition : table.getAllPhysicalPartitions()) {
            versions.put(partition.getId(), versionAt(partition, point, tableName));
        }
        return new Snapshot(point, versions);
    }

    /** The version of {@code partition} visible at {@code point}. */
    static long versionAt(PhysicalPartition partition, long point, String tableName) {
        if (point >= partition.getVisibleVersionTime()) {
            return partition.getVisibleVersion();
        }
        if (point < partition.getHistoryFloorTime()) {
            throw new SemanticException("Time travel point %s on table %s is before %s, when its data was replaced "
                    + "(TRUNCATE, INSERT OVERWRITE, a replace of temporary partitions or a schema change): time travel "
                    + "cannot read it before then", format(point), tableName, format(partition.getHistoryFloorTime()));
        }
        VersionHistory history = partition.getVersionHistory();
        long version = history == null ? -1 : history.versionAt(point);
        if (version > 0) {
            return version;
        }
        long[] oldest = history == null ? null : history.oldest();
        long oldestVersion = oldest == null ? partition.getVisibleVersion() : oldest[0];
        if (oldestVersion <= PhysicalPartition.PARTITION_INIT_VERSION) {
            // the partition had no data yet (or did not exist) at that point
            return PhysicalPartition.PARTITION_INIT_VERSION;
        }
        long since = oldest == null ? partition.getVisibleVersionTime() : oldest[1];
        throw new SemanticException("Time travel point %s on table %s is older than the history kept for it, which "
                + "starts at %s", format(point), tableName, format(since));
    }

    private static long toEpochMillis(Optional<ConnectorTableVersion> version, String tableName) {
        ConstantOperator constant = version.map(ConnectorTableVersion::getConstantOperator).orElse(null);
        Optional<ConstantOperator> datetime = constant == null || constant.isNull()
                ? Optional.empty() : constant.castTo(DateType.DATETIME);
        if (datetime.isEmpty()) {
            throw new SemanticException("Time travel point on table %s must be a timestamp, but is %s", tableName,
                    constant);
        }
        LocalDateTime time = datetime.get().getDatetime();
        return time.atZone(TimeUtils.getTimeZone().toZoneId()).toInstant().toEpochMilli();
    }

    private static String format(long epochMillis) {
        return LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(epochMillis), TimeUtils.getTimeZone().toZoneId())
                .format(FORMAT);
    }
}
