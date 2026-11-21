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
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.sql.analyzer.SemanticException;

/**
 * How long tables keep their history for time travel: the table property data_retention_time_in_days when set,
 * else the global variable of the same name. 0 means no time travel.
 */
public final class TimeTravelRetention {
    public static final long DAY_MS = 24L * 3600 * 1000;

    private TimeTravelRetention() {
    }

    public static void checkDays(String name, int days) {
        if (days < 0 || days > Config.time_travel_max_retention_days) {
            throw new SemanticException("%s must be between 0 and %d days (FE config time_travel_max_retention_days), "
                    + "but is %d", name, Config.time_travel_max_retention_days, days);
        }
    }

    /** Time travel reads old versions the BEs of a shared-nothing cluster keep; shared-data tables are not covered. */
    public static boolean isSupported(OlapTable table) {
        return !table.isCloudNativeTableOrMaterializedView();
    }

    public static void checkTableSupported(OlapTable table) throws DdlException {
        if (!isSupported(table)) {
            throw new DdlException("Time travel is not supported for table " + table.getName()
                    + " in shared-data mode");
        }
    }

    public static int effectiveDays(OlapTable table) {
        int own = table.getDataRetentionTimeInDays();
        return own >= 0 ? own : GlobalVariable.getDataRetentionTimeInDays();
    }

    /**
     * Called when {@code version} of {@code partition} becomes visible at {@code versionTime}, on the leader and on
     * replay alike (versionTime comes from the edit log), so every FE builds the same history.
     */
    public static void onVersionVisible(OlapTable table, PhysicalPartition partition, long version, long versionTime) {
        if (!isSupported(table)) {
            return;
        }
        int days = effectiveDays(table);
        if (days <= 0) {
            if (partition.getVersionHistory() != null) {
                partition.clearVersionHistory();
            }
            return;
        }
        partition.recordVersionHistory(version, versionTime, versionTime - days * DAY_MS,
                Config.time_travel_max_versions_per_partition);
    }
}
