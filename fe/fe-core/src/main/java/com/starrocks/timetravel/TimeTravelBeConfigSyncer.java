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

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.NetUtils;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.Backend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Leader daemon that makes BEs keep old versions as long as time travel may read them. BEs drop the rowsets a
 * compaction replaced, and the versions a primary key table expired, after tablet_rowset_stale_sweep_time_sec;
 * this raises that BE config to the longest data_retention_time_in_days (global variable or table property),
 * applies it again to a BE that restarted, and puts it back to the BE default once no table needs more.
 */
public class TimeTravelBeConfigSyncer extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(TimeTravelBeConfigSyncer.class);

    public static final String BE_CONFIG = "tablet_rowset_stale_sweep_time_sec";
    // the BE default of tablet_rowset_stale_sweep_time_sec
    public static final long BE_DEFAULT_SECONDS = 1800;
    // queries started just inside the retention period still need their versions while they run
    private static final long MARGIN_SECONDS = 3600;
    private static final int TIMEOUT_MS = 5000;

    // backend id -> "value@start time" last applied to it
    private final Map<Long, String> applied = new HashMap<>();
    private boolean raised = false;

    public TimeTravelBeConfigSyncer() {
        super("time-travel-be-config-syncer", Config.time_travel_be_config_sync_interval_sec * 1000L);
    }

    @Override
    protected void runAfterCatalogReady() {
        setInterval(Math.max(1, Config.time_travel_be_config_sync_interval_sec) * 1000L);
        if (!Config.enable_time_travel_be_config_sync || RunMode.isSharedDataMode()) {
            return;
        }
        long target = targetSeconds(maxRetentionDays());
        if (target <= BE_DEFAULT_SECONDS && !raised) {
            return; // nothing asked for more than the default: leave the BE config alone
        }
        boolean allApplied = true;
        List<Backend> backends = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends();
        for (Backend backend : backends) {
            if (!backend.isAlive()) {
                continue;
            }
            String key = target + "@" + backend.getLastStartTime();
            if (key.equals(applied.get(backend.getId()))) {
                continue;
            }
            if (apply(backend, target)) {
                applied.put(backend.getId(), key);
                LOG.info("Set BE config {}={} on backend {} for time travel", BE_CONFIG, target, backend.getId());
            } else {
                allApplied = false;
            }
        }
        raised = target > BE_DEFAULT_SECONDS || !allApplied;
    }

    @VisibleForTesting
    static long targetSeconds(int maxRetentionDays) {
        if (maxRetentionDays <= 0) {
            return BE_DEFAULT_SECONDS;
        }
        return Math.max(BE_DEFAULT_SECONDS, maxRetentionDays * 86400L + MARGIN_SECONDS);
    }

    /** The longest retention any native table has, from its property or the global variable. */
    @VisibleForTesting
    static int maxRetentionDays() {
        int max = Math.max(0, GlobalVariable.getDataRetentionTimeInDays());
        for (Database db : GlobalStateMgr.getCurrentState().getLocalMetastore().getAllDbs()) {
            for (Table table : db.getTables()) {
                if (table instanceof OlapTable && TimeTravelRetention.isSupported((OlapTable) table)) {
                    max = Math.max(max, ((OlapTable) table).getDataRetentionTimeInDays());
                }
            }
        }
        return max;
    }

    private static boolean apply(Backend backend, long seconds) {
        String url = "http://" + NetUtils.getHostPortInAccessibleFormat(backend.getHost(), backend.getHttpPort())
                + "/api/update_config?" + BE_CONFIG + "=" + seconds;
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) URI.create(url).toURL().openConnection();
            connection.setRequestMethod("POST");
            connection.setConnectTimeout(TIMEOUT_MS);
            connection.setReadTimeout(TIMEOUT_MS);
            int code = connection.getResponseCode();
            String body;
            try (InputStream in = code < 400 ? connection.getInputStream() : connection.getErrorStream()) {
                body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
            // the BE answers 200 with {"status": "OK" | "BAD", "msg": ...}
            if (code != HttpURLConnection.HTTP_OK || !body.replace(" ", "").contains("\"status\":\"OK\"")) {
                LOG.warn("Failed to set BE config {} on backend {}: HTTP {} {}", BE_CONFIG, backend.getId(), code, body);
                return false;
            }
            return true;
        } catch (Exception e) {
            LOG.warn("Failed to set BE config {} on backend {}: {}", BE_CONFIG, backend.getId(), e.getMessage());
            return false;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
}
