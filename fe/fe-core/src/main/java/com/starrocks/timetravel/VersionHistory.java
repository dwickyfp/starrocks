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

import com.google.gson.annotations.SerializedName;

import java.util.Arrays;

/**
 * The versions a physical partition made visible over time, oldest first, for time travel. A point (v, t) means
 * version v became visible at t (ms) and stayed visible until the next point. Points older than the retention
 * period are dropped, except the newest of them, which still tells the version visible at the start of the period.
 */
public class VersionHistory {
    @SerializedName(value = "v")
    private long[] versions = new long[4];
    @SerializedName(value = "t")
    private long[] times = new long[4];
    @SerializedName(value = "n")
    private int size;

    public synchronized void record(long version, long time, long oldestTimeToKeep, int maxPoints) {
        if (size > 0 && versions[size - 1] >= version) {
            return; // replayed or out of order: the history only moves forward
        }
        if (size > 0 && time < times[size - 1]) {
            time = times[size - 1]; // keep the times ordered even if clocks moved backwards
        }
        if (size == versions.length) {
            versions = Arrays.copyOf(versions, size * 2);
            times = Arrays.copyOf(times, size * 2);
        }
        versions[size] = version;
        times[size] = time;
        size++;
        prune(oldestTimeToKeep, maxPoints);
    }

    /** Drops what the retention period and the point limit no longer need. */
    public synchronized void prune(long oldestTimeToKeep, int maxPoints) {
        int drop = 0;
        while (drop + 1 < size && times[drop + 1] <= oldestTimeToKeep) {
            drop++;
        }
        drop = Math.max(drop, size - Math.max(1, maxPoints));
        if (drop > 0) {
            System.arraycopy(versions, drop, versions, 0, size - drop);
            System.arraycopy(times, drop, times, 0, size - drop);
            size -= drop;
            if (versions.length > 16 && size < versions.length / 4) {
                versions = Arrays.copyOf(versions, Math.max(4, size * 2));
                times = Arrays.copyOf(times, Math.max(4, size * 2));
            }
        }
    }

    /** The version visible at {@code time}, or -1 when {@code time} is before the oldest point. */
    public synchronized long versionAt(long time) {
        int low = 0;
        int high = size - 1;
        long found = -1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (times[mid] <= time) {
                found = versions[mid];
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return found;
    }

    public synchronized int size() {
        return size;
    }

    /** The oldest point as {version, time}, or null when empty. */
    public synchronized long[] oldest() {
        return size == 0 ? null : new long[] {versions[0], times[0]};
    }
}
