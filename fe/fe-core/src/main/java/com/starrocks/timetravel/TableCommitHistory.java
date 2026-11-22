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

import java.util.ArrayList;
import java.util.List;

/**
 * The transactions that changed a table, oldest first, for DESCRIBE HISTORY: when each became visible, the statement
 * or load that wrote it and how much it wrote. Kept for the time travel retention period of the table, like the
 * version history of its partitions, and built the same way on every FE from the edit log.
 */
public class TableCommitHistory {

    public static final class Commit {
        @SerializedName("t")
        private final long time;
        @SerializedName("o")
        private final String operation;
        /** rows the transaction wrote, over all its tables; -1 when not known */
        @SerializedName("r")
        private final long rows;
        @SerializedName("p")
        private final int partitions;
        @SerializedName("x")
        private final long txnId;
        @SerializedName("l")
        private final String label;

        public Commit(long time, String operation, long rows, int partitions, long txnId, String label) {
            this.time = time;
            this.operation = operation;
            this.rows = rows;
            this.partitions = partitions;
            this.txnId = txnId;
            this.label = label;
        }

        public long getTime() {
            return time;
        }

        public String getOperation() {
            return operation;
        }

        public long getRows() {
            return rows;
        }

        public int getPartitions() {
            return partitions;
        }

        public long getTxnId() {
            return txnId;
        }

        public String getLabel() {
            return label;
        }
    }

    @SerializedName("c")
    private List<Commit> commits = new ArrayList<>();

    public synchronized void record(Commit commit, long oldestTimeToKeep, int maxCommits) {
        if (commits == null) {
            commits = new ArrayList<>();
        }
        for (int i = commits.size() - 1; i >= 0 && i >= commits.size() - 8; i--) {
            if (commits.get(i).txnId == commit.txnId) {
                return; // replayed
            }
        }
        long time = commits.isEmpty() ? commit.time : Math.max(commit.time, commits.get(commits.size() - 1).time);
        commits.add(time == commit.time ? commit : new Commit(time, commit.operation, commit.rows, commit.partitions,
                commit.txnId, commit.label));
        prune(oldestTimeToKeep, maxCommits);
    }

    /** Drops the commits older than the retention period, and the oldest ones beyond {@code maxCommits}. */
    public synchronized void prune(long oldestTimeToKeep, int maxCommits) {
        if (commits == null) {
            return;
        }
        int drop = 0;
        while (drop < commits.size() && commits.get(drop).time < oldestTimeToKeep) {
            drop++;
        }
        drop = Math.max(drop, commits.size() - Math.max(1, maxCommits));
        if (drop > 0) {
            commits = new ArrayList<>(commits.subList(drop, commits.size()));
        }
    }

    /** The commits, oldest first. */
    public synchronized List<Commit> commits() {
        return commits == null ? new ArrayList<>() : new ArrayList<>(commits);
    }
}
