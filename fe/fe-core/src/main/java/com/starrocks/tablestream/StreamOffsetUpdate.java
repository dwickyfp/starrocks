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

import com.google.gson.annotations.SerializedName;

import java.util.HashMap;
import java.util.Map;

/**
 * The offset a transaction moves a stream to when it commits: the versions the stream was read up to. Carried by
 * the commit attachment of the transaction, so the offset moves with the commit, on the leader and on replay.
 */
public class StreamOffsetUpdate {
    @SerializedName("dbId")
    private final long dbId;
    @SerializedName("streamId")
    private final long streamId;
    @SerializedName("name")
    private final String name;
    /** the offset the stream was read from, which must still be its offset when the transaction commits */
    @SerializedName("fromTime")
    private final long fromTime;
    @SerializedName("from")
    private final Map<Long, Long> fromVersions;
    @SerializedName("toTime")
    private final long toTime;
    @SerializedName("to")
    private final Map<Long, Long> toVersions;

    public StreamOffsetUpdate(long dbId, long streamId, String name, long fromTime, Map<Long, Long> fromVersions,
                              long toTime, Map<Long, Long> toVersions) {
        this.dbId = dbId;
        this.streamId = streamId;
        this.name = name;
        this.fromTime = fromTime;
        this.fromVersions = new HashMap<>(fromVersions);
        this.toTime = toTime;
        this.toVersions = new HashMap<>(toVersions);
    }

    public long getDbId() {
        return dbId;
    }

    public long getStreamId() {
        return streamId;
    }

    public String getName() {
        return name;
    }

    public long getFromTime() {
        return fromTime;
    }

    public Map<Long, Long> getFromVersions() {
        return fromVersions;
    }

    public long getToTime() {
        return toTime;
    }

    public Map<Long, Long> getToVersions() {
        return toVersions;
    }
}
