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
import com.starrocks.common.io.Writable;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * A Snowflake style stream on a native table: an offset, the version of every physical partition of the table at a
 * point in time, from which reading the stream returns the changes of the table up to now. The offset moves forward
 * only when a DML statement that reads the stream commits. Kept in its database and persisted with it, see
 * {@link TableStreamMgr}.
 */
public class TableStream implements Writable {
    @SerializedName("id")
    private long id;
    @SerializedName("dbId")
    private long dbId;
    @SerializedName("name")
    private String name;
    @SerializedName("tableId")
    private long tableId;
    @SerializedName("tableName")
    private String tableName;
    @SerializedName("appendOnly")
    private boolean appendOnly;
    @SerializedName("createTime")
    private long createTime;
    @SerializedName("owner")
    private String owner;
    @SerializedName("comment")
    private String comment;
    // the offset: the version of each physical partition, and when the table had them; a partition that is not in
    // the map had no data at the offset
    @SerializedName("offsetVersions")
    private Map<Long, Long> offsetVersions = new HashMap<>();
    @SerializedName("offsetTime")
    private long offsetTime;

    public TableStream(long id, long dbId, String name, long tableId, String tableName, boolean appendOnly,
                       long createTime, String owner, String comment, Map<Long, Long> offsetVersions,
                       long offsetTime) {
        this.id = id;
        this.dbId = dbId;
        this.name = name;
        this.tableId = tableId;
        this.tableName = tableName;
        this.appendOnly = appendOnly;
        this.createTime = createTime;
        this.owner = owner;
        this.comment = comment;
        this.offsetVersions = new HashMap<>(offsetVersions);
        this.offsetTime = offsetTime;
    }

    public static String key(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    public long getId() {
        return id;
    }

    public long getDbId() {
        return dbId;
    }

    public String getName() {
        return name;
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isAppendOnly() {
        return appendOnly;
    }

    public long getCreateTime() {
        return createTime;
    }

    public String getOwner() {
        return owner;
    }

    public String getComment() {
        return comment;
    }

    public synchronized Map<Long, Long> getOffsetVersions() {
        return new HashMap<>(offsetVersions);
    }

    public synchronized long getOffsetTime() {
        return offsetTime;
    }

    /** Moves the offset; done when a transaction that read the stream commits, on the leader and on replay. */
    synchronized void setOffset(Map<Long, Long> versions, long time) {
        this.offsetVersions = new HashMap<>(versions);
        this.offsetTime = time;
    }
}
