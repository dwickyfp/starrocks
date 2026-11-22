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

package com.starrocks.policy;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The policies attached to a table: at most one row access policy, on some of its columns, and at most one masking
 * policy per column. Kept in the database of the table and persisted with it; a change replaces it.
 */
public class TablePolicies implements Writable {
    /** A policy attached to a table, and the columns of the table it is given, in the order of its arguments. */
    public static class Ref {
        @SerializedName("dbId")
        private long policyDbId;
        @SerializedName("id")
        private long policyId;
        @SerializedName("columns")
        private List<String> columns = new ArrayList<>();

        public Ref(long policyDbId, long policyId, List<String> columns) {
            this.policyDbId = policyDbId;
            this.policyId = policyId;
            this.columns = new ArrayList<>(columns);
        }

        public long getPolicyDbId() {
            return policyDbId;
        }

        public long getPolicyId() {
            return policyId;
        }

        public List<String> getColumns() {
            return columns;
        }

        public boolean refersTo(SecurityPolicy policy) {
            return policyDbId == policy.getDbId() && policyId == policy.getId();
        }
    }

    @SerializedName("dbId")
    private long dbId;
    @SerializedName("tableId")
    private long tableId;
    @SerializedName("rowAccess")
    private Ref rowAccess;
    // lower-case column name -> its masking policy
    @SerializedName("masks")
    private Map<String, Ref> masks = new LinkedHashMap<>();

    public TablePolicies(long dbId, long tableId, Ref rowAccess, Map<String, Ref> masks) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.rowAccess = rowAccess;
        this.masks = new LinkedHashMap<>(masks);
    }

    public static String key(String column) {
        return column.toLowerCase(Locale.ROOT);
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public Ref getRowAccess() {
        return rowAccess;
    }

    public Map<String, Ref> getMasks() {
        return masks;
    }

    public boolean isEmpty() {
        return rowAccess == null && masks.isEmpty();
    }

    /** The columns of the table that a policy reads or masks. */
    public List<String> referencedColumns() {
        List<String> columns = new ArrayList<>();
        if (rowAccess != null) {
            columns.addAll(rowAccess.getColumns());
        }
        for (Ref mask : masks.values()) {
            columns.addAll(mask.getColumns());
        }
        return columns;
    }
}
