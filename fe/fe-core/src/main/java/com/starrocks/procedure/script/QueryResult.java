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

package com.starrocks.procedure.script;

import java.util.List;

/** The rows a query returned, as text (null for SQL NULL), with its column names and types. */
public final class QueryResult {
    private final List<String> columns;
    private final List<String> types;
    private final List<List<String>> rows;

    public QueryResult(List<String> columns, List<String> types, List<List<String>> rows) {
        this.columns = columns;
        this.types = types;
        this.rows = rows;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<String> getTypes() {
        return types;
    }

    public List<List<String>> getRows() {
        return rows;
    }

    public ScriptValue value(int row, int column) {
        return new ScriptValue(types.get(column), rows.get(row).get(column));
    }
}
