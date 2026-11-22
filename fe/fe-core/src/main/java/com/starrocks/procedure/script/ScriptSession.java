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

/**
 * What a script needs from the database. Every method takes SQL with the variables already bound.
 */
public interface ScriptSession {
    /** The value of a scalar expression. */
    ScriptValue evaluate(String expressionSql);

    /** Runs a query and returns all its rows. */
    QueryResult query(String sql);

    /** Runs any statement and returns the number of rows it changed, or returned for a query. */
    long execute(String sql);

    /** Fails when the call was cancelled or ran out of time. */
    void checkAlive();

    /** Runs the stored procedure {@code nameParts} with the given arguments and returns its value. */
    ScriptValue call(List<String> nameParts, List<ScriptValue> arguments);

    /** Runs the RETURNS TABLE procedure {@code nameParts} with the given arguments and returns its table. */
    QueryResult callTable(List<String> nameParts, List<ScriptValue> arguments);

    /** Rolls back a transaction the script opened and did not finish. */
    void rollbackOpenTransaction();
}
