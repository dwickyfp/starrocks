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

package com.starrocks.sql.ast;

/**
 * The CHANGES clause of a table relation: the changes of the table between a start and an end point, as a stream
 * returns them. The start is a time travel point ({@code AT} or {@code BEFORE}) or the offset of a stream
 * ({@code AT(STREAM => 'name')}); the end is a time travel point, or now when there is none.
 *
 * @param appendOnly {@code INFORMATION => APPEND_ONLY}: the inserted rows only
 * @param start      the start point, or null with a stream
 * @param stream     the stream whose offset is the start point, or null
 * @param end        the end point, or null for now
 * @param sql        the text of the clause, to print the relation
 */
public record TableChanges(boolean appendOnly, QueryPeriod start, String stream, QueryPeriod end, String sql) {
}
