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

import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class MergeWhenMatchedUpdateClause extends MergeWhenClause {
    private final List<ColumnAssignment> assignments;
    private final boolean star;

    public MergeWhenMatchedUpdateClause(Expr optionalCondition,
                                        List<ColumnAssignment> assignments,
                                        NodePosition pos) {
        this(optionalCondition, assignments, false, false, pos);
    }

    /**
     * @param star true for {@code UPDATE SET *}, which sets every column of the target to the source column of
     *             the same name; {@code assignments} is then empty.
     */
    public MergeWhenMatchedUpdateClause(Expr optionalCondition, List<ColumnAssignment> assignments, boolean star,
                                        boolean notMatchedBySource, NodePosition pos) {
        super(optionalCondition, notMatchedBySource, pos);
        this.assignments = assignments;
        this.star = star;
    }

    public boolean isStar() {
        return star;
    }

    public List<ColumnAssignment> getAssignments() {
        return assignments;
    }
}
