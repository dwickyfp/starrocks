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

public abstract class MergeWhenClause implements ParseNode {
    private final Expr optionalCondition;
    private final NodePosition pos;
    private final boolean notMatchedBySource;

    protected MergeWhenClause(Expr optionalCondition, NodePosition pos) {
        this(optionalCondition, false, pos);
    }

    /**
     * @param notMatchedBySource true for {@code WHEN NOT MATCHED BY SOURCE}, which acts on target rows that no
     *                           source row matches. Only an UPDATE or a DELETE clause can be one.
     */
    protected MergeWhenClause(Expr optionalCondition, boolean notMatchedBySource, NodePosition pos) {
        this.optionalCondition = optionalCondition;
        this.notMatchedBySource = notMatchedBySource;
        this.pos = pos;
    }

    public Expr getOptionalCondition() {
        return optionalCondition;
    }

    /**
     * True for a {@code WHEN MATCHED} clause.
     */
    public boolean isMatched() {
        return !notMatchedBySource && (this instanceof MergeWhenMatchedUpdateClause
                || this instanceof MergeWhenMatchedDeleteClause);
    }

    /**
     * True for a {@code WHEN NOT MATCHED BY SOURCE} clause.
     */
    public boolean isNotMatchedBySource() {
        return notMatchedBySource;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
