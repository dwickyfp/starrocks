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
import com.starrocks.sql.ast.policy.SecurityPolicyStmt;

import java.util.ArrayList;
import java.util.List;

/**
 * A Snowflake style masking or row access policy: a function of the columns it is given, written as an expression
 * over its arguments. A masking policy returns the value a query reads in place of a column; a row access policy
 * returns whether a query reads a row. Kept in its database and persisted with it, see {@link SecurityPolicyMgr}.
 * Immutable: a change replaces it.
 */
public class SecurityPolicy implements Writable {
    @SerializedName("id")
    private long id;
    @SerializedName("dbId")
    private long dbId;
    @SerializedName("name")
    private String name;
    // SecurityPolicyStmt.Kind
    @SerializedName("kind")
    private String kind;
    @SerializedName("argNames")
    private List<String> argNames = new ArrayList<>();
    // the SQL of the types of the arguments
    @SerializedName("argTypes")
    private List<String> argTypes = new ArrayList<>();
    @SerializedName("returnType")
    private String returnType;
    // the SQL text of the expression
    @SerializedName("body")
    private String body;
    @SerializedName("comment")
    private String comment;
    @SerializedName("owner")
    private String owner;
    @SerializedName("createTime")
    private long createTime;

    public SecurityPolicy(long id, long dbId, String name, SecurityPolicyStmt.Kind kind, List<String> argNames,
                          List<String> argTypes, String returnType, String body, String comment, String owner,
                          long createTime) {
        this.id = id;
        this.dbId = dbId;
        this.name = name;
        this.kind = kind.name();
        this.argNames = new ArrayList<>(argNames);
        this.argTypes = new ArrayList<>(argTypes);
        this.returnType = returnType;
        this.body = body;
        this.comment = comment;
        this.owner = owner;
        this.createTime = createTime;
    }

    /** This policy with another name, body or comment. */
    public SecurityPolicy with(String newName, String newBody, String newComment) {
        return new SecurityPolicy(id, dbId, newName, getKind(), argNames, argTypes, returnType, newBody, newComment,
                owner, createTime);
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

    public SecurityPolicyStmt.Kind getKind() {
        return SecurityPolicyStmt.Kind.valueOf(kind);
    }

    public List<String> getArgNames() {
        return argNames;
    }

    public List<String> getArgTypes() {
        return argTypes;
    }

    public String getReturnType() {
        return returnType;
    }

    public String getBody() {
        return body;
    }

    public String getComment() {
        return comment;
    }

    public String getOwner() {
        return owner;
    }

    public long getCreateTime() {
        return createTime;
    }

    /** {@code (arg type, ...)}. */
    public String signature() {
        List<String> args = new ArrayList<>();
        for (int i = 0; i < argNames.size(); i++) {
            args.add(argNames.get(i) + " " + argTypes.get(i));
        }
        return "(" + String.join(", ", args) + ")";
    }
}
