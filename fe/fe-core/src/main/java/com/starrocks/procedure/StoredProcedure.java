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

package com.starrocks.procedure;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.io.Writable;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A Snowflake style stored procedure written in Snowflake Scripting. It is kept in its database and persisted with
 * it, see {@link StoredProcedureMgr}.
 */
public class StoredProcedure implements Writable {
    public static final String EXECUTE_AS_OWNER = "OWNER";
    public static final String EXECUTE_AS_CALLER = "CALLER";

    /** A declared argument; the type and the default are SQL text. */
    public static class Parameter {
        @SerializedName("n")
        private String name;
        @SerializedName("t")
        private String typeSql;
        @SerializedName("d")
        private String defaultSql;

        public Parameter(String name, String typeSql, String defaultSql) {
            this.name = name;
            this.typeSql = typeSql;
            this.defaultSql = defaultSql;
        }

        public String getName() {
            return name;
        }

        public String getTypeSql() {
            return typeSql;
        }

        public String getDefaultSql() {
            return defaultSql;
        }
    }

    @SerializedName("id")
    private long id;
    @SerializedName("dbId")
    private long dbId;
    @SerializedName("name")
    private String name;
    @SerializedName("params")
    private List<Parameter> parameters = new ArrayList<>();
    @SerializedName("ret")
    private String returnTypeSql;
    @SerializedName("retNotNull")
    private boolean returnNotNull;
    // RETURNS TABLE (...): its columns (a column-less TABLE () takes those of the returned result); null for a scalar
    @SerializedName("retTable")
    private List<Parameter> returnColumns;
    @SerializedName("lang")
    private String language;
    @SerializedName("execAs")
    private String executeAs;
    @SerializedName("owner")
    private UserIdentity owner;
    @SerializedName("body")
    private String body;
    @SerializedName("comment")
    private String comment;
    @SerializedName("createTime")
    private long createTime;

    public StoredProcedure() {
    }

    public StoredProcedure(long id, long dbId, String name, List<Parameter> parameters, String returnTypeSql,
                           boolean returnNotNull, String language, String executeAs, UserIdentity owner, String body,
                           String comment, long createTime) {
        this.id = id;
        this.dbId = dbId;
        this.name = name;
        this.parameters = parameters;
        this.returnTypeSql = returnTypeSql;
        this.returnNotNull = returnNotNull;
        this.language = language;
        this.executeAs = executeAs;
        this.owner = owner;
        this.body = body;
        this.comment = comment;
        this.createTime = createTime;
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

    public List<Parameter> getParameters() {
        return parameters;
    }

    public String getReturnTypeSql() {
        return returnTypeSql;
    }

    public boolean isReturnNotNull() {
        return returnNotNull;
    }

    public boolean returnsTable() {
        return returnColumns != null;
    }

    public List<Parameter> getReturnColumns() {
        return returnColumns;
    }

    public void setReturnColumns(List<Parameter> returnColumns) {
        this.returnColumns = returnColumns;
    }

    /** The return type as SHOW and DESC print it: {@code INT} or {@code TABLE (id INT, name VARCHAR)}. */
    public String getReturnDisplay() {
        if (!returnsTable()) {
            return displayType(returnTypeSql);
        }
        return "TABLE (" + returnColumns.stream().map(c -> c.getName() + " " + displayType(c.getTypeSql()))
                .collect(Collectors.joining(", ")) + ")";
    }

    public String getLanguage() {
        return language;
    }

    public String getExecuteAs() {
        return executeAs;
    }

    public boolean isExecuteAsOwner() {
        return !EXECUTE_AS_CALLER.equals(executeAs);
    }

    public UserIdentity getOwner() {
        return owner;
    }

    public String getBody() {
        return body;
    }

    public String getComment() {
        return comment;
    }

    private static final Pattern INTEGER_DISPLAY_WIDTH =
            Pattern.compile("(?i)\\b(tinyint|smallint|int|bigint|largeint)\\(\\d+\\)");

    public long getCreateTime() {
        return createTime;
    }

    public List<String> getArgumentTypes() {
        return parameters.stream().map(Parameter::getTypeSql).collect(Collectors.toList());
    }

    /** {@code name(type, type)}. */
    public String getSignature() {
        return name + "(" + getArgumentTypes().stream().map(StoredProcedure::displayType)
                .collect(Collectors.joining(", ")) + ")";
    }

    /** {@code (name type [DEFAULT expr], ...)}, as written in CREATE PROCEDURE. */
    public String getArgumentsSql() {
        return "(" + parameters.stream()
                .map(p -> p.getName() + " " + displayType(p.getTypeSql()) + (p.getDefaultSql() == null ? "" : " DEFAULT "
                        + p.getDefaultSql()))
                .collect(Collectors.joining(", ")) + ")";
    }

    /** A type as users write it: upper case, without the display width of integer types. */
    public static String displayType(String typeSql) {
        if (typeSql == null) {
            return null;
        }
        return INTEGER_DISPLAY_WIDTH.matcher(typeSql).replaceAll("$1").toUpperCase(Locale.ROOT);
    }

    /** The key a procedure is stored under in its database: names are case-insensitive. */
    public static String key(String name) {
        return name.toLowerCase(Locale.ROOT);
    }
}
