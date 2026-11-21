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

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * A value of a script variable: its SQL type (null when unknown) and its text, null for SQL NULL. The interpreter
 * binds a value into SQL as a literal of its type.
 */
public final class ScriptValue {
    private static final Pattern NUMBER = Pattern.compile("[-+]?(\\d+(\\.\\d*)?|\\.\\d+)([eE][-+]?\\d+)?");

    private static final Pattern DECIMAL_VARIANT = Pattern.compile("(?i)^decimal(32|64|128|256|v2)");

    private final String type;
    private final String text;

    public ScriptValue(String type, String text) {
        this.type = normalizeType(type);
        this.text = text;
    }

    public static ScriptValue nullOf(String type) {
        return new ScriptValue(type, null);
    }

    public String getType() {
        return type;
    }

    /** The value as text, null for SQL NULL. */
    public String getText() {
        return text;
    }

    public boolean isNull() {
        return text == null;
    }

    /** Whether the value is a true boolean; NULL is not true. */
    public boolean isTrue() {
        return text != null && (text.equalsIgnoreCase("true") || text.equals("1"));
    }

    /** The upper-case type name without its parameters, for example DECIMAL for DECIMAL(10, 2). */
    public static String baseType(String type) {
        if (type == null) {
            return null;
        }
        int paren = type.indexOf('(');
        return (paren < 0 ? type : type.substring(0, paren)).trim().toUpperCase(Locale.ROOT);
    }

    /** The value as a SQL literal of its type. */
    public String toSqlLiteral() {
        String base = baseType(type);
        if (text == null) {
            return type == null ? "NULL" : "CAST(NULL AS " + type + ")";
        }
        if (base == null) {
            return quote(text);
        }
        switch (base) {
            case "BOOLEAN":
                return isTrue() ? "TRUE" : "FALSE";
            case "TINYINT":
            case "SMALLINT":
            case "INT":
            case "INTEGER":
            case "BIGINT":
            case "LARGEINT":
            case "FLOAT":
            case "DOUBLE":
            case "DECIMAL":
            case "DECIMALV2":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
            case "DECIMAL256":
            case "NUMERIC":
                if (NUMBER.matcher(text).matches()) {
                    // parenthesized, so a negative value does not turn "a-" + "-1" into a comment
                    return text.startsWith("-") ? "(" + text + ")" : text;
                }
                return "CAST(" + quote(text) + " AS " + type + ")";
            case "VARCHAR":
            case "CHAR":
            case "STRING":
            case "TEXT":
                return quote(text);
            default:
                return "CAST(" + quote(text) + " AS " + type + ")";
        }
    }

    public static String quote(String text) {
        return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }

    /** The type as CAST takes it; null for the NULL type. */
    public static String normalizeType(String type) {
        if (type == null) {
            return null;
        }
        String base = baseType(type);
        // NULL literals have no real type; leave them untyped
        if (base.isEmpty() || base.equals("NULL_TYPE") || base.equals("NULL")) {
            return null;
        }
        // DECIMAL32(p, s) and friends are internal names; CAST only takes DECIMAL(p, s)
        return DECIMAL_VARIANT.matcher(type.trim()).replaceFirst("DECIMAL");
    }

    @Override
    public String toString() {
        return text == null ? "NULL" : text;
    }
}
