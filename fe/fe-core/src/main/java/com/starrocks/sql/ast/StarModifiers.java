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
import com.starrocks.sql.ast.expression.ExprUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The Snowflake modifiers of a {@code [name.]*} select item besides EXCLUDE:
 * {@code ILIKE 'pattern'} keeps the columns whose name matches the pattern ignoring case,
 * {@code REPLACE (expr AS col, ...)} outputs an expression in place of a column under the column's name, and
 * {@code RENAME (col AS new_name, ...)} outputs a column under another name.
 */
public final class StarModifiers {
    public static final StarModifiers NONE = new StarModifiers(null, Collections.emptyMap(), Collections.emptyMap());

    private final String ilikePattern;
    // column name -> expression, in the written order
    private final Map<String, Expr> replacements;
    // column name -> new name, in the written order
    private final Map<String, String> renames;
    private Pattern ilikeRegex;

    public StarModifiers(String ilikePattern, Map<String, Expr> replacements, Map<String, String> renames) {
        this.ilikePattern = ilikePattern;
        this.replacements = Collections.unmodifiableMap(new LinkedHashMap<>(replacements));
        this.renames = Collections.unmodifiableMap(new LinkedHashMap<>(renames));
    }

    public boolean isEmpty() {
        return ilikePattern == null && replacements.isEmpty() && renames.isEmpty();
    }

    public String getIlikePattern() {
        return ilikePattern;
    }

    public Map<String, Expr> getReplacements() {
        return replacements;
    }

    public Map<String, String> getRenames() {
        return renames;
    }

    /**
     * Whether a column name matches the ILIKE pattern ignoring case: {@code %} matches any characters,
     * {@code _} one character and a backslash escapes the next character. Without a pattern every name matches.
     */
    public boolean matchesIlike(String name) {
        if (ilikePattern == null) {
            return true;
        }
        if (ilikeRegex == null) {
            ilikeRegex = Pattern.compile(likeToRegex(ilikePattern), Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE
                    | Pattern.DOTALL);
        }
        return ilikeRegex.matcher(name).matches();
    }

    static String likeToRegex(String pattern) {
        StringBuilder regex = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '\\' && i + 1 < pattern.length()) {
                regex.append(Pattern.quote(String.valueOf(pattern.charAt(++i))));
            } else if (c == '%') {
                regex.append(".*");
            } else if (c == '_') {
                regex.append('.');
            } else {
                regex.append(Pattern.quote(String.valueOf(c)));
            }
        }
        return regex.toString();
    }

    StarModifiers copy() {
        if (replacements.isEmpty()) {
            return this;
        }
        Map<String, Expr> copied = new LinkedHashMap<>();
        replacements.forEach((name, expr) -> copied.put(name, ExprUtils.reset(expr.clone())));
        return new StarModifiers(ilikePattern, copied, renames);
    }
}
