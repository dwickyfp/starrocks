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

/**
 * A token of a Snowflake Scripting block. Tokens keep their offsets in the source so a SQL statement inside the
 * block can be cut out as it was written.
 */
public final class ScriptToken {
    public enum Kind {
        WORD,          // identifier or keyword, unquoted
        QUOTED_IDENT,  // "name" or `name`
        STRING,        // 'text' or $$text$$
        NUMBER,
        SYMBOL,        // operators and punctuation, ';' included
        EOF
    }

    private final Kind kind;
    private final String text;
    private final int start;
    private final int end;
    private final int line;

    public ScriptToken(Kind kind, String text, int start, int end, int line) {
        this.kind = kind;
        this.text = text;
        this.start = start;
        this.end = end;
        this.line = line;
    }

    public Kind getKind() {
        return kind;
    }

    public String getText() {
        return text;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    public int getLine() {
        return line;
    }

    /** Whether this is the unquoted word {@code keyword}, ignoring case. */
    public boolean is(String keyword) {
        return kind == Kind.WORD && text.equalsIgnoreCase(keyword);
    }

    public boolean isSymbol(String symbol) {
        return kind == Kind.SYMBOL && text.equals(symbol);
    }

    @Override
    public String toString() {
        return kind == Kind.EOF ? "end of script" : "'" + text + "'";
    }
}
