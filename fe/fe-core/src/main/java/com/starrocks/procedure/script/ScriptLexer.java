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

import java.util.ArrayList;
import java.util.List;

/**
 * Splits a Snowflake Scripting block into tokens. Comments ({@code --}, {@code //} and block comments) are dropped,
 * strings and quoted identifiers stay whole.
 */
public final class ScriptLexer {
    private static final String[] MULTI_CHAR_SYMBOLS = {":=", "<=>", "<=", ">=", "<>", "!=", "||", "&&", "::", "=>"};

    private final String source;
    private int pos = 0;
    private int line = 1;

    private ScriptLexer(String source) {
        this.source = source;
    }

    public static List<ScriptToken> tokenize(String source) {
        return new ScriptLexer(source).run();
    }

    private List<ScriptToken> run() {
        List<ScriptToken> tokens = new ArrayList<>();
        while (true) {
            skipSpaceAndComments();
            if (pos >= source.length()) {
                tokens.add(new ScriptToken(ScriptToken.Kind.EOF, "", pos, pos, line));
                return tokens;
            }
            tokens.add(next());
        }
    }

    private void skipSpaceAndComments() {
        while (pos < source.length()) {
            char c = source.charAt(pos);
            if (c == '\n') {
                line++;
                pos++;
            } else if (Character.isWhitespace(c)) {
                pos++;
            } else if (source.startsWith("--", pos) || source.startsWith("//", pos)) {
                while (pos < source.length() && source.charAt(pos) != '\n') {
                    pos++;
                }
            } else if (source.startsWith("/*", pos)) {
                int end = source.indexOf("*/", pos + 2);
                int stop = end < 0 ? source.length() : end + 2;
                countLines(pos, stop);
                pos = stop;
            } else {
                return;
            }
        }
    }

    private ScriptToken next() {
        int start = pos;
        int startLine = line;
        char c = source.charAt(pos);
        if (c == '\'') {
            pos = endOfQuoted(pos, '\'');
            String raw = source.substring(start, pos);
            String value = raw.substring(1, Math.max(1, raw.length() - 1)).replace("''", "'").replace("\\'", "'");
            countLines(start, pos);
            return new ScriptToken(ScriptToken.Kind.STRING, value, start, pos, startLine);
        }
        if (source.startsWith("$$", pos)) {
            int end = source.indexOf("$$", pos + 2);
            if (end < 0) {
                throw new ScriptException(ScriptException.STATEMENT_ERROR, ScriptException.STATEMENT_ERROR_CODE,
                        "Unterminated $$ string", startLine);
            }
            pos = end + 2;
            countLines(start, pos);
            return new ScriptToken(ScriptToken.Kind.STRING, source.substring(start + 2, end), start, pos, startLine);
        }
        if (c == '"' || c == '`') {
            pos = endOfQuoted(pos, c);
            String raw = source.substring(start, pos);
            String doubled = String.valueOf(c) + c;
            String value = raw.substring(1, Math.max(1, raw.length() - 1)).replace(doubled, String.valueOf(c));
            return new ScriptToken(ScriptToken.Kind.QUOTED_IDENT, value, start, pos, startLine);
        }
        if (Character.isDigit(c) || (c == '.' && pos + 1 < source.length() && Character.isDigit(source.charAt(pos + 1)))) {
            while (pos < source.length() && (Character.isLetterOrDigit(source.charAt(pos)) || source.charAt(pos) == '.'
                    || ((source.charAt(pos) == '+' || source.charAt(pos) == '-')
                    && (source.charAt(pos - 1) == 'e' || source.charAt(pos - 1) == 'E')))) {
                pos++;
            }
            return new ScriptToken(ScriptToken.Kind.NUMBER, source.substring(start, pos), start, pos, startLine);
        }
        if (Character.isLetter(c) || c == '_' || c == '$' || c == '@') {
            while (pos < source.length() && (Character.isLetterOrDigit(source.charAt(pos))
                    || source.charAt(pos) == '_' || source.charAt(pos) == '$' || source.charAt(pos) == '@')) {
                pos++;
            }
            return new ScriptToken(ScriptToken.Kind.WORD, source.substring(start, pos), start, pos, startLine);
        }
        for (String symbol : MULTI_CHAR_SYMBOLS) {
            if (source.startsWith(symbol, pos)) {
                pos += symbol.length();
                return new ScriptToken(ScriptToken.Kind.SYMBOL, symbol, start, pos, startLine);
            }
        }
        pos++;
        return new ScriptToken(ScriptToken.Kind.SYMBOL, String.valueOf(c), start, pos, startLine);
    }

    /** The offset just past a quoted run that starts at {@code from}; a doubled quote or a backslash escapes. */
    private int endOfQuoted(int from, char quote) {
        int i = from + 1;
        while (i < source.length()) {
            char c = source.charAt(i);
            if (c == '\\' && quote == '\'') {
                i += 2;
                continue;
            }
            if (c == quote) {
                if (i + 1 < source.length() && source.charAt(i + 1) == quote) {
                    i += 2;
                    continue;
                }
                return i + 1;
            }
            i++;
        }
        throw new ScriptException(ScriptException.STATEMENT_ERROR, ScriptException.STATEMENT_ERROR_CODE,
                "Unterminated quoted text starting with " + quote, line);
    }

    private void countLines(int from, int to) {
        for (int i = from; i < to; i++) {
            if (source.charAt(i) == '\n') {
                line++;
            }
        }
    }
}
