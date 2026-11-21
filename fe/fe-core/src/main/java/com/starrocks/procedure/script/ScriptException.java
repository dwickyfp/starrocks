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
 * An error raised while a script runs, as Snowflake Scripting reports it: an exception name that handlers match
 * ({@code STATEMENT_ERROR}, {@code EXPRESSION_ERROR} or a declared exception), a SQLCODE and a message.
 */
public class ScriptException extends RuntimeException {
    public static final String STATEMENT_ERROR = "STATEMENT_ERROR";
    public static final String EXPRESSION_ERROR = "EXPRESSION_ERROR";

    // Default SQLCODE of a declared exception raised without its own code (as in Snowflake).
    public static final int USER_EXCEPTION_CODE = -20000;
    public static final int STATEMENT_ERROR_CODE = 100183;
    public static final int EXPRESSION_ERROR_CODE = 100069;

    private final String exceptionName;
    private final int sqlCode;
    private final int line;

    public ScriptException(String exceptionName, int sqlCode, String message, int line) {
        super(message);
        this.exceptionName = exceptionName;
        this.sqlCode = sqlCode;
        this.line = line;
    }

    public String getExceptionName() {
        return exceptionName;
    }

    public int getSqlCode() {
        return sqlCode;
    }

    public int getLine() {
        return line;
    }

    /** The message with the line it came from, for errors that leave the script. */
    public String describe() {
        String where = line > 0 ? " at line " + line : "";
        return "Uncaught exception of type '" + exceptionName + "'" + where + ": " + getMessage();
    }
}
