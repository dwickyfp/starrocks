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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.procedure.script.ScriptException;
import com.starrocks.procedure.script.ScriptInterpreter;
import com.starrocks.procedure.script.ScriptNode;
import com.starrocks.procedure.script.ScriptParser;
import com.starrocks.procedure.script.ScriptValue;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CallProcedureStatement;
import com.starrocks.sql.ast.ProcedureArgument;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.procedure.CreateProcedureStmt;
import com.starrocks.sql.ast.procedure.DescProcedureStmt;
import com.starrocks.sql.ast.procedure.DropProcedureStmt;
import com.starrocks.sql.ast.procedure.ExecuteImmediateStmt;
import com.starrocks.sql.ast.procedure.ShowProceduresStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.type.TypeFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Executes the stored procedure statements: CREATE, DROP, SHOW, DESC, CALL and EXECUTE IMMEDIATE.
 *
 * <p>A CALL runs in a session of its own whose budget is what is left of the caller's query_timeout. A procedure
 * declared EXECUTE AS OWNER (the default, as in Snowflake) runs as its owner, in its own database; EXECUTE AS
 * CALLER runs as the caller, in the caller's database. Calls may nest up to {@link #MAX_CALL_DEPTH} levels.
 */
public final class ProcedureExecutor {
    private static final Logger LOG = LogManager.getLogger(ProcedureExecutor.class);

    public static final int MAX_CALL_DEPTH = 16;

    public static final ShowResultSetMetaData SHOW_META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("NAME", TypeFactory.createVarcharType(256)))
            .addColumn(new Column("DATABASE", TypeFactory.createVarcharType(256)))
            .addColumn(new Column("ARGUMENTS", TypeFactory.createVarcharType(1024)))
            .addColumn(new Column("RETURNS", TypeFactory.createVarcharType(128)))
            .addColumn(new Column("LANGUAGE", TypeFactory.createVarcharType(32)))
            .addColumn(new Column("EXECUTE_AS", TypeFactory.createVarcharType(16)))
            .addColumn(new Column("OWNER", TypeFactory.createVarcharType(256)))
            .addColumn(new Column("COMMENT", TypeFactory.createVarcharType(1024)))
            .addColumn(new Column("CREATED_ON", TypeFactory.createVarcharType(32)))
            .build();

    public static final ShowResultSetMetaData DESC_META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("PROPERTY", TypeFactory.createVarcharType(64)))
            .addColumn(new Column("VALUE", TypeFactory.createVarcharType(65533)))
            .build();

    private ProcedureExecutor() {
    }

    // ------------------------------------------------ DDL ---------------------------------------------------------

    public static void create(CreateProcedureStmt stmt, ConnectContext ctx) throws DdlException {
        if (!"SQL".equalsIgnoreCase(stmt.getLanguage())) {
            throw new DdlException("Unsupported procedure language " + stmt.getLanguage() + ", only SQL is supported");
        }
        Database db = database(stmt.getName(), ctx);
        String name = procedureName(stmt.getName());
        Set<String> names = new HashSet<>();
        List<StoredProcedure.Parameter> parameters = new ArrayList<>();
        for (CreateProcedureStmt.Parameter p : stmt.getParameters()) {
            if (!names.add(p.getName().toLowerCase(Locale.ROOT))) {
                throw new DdlException("Duplicate argument " + p.getName());
            }
            if (p.getDefaultSql() != null) {
                try {
                    SqlParser.parseSqlToExpr(p.getDefaultSql(), ctx.getSessionVariable().getSqlMode());
                } catch (Exception e) {
                    throw new DdlException("Invalid default of argument " + p.getName() + ": " + e.getMessage());
                }
            } else if (!parameters.isEmpty() && parameters.get(parameters.size() - 1).getDefaultSql() != null) {
                throw new DdlException("Argument " + p.getName() + " needs a DEFAULT: it follows an argument "
                        + "with one");
            }
            parameters.add(new StoredProcedure.Parameter(p.getName(), ScriptValue.normalizeType(p.getTypeSql()),
                    p.getDefaultSql()));
        }
        try {
            ScriptParser.parse(stmt.getBody());
        } catch (ScriptException e) {
            throw new DdlException("Invalid procedure body: " + e.getMessage()
                    + (e.getLine() > 0 ? " (line " + e.getLine() + ")" : ""));
        }
        StoredProcedure existing = StoredProcedureMgr.get(db, name);
        if (existing != null && stmt.isOrReplace()) {
            checkOwnerOrOperate(existing, ctx);
        }
        StoredProcedure procedure = new StoredProcedure(GlobalStateMgr.getCurrentState().getNextId(), db.getId(),
                name, parameters, ScriptValue.normalizeType(stmt.getReturnTypeSql()), stmt.isReturnNotNull(), "SQL",
                stmt.isExecuteAsCaller() ? StoredProcedure.EXECUTE_AS_CALLER : StoredProcedure.EXECUTE_AS_OWNER,
                ctx.getCurrentUserIdentity(), stmt.getBody(), stmt.getComment(), System.currentTimeMillis());
        StoredProcedureMgr.create(db, procedure, stmt.isOrReplace(), stmt.isIfNotExists());
    }

    public static void drop(DropProcedureStmt stmt, ConnectContext ctx) throws DdlException {
        Database db = database(stmt.getName(), ctx);
        String name = procedureName(stmt.getName());
        StoredProcedure procedure = StoredProcedureMgr.get(db, name);
        if (procedure == null || !matchesTypes(procedure, stmt.getArgTypeSqls())) {
            if (stmt.isIfExists()) {
                return;
            }
            throw new DdlException("Procedure " + name + signatureSuffix(stmt.getArgTypeSqls())
                    + " does not exist in database " + db.getFullName());
        }
        checkOwnerOrOperate(procedure, ctx);
        StoredProcedureMgr.drop(db, name);
    }

    public static ShowResultSet show(ShowProceduresStmt stmt, ConnectContext ctx) {
        String dbName = stmt.getDbName() != null ? stmt.getDbName() : ctx.getDatabase();
        if (dbName == null || dbName.isEmpty()) {
            throw new SemanticException("No database selected");
        }
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new SemanticException("Unknown database '" + dbName + "'");
        }
        PatternMatcher matcher = stmt.getPattern() == null ? null :
                PatternMatcher.createMysqlPattern(stmt.getPattern(), false);
        List<List<String>> rows = new ArrayList<>();
        for (StoredProcedure p : StoredProcedureMgr.list(db)) {
            if ((matcher == null || matcher.match(p.getName())) && canSee(p, db, ctx)) {
                rows.add(Lists.newArrayList(p.getName(), db.getFullName(),
                        p.getName() + p.getArgumentsSql() + " RETURN " + StoredProcedure.displayType(p.getReturnTypeSql()),
                        StoredProcedure.displayType(p.getReturnTypeSql()),
                        p.getLanguage(), p.getExecuteAs(), owner(p), p.getComment() == null ? "" : p.getComment(),
                        TimeUtils.longToTimeString(p.getCreateTime())));
            }
        }
        return new ShowResultSet(SHOW_META_DATA, rows);
    }

    public static ShowResultSet desc(DescProcedureStmt stmt, ConnectContext ctx) {
        Database db;
        try {
            db = database(stmt.getName(), ctx);
        } catch (DdlException e) {
            throw new SemanticException(e.getMessage());
        }
        StoredProcedure p = StoredProcedureMgr.get(db, procedureName(stmt.getName()));
        if (p == null || !matchesTypes(p, stmt.getArgTypeSqls()) || !canSee(p, db, ctx)) {
            throw new SemanticException("Procedure " + procedureName(stmt.getName())
                    + signatureSuffix(stmt.getArgTypeSqls()) + " does not exist");
        }
        List<List<String>> rows = new ArrayList<>();
        rows.add(Lists.newArrayList("signature", p.getArgumentsSql()));
        rows.add(Lists.newArrayList("returns",
                StoredProcedure.displayType(p.getReturnTypeSql()) + (p.isReturnNotNull() ? " NOT NULL" : "")));
        rows.add(Lists.newArrayList("language", p.getLanguage()));
        rows.add(Lists.newArrayList("execute as", p.getExecuteAs()));
        rows.add(Lists.newArrayList("owner", owner(p)));
        rows.add(Lists.newArrayList("database", db.getFullName()));
        rows.add(Lists.newArrayList("comment", p.getComment() == null ? "" : p.getComment()));
        rows.add(Lists.newArrayList("created_on", TimeUtils.longToTimeString(p.getCreateTime())));
        rows.add(Lists.newArrayList("body", p.getBody()));
        return new ShowResultSet(DESC_META_DATA, rows);
    }

    // ------------------------------------------------ CALL --------------------------------------------------------

    /** The stored procedure {@code nameParts} names, or null when there is none (it may be a connector procedure). */
    public static StoredProcedure resolve(List<String> nameParts, ConnectContext ctx) {
        String dbName;
        String name;
        if (nameParts.size() == 1) {
            if (!InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME.equals(ctx.getCurrentCatalog())) {
                return null;
            }
            dbName = ctx.getDatabase();
            name = nameParts.get(0);
        } else if (nameParts.size() == 2) {
            if (!InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME.equals(ctx.getCurrentCatalog())) {
                return null;
            }
            dbName = nameParts.get(0);
            name = nameParts.get(1);
        } else if (nameParts.size() == 3) {
            if (!InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME.equalsIgnoreCase(nameParts.get(0))) {
                return null;
            }
            dbName = nameParts.get(1);
            name = nameParts.get(2);
        } else {
            return null;
        }
        if (dbName == null || dbName.isEmpty()) {
            return null;
        }
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        return db == null ? null : StoredProcedureMgr.get(db, name);
    }

    /** {@code CALL p(args)} of a stored procedure: one row, one column named after the procedure. */
    public static ShowResultSet call(CallProcedureStatement stmt, ConnectContext ctx) {
        StoredProcedure procedure = stmt.getStoredProcedure();
        List<String> argumentSqls = new ArrayList<>();
        List<String> argumentNames = new ArrayList<>();
        for (ProcedureArgument argument : stmt.getArguments()) {
            argumentSqls.add(AstToSQLBuilder.toSQL(argument.getValue()));
            argumentNames.add(argument.getName().orElse(null));
        }
        boolean named = argumentNames.stream().anyMatch(n -> n != null);
        ScriptValue result;
        try {
            result = invoke(procedure, argumentSqls, named ? argumentNames : null, ctx, 1);
        } catch (ScriptException e) {
            throw new SemanticException(e.describe());
        } finally {
            ctx.setThreadLocalInfo();
        }
        ShowResultSetMetaData meta = ShowResultSetMetaData.builder()
                .addColumn(new Column(procedure.getName(), TypeFactory.createVarcharType(65533)))
                .build();
        List<String> row = new ArrayList<>();
        row.add(result.getText());
        return new ShowResultSet(meta, ImmutableList.of(row));
    }

    /** {@code EXECUTE IMMEDIATE}: runs an anonymous block as the current user. */
    public static ShowResultSet executeImmediate(ExecuteImmediateStmt stmt, ConnectContext caller) {
        ScriptNode.Block block;
        try {
            block = ScriptParser.parse(stmt.getScript());
        } catch (ScriptException e) {
            throw new SemanticException(e.describe());
        }
        ConnectContext ctx = newSession(caller, "anonymous block");
        ScriptValue result;
        try {
            ctx.setThreadLocalInfo();
            StarRocksScriptSession session = new StarRocksScriptSession(ctx, "anonymous block", 1);
            result = new ScriptInterpreter(session).run(block, new LinkedHashMap<>());
        } catch (ScriptException e) {
            throw new SemanticException(e.describe());
        } finally {
            caller.setThreadLocalInfo();
        }
        ShowResultSetMetaData meta = ShowResultSetMetaData.builder()
                .addColumn(new Column("anonymous block", TypeFactory.createVarcharType(65533)))
                .build();
        List<String> row = new ArrayList<>();
        row.add(result.getText());
        return new ShowResultSet(meta, ImmutableList.of(row));
    }

    /**
     * Runs {@code procedure} with arguments given as SQL ({@code argumentNames} null for positional arguments) and
     * returns its value cast to its return type. Errors leave as {@link ScriptException}, so a calling script can
     * catch them.
     */
    static ScriptValue invoke(StoredProcedure procedure, List<String> argumentSqls, List<String> argumentNames,
                              ConnectContext caller, int depth) {
        if (depth > MAX_CALL_DEPTH) {
            throw new ScriptException(ScriptException.STATEMENT_ERROR, ScriptException.STATEMENT_ERROR_CODE,
                    "Procedure calls nest deeper than " + MAX_CALL_DEPTH, 0);
        }
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(procedure.getDbId());
        if (db == null) {
            throw new ScriptException(ScriptException.STATEMENT_ERROR, ScriptException.STATEMENT_ERROR_CODE,
                    "The database of procedure " + procedure.getName() + " no longer exists", 0);
        }
        checkCall(procedure, db, caller);
        List<String> ordered = orderArguments(procedure, argumentSqls, argumentNames);
        ScriptNode.Block block = ScriptParser.parse(procedure.getBody());

        ConnectContext ctx = newSession(caller, procedure.getName());
        if (procedure.isExecuteAsOwner()) {
            runAsOwner(ctx, procedure, db);
        }
        ConnectContext previous = ConnectContext.get();
        long start = System.currentTimeMillis();
        try {
            ctx.setThreadLocalInfo();
            StarRocksScriptSession session = new StarRocksScriptSession(ctx, procedure.getName(), depth);
            Map<String, ScriptValue> parameters = new LinkedHashMap<>();
            for (int i = 0; i < procedure.getParameters().size(); i++) {
                StoredProcedure.Parameter p = procedure.getParameters().get(i);
                String sql = ordered.get(i) != null ? ordered.get(i) : p.getDefaultSql();
                parameters.put(p.getName(), castArgument(session, sql, p));
            }
            ScriptValue result = new ScriptInterpreter(session).run(block, parameters);
            if (!result.isNull()) {
                result = new ScriptValue(procedure.getReturnTypeSql(), session.evaluate(
                        "CAST(" + result.toSqlLiteral() + " AS " + procedure.getReturnTypeSql() + ")").getText());
            }
            if (result.isNull() && procedure.isReturnNotNull()) {
                throw new ScriptException(ScriptException.STATEMENT_ERROR, ScriptException.STATEMENT_ERROR_CODE,
                        "Procedure " + procedure.getName() + " returned NULL but is declared NOT NULL", 0);
            }
            LOG.info("Procedure {}.{} called by {} finished in {} ms", db.getFullName(), procedure.getName(),
                    caller.getQualifiedUser(), System.currentTimeMillis() - start);
            return result;
        } catch (ScriptException e) {
            LOG.info("Procedure {}.{} called by {} failed: {}", db.getFullName(), procedure.getName(),
                    caller.getQualifiedUser(), e.describe());
            throw e;
        } finally {
            if (previous != null) {
                previous.setThreadLocalInfo();
            } else {
                ConnectContext.remove();
            }
        }
    }

    private static ScriptValue castArgument(StarRocksScriptSession session, String sql, StoredProcedure.Parameter p) {
        try {
            ScriptValue value = session.evaluate("CAST((" + sql + ") AS " + p.getTypeSql() + ")");
            return new ScriptValue(p.getTypeSql(), value.getText());
        } catch (ScriptException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new ScriptException(ScriptException.EXPRESSION_ERROR, ScriptException.EXPRESSION_ERROR_CODE,
                    "Invalid value for argument " + p.getName() + ": " + e.getMessage(), 0);
        }
    }

    /** The argument SQL by parameter position; null where the default applies. */
    private static List<String> orderArguments(StoredProcedure procedure, List<String> argumentSqls,
                                               List<String> argumentNames) {
        List<StoredProcedure.Parameter> parameters = procedure.getParameters();
        List<String> ordered = new ArrayList<>();
        for (int i = 0; i < parameters.size(); i++) {
            ordered.add(null);
        }
        if (argumentSqls.size() > parameters.size()) {
            throw argumentError(procedure, "takes " + parameters.size() + " arguments, got " + argumentSqls.size());
        }
        for (int i = 0; i < argumentSqls.size(); i++) {
            int index = i;
            if (argumentNames != null) {
                String name = argumentNames.get(i);
                if (name == null) {
                    throw argumentError(procedure, "cannot mix named and positional arguments");
                }
                index = -1;
                for (int k = 0; k < parameters.size(); k++) {
                    if (parameters.get(k).getName().equalsIgnoreCase(name)) {
                        index = k;
                    }
                }
                if (index < 0) {
                    throw argumentError(procedure, "has no argument " + name);
                }
                if (ordered.get(index) != null) {
                    throw argumentError(procedure, "got argument " + name + " twice");
                }
            }
            ordered.set(index, argumentSqls.get(i));
        }
        for (int i = 0; i < parameters.size(); i++) {
            if (ordered.get(i) == null && parameters.get(i).getDefaultSql() == null) {
                throw argumentError(procedure, "is missing argument " + parameters.get(i).getName());
            }
        }
        return ordered;
    }

    private static ScriptException argumentError(StoredProcedure procedure, String message) {
        return new ScriptException(ScriptException.STATEMENT_ERROR, ScriptException.STATEMENT_ERROR_CODE,
                "Procedure " + procedure.getSignature() + " " + message, 0);
    }

    private static ConnectContext newSession(ConnectContext caller, String name) {
        int timeout = SimpleExecutor.remainingQueryTimeoutS(caller);
        if (timeout <= 0) {
            throw new ScriptException(ScriptException.STATEMENT_ERROR, ScriptException.STATEMENT_ERROR_CODE,
                    name + " has no query_timeout left to run", 0);
        }
        ConnectContext ctx = SimpleExecutor.createCallerQueryContext(caller, timeout);
        ctx.setCurrentCatalog(caller.getCurrentCatalog());
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        // Snowflake Scripting concatenates strings with ||
        ctx.getSessionVariable().setSqlMode(ctx.getSessionVariable().getSqlMode() | SqlModeHelper.MODE_PIPES_AS_CONCAT);
        return ctx;
    }

    /** Owner's rights: the owner's identity and roles, and the procedure's database. */
    private static void runAsOwner(ConnectContext ctx, StoredProcedure procedure, Database db) {
        UserIdentity owner = procedure.getOwner();
        ctx.setQualifiedUser(owner.getUser());
        ctx.setCurrentUserIdentity(owner);
        try {
            ctx.setCurrentRoleIds(GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdsByUser(owner));
        } catch (PrivilegeException e) {
            throw new ScriptException(ScriptException.STATEMENT_ERROR, ScriptException.STATEMENT_ERROR_CODE,
                    "Cannot run procedure " + procedure.getName() + " as its owner " + owner + ": "
                            + e.getMessage(), 0);
        }
        ctx.setCurrentCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        ctx.setDatabase(db.getFullName());
    }

    // ------------------------------------------------ privileges --------------------------------------------------

    /**
     * Calling needs a privilege on the procedure's database. An owner's rights procedure runs with the owner's
     * privileges, so until procedures can be granted only its owner and users with OPERATE may call it.
     */
    private static void checkCall(StoredProcedure procedure, Database db, ConnectContext ctx) {
        if (isOwner(procedure, ctx)) {
            return;
        }
        if (procedure.isExecuteAsOwner()) {
            try {
                Authorizer.checkSystemAction(ctx, PrivilegeType.OPERATE);
            } catch (AccessDeniedException e) {
                throw new ScriptException(ScriptException.STATEMENT_ERROR, ScriptException.STATEMENT_ERROR_CODE,
                        "Access denied: procedure " + procedure.getName() + " runs with its owner's rights; only "
                                + "its owner or a user with the OPERATE privilege can call it", 0);
            }
            return;
        }
        try {
            Authorizer.checkAnyActionOnOrInDb(ctx, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db.getFullName());
        } catch (AccessDeniedException e) {
            throw new ScriptException(ScriptException.STATEMENT_ERROR, ScriptException.STATEMENT_ERROR_CODE,
                    "Access denied: calling procedure " + procedure.getName() + " needs a privilege on database "
                            + db.getFullName(), 0);
        }
    }

    private static void checkOwnerOrOperate(StoredProcedure procedure, ConnectContext ctx) throws DdlException {
        if (isOwner(procedure, ctx)) {
            return;
        }
        try {
            Authorizer.checkSystemAction(ctx, PrivilegeType.OPERATE);
        } catch (AccessDeniedException e) {
            throw new DdlException("Access denied: only the owner of procedure " + procedure.getName()
                    + " or a user with the OPERATE privilege can change or drop it");
        }
    }

    private static boolean isOwner(StoredProcedure procedure, ConnectContext ctx) {
        UserIdentity current = ctx.getCurrentUserIdentity();
        return current != null && current.equals(procedure.getOwner());
    }

    private static boolean canSee(StoredProcedure procedure, Database db, ConnectContext ctx) {
        if (isOwner(procedure, ctx)) {
            return true;
        }
        try {
            Authorizer.checkAnyActionOnOrInDb(ctx, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db.getFullName());
            return true;
        } catch (AccessDeniedException e) {
            return false;
        }
    }

    // ------------------------------------------------ names -------------------------------------------------------

    private static Database database(QualifiedName name, ConnectContext ctx) throws DdlException {
        List<String> parts = name.getParts();
        String dbName;
        if (parts.size() == 1) {
            dbName = ctx.getDatabase();
        } else if (parts.size() == 2) {
            dbName = parts.get(0);
        } else if (parts.size() == 3) {
            if (!InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME.equalsIgnoreCase(parts.get(0))) {
                throw new DdlException("Stored procedures live in " + InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME
                        + ", not in catalog " + parts.get(0));
            }
            dbName = parts.get(1);
        } else {
            throw new DdlException("Invalid procedure name " + name);
        }
        if (dbName == null || dbName.isEmpty()) {
            throw new DdlException("No database selected");
        }
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new DdlException("Unknown database '" + dbName + "'");
        }
        return db;
    }

    private static String procedureName(QualifiedName name) {
        List<String> parts = name.getParts();
        return parts.get(parts.size() - 1);
    }

    private static boolean matchesTypes(StoredProcedure procedure, List<String> types) {
        if (types == null) {
            return true;
        }
        List<String> declared = procedure.getArgumentTypes();
        if (declared.size() != types.size()) {
            return false;
        }
        for (int i = 0; i < types.size(); i++) {
            if (!ScriptValue.baseType(declared.get(i)).equals(
                    ScriptValue.baseType(ScriptValue.normalizeType(types.get(i))))) {
                return false;
            }
        }
        return true;
    }

    private static String signatureSuffix(List<String> types) {
        return types == null ? "" : "(" + String.join(", ", types) + ")";
    }

    private static String owner(StoredProcedure procedure) {
        return procedure.getOwner() == null ? "" : procedure.getOwner().toString();
    }
}
