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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.DdlException;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.policy.SecurityPolicyStmt;
import com.starrocks.sql.ast.policy.ShowSecurityPoliciesStmt;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Runs the statements of masking and row access policies. They need the SECURITY privilege, checked by the
 * authorizer: whoever attaches a policy to a table decides what its readers see, so the owner of a table cannot
 * detach one.
 */
public final class SecurityPolicyExecutor {
    public static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("created_on", TypeFactory.createVarcharType(32)))
            .addColumn(new Column("name", TypeFactory.createVarcharType(256)))
            .addColumn(new Column("database_name", TypeFactory.createVarcharType(256)))
            .addColumn(new Column("kind", TypeFactory.createVarcharType(32)))
            .addColumn(new Column("owner", TypeFactory.createVarcharType(256)))
            .addColumn(new Column("comment", TypeFactory.createVarcharType(1024)))
            .addColumn(new Column("signature", TypeFactory.createVarcharType(1024)))
            .addColumn(new Column("return_type", TypeFactory.createVarcharType(256)))
            .addColumn(new Column("body", TypeFactory.createVarcharType(65533)))
            .addColumn(new Column("attached_to", TypeFactory.createVarcharType(65533)))
            .build();

    private SecurityPolicyExecutor() {
    }

    private static Database db(String name) {
        Database db = name == null ? null : GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(name);
        if (db == null) {
            throw new SemanticException("Unknown database '%s'", name);
        }
        return db;
    }

    /** The database a qualified name is in: its qualifier, or {@code defaultDb}. */
    private static Database dbOf(QualifiedName name, String defaultDb) {
        List<String> parts = name.getParts();
        if (parts.size() > 2) {
            throw new SemanticException("Invalid name %s: use name or db.name", name);
        }
        String dbName = parts.size() == 2 ? parts.get(0) : defaultDb;
        if (dbName == null || dbName.isEmpty()) {
            throw new SemanticException("No database selected");
        }
        return db(dbName);
    }

    private static String last(QualifiedName name) {
        return name.getParts().get(name.getParts().size() - 1);
    }

    private static SecurityPolicy policy(Database db, SecurityPolicyStmt.Kind kind, String name) {
        SecurityPolicy policy = SecurityPolicyMgr.get(db, kind, name);
        if (policy == null) {
            throw new SemanticException("%s %s does not exist in database %s",
                    capitalized(kind), name, db.getFullName());
        }
        return policy;
    }

    private static String capitalized(SecurityPolicyStmt.Kind kind) {
        return kind == SecurityPolicyStmt.Kind.MASKING ? "Masking policy" : "Row access policy";
    }

    public static void execute(SecurityPolicyStmt stmt, ConnectContext ctx) throws DdlException {
        if (stmt.isAttachment()) {
            attach(stmt, ctx);
            return;
        }
        Database db = dbOf(stmt.getName(), ctx.getDatabase());
        String name = last(stmt.getName());
        SecurityPolicy existing = SecurityPolicyMgr.get(db, stmt.getKind(), name);
        switch (stmt.getAction()) {
            case CREATE:
                create(stmt, ctx, db, name, existing);
                return;
            case DROP:
                if (existing == null) {
                    if (stmt.isIfExists()) {
                        return;
                    }
                    policy(db, stmt.getKind(), name);
                }
                checkDetached(existing, "dropped");
                SecurityPolicyMgr.drop(existing);
                return;
            default:
                break;
        }
        if (existing == null) {
            if (stmt.isIfExists()) {
                return;
            }
            policy(db, stmt.getKind(), name);
        }
        switch (stmt.getAction()) {
            case SET_BODY: {
                SecurityPolicy changed = existing.with(existing.getName(), stmt.getBody(), existing.getComment());
                checkBody(changed, ctx);
                for (String attachment : SecurityPolicyMgr.attachments(existing)) {
                    checkAttachment(changed, attachment, ctx);
                }
                SecurityPolicyMgr.put(changed);
                return;
            }
            case SET_COMMENT:
                SecurityPolicyMgr.put(existing.with(existing.getName(), existing.getBody(), stmt.getComment()));
                return;
            case RENAME:
                if (SecurityPolicyMgr.get(db, stmt.getKind(), stmt.getNewName()) != null) {
                    throw new SemanticException("%s %s already exists in database %s", capitalized(stmt.getKind()),
                            stmt.getNewName(), db.getFullName());
                }
                SecurityPolicyMgr.put(existing.with(stmt.getNewName(), existing.getBody(), existing.getComment()));
                return;
            default:
                throw new SemanticException("Unsupported policy statement %s", stmt.getAction());
        }
    }

    private static void checkDetached(SecurityPolicy policy, String what) {
        List<String> attachments = SecurityPolicyMgr.attachments(policy);
        if (!attachments.isEmpty()) {
            throw new SemanticException("%s %s cannot be %s: it is attached to %s; detach it first",
                    capitalized(policy.getKind()), policy.getName(), what, String.join(", ", attachments));
        }
    }

    private static void create(SecurityPolicyStmt stmt, ConnectContext ctx, Database db, String name,
                               SecurityPolicy existing) {
        if (existing != null) {
            if (stmt.isIfExists() && !stmt.isOrReplace()) {
                return;
            }
            if (!stmt.isOrReplace()) {
                throw new SemanticException("%s %s already exists in database %s", capitalized(stmt.getKind()), name,
                        db.getFullName());
            }
            // its attachments are to the arguments it has
            checkDetached(existing, "replaced");
        }
        Set<String> names = new HashSet<>();
        List<String> argNames = new ArrayList<>();
        List<String> argTypes = new ArrayList<>();
        for (SecurityPolicyStmt.Arg arg : stmt.getArgs()) {
            if (!names.add(arg.name().toLowerCase(Locale.ROOT))) {
                throw new SemanticException("Argument %s of policy %s is given twice", arg.name(), name);
            }
            argNames.add(arg.name());
            argTypes.add(arg.type());
        }
        String returnType = stmt.getReturnType();
        if (stmt.getKind() == SecurityPolicyStmt.Kind.ROW_ACCESS && !returnType.equalsIgnoreCase("BOOLEAN")) {
            throw new SemanticException("A row access policy returns BOOLEAN, not %s", returnType);
        }
        if (stmt.getKind() == SecurityPolicyStmt.Kind.MASKING && !sameType(returnType, argTypes.get(0))) {
            throw new SemanticException("A masking policy returns the type of its first argument, the column it "
                    + "masks: %s, not %s", argTypes.get(0), returnType);
        }
        long id = existing != null ? existing.getId() : GlobalStateMgr.getCurrentState().getNextId();
        SecurityPolicy policy = new SecurityPolicy(id, db.getId(), name, stmt.getKind(), argNames, argTypes,
                returnType, stmt.getBody(), stmt.getComment(), ctx.getCurrentUserIdentity().toString(),
                System.currentTimeMillis());
        checkBody(policy, ctx);
        SecurityPolicyMgr.put(policy);
    }

    private static boolean sameType(String a, String b) {
        String x = a.toUpperCase(Locale.ROOT);
        String y = b.toUpperCase(Locale.ROOT);
        return x.equals(y) || isString(x) && isString(y);
    }

    private static boolean isString(String type) {
        return type.startsWith("VARCHAR") || type.startsWith("CHAR") || type.startsWith("STRING");
    }

    /** The body must be an expression of its arguments of the type the policy returns. */
    private static void checkBody(SecurityPolicy policy, ConnectContext ctx) {
        QueryStatement query;
        try {
            query = SecurityPolicyMgr.analyze(SecurityPolicyMgr.checkSql(policy), ctx);
        } catch (RuntimeException e) {
            throw new SemanticException("Invalid body of policy %s: %s", policy.getName(), e.getMessage());
        }
        Type type = query.getQueryRelation().getOutputExpression().get(0).getType();
        if (policy.getKind() == SecurityPolicyStmt.Kind.ROW_ACCESS) {
            if (!type.isBoolean() && !type.isNull()) {
                throw new SemanticException("The body of row access policy %s must be a condition, but is of type %s",
                        policy.getName(), type.toSql());
            }
        } else {
            try {
                SecurityPolicyMgr.analyze("SELECT CAST(x AS " + policy.getReturnType() + ") FROM ("
                        + SecurityPolicyMgr.checkSql(policy) + ") q(x)", ctx);
            } catch (RuntimeException e) {
                throw new SemanticException("The body of masking policy %s is of type %s, which is not %s",
                        policy.getName(), type.toSql(), policy.getReturnType());
            }
        }
    }

    /** {@code db.table[.column]}: checks the policy, as changed, on the table it is attached to. */
    private static void checkAttachment(SecurityPolicy policy, String attachment, ConnectContext ctx) {
        String[] parts = attachment.split("\\.", 3);
        Database db = db(parts[0]);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), parts[1]);
        TablePolicies policies = SecurityPolicyMgr.policiesOf(db, table);
        if (policies == null) {
            return;
        }
        TableName name = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db.getFullName(),
                table.getName());
        TablePolicies.Ref ref = parts.length == 3 ? policies.getMasks().get(TablePolicies.key(parts[2]))
                : policies.getRowAccess();
        if (ref != null) {
            check(policy, name, table, ref, ctx);
        }
    }

    private static void check(SecurityPolicy policy, TableName name, Table table, TablePolicies.Ref ref,
                              ConnectContext ctx) {
        try {
            SecurityPolicyMgr.analyze(SecurityPolicyMgr.attachmentCheckSql(policy, name, table, ref), ctx);
        } catch (RuntimeException e) {
            throw new SemanticException("%s %s cannot apply to %s(%s): %s", capitalized(policy.getKind()),
                    policy.getName(), table.getName(), String.join(", ", ref.getColumns()), e.getMessage());
        }
    }

    // ---------------------------------------- attachments ---------------------------------------------------------

    private static String column(Table table, String name) {
        Column column = table.getColumn(name);
        if (column == null) {
            throw new SemanticException("Unknown column '%s' in table %s", name, table.getName());
        }
        return column.getName();
    }

    private static void attach(SecurityPolicyStmt stmt, ConnectContext ctx) {
        Database db = dbOf(stmt.getTable(), ctx.getDatabase());
        String tableName = last(stmt.getTable());
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        if (table == null) {
            throw new SemanticException("Table %s does not exist in database %s", tableName, db.getFullName());
        }
        if (!table.isNativeTable() || table.isTemporaryTable()) {
            throw new SemanticException("Policies can be attached to native tables only, and %s is not one",
                    table.getName());
        }
        TableName name = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db.getFullName(),
                table.getName());
        TablePolicies current = SecurityPolicyMgr.policiesOf(db, table);
        TablePolicies.Ref rowAccess = current == null ? null : current.getRowAccess();
        Map<String, TablePolicies.Ref> masks = current == null ? new LinkedHashMap<>()
                : new LinkedHashMap<>(current.getMasks());
        SecurityPolicy policy = null;
        if (stmt.getName() != null) {
            policy = policy(dbOf(stmt.getName(), db.getFullName()), stmt.getKind(), last(stmt.getName()));
        }
        switch (stmt.getAction()) {
            case SET_MASKING: {
                String column = column(table, stmt.getColumn());
                List<String> columns = new ArrayList<>();
                if (stmt.getColumns() == null) {
                    columns.add(column);
                } else {
                    for (String c : stmt.getColumns()) {
                        columns.add(column(table, c));
                    }
                    if (!columns.get(0).equalsIgnoreCase(column)) {
                        throw new SemanticException("The first column of USING must be the masked column %s",
                                column);
                    }
                }
                checkArity(policy, columns);
                Type columnType = table.getColumn(column).getType();
                if (!sameType(columnType.toSql(), policy.getArgTypes().get(0))) {
                    throw new SemanticException("Masking policy %s masks values of type %s, and column %s is of type %s",
                            policy.getName(), policy.getArgTypes().get(0), column, columnType.toSql());
                }
                TablePolicies.Ref existing = masks.get(TablePolicies.key(column));
                if (existing != null && !stmt.isForce()) {
                    SecurityPolicy other = SecurityPolicyMgr.byId(existing.getPolicyDbId(), existing.getPolicyId());
                    throw new SemanticException("Column %s already has masking policy %s: use FORCE to replace it, "
                            + "or UNSET it first", column, other == null ? "?" : other.getName());
                }
                checkNoMaterializedViews(table);
                TablePolicies.Ref ref = new TablePolicies.Ref(policy.getDbId(), policy.getId(), columns);
                check(policy, name, table, ref, ctx);
                masks.put(TablePolicies.key(column), ref);
                break;
            }
            case UNSET_MASKING: {
                String column = column(table, stmt.getColumn());
                if (masks.remove(TablePolicies.key(column)) == null) {
                    throw new SemanticException("Column %s of table %s has no masking policy", column,
                            table.getName());
                }
                break;
            }
            case ADD_ROW_ACCESS: {
                if (rowAccess != null) {
                    SecurityPolicy other = SecurityPolicyMgr.byId(rowAccess.getPolicyDbId(),
                            rowAccess.getPolicyId());
                    throw new SemanticException("Table %s already has row access policy %s: a table has one; drop it "
                            + "first", table.getName(), other == null ? "?" : other.getName());
                }
                List<String> columns = new ArrayList<>();
                for (String c : stmt.getColumns()) {
                    columns.add(column(table, c));
                }
                checkArity(policy, columns);
                checkNoMaterializedViews(table);
                TablePolicies.Ref ref = new TablePolicies.Ref(policy.getDbId(), policy.getId(), columns);
                check(policy, name, table, ref, ctx);
                rowAccess = ref;
                break;
            }
            case DROP_ROW_ACCESS:
                if (rowAccess == null || !rowAccess.refersTo(policy)) {
                    throw new SemanticException("Row access policy %s is not attached to table %s", policy.getName(),
                            table.getName());
                }
                rowAccess = null;
                break;
            case DROP_ALL_ROW_ACCESS:
                rowAccess = null;
                break;
            default:
                throw new SemanticException("Unsupported policy statement %s", stmt.getAction());
        }
        SecurityPolicyMgr.setTablePolicies(new TablePolicies(db.getId(), table.getId(), rowAccess, masks));
    }

    private static void checkArity(SecurityPolicy policy, List<String> columns) {
        if (columns.size() != policy.getArgNames().size()) {
            throw new SemanticException("%s %s takes %d columns %s, but is given %d", capitalized(policy.getKind()),
                    policy.getName(), policy.getArgNames().size(), policy.signature(), columns.size());
        }
    }

    /**
     * A materialized view keeps what it read of its tables, and a query of it, or rewritten to it, would read that
     * without the policies of the tables.
     */
    private static void checkNoMaterializedViews(Table table) {
        if (!table.getRelatedMaterializedViews().isEmpty()) {
            throw new SemanticException("Table %s has materialized views, which would read it without its policies: "
                    + "drop them first", table.getName());
        }
    }

    // ---------------------------------------- SHOW and DESC -------------------------------------------------------

    public static ShowResultSet show(ShowSecurityPoliciesStmt stmt, ConnectContext ctx) {
        List<List<String>> rows = new ArrayList<>();
        if (stmt.getName() != null) {
            Database db = dbOf(stmt.getName(), ctx.getDatabase());
            rows.add(row(db, policy(db, stmt.getKind(), last(stmt.getName()))));
            return new ShowResultSet(META_DATA, rows);
        }
        String dbName = stmt.getDbName() != null ? stmt.getDbName() : ctx.getDatabase();
        if (dbName == null || dbName.isEmpty()) {
            throw new SemanticException("No database selected");
        }
        Database db = db(dbName);
        PatternMatcher matcher = stmt.getPattern() == null ? null
                : PatternMatcher.createMysqlPattern(stmt.getPattern(), false);
        for (SecurityPolicy policy : SecurityPolicyMgr.list(db, stmt.getKind())) {
            if (matcher == null || matcher.match(policy.getName())) {
                rows.add(row(db, policy));
            }
        }
        return new ShowResultSet(META_DATA, rows);
    }

    private static List<String> row(Database db, SecurityPolicy policy) {
        return List.of(TimeUtils.longToTimeString(policy.getCreateTime()), policy.getName(), db.getFullName(),
                policy.getKind().sql().replace(" POLICY", ""), policy.getOwner() == null ? "" : policy.getOwner(),
                policy.getComment() == null ? "" : policy.getComment(), policy.signature(), policy.getReturnType(),
                policy.getBody(), String.join(", ", SecurityPolicyMgr.attachments(policy)));
    }
}
