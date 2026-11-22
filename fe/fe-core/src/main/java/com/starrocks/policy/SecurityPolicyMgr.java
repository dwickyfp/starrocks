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
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.policy.SecurityPolicyStmt;
import com.starrocks.sql.parser.CaseInsensitiveStream;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.parser.StarRocksLexer;
import com.starrocks.type.Type;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Keeps masking and row access policies and their attachments to tables, and gives the expressions a query reads a
 * table through: a policy lives in the map of its {@link Database}, the policies of a table in the map of the
 * database of the table, both persisted with the database in the image and changed through the edit log
 * ({@code OP_PUT_SECURITY_POLICY}, {@code OP_DROP_SECURITY_POLICY}, {@code OP_SET_TABLE_POLICIES}).
 *
 * <p>A policy is applied by {@link com.starrocks.authorization.SecurityPolicyRewriteRule}, which reads a table
 * through a subquery that selects the masked columns and filters the rows; the arguments of the policy become the
 * columns of the table it is attached to, cast to the types of the arguments.
 */
public final class SecurityPolicyMgr {
    private static final Logger LOG = LogManager.getLogger(SecurityPolicyMgr.class);
    private static final Object LOCK = new Object();
    private static final Pattern WORD = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    /** The relation the arguments of a policy are columns of, to check its body. */
    public static final String ARGS_ALIAS = "__policy_args";

    private SecurityPolicyMgr() {
    }

    // ---------------------------------------- policies ------------------------------------------------------------

    public static List<SecurityPolicy> list(Database db, SecurityPolicyStmt.Kind kind) {
        List<SecurityPolicy> policies = new ArrayList<>();
        for (SecurityPolicy policy : db.getSecurityPolicies().values()) {
            if (policy.getKind() == kind) {
                policies.add(policy);
            }
        }
        policies.sort(Comparator.comparing(SecurityPolicy::getName, String.CASE_INSENSITIVE_ORDER));
        return policies;
    }

    public static SecurityPolicy get(Database db, SecurityPolicyStmt.Kind kind, String name) {
        for (SecurityPolicy policy : db.getSecurityPolicies().values()) {
            if (policy.getKind() == kind && policy.getName().equalsIgnoreCase(name)) {
                return policy;
            }
        }
        return null;
    }

    public static SecurityPolicy byId(long dbId, long id) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        return db == null ? null : db.getSecurityPolicies().get(id);
    }

    /** Creates or replaces the policy of its id. */
    public static void put(SecurityPolicy policy) {
        synchronized (LOCK) {
            GlobalStateMgr.getCurrentState().getEditLog().logPutSecurityPolicy(policy,
                    wal -> replayPut((SecurityPolicy) wal));
        }
    }

    public static void drop(SecurityPolicy policy) {
        synchronized (LOCK) {
            GlobalStateMgr.getCurrentState().getEditLog().logDropSecurityPolicy(
                    new DropSecurityPolicyLog(policy.getDbId(), policy.getId()),
                    wal -> replayDrop((DropSecurityPolicyLog) wal));
        }
    }

    public static void setTablePolicies(TablePolicies policies) {
        synchronized (LOCK) {
            GlobalStateMgr.getCurrentState().getEditLog().logSetTablePolicies(policies,
                    wal -> replaySetTablePolicies((TablePolicies) wal));
        }
    }

    public static void replayPut(SecurityPolicy policy) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(policy.getDbId());
        if (db == null) {
            LOG.warn("Skip replaying policy {}: database {} does not exist", policy.getName(), policy.getDbId());
            return;
        }
        db.getSecurityPolicies().put(policy.getId(), policy);
    }

    public static void replayDrop(DropSecurityPolicyLog log) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(log.getDbId());
        if (db != null) {
            db.getSecurityPolicies().remove(log.getId());
        }
    }

    public static void replaySetTablePolicies(TablePolicies policies) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(policies.getDbId());
        if (db == null) {
            LOG.warn("Skip replaying the policies of table {}: database {} does not exist", policies.getTableId(),
                    policies.getDbId());
            return;
        }
        if (policies.isEmpty()) {
            db.getTablePolicies().remove(policies.getTableId());
        } else {
            db.getTablePolicies().put(policies.getTableId(), policies);
        }
    }

    /** The policies attached to the table, or null. */
    public static TablePolicies policiesOf(Database db, Table table) {
        return db == null || table == null ? null : db.getTablePolicies().get(table.getId());
    }

    /** Rejects dropping or renaming a column that a policy attached to the table reads. */
    public static void checkColumnUnused(Table table, String column, String what) {
        if (table == null || table.isTemporaryTable()) {
            return;
        }
        for (Database db : GlobalStateMgr.getCurrentState().getLocalMetastore().getAllDbs()) {
            TablePolicies policies = db.getTablePolicies().get(table.getId());
            if (policies == null) {
                continue;
            }
            for (String used : policies.referencedColumns()) {
                if (used.equalsIgnoreCase(column)) {
                    throw new SemanticException("Column %s of table %s cannot be %s: a masking or row access policy "
                            + "attached to the table reads it; detach the policy first", column, table.getName(),
                            what);
                }
            }
        }
    }

    /**
     * Rejects a materialized view that reads a table with policies: it would keep what it read without them, for
     * its readers and for the queries rewritten to it.
     */
    public static void checkNoPolicies(List<TableRelation> relations, String operation) {
        for (TableRelation relation : relations) {
            TableName name = relation.getResolveTableName() != null ? relation.getResolveTableName()
                    : relation.getName();
            if (relation.getTable() == null || name == null || name.getDb() == null) {
                continue;
            }
            if (hasPolicies(dbOf(name), relation.getTable())) {
                throw new SemanticException("Do not support %s that reads table %s: it has masking or row access "
                        + "policies", operation, relation.getTable().getName());
            }
        }
    }

    public static boolean hasPolicies(Database db, Table table) {
        TablePolicies policies = policiesOf(db, table);
        return policies != null && !policies.isEmpty();
    }

    /** Where the policy is attached: {@code db.table.column} for a masking policy, {@code db.table} otherwise. */
    public static List<String> attachments(SecurityPolicy policy) {
        List<String> attachments = new ArrayList<>();
        for (Database db : GlobalStateMgr.getCurrentState().getLocalMetastore().getAllDbs()) {
            for (TablePolicies policies : db.getTablePolicies().values()) {
                Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(),
                        policies.getTableId());
                if (table == null) {
                    continue;
                }
                String name = db.getFullName() + "." + table.getName();
                if (policies.getRowAccess() != null && policies.getRowAccess().refersTo(policy)) {
                    attachments.add(name);
                }
                for (TablePolicies.Ref mask : policies.getMasks().values()) {
                    if (mask.refersTo(policy)) {
                        attachments.add(name + "." + mask.getColumns().get(0));
                    }
                }
            }
        }
        attachments.sort(String::compareTo);
        return attachments;
    }

    // ---------------------------------------- expressions ---------------------------------------------------------

    public static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    /**
     * The body of a policy with each reference to one of its arguments replaced: a word or a quoted identifier that
     * names the argument and is not a part of a qualified name, a function name or an alias. The text is changed
     * token by token, so strings and comments stay as they are.
     */
    static String substitute(String body, Map<String, String> replacements) {
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(body)));
        lexer.removeErrorListeners();
        CommonTokenStream stream = new CommonTokenStream(lexer);
        stream.fill();
        List<Token> tokens = stream.getTokens();
        StringBuilder sql = new StringBuilder();
        for (int i = 0; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (token.getType() == Token.EOF) {
                break;
            }
            String text = token.getText();
            String replacement = null;
            if (token.getChannel() == Token.DEFAULT_CHANNEL) {
                String name = identifierName(token);
                if (name != null) {
                    replacement = replacements.get(name.toLowerCase(Locale.ROOT));
                }
                if (replacement != null) {
                    String previous = neighbor(tokens, i, -1);
                    String next = neighbor(tokens, i, 1);
                    if (".".equals(previous) || "AS".equalsIgnoreCase(previous) || ".".equals(next)
                            || "(".equals(next) || "=>".equals(next) || "->".equals(next)) {
                        replacement = null;
                    }
                }
            }
            sql.append(replacement != null ? replacement : text);
        }
        return sql.toString();
    }

    private static String identifierName(Token token) {
        String text = token.getText();
        if (token.getType() == StarRocksLexer.BACKQUOTED_IDENTIFIER) {
            return text.substring(1, text.length() - 1).replace("``", "`");
        }
        return WORD.matcher(text).matches() ? text : null;
    }

    private static String neighbor(List<Token> tokens, int index, int step) {
        for (int i = index + step; i >= 0 && i < tokens.size(); i += step) {
            Token token = tokens.get(i);
            if (token.getType() == Token.EOF) {
                return null;
            }
            if (token.getChannel() == Token.DEFAULT_CHANNEL) {
                return token.getText();
            }
        }
        return null;
    }

    /** The body of the policy over the given columns, each cast to the type of its argument. */
    static String bodyOver(SecurityPolicy policy, List<String> columnRefs) {
        Map<String, String> replacements = new HashMap<>();
        for (int i = 0; i < policy.getArgNames().size(); i++) {
            replacements.put(policy.getArgNames().get(i).toLowerCase(Locale.ROOT),
                    "CAST(" + columnRefs.get(i) + " AS " + policy.getArgTypes().get(i) + ")");
        }
        return "(" + substitute(policy.getBody(), replacements) + ")";
    }

    /** The SQL of a masking policy over columns, as a value of the type of the masked column. */
    public static String maskSql(SecurityPolicy policy, List<String> columnRefs, Type columnType) {
        return "CAST(" + bodyOver(policy, columnRefs) + " AS " + columnType.toSql() + ")";
    }

    /**
     * The query that checks the body of a policy: the body over a relation whose columns are its arguments, NULL
     * of their types.
     */
    public static String checkSql(SecurityPolicy policy) {
        List<String> refs = new ArrayList<>();
        List<String> args = new ArrayList<>();
        for (int i = 0; i < policy.getArgNames().size(); i++) {
            refs.add(quote(ARGS_ALIAS) + "." + quote(policy.getArgNames().get(i)));
            args.add("CAST(NULL AS " + policy.getArgTypes().get(i) + ") AS " + quote(policy.getArgNames().get(i)));
        }
        return "SELECT " + bodyOver(policy, refs) + " FROM (SELECT " + String.join(", ", args) + ") "
                + quote(ARGS_ALIAS);
    }

    /** Analyzes a query, under the locks of the tables it reads; returns it analyzed. */
    public static QueryStatement analyze(String sql, ConnectContext context) {
        StatementBase stmt = SqlParser.parse(sql, context.getSessionVariable()).get(0);
        if (!(stmt instanceof QueryStatement query)) {
            throw new SemanticException("The body of a policy must be an expression");
        }
        PlannerMetaLocker locker = new PlannerMetaLocker(context, stmt);
        locker.lockForPlanning();
        try {
            Analyzer.analyze(stmt, context);
        } finally {
            locker.unlock();
        }
        return query;
    }

    private static String columnRef(TableName table, String column) {
        return quote(table.getDb()) + "." + quote(table.getTbl()) + "." + quote(column);
    }

    private static Expr parse(String sql, ConnectContext context) {
        long sqlMode = context == null ? 0 : context.getSessionVariable().getSqlMode();
        return SqlParser.parseSqlToExpr(sql, sqlMode);
    }

    private static Database dbOf(TableName name) {
        if (name == null || name.getDb() == null || name.getCatalog() != null
                && !InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME.equals(name.getCatalog())) {
            return null;
        }
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(name.getDb());
    }

    private static SecurityPolicy resolve(TablePolicies.Ref ref, TableName table) {
        SecurityPolicy policy = byId(ref.getPolicyDbId(), ref.getPolicyId());
        if (policy == null) {
            // fail closed: a table whose policy is gone cannot be read
            throw new SemanticException("Table %s cannot be read: a policy attached to it no longer exists; detach it",
                    table.getTbl());
        }
        return policy;
    }

    private static List<String> refs(TableName name, Table table, TablePolicies.Ref ref) {
        List<String> refs = new ArrayList<>();
        for (String column : ref.getColumns()) {
            Column col = table.getColumn(column);
            if (col == null) {
                throw new SemanticException("Table %s cannot be read: column %s of a policy attached to it no longer "
                        + "exists", name.getTbl(), column);
            }
            refs.add(columnRef(name, col.getName()));
        }
        return refs;
    }

    /** The expressions that mask the columns of a native table, by column name, or null. */
    public static Map<String, Expr> masking(ConnectContext context, TableName name, List<Column> columns) {
        Database db = dbOf(name);
        Table table = db == null ? null : GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), name.getTbl());
        TablePolicies policies = policiesOf(db, table);
        if (policies == null || policies.getMasks().isEmpty()) {
            return null;
        }
        Map<String, Expr> masks = new LinkedHashMap<>();
        for (Column column : columns) {
            TablePolicies.Ref ref = policies.getMasks().get(TablePolicies.key(column.getName()));
            if (ref != null) {
                SecurityPolicy policy = resolve(ref, name);
                masks.put(column.getName(), parse(maskSql(policy, refs(name, table, ref), column.getType()), context));
            }
        }
        return masks;
    }

    /** The predicate of the row access policy of a native table, or null. */
    public static Expr rowAccess(ConnectContext context, TableName name) {
        Database db = dbOf(name);
        Table table = db == null ? null : GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), name.getTbl());
        TablePolicies policies = policiesOf(db, table);
        if (policies == null || policies.getRowAccess() == null) {
            return null;
        }
        TablePolicies.Ref ref = policies.getRowAccess();
        SecurityPolicy policy = resolve(ref, name);
        return parse("coalesce(" + bodyOver(policy, refs(name, table, ref)) + ", FALSE)", context);
    }

    /**
     * The predicate of the row access policy of the native table a DML statement writes, over its columns as the
     * statement names them, or null: UPDATE, DELETE and MERGE change only the rows the policy lets through.
     */
    public static Expr targetRowAccess(ConnectContext context, Table table, TableName qualifier) {
        String dbName = qualifier.getDb() != null ? qualifier.getDb() : context.getDatabase();
        Database db = dbName == null || qualifier.getCatalog() != null
                && !InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME.equals(qualifier.getCatalog()) ? null
                : GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        TablePolicies policies = policiesOf(db, table);
        if (policies == null || policies.getRowAccess() == null) {
            return null;
        }
        TablePolicies.Ref ref = policies.getRowAccess();
        SecurityPolicy policy = resolve(ref, qualifier);
        List<String> refs = new ArrayList<>();
        String prefix = (qualifier.getDb() != null ? quote(qualifier.getDb()) + "." : "") + quote(qualifier.getTbl())
                + ".";
        for (String column : ref.getColumns()) {
            Column col = table.getColumn(column);
            if (col == null) {
                throw new SemanticException("Table %s cannot be written: column %s of a policy attached to it no "
                        + "longer exists", table.getName(), column);
            }
            refs.add(prefix + quote(col.getName()));
        }
        return parse("coalesce(" + bodyOver(policy, refs) + ", FALSE)", context);
    }

    /** The predicate of a WHERE clause and the row access policy of the target of a DML statement. */
    public static Expr andTargetRowAccess(ConnectContext context, Table table, TableName qualifier, Expr where) {
        Expr rowAccess = targetRowAccess(context, table, qualifier);
        if (rowAccess == null) {
            return where;
        }
        return where == null ? rowAccess : new CompoundPredicate(CompoundPredicate.Operator.AND, where, rowAccess);
    }

    /** The query that checks a policy attached to a table: what a query of the table would read through it. */
    public static String attachmentCheckSql(SecurityPolicy policy, TableName name, Table table,
                                            TablePolicies.Ref ref) {
        List<String> refs = refs(name, table, ref);
        String from = " FROM " + quote(name.getDb()) + "." + quote(name.getTbl());
        if (policy.getKind() == SecurityPolicyStmt.Kind.MASKING) {
            Column column = table.getColumn(ref.getColumns().get(0));
            return "SELECT " + maskSql(policy, refs, column.getType()) + from;
        }
        return "SELECT 1" + from + " WHERE coalesce(" + bodyOver(policy, refs) + ", FALSE)";
    }

    static String typeName(Type type) {
        return type.toSql();
    }
}
