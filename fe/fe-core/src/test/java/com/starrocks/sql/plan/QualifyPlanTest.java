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

package com.starrocks.sql.plan;

import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * QUALIFY with any boolean expression, and the Snowflake select list and predicate extensions:
 * {@code * ILIKE / EXCLUDE / REPLACE / RENAME}, {@code ILIKE}, {@code LIKE / ILIKE ANY | ALL} and the
 * compatibility functions.
 */
public class QualifyPlanTest extends PlanTestBase {

    private List<String> outputNames(String sql) throws Exception {
        QueryStatement stmt = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        return stmt.getQueryRelation().getColumnOutputNames();
    }

    private static int count(String text, String part) {
        int n = 0;
        for (int i = text.indexOf(part); i >= 0; i = text.indexOf(part, i + 1)) {
            n++;
        }
        return n;
    }

    private void assertError(String sql, String message) {
        Exception e = Assertions.assertThrows(Exception.class, () -> getFragmentPlan(sql), sql);
        Assertions.assertTrue(e.getMessage().contains(message), e.getMessage());
    }

    // ---------------------------------------------------------------- QUALIFY

    @Test
    public void testQualifyAliasIsNotComputedTwice() throws Exception {
        String sql = "select ta, row_number() over (partition by ta order by tg) rn from tall qualify rn = 1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: row_number() = 1");
        assertContains(plan, "PARTITION-TOP-N");
        Assertions.assertEquals(1, count(plan, "functions: [, row_number(), ]"), plan);
        Assertions.assertEquals(List.of("ta", "rn"), outputNames(sql));

        // the same window written again in QUALIFY is computed once
        sql = "select ta, row_number() over (partition by ta order by tg) rn from tall "
                + "qualify row_number() over (partition by ta order by tg) = 1";
        plan = getFragmentPlan(sql);
        Assertions.assertEquals(1, count(plan, "functions: [, row_number(), ]"), plan);
    }

    @Test
    public void testQualifyBooleanExpression() throws Exception {
        String sql = "select ta, tg from tall qualify rank() over (partition by ta order by tg) <= 2 "
                + "and sum(tg) over (partition by ta) > 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: rank() <= 2, 12: sum(7: tg) > 10");
        assertContains(plan, "partition limit: 2");
        Assertions.assertEquals(List.of("ta", "tg"), outputNames(sql));

        sql = "select ta, tc from tall qualify row_number() over (order by tg) > tc or tc is null";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: (11: row_number() > CAST(3: tc AS BIGINT)) OR (3: tc IS NULL)");

        sql = "select ta from tall qualify not (count(*) over (partition by ta) between 2 and 5)";
        plan = getFragmentPlan(sql);
        assertContains(plan, "count(*)");
        Assertions.assertEquals(List.of("ta"), outputNames(sql));
    }

    @Test
    public void testQualifyStarDistinctGroupBy() throws Exception {
        String sql = "select * from tall qualify row_number() over (partition by ta order by tg) = 1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "OUTPUT EXPRS:1: ta | 2: tb | 3: tc | 4: td | 5: te | 6: tf | 7: tg | 8: th | 9: ti | 10: tt");
        assertContains(plan, "partition limit: 1");

        // QUALIFY is evaluated before DISTINCT
        sql = "select distinct ta from tall qualify row_number() over (partition by ta order by tg) = 1";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.indexOf("AGGREGATE") < plan.indexOf("predicates: 11: row_number() = 1"), plan);

        // a window function over an aggregate
        sql = "select ta, sum(tg) s from tall group by ta qualify rank() over (order by sum(tg) desc) <= 3";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 12: rank() <= 3");
        assertContains(plan, "type: RANK");
        Assertions.assertEquals(List.of("ta", "s"), outputNames(sql));

        sql = "select ta, tg from tall where tb > 1 qualify row_number() over (partition by ta order by tg) = 1 "
                + "order by tg limit 5";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: row_number() = 1");
        assertContains(plan, "limit: 5");
        assertContains(plan, "PREDICATES: 2: tb > 1");
    }

    @Test
    public void testQualifyColumnsOutsideSelect() throws Exception {
        // a column of the FROM clause that is not selected
        String sql = "select ta from tall qualify row_number() over (partition by ta order by tg) = 1 and tg > 3";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: row_number() = 1");
        assertContains(plan, "7: tg > 3");
        Assertions.assertEquals(List.of("ta"), outputNames(sql));

        // qualified names
        sql = "select t.ta from tall t qualify row_number() over (partition by t.ta order by t.tg) = 1 "
                + "and t.ta is not null";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: row_number() = 1");
        Assertions.assertEquals(List.of("ta"), outputNames(sql));

        // a subquery keeps its own scope
        sql = "select ta from tall qualify row_number() over (order by tg) = 1 "
                + "and ta in (select ta from tall where tg > 0)";
        Assertions.assertEquals(List.of("ta"), outputNames(sql));
    }

    @Test
    public void testQualifyInViewAndCte() throws Exception {
        starRocksAssert.withView("create view qualify_view as select ta, tg from tall "
                + "qualify row_number() over (partition by ta order by tg desc) = 1");
        try {
            String plan = getFragmentPlan("select * from qualify_view");
            assertContains(plan, "predicates: 11: row_number() = 1");
            Assertions.assertEquals(List.of("ta", "tg"), outputNames("select * from qualify_view"));
        } finally {
            starRocksAssert.dropView("qualify_view");
        }
        String sql = "with c as (select ta, tg from tall qualify rank() over (partition by ta order by tg) = 1) "
                + "select count(*) from c";
        assertContains(getFragmentPlan(sql), "predicates: 11: rank() = 1");
    }

    @Test
    public void testQualifyErrors() {
        // without an expression, QUALIFY is a table alias
        assertError("select ta from tall qualify row_number() over (order by tg) =", "");
        assertError("select ta, sum(tg) from tall group by ta qualify tg > 1", "");
    }

    // ---------------------------------------------------------------- star modifiers

    @Test
    public void testStarIlikeExcludeReplaceRename() throws Exception {
        Assertions.assertEquals(List.of("ta", "tt"), outputNames("select * ilike 't_' exclude (tb, tc, td, te, tf, tg, "
                + "th, ti) from tall"));
        Assertions.assertEquals(List.of("ta", "tb"), outputNames("select * ilike 'T%' exclude tc from "
                + "(select ta, tb, tc from tall) t"));
        Assertions.assertEquals(List.of("ta", "tb", "tc"),
                outputNames("select * replace (tb + 1 as tb, upper(ta) as ta) from (select ta, tb, tc from tall) t"));
        Assertions.assertEquals(List.of("a", "tb", "c"),
                outputNames("select * rename (ta as a, tc as c) from (select ta, tb, tc from tall) t"));
        Assertions.assertEquals(List.of("a"), outputNames("select t.* rename ta as a from (select ta from tall) t"));
        // all modifiers together, and a replaced column that is also renamed
        Assertions.assertEquals(List.of("a", "tc"), outputNames("select * ilike 't%' exclude (tb) "
                + "replace (lower(ta) as ta) rename (ta as a) from (select ta, tb, tc from tall) t"));

        String plan = getFragmentPlan("select * replace (tb * 2 as tb) from (select ta, tb from tall) t");
        assertContains(plan, "CAST(2: tb AS INT) * 2");

        // other select items, ORDER BY on the new names
        String sql = "select t.* rename (ta as a) replace (tb + 1 as tb), 1 as one from (select ta, tb from tall) t "
                + "order by tb, 1";
        Assertions.assertEquals(List.of("ta", "tb", "one"), outputNames("select t.* replace (tb + 1 as tb), 1 as one "
                + "from (select ta, tb from tall) t order by tb"));
        assertError(sql, "");
        plan = getFragmentPlan("select * replace (tb + 1 as tb) from (select ta, tb from tall) t order by tb desc");
        assertContains(plan, "order by: <slot 11> 11: expr DESC");
    }

    @Test
    public void testStarModifierErrors() {
        assertError("select * ilike 'zz%' from tall", "ILIKE 'zz%' matches no column");
        assertError("select * replace (1 as nope) from tall", "Column(s) [nope] in REPLACE are not in the expansion");
        assertError("select * rename (nope as x) from tall", "Column(s) [nope] in RENAME are not in the expansion");
        assertError("select * exclude (ta) rename (ta as x) from tall", "in RENAME are not in the expansion");
        assertError("select * replace (1 as ta, 2 as TA) from tall", "Column 'TA' is replaced more than once");
        assertError("select * rename (ta as x, ta as y) from tall", "Column 'ta' is renamed more than once");
    }

    // ---------------------------------------------------------------- predicates

    @Test
    public void testIlikeAndQuantifiedLike() throws Exception {
        String plan = getFragmentPlan("select ta from tall where ta ilike 'Ab%'");
        assertContains(plan, "lower(1: ta) LIKE 'ab%'");
        plan = getFragmentPlan("select ta from tall where ta not ilike 'Ab%'");
        assertContains(plan, "NOT (lower(1: ta) LIKE 'ab%')");

        plan = getFragmentPlan("select ta from tall where ta like any ('a%', '%b')");
        assertContains(plan, "(1: ta LIKE 'a%') OR (1: ta LIKE '%b')");
        plan = getFragmentPlan("select ta from tall where ta like all ('a%', '%b')");
        assertContains(plan, "1: ta LIKE 'a%', 1: ta LIKE '%b'");
        plan = getFragmentPlan("select ta from tall where ta ilike any ('A%', 'B%', 'C%')");
        assertContains(plan, "lower(1: ta) LIKE 'a%'");
        assertContains(plan, "lower(1: ta) LIKE 'c%'");
        plan = getFragmentPlan("select ta from tall where ta not like any ('a%', 'b%')");
        assertContains(plan, "(NOT (1: ta LIKE 'a%')) OR (NOT (1: ta LIKE 'b%'))");
        plan = getFragmentPlan("select ta from tall where ta not like all ('a%', 'b%')");
        assertContains(plan, "NOT (1: ta LIKE 'a%'), NOT (1: ta LIKE 'b%')");
        // a pattern can be any expression
        plan = getFragmentPlan("select ta from tall where ta like any (concat(tt, '%'), 'x')");
        assertContains(plan, "concat");

        // the new keywords stay usable as names
        getFragmentPlan("select ta as ilike, tb as any, tc as try_cast from tall");
        getFragmentPlan("select any_value(ta) from tall");
    }

    // ---------------------------------------------------------------- functions

    @Test
    public void testCompatFunctions() throws Exception {
        assertContains(getFragmentPlan("select iff(tb > 1, 'x', 'y') from tall"), "if(2: tb > 1, 'x', 'y')");
        assertContains(getFragmentPlan("select nvl2(ta, 1, 2) from tall"), "if(1: ta IS NOT NULL, 1, 2)");
        assertContains(getFragmentPlan("select zeroifnull(tc) from tall"), "ifnull(3: tc, 0)");
        assertContains(getFragmentPlan("select nullifzero(tc) from tall"), "nullif(3: tc, 0)");
        assertContains(getFragmentPlan("select div0(tc, td) from tall"),
                "if(4: td = 0, 0.0, CAST(3: tc AS DOUBLE) / CAST(4: td AS DOUBLE))");
        assertContains(getFragmentPlan("select div0null(tc, td) from tall"), "(4: td = 0) OR (4: td IS NULL)");
        assertContains(getFragmentPlan("select equal_null(ta, tt) from tall"), "<=>");
        assertContains(getFragmentPlan("select try_cast(ta as int) from tall"), "CAST(1: ta AS INT)");
        assertContains(getFragmentPlan("select try_to_number(ta) from tall"), "CAST(1: ta AS DECIMAL128(38,0))");
        assertContains(getFragmentPlan("select try_to_decimal(ta, 10, 2) from tall"), "CAST(1: ta AS DECIMAL64(10,2))");
        assertContains(getFragmentPlan("select try_to_double(ta) from tall"), "CAST(1: ta AS DOUBLE)");
        assertContains(getFragmentPlan("select try_to_boolean(ta) from tall"), "CAST(1: ta AS BOOLEAN)");
        assertContains(getFragmentPlan("select try_to_date(ta) from tall"), "CAST(1: ta AS DATE)");
        assertContains(getFragmentPlan("select try_to_date(ta, 'DD/MM/YYYY') from tall"),
                "str_to_date(1: ta, '%d/%m/%Y')");
        assertContains(getFragmentPlan("select try_to_timestamp(ta, 'YYYY-MM-DD HH24:MI:SS') from tall"),
                "str_to_date(1: ta, '%Y-%m-%d %H:%i:%s')");
        assertContains(getFragmentPlan("select try_to_timestamp(ta) from tall"), "CAST(1: ta AS DATETIME)");
        // names are case insensitive, functions nest
        assertContains(getFragmentPlan("select IFF(ZEROIFNULL(tc) = 0, 'zero', 'other') from tall"),
                "if(ifnull(3: tc, 0) = 0, 'zero', 'other')");
    }

    @Test
    public void testCompatFunctionErrors() {
        assertError("select iff(tb > 1, 'x') from tall", "IFF takes 3 arguments, but 2 were given");
        assertError("select div0(1) from tall", "DIV0 takes 2 arguments, but 1 were given");
        assertError("select try_to_decimal(ta, 50) from tall", "precision must be between 1 and 38");
        assertError("select try_to_decimal(ta, 5, 6) from tall", "scale must be between 0 and the precision");
        assertError("select try_to_decimal(ta, tb) from tall", "must be an integer literal");
        assertError("select try_to_date(ta, tt) from tall", "must be a string literal");
    }

    // ---------------------------------------------------------------- UNPIVOT

    @Test
    public void testUnpivot() throws Exception {
        String sql = "select * from (select ta, tb, tc, td from tall) t unpivot (v for k in (tb, tc, td))";
        Assertions.assertEquals(List.of("ta", "k", "v"), outputNames(sql));
        String plan = getFragmentPlan(sql);
        Assertions.assertEquals(1, count(plan, "OlapScanNode"), plan);
        assertContains(plan, "tableFunctionName: unnest");
        assertContains(plan, "IS NOT NULL");

        // labels, INCLUDE NULLS, an alias, and the result used by the outer query
        sql = "select u.k, sum(u.v) from (select ta, tb, tc from tall) t unpivot include nulls "
                + "(v for k in (tb as 'small', tc as \"int\")) as u group by u.k";
        plan = getFragmentPlan(sql);
        assertContains(plan, "['small','int']");
        Assertions.assertFalse(plan.contains("IS NOT NULL"), plan);

        // several value columns
        sql = "select * from (select ta, tb, tc, td, tg from tall) t "
                + "unpivot ((v1, v2) for k in ((tb, tc) as x, (td, tg) as y))";
        Assertions.assertEquals(List.of("ta", "k", "v1", "v2"), outputNames(sql));
        assertContains(getFragmentPlan(sql), "OR");

        // a table without a subquery, then WHERE and ORDER BY on the result
        sql = "select ta, k, v from tall unpivot (v for k in (tc, td)) where v > 0 order by ta, k";
        Assertions.assertEquals(List.of("ta", "k", "v"), outputNames(sql));
        getFragmentPlan(sql);
    }

    @Test
    public void testUnpivotErrors() {
        assertError("select * from tall unpivot ((v1, v2) for k in ((tb, tc), td))", "has 1 columns, but 2 value");
        assertError("select * from tall unpivot (v for k in (tb as a, tc as a))", "name 'a' is given more than once");
        assertError("select * from tall unpivot (v for v in (tb, tc))", "is also a value column");
        assertError("select * from tall unpivot (v for k in (nope))", "nope");
    }
}
