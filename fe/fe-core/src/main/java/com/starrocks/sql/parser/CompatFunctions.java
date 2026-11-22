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

package com.starrocks.sql.parser;

import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.IsNullPredicate;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;

import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Snowflake functions that are written with built-in StarRocks expressions while parsing, so they need no
 * backend support. A qualified name such as {@code db.iff} is a user-defined function and is not rewritten.
 */
final class CompatFunctions {
    private static final int MAX_DECIMAL_PRECISION = 38;

    static final Set<String> NAMES = Set.of("iff", "nvl2", "zeroifnull", "nullifzero", "div0", "div0null",
            "equal_null", "try_to_number", "try_to_numeric", "try_to_decimal", "try_to_double", "try_to_boolean",
            "try_to_date", "try_to_timestamp", "try_to_timestamp_ntz");

    private CompatFunctions() {
    }

    /**
     * Returns the rewritten expression for a Snowflake function call, or null when the name is not one of them.
     */
    static Expr rewrite(String name, List<Expr> args, NodePosition pos) {
        switch (name) {
            case "iff":
                checkArgs(name, args, 3, 3, pos);
                return call(FunctionSet.IF, pos, args.get(0), args.get(1), args.get(2));
            case "nvl2":
                // NVL2(e, a, b): a when e is not null, b otherwise.
                checkArgs(name, args, 3, 3, pos);
                return call(FunctionSet.IF, pos, new IsNullPredicate(args.get(0), true, pos), args.get(1), args.get(2));
            case "zeroifnull":
                checkArgs(name, args, 1, 1, pos);
                return call(FunctionSet.IFNULL, pos, args.get(0), new IntLiteral(0));
            case "nullifzero":
                checkArgs(name, args, 1, 1, pos);
                return call(FunctionSet.NULLIF, pos, args.get(0), new IntLiteral(0));
            case "div0":
                // DIV0(a, b): 0 when b is 0, a / b otherwise (NULL when b is NULL).
                checkArgs(name, args, 2, 2, pos);
                return call(FunctionSet.IF, pos, isZero(args.get(1), pos), new IntLiteral(0),
                        divide(args.get(0), args.get(1).clone(), pos));
            case "div0null":
                // DIV0NULL(a, b): 0 when b is 0 or NULL, a / b otherwise.
                checkArgs(name, args, 2, 2, pos);
                Expr zeroOrNull = new CompoundPredicate(CompoundPredicate.Operator.OR, isZero(args.get(1), pos),
                        new IsNullPredicate(args.get(1).clone(), false, pos), pos);
                return call(FunctionSet.IF, pos, zeroOrNull, new IntLiteral(0),
                        divide(args.get(0), args.get(1).clone(), pos));
            case "equal_null":
                checkArgs(name, args, 2, 2, pos);
                return new BinaryPredicate(BinaryType.EQ_FOR_NULL, args.get(0), args.get(1), pos);
            case "try_to_number":
            case "try_to_numeric":
            case "try_to_decimal":
                return toDecimal(name, args, pos);
            case "try_to_double":
                checkArgs(name, args, 1, 1, pos);
                return cast(args.get(0), FloatType.DOUBLE, pos);
            case "try_to_boolean":
                checkArgs(name, args, 1, 1, pos);
                return cast(args.get(0), BooleanType.BOOLEAN, pos);
            case "try_to_date":
                return toDateOrTimestamp(name, args, DateType.DATE, pos);
            case "try_to_timestamp":
            case "try_to_timestamp_ntz":
                return toDateOrTimestamp(name, args, DateType.DATETIME, pos);
            default:
                return null;
        }
    }

    private static Expr toDecimal(String name, List<Expr> args, NodePosition pos) {
        checkArgs(name, args, 1, 3, pos);
        int precision = MAX_DECIMAL_PRECISION;
        int scale = 0;
        if (args.size() >= 2) {
            precision = intLiteral(name, args.get(1), "precision", pos);
            if (precision < 1 || precision > MAX_DECIMAL_PRECISION) {
                throw new ParsingException(String.format("%s precision must be between 1 and %d", name,
                        MAX_DECIMAL_PRECISION), pos);
            }
        }
        if (args.size() == 3) {
            scale = intLiteral(name, args.get(2), "scale", pos);
            if (scale < 0 || scale > precision) {
                throw new ParsingException(String.format("%s scale must be between 0 and the precision", name), pos);
            }
        }
        return cast(args.get(0), TypeFactory.createUnifiedDecimalType(precision, scale), pos);
    }

    private static Expr toDateOrTimestamp(String name, List<Expr> args, ScalarType type, NodePosition pos) {
        checkArgs(name, args, 1, 2, pos);
        if (args.size() == 1) {
            return cast(args.get(0), type, pos);
        }
        if (!(args.get(1) instanceof StringLiteral)) {
            throw new ParsingException(String.format("The format of %s must be a string literal", name), pos);
        }
        String format = ((StringLiteral) args.get(1)).getStringValue();
        if (format.equalsIgnoreCase("AUTO")) {
            return cast(args.get(0), type, pos);
        }
        Expr parsed = call(FunctionSet.STR_TO_DATE, pos, args.get(0), new StringLiteral(toMysqlFormat(format, pos)));
        return type.isDate() ? cast(parsed, type, pos) : parsed;
    }

    /**
     * Converts a Snowflake date and time format such as {@code YYYY-MM-DD HH24:MI:SS} to a MySQL format of
     * {@code str_to_date}. Text in double quotes is kept as it is.
     */
    static String toMysqlFormat(String format, NodePosition pos) {
        StringBuilder out = new StringBuilder();
        String upper = format.toUpperCase(Locale.ROOT);
        int i = 0;
        while (i < format.length()) {
            char c = format.charAt(i);
            if (c == '"') {
                int end = format.indexOf('"', i + 1);
                if (end < 0) {
                    throw new ParsingException("Unterminated quoted text in format '" + format + "'", pos);
                }
                out.append(format.substring(i + 1, end).replace("%", "%%"));
                i = end + 1;
                continue;
            }
            String rest = upper.substring(i);
            String[][] elements = {
                    {"YYYY", "%Y"}, {"MMMM", "%M"}, {"HH24", "%H"}, {"HH12", "%h"}, {"FF9", "%f"}, {"FF6", "%f"},
                    {"FF3", "%f"}, {"MON", "%b"}, {"YY", "%y"}, {"MM", "%m"}, {"DD", "%d"}, {"DY", "%a"},
                    {"HH", "%H"}, {"MI", "%i"}, {"SS", "%s"}, {"FF", "%f"}, {"AM", "%p"}, {"PM", "%p"},
            };
            String converted = null;
            for (String[] element : elements) {
                if (rest.startsWith(element[0])) {
                    converted = element[1];
                    i += element[0].length();
                    break;
                }
            }
            if (converted != null) {
                out.append(converted);
            } else {
                out.append(c == '%' ? "%%" : String.valueOf(c));
                i++;
            }
        }
        return out.toString();
    }

    private static int intLiteral(String name, Expr expr, String what, NodePosition pos) {
        if (!(expr instanceof IntLiteral)) {
            throw new ParsingException(String.format("The %s of %s must be an integer literal", what, name), pos);
        }
        return (int) ((IntLiteral) expr).getValue();
    }

    private static void checkArgs(String name, List<Expr> args, int min, int max, NodePosition pos) {
        if (args.size() < min || args.size() > max) {
            String expected = min == max ? String.valueOf(min) : min + " to " + max;
            throw new ParsingException(String.format("%s takes %s arguments, but %d were given",
                    name.toUpperCase(Locale.ROOT), expected, args.size()), pos);
        }
    }

    private static Expr call(String name, NodePosition pos, Expr... args) {
        return new FunctionCallExpr(name, Lists.newArrayList(args), pos);
    }

    private static Expr cast(Expr expr, Type type, NodePosition pos) {
        return new CastExpr(new TypeDef(type), expr, pos);
    }

    private static Expr isZero(Expr expr, NodePosition pos) {
        return new BinaryPredicate(BinaryType.EQ, expr, new IntLiteral(0), pos);
    }

    private static Expr divide(Expr left, Expr right, NodePosition pos) {
        return new ArithmeticExpr(ArithmeticExpr.Operator.DIVIDE, left, right, pos);
    }
}
