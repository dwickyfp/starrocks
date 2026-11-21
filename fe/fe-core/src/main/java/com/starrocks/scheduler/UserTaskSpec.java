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

package com.starrocks.scheduler;

import com.google.common.base.Strings;
import com.starrocks.common.Config;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.SessionVariable;
import com.starrocks.scheduler.persist.TaskSchedule;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import org.quartz.CronExpression;

import java.text.ParseException;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Options and body of a Snowflake style task (source {@link Constants.TaskSource#USER_SQL}).
 *
 * <p>Task options are {@code KEY = value}. SCHEDULE, WAREHOUSE, COMMENT, USER_TASK_TIMEOUT_MS and
 * SUSPEND_TASK_AFTER_NUM_FAILURES are task options; any other key must be a session variable, which is set for
 * every run of the task.
 */
public final class UserTaskSpec {
    public static final String SCHEDULE = "SCHEDULE";
    public static final String WAREHOUSE = "WAREHOUSE";
    public static final String COMMENT = "COMMENT";
    public static final String USER_TASK_TIMEOUT_MS = "USER_TASK_TIMEOUT_MS";
    public static final String SUSPEND_TASK_AFTER_NUM_FAILURES = "SUSPEND_TASK_AFTER_NUM_FAILURES";
    // FINALIZE = <root task>: the task runs once after every run of that root's task graph.
    public static final String FINALIZE = "FINALIZE";
    // Kept with the options, but set only by the AFTER and WHEN clauses: the predecessor task names, comma
    // separated, and the SYSTEM$PREDECESSORS_STATE value.
    public static final String AFTER = "AFTER";
    public static final String WHEN = "WHEN";

    // Snowflake suspends a task after 10 consecutive failed runs by default.
    public static final int DEFAULT_SUSPEND_TASK_AFTER_NUM_FAILURES = 10;
    public static final int MAX_SCRIPT_STATEMENTS = 1000;

    private static final Pattern CRON_SCHEDULE =
            Pattern.compile("^\\s*USING\\s+CRON\\s+(.+?)\\s+(\\S+)\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern INTERVAL_SCHEDULE =
            Pattern.compile("^\\s*(\\d+)\\s*(SECONDS?|MINUTES?|HOURS?|DAYS?)\\s*$", Pattern.CASE_INSENSITIVE);

    private UserTaskSpec() {
    }

    /**
     * Validates the options and applies them to the task. Throws {@link SemanticException} on a bad option.
     */
    public static void applyOptions(Task task, Map<String, String> options) {
        Map<String, String> taskOptions = new HashMap<>(task.getTaskOptions());
        Map<String, String> properties = task.getProperties() == null ? new HashMap<>() : new HashMap<>(task.getProperties());
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey().toUpperCase(Locale.ROOT);
            String value = entry.getValue();
            switch (key) {
                case SCHEDULE:
                    TaskSchedule schedule = parseSchedule(value);
                    task.setSchedule(schedule);
                    task.setType(Constants.TaskType.PERIODICAL);
                    taskOptions.put(SCHEDULE, value.trim());
                    break;
                case WAREHOUSE:
                    if (!GlobalStateMgr.getCurrentState().getWarehouseMgr().warehouseExists(value)) {
                        throw new SemanticException("Warehouse " + value + " does not exist");
                    }
                    properties.put(PropertyAnalyzer.PROPERTIES_WAREHOUSE, value);
                    break;
                case COMMENT:
                    taskOptions.put(COMMENT, value);
                    break;
                case FINALIZE:
                    if (Strings.isNullOrEmpty(value) || value.trim().isEmpty()) {
                        throw new SemanticException("FINALIZE must name the root task of a task graph");
                    }
                    taskOptions.put(FINALIZE, value.trim());
                    break;
                case AFTER:
                case WHEN:
                    throw new SemanticException(key + " is a clause, not an option: write " + key
                            + " after the options, or use ALTER TASK ... "
                            + (key.equals(AFTER) ? "ADD AFTER" : "MODIFY WHEN"));
                case USER_TASK_TIMEOUT_MS:
                    taskOptions.put(USER_TASK_TIMEOUT_MS, String.valueOf(parsePositiveLong(key, value)));
                    break;
                case SUSPEND_TASK_AFTER_NUM_FAILURES:
                    long failures = parseNonNegativeLong(key, value);
                    if (failures > Integer.MAX_VALUE) {
                        throw new SemanticException(key + " is too large: " + value);
                    }
                    taskOptions.put(SUSPEND_TASK_AFTER_NUM_FAILURES, String.valueOf(failures));
                    break;
                default:
                    String variable = key.toLowerCase(Locale.ROOT);
                    if (!GlobalStateMgr.getCurrentState().getVariableMgr().containsVariable(variable)
                            || variable.equals(SessionVariable.WAREHOUSE_NAME)) {
                        throw new SemanticException("Unknown task option or session variable: " + entry.getKey());
                    }
                    properties.put(variable, value);
                    break;
            }
        }
        task.setTaskOptions(taskOptions);
        task.setProperties(properties);
    }

    /**
     * Removes options, back to their defaults. Removing SCHEDULE makes the task run only by EXECUTE TASK.
     */
    public static void unsetOptions(Task task, List<String> keys) {
        Map<String, String> taskOptions = new HashMap<>(task.getTaskOptions());
        Map<String, String> properties = task.getProperties() == null ? new HashMap<>() : new HashMap<>(task.getProperties());
        for (String rawKey : keys) {
            String key = rawKey.toUpperCase(Locale.ROOT);
            switch (key) {
                case SCHEDULE:
                    task.setSchedule(null);
                    task.setType(Constants.TaskType.MANUAL);
                    taskOptions.remove(SCHEDULE);
                    break;
                case WAREHOUSE:
                    properties.remove(PropertyAnalyzer.PROPERTIES_WAREHOUSE);
                    break;
                case COMMENT:
                case USER_TASK_TIMEOUT_MS:
                case SUSPEND_TASK_AFTER_NUM_FAILURES:
                case FINALIZE:
                    taskOptions.remove(key);
                    break;
                case AFTER:
                case WHEN:
                    throw new SemanticException("Use ALTER TASK ... REMOVE " + key + " to remove the " + key
                            + " clause");
                default:
                    String variable = key.toLowerCase(Locale.ROOT);
                    if (properties.remove(variable) == null) {
                        throw new SemanticException("Task option or session variable " + rawKey + " is not set");
                    }
                    break;
            }
        }
        task.setTaskOptions(taskOptions);
        task.setProperties(properties);
    }

    /**
     * The states a task's predecessors can be required to be in, as in Snowflake's SYSTEM$PREDECESSORS_STATE.
     * A predecessor that was skipped, because its own condition was not met, counts as neither failed nor
     * succeeded.
     */
    public enum PredecessorsState {
        // every predecessor succeeded (the default)
        ALL_SUCCEEDED,
        // at least one predecessor failed
        AT_LEAST_ONE_FAILED,
        // every predecessor failed
        ALL_FAILED,
        // every predecessor finished, whatever its state
        ALL_DONE,
        // no predecessor failed and at least one succeeded
        NONE_FAILED_MIN_ONE_SUCCEEDED;

        public boolean test(int succeeded, int failed, int skipped) {
            int total = succeeded + failed + skipped;
            switch (this) {
                case ALL_SUCCEEDED:
                    return total > 0 && succeeded == total;
                case AT_LEAST_ONE_FAILED:
                    return failed > 0;
                case ALL_FAILED:
                    return total > 0 && failed == total;
                case ALL_DONE:
                    return true;
                case NONE_FAILED_MIN_ONE_SUCCEEDED:
                    return failed == 0 && succeeded > 0;
                default:
                    return false;
            }
        }

        public static PredecessorsState parse(String value) {
            try {
                return valueOf(value.trim().toUpperCase(Locale.ROOT));
            } catch (Exception e) {
                throw new SemanticException("Unknown SYSTEM$PREDECESSORS_STATE value '" + value + "', expected one of "
                        + Arrays.toString(values()));
            }
        }
    }

    public static List<String> getPredecessors(Task task) {
        String after = task.getTaskOption(AFTER);
        if (Strings.isNullOrEmpty(after)) {
            return List.of();
        }
        return List.of(after.split(","));
    }

    /**
     * Sets the predecessors, in the order given, without duplicates. An empty list makes the task standalone.
     */
    public static void setPredecessors(Task task, List<String> predecessors) {
        Map<String, String> taskOptions = new HashMap<>(task.getTaskOptions());
        List<String> names = predecessors.stream().distinct().collect(Collectors.toList());
        if (names.isEmpty()) {
            taskOptions.remove(AFTER);
        } else {
            taskOptions.put(AFTER, String.join(",", names));
        }
        task.setTaskOptions(taskOptions);
    }

    public static PredecessorsState getCondition(Task task) {
        String when = task.getTaskOption(WHEN);
        return when == null ? PredecessorsState.ALL_SUCCEEDED : PredecessorsState.valueOf(when);
    }

    /**
     * Sets the WHEN condition; null removes it, which means ALL_SUCCEEDED.
     */
    public static void setCondition(Task task, String condition) {
        Map<String, String> taskOptions = new HashMap<>(task.getTaskOptions());
        if (condition == null) {
            taskOptions.remove(WHEN);
        } else {
            taskOptions.put(WHEN, PredecessorsState.parse(condition).name());
        }
        task.setTaskOptions(taskOptions);
    }

    public static String getFinalizeRoot(Task task) {
        return task.getTaskOption(FINALIZE);
    }

    /**
     * Parses the body to check its syntax. Objects are not resolved here: like Snowflake, a statement may use a
     * table that an earlier statement of the same script creates, so names are only checked when the task runs.
     */
    public static List<StatementBase> parseBody(String body, SessionVariable sessionVariable) {
        if (Strings.isNullOrEmpty(body) || body.trim().isEmpty()) {
            throw new SemanticException("Task body is empty");
        }
        List<StatementBase> statements;
        try {
            statements = SqlParser.parse(body, sessionVariable);
        } catch (Exception e) {
            throw new SemanticException("Invalid task body: " + e.getMessage());
        }
        if (statements.isEmpty()) {
            throw new SemanticException("Task body is empty");
        }
        if (statements.size() > MAX_SCRIPT_STATEMENTS) {
            throw new SemanticException("Task body has " + statements.size() + " statements, more than the limit "
                    + MAX_SCRIPT_STATEMENTS);
        }
        for (StatementBase statement : statements) {
            if (statement instanceof ExecuteAsStmt) {
                throw new SemanticException("EXECUTE AS is not allowed in a task: a task always runs as its owner");
            }
        }
        return statements;
    }

    /**
     * Parses a Snowflake schedule: {@code '<n> MINUTE'} (also SECOND, HOUR, DAY) or
     * {@code 'USING CRON <minute> <hour> <day of month> <month> <day of week> <time zone>'}.
     */
    public static TaskSchedule parseSchedule(String value) {
        if (value == null) {
            throw new SemanticException("SCHEDULE is empty");
        }
        Matcher cron = CRON_SCHEDULE.matcher(value);
        if (cron.matches()) {
            String expression = cron.group(1).trim().replaceAll("\\s+", " ");
            String timeZone = cron.group(2);
            ZoneId zoneId;
            try {
                zoneId = ZoneId.of(timeZone);
            } catch (DateTimeException e) {
                throw new SemanticException("Invalid time zone in SCHEDULE: " + timeZone);
            }
            // Validate now so that a bad expression is rejected by CREATE / ALTER, not when the task is resumed.
            toQuartzCron(expression, zoneId);
            return TaskSchedule.ofCron(expression, zoneId.getId());
        }
        Matcher interval = INTERVAL_SCHEDULE.matcher(value);
        if (interval.matches()) {
            long period = parsePositiveLong(SCHEDULE, interval.group(1));
            String unit = interval.group(2).toUpperCase(Locale.ROOT);
            TimeUnit timeUnit;
            if (unit.startsWith("SECOND")) {
                timeUnit = TimeUnit.SECONDS;
            } else if (unit.startsWith("MINUTE")) {
                timeUnit = TimeUnit.MINUTES;
            } else if (unit.startsWith("HOUR")) {
                timeUnit = TimeUnit.HOURS;
            } else {
                timeUnit = TimeUnit.DAYS;
            }
            if (timeUnit.toSeconds(period) < Config.task_min_schedule_interval_s) {
                throw new SemanticException("SCHEDULE interval is too small, the minimum value is "
                        + Config.task_min_schedule_interval_s + " SECONDS");
            }
            return new TaskSchedule(TimeUtils.getEpochSeconds(), period, timeUnit);
        }
        throw new SemanticException("Invalid SCHEDULE '" + value + "', expected '<n> MINUTE' or "
                + "'USING CRON <minute> <hour> <day of month> <month> <day of week> <time zone>'");
    }

    /**
     * Converts a 5 field Unix cron expression to Quartz, which has a leading seconds field, numbers days of the week
     * 1-7 from Sunday (Unix: 0-7, both 0 and 7 are Sunday) and needs '?' in one of the two day fields.
     */
    public static CronExpression toQuartzCron(String expression, ZoneId zoneId) {
        String[] fields = expression.trim().split("\\s+");
        if (fields.length != 5) {
            throw new SemanticException("Invalid cron expression '" + expression
                    + "', expected 5 fields: <minute> <hour> <day of month> <month> <day of week>");
        }
        String dayOfMonth = fields[2];
        String dayOfWeek = toQuartzDayOfWeek(fields[4], expression);
        if (dayOfWeek.equals("*")) {
            dayOfWeek = "?";
        } else if (dayOfMonth.equals("*")) {
            dayOfMonth = "?";
        } else {
            throw new SemanticException("Invalid cron expression '" + expression
                    + "': day of month and day of week cannot both be set");
        }
        String quartz = String.join(" ", "0", fields[0], fields[1], dayOfMonth, fields[3], dayOfWeek);
        try {
            CronExpression cronExpression = new CronExpression(quartz);
            cronExpression.setTimeZone(TimeZone.getTimeZone(zoneId));
            return cronExpression;
        } catch (ParseException e) {
            throw new SemanticException("Invalid cron expression '" + expression + "': " + e.getMessage());
        }
    }

    private static String toQuartzDayOfWeek(String field, String expression) {
        if (!field.matches(".*\\d.*")) {
            return field;
        }
        // Map each number n (Unix, 0 or 7 = Sunday) to Quartz (1 = Sunday): n % 7 + 1. Step values after '/' are counts,
        // not days, so they are kept.
        StringBuilder result = new StringBuilder();
        for (String part : field.split(",", -1)) {
            if (result.length() > 0) {
                result.append(',');
            }
            String step = "";
            int slash = part.indexOf('/');
            if (slash >= 0) {
                step = part.substring(slash);
                part = part.substring(0, slash);
            }
            String[] range = part.split("-", -1);
            if (range.length > 2) {
                throw new SemanticException("Invalid day of week in cron expression '" + expression + "'");
            }
            Integer previous = null;
            for (int i = 0; i < range.length; i++) {
                String day = range[i];
                if (i > 0) {
                    result.append('-');
                }
                if (day.matches("\\d+")) {
                    int n = Integer.parseInt(day);
                    if (n > 7) {
                        throw new SemanticException("Invalid day of week " + n + " in cron expression '" + expression + "'");
                    }
                    int quartzDay = n % 7 + 1;
                    // A Unix range ending on Sunday (7), like 5-7, would wrap around in Quartz.
                    if (previous != null && quartzDay < previous) {
                        throw new SemanticException("Day of week range " + part + " in cron expression '" + expression
                                + "' wraps around Sunday, use a list such as 5,6,0");
                    }
                    previous = quartzDay;
                    result.append(quartzDay);
                } else {
                    result.append(day);
                }
            }
            result.append(step);
        }
        return result.toString();
    }

    /**
     * The next time a cron schedule fires strictly after the given time, in epoch milliseconds, or -1 if never.
     */
    public static long nextCronFireTimeMs(TaskSchedule schedule, long afterMs) {
        CronExpression cron = toQuartzCron(schedule.getCronExpression(), ZoneId.of(schedule.getCronTimeZone()));
        Date next = cron.getNextValidTimeAfter(new Date(afterMs));
        return next == null ? -1 : next.getTime();
    }

    public static long getUserTaskTimeoutMs(Task task) {
        String value = task.getTaskOption(USER_TASK_TIMEOUT_MS);
        return value == null ? 0 : Long.parseLong(value);
    }

    public static int getSuspendAfterNumFailures(Task task) {
        String value = task.getTaskOption(SUSPEND_TASK_AFTER_NUM_FAILURES);
        return value == null ? DEFAULT_SUSPEND_TASK_AFTER_NUM_FAILURES : Integer.parseInt(value);
    }

    private static long parsePositiveLong(String key, String value) {
        long result = parseNonNegativeLong(key, value);
        if (result == 0) {
            throw new SemanticException(key + " must be greater than 0: " + value);
        }
        return result;
    }

    private static long parseNonNegativeLong(String key, String value) {
        try {
            long result = Long.parseLong(value.trim());
            if (result < 0) {
                throw new SemanticException(key + " must not be negative: " + value);
            }
            return result;
        } catch (NumberFormatException e) {
            throw new SemanticException(key + " must be a number: " + value);
        }
    }
}
