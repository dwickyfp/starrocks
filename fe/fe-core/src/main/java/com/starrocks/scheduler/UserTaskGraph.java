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
import com.starrocks.common.DdlException;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Task graphs of Snowflake style tasks.
 *
 * <p>A task with AFTER is a child task: it has no schedule and runs when its predecessors finish in a run of
 * the graph. A task with no predecessors that other tasks run after is the root task of the graph; only the root
 * has a schedule. A task with FINALIZE = root runs once after all the other tasks of each run of that graph.
 * All the tasks of a graph are in the same database and have the same owner, and a graph has one root.
 */
public final class UserTaskGraph {
    public static final int MAX_PREDECESSORS = 100;
    public static final int MAX_GRAPH_TASKS = 1000;

    private UserTaskGraph() {
    }

    /**
     * Checks that the task, with its new definition, fits into the graphs of the other tasks.
     *
     * @param task   the new definition of the task
     * @param lookup the current tasks by name; the task itself, if it exists, is its old definition
     * @param all    all the current tasks
     */
    public static void validate(Task task, Function<String, Task> lookup, Collection<Task> all) throws DdlException {
        String name = task.getName();
        List<String> predecessors = UserTaskSpec.getPredecessors(task);
        String finalizeRoot = UserTaskSpec.getFinalizeRoot(task);
        boolean scheduled = task.getSchedule() != null;

        if (task.getTaskOption(UserTaskSpec.WHEN) != null && predecessors.isEmpty()) {
            throw new DdlException("Task [" + name + "] has a WHEN condition but no AFTER: only a child task, "
                    + "which runs after other tasks, can have a condition on its predecessors");
        }
        if (!predecessors.isEmpty()) {
            if (scheduled) {
                throw new DdlException("Task [" + name + "] cannot have both SCHEDULE and AFTER: a child task "
                        + "runs when its predecessors finish, only the root task of a task graph is scheduled");
            }
            if (finalizeRoot != null) {
                throw new DdlException("Finalizer task [" + name + "] cannot have AFTER");
            }
            if (predecessors.size() > MAX_PREDECESSORS) {
                throw new DdlException("Task [" + name + "] has " + predecessors.size()
                        + " predecessors, more than the limit " + MAX_PREDECESSORS);
            }
            for (String predecessorName : predecessors) {
                if (predecessorName.equals(name)) {
                    throw new DdlException("Task [" + name + "] cannot run after itself");
                }
                Task predecessor = checkSameGraphTask(task, predecessorName, lookup, "Predecessor");
                if (UserTaskSpec.getFinalizeRoot(predecessor) != null) {
                    throw new DdlException("Finalizer task [" + predecessorName + "] cannot have child tasks");
                }
            }
            checkNoCycle(name, predecessors, lookup);
            for (Task other : all) {
                if (!other.getName().equals(name) && name.equals(UserTaskSpec.getFinalizeRoot(other))) {
                    throw new DdlException("Task [" + name + "] is the root task of the finalizer task ["
                            + other.getName() + "] and cannot run after other tasks");
                }
            }

            Set<String> roots = predecessors.stream()
                    .map(predecessor -> rootOf(lookup.apply(predecessor), lookup).getName())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            if (roots.size() > 1) {
                throw new DdlException("The predecessors of task [" + name + "] are in different task graphs, "
                        + "with the root tasks " + roots + ": a task graph has one root task");
            }
            String root = roots.iterator().next();
            Set<String> graph = graphTasks(root, all);
            graph.add(name);
            graph.addAll(graphTasks(name, all));
            if (graph.size() > MAX_GRAPH_TASKS) {
                throw new DdlException("Task graph of root task [" + root + "] would have " + graph.size()
                        + " tasks, more than the limit " + MAX_GRAPH_TASKS);
            }
        }

        if (finalizeRoot != null) {
            if (scheduled) {
                throw new DdlException("Finalizer task [" + name + "] cannot have a SCHEDULE: it runs after the "
                        + "other tasks of its task graph");
            }
            if (finalizeRoot.equals(name)) {
                throw new DdlException("Task [" + name + "] cannot finalize itself");
            }
            Task root = checkSameGraphTask(task, finalizeRoot, lookup, "Root");
            if (!UserTaskSpec.getPredecessors(root).isEmpty() || UserTaskSpec.getFinalizeRoot(root) != null) {
                throw new DdlException("FINALIZE of task [" + name + "] must name a root task, but [" + finalizeRoot
                        + "] runs after other tasks");
            }
            for (Task other : all) {
                if (!other.getName().equals(name) && finalizeRoot.equals(UserTaskSpec.getFinalizeRoot(other))) {
                    throw new DdlException("Root task [" + finalizeRoot + "] already has the finalizer task ["
                            + other.getName() + "]");
                }
            }
        }

        if (finalizeRoot != null) {
            for (Task other : all) {
                if (UserTaskSpec.getPredecessors(other).contains(name)) {
                    throw new DdlException("Finalizer task [" + name + "] cannot have child tasks, but ["
                            + other.getName() + "] runs after it");
                }
            }
        }
    }

    private static Task checkSameGraphTask(Task task, String otherName, Function<String, Task> lookup, String role)
            throws DdlException {
        Task other = lookup.apply(otherName);
        if (other == null) {
            throw new DdlException(role + " task [" + otherName + "] of task [" + task.getName() + "] does not exist");
        }
        if (!other.isUserSqlTask()) {
            throw new DdlException(role + " task [" + otherName + "] was not created by CREATE TASK");
        }
        if (!Objects.equals(Strings.nullToEmpty(other.getDbName()).toLowerCase(),
                Strings.nullToEmpty(task.getDbName()).toLowerCase())) {
            throw new DdlException(role + " task [" + otherName + "] is in database " + other.getDbName()
                    + ", but the tasks of a task graph must be in the same database as [" + task.getName() + "] ("
                    + task.getDbName() + ")");
        }
        if (!Objects.equals(ownerOf(other), ownerOf(task))) {
            throw new DdlException(role + " task [" + otherName + "] is owned by " + ownerOf(other)
                    + ", but the tasks of a task graph must have the same owner as [" + task.getName() + "] ("
                    + ownerOf(task) + ")");
        }
        return other;
    }

    private static void checkNoCycle(String name, List<String> predecessors, Function<String, Task> lookup)
            throws DdlException {
        Deque<String> pending = new ArrayDeque<>(predecessors);
        Set<String> seen = new HashSet<>();
        while (!pending.isEmpty()) {
            String current = pending.pop();
            if (current.equals(name)) {
                throw new DdlException("Task [" + name + "] cannot run after " + predecessors
                        + ": that would make a cycle in the task graph");
            }
            if (!seen.add(current)) {
                continue;
            }
            Task task = lookup.apply(current);
            if (task != null) {
                pending.addAll(UserTaskSpec.getPredecessors(task));
            }
        }
    }

    public static String ownerOf(Task task) {
        return task.getUserIdentity() != null ? task.getUserIdentity().toString()
                : Strings.nullToEmpty(task.getCreateUser());
    }

    /**
     * The root task of the graph a task belongs to: itself for a standalone or root task, the FINALIZE root for
     * a finalizer.
     */
    public static Task rootOf(Task task, Function<String, Task> lookup) {
        String finalizeRoot = UserTaskSpec.getFinalizeRoot(task);
        if (finalizeRoot != null) {
            Task root = lookup.apply(finalizeRoot);
            return root == null ? task : root;
        }
        Task current = task;
        for (int i = 0; i <= MAX_GRAPH_TASKS; i++) {
            List<String> predecessors = UserTaskSpec.getPredecessors(current);
            if (predecessors.isEmpty()) {
                return current;
            }
            Task predecessor = lookup.apply(predecessors.get(0));
            if (predecessor == null) {
                return current;
            }
            current = predecessor;
        }
        return current;
    }

    /**
     * Whether the task is in a task graph and is not its root: a child or a finalizer task.
     */
    public static boolean isChildOrFinalizer(Task task) {
        return !UserTaskSpec.getPredecessors(task).isEmpty() || UserTaskSpec.getFinalizeRoot(task) != null;
    }

    /**
     * The tasks that run after the given task, directly or not, including itself.
     */
    public static Set<String> graphTasks(String name, Collection<Task> all) {
        Set<String> result = new LinkedHashSet<>();
        result.add(name);
        boolean changed = true;
        while (changed) {
            changed = false;
            for (Task task : all) {
                if (!result.contains(task.getName()) && task.isUserSqlTask()
                        && UserTaskSpec.getPredecessors(task).stream().anyMatch(result::contains)) {
                    result.add(task.getName());
                    changed = true;
                }
            }
        }
        return result;
    }
}
