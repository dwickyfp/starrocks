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

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.scheduler.history.TaskRunHistory;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Runs the task graphs of Snowflake style tasks on the leader.
 *
 * <p>When a run of a root task finishes, successfully or not, a run of its graph starts: the resumed child tasks
 * reachable from the root through resumed tasks take part. Each child is decided once all its predecessors that
 * take part have finished: it runs if its WHEN condition (SYSTEM$PREDECESSORS_STATE, by default ALL_SUCCEEDED) holds
 * on their states, and is skipped otherwise. After every child is decided and finished, the finalizer task of the
 * root, if it is resumed, runs. A run of the root while its graph still runs is skipped (Snowflake's default
 * NO_OVERLAP policy).
 *
 * <p>With TASK_AUTO_RETRY_ATTEMPTS = n on the root, a failed run of the root, a child or the finalizer runs again,
 * up to n more times, before it counts as failed. A child that is skipped gets a SKIPPED run in the task run
 * history. A run may set a value with CALL SYSTEM$SET_RETURN_VALUE(...), which the tasks after it read with
 * SYSTEM$GET_PREDECESSOR_RETURN_VALUE(...).
 *
 * <p>Graph runs are kept in memory: after a leader change the graph runs in progress stop, and the next run of
 * each root starts a new one.
 *
 * <p>All graph run state is changed on one thread.
 */
public class UserTaskGraphRunner {
    private static final Logger LOG = LogManager.getLogger(UserTaskGraphRunner.class);

    public enum RunState {
        RUNNING,
        SUCCEEDED,
        FAILED,
        SKIPPED;

        boolean isDone() {
            return this != RUNNING;
        }
    }

    private final TaskManager taskManager;
    private final ExecutorService executor =
            ThreadPoolManager.newDaemonFixedThreadPool(1, Integer.MAX_VALUE, "user-task-graph", false);
    // root task name -> its graph run in progress
    private final Map<String, GraphRun> graphRuns = new ConcurrentHashMap<>();
    // root task name -> the task states of its last finished graph run
    private final Map<String, Map<String, RunState>> lastGraphRuns = new ConcurrentHashMap<>();
    // query id of a child or finalizer run -> its graph run, used only by the executor thread
    private final Map<String, GraphRun> runsByQueryId = new HashMap<>();
    // query id of a retry of a root run -> the retry attempt, used only by the executor thread
    private final Map<String, Integer> rootRetries = new HashMap<>();

    public UserTaskGraphRunner(TaskManager taskManager) {
        this.taskManager = taskManager;
    }

    /**
     * Called when a run of a Snowflake style task finishes.
     */
    public void onTaskRunFinished(TaskRun taskRun) {
        Task task = taskRun.getTask();
        TaskRunStatus status = taskRun.getStatus();
        if (task == null || !task.isUserSqlTask() || status == null || status.getQueryId() == null) {
            return;
        }
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            return;
        }
        String taskName = task.getName();
        String queryId = status.getQueryId();
        RunState state = toRunState(status.getState());
        String error = status.getErrorMessage();
        String returnValue = status.getUserTaskReturnValue();
        try {
            executor.execute(() -> handleFinished(taskName, queryId, state, error, returnValue));
        } catch (Exception e) {
            LOG.warn("failed to hand the finished run {} of task {} to the task graph runner", queryId, taskName, e);
        }
    }

    /**
     * Whether a run of the graph of this root task is in progress. Runs whose task run is gone, such as a pending
     * run that was dropped, count as failed.
     */
    public boolean isGraphRunning(String rootName) {
        if (!graphRuns.containsKey(rootName)) {
            return false;
        }
        Future<Boolean> result = executor.submit(() -> {
            GraphRun graphRun = graphRuns.get(rootName);
            if (graphRun != null) {
                reconcile(graphRun);
            }
            return graphRuns.containsKey(rootName);
        });
        try {
            return result.get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.warn("failed to check the task graph run of root task {}", rootName, e);
            return true;
        }
    }

    /**
     * The task states of the graph run in progress of this root task, or null if there is none.
     */
    public Map<String, RunState> getGraphRunStates(String rootName) {
        GraphRun graphRun = graphRuns.get(rootName);
        return graphRun == null ? null : new LinkedHashMap<>(graphRun.states);
    }

    /**
     * The task states of the last finished graph run of this root task, or null if there is none.
     */
    public Map<String, RunState> getLastGraphRunStates(String rootName) {
        Map<String, RunState> states = lastGraphRuns.get(rootName);
        return states == null ? null : new LinkedHashMap<>(states);
    }

    /**
     * The value that a predecessor of the task run {@code queryId} set with SYSTEM$SET_RETURN_VALUE in the same
     * graph run, or null if it set none. With no predecessor name, the task must have exactly one predecessor.
     * Throws {@link IllegalArgumentException} when the run is not part of a graph run or the name is not one of its
     * predecessors.
     */
    public String getPredecessorReturnValue(String queryId, String taskName, String predecessor) {
        Future<String> result = executor.submit(() -> {
            GraphRun graphRun = runsByQueryId.get(queryId);
            if (graphRun == null) {
                throw new IllegalArgumentException("SYSTEM$GET_PREDECESSOR_RETURN_VALUE is only available in a run of "
                        + "a child task or a finalizer task started by its task graph");
            }
            List<String> candidates = graphRun.root.equals(taskName) ? List.of()
                    : taskName.equals(graphRun.finalizer) ? graphRun.states.keySet().stream()
                            .filter(name -> !name.equals(taskName)).collect(Collectors.toList())
                    : graphRun.predecessors.getOrDefault(taskName, List.of());
            String name = predecessor;
            if (name == null) {
                if (candidates.size() != 1) {
                    throw new IllegalArgumentException("Task " + taskName + " has " + candidates.size()
                            + " predecessors: name the one to read in SYSTEM$GET_PREDECESSOR_RETURN_VALUE('<task>')");
                }
                name = candidates.get(0);
            }
            String found = null;
            for (String candidate : candidates) {
                if (candidate.equalsIgnoreCase(name)) {
                    found = candidate;
                }
            }
            if (found == null) {
                throw new IllegalArgumentException("Task " + name + " is not a predecessor of task " + taskName);
            }
            return graphRun.returnValues.get(found);
        });
        try {
            return result.get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IllegalArgumentException iae) {
                throw iae;
            }
            throw new IllegalStateException("failed to read the return value of a predecessor task", e.getCause());
        } catch (Exception e) {
            throw new IllegalStateException("failed to read the return value of a predecessor task", e);
        }
    }

    @VisibleForTesting
    void awaitIdle() throws Exception {
        executor.submit(() -> { }).get(60, TimeUnit.SECONDS);
    }

    private static RunState toRunState(Constants.TaskRunState state) {
        if (state == null) {
            return RunState.FAILED;
        }
        switch (state) {
            case SUCCESS:
            case SKIPPED:
                return RunState.SUCCEEDED;
            case MERGED:
                // a merged run never ran; the run it was merged into reports for it
                return null;
            default:
                return RunState.FAILED;
        }
    }

    private void handleFinished(String taskName, String queryId, RunState state, String error, String returnValue) {
        if (state == null) {
            return;
        }
        GraphRun graphRun = runsByQueryId.remove(queryId);
        if (graphRun != null) {
            if (state == RunState.FAILED && retry(graphRun, taskName, error)) {
                return;
            }
            if (returnValue != null) {
                graphRun.returnValues.put(taskName, returnValue);
            }
            graphRun.finish(taskName, state, error);
            advance(graphRun);
            return;
        }
        Task task = taskManager.getTask(taskName);
        int attempt = rootRetries.getOrDefault(queryId, 0);
        rootRetries.remove(queryId);
        if (task == null || !task.isUserSqlTask() || UserTaskGraph.isChildOrFinalizer(task)) {
            // a child or finalizer run by EXECUTE TASK runs alone
            return;
        }
        if (state == RunState.FAILED && attempt < UserTaskSpec.getAutoRetryAttempts(task)
                && !graphRuns.containsKey(taskName)) {
            SubmitResult retry = submitQuietly(taskName);
            if (retry != null) {
                rootRetries.put(retry.getQueryId(), attempt + 1);
                LOG.info("task {} failed ({}), retry {} of {}, query id {}", taskName, error, attempt + 1,
                        UserTaskSpec.getAutoRetryAttempts(task), retry.getQueryId());
                return;
            }
        }
        if (graphRuns.containsKey(taskName)) {
            LOG.info("task graph of root task {} is still running, run {} of the root does not start another one",
                    taskName, queryId);
            return;
        }
        GraphRun newRun = plan(task, queryId, state);
        if (newRun != null && returnValue != null) {
            newRun.returnValues.put(taskName, returnValue);
        }
        if (newRun == null) {
            return;
        }
        lastGraphRuns.remove(taskName);
        graphRuns.put(taskName, newRun);
        LOG.info("start task graph run {} of root task {} ({}), tasks: {}, finalizer: {}", newRun.id, taskName, state,
                newRun.predecessors.keySet(), newRun.finalizer);
        advance(newRun);
    }

    /**
     * Plans a run of the graph of the root task, or returns null if the root has no resumed child or finalizer.
     */
    private GraphRun plan(Task root, String rootQueryId, RunState rootState) {
        List<Task> tasks = taskManager.filterTasks(null).stream()
                .filter(Task::isUserSqlTask)
                .collect(Collectors.toList());
        Set<String> members = new LinkedHashSet<>();
        members.add(root.getName());
        boolean changed = true;
        while (changed) {
            changed = false;
            for (Task task : tasks) {
                if (!members.contains(task.getName()) && task.getState() == Constants.TaskState.ACTIVE
                        && UserTaskSpec.getPredecessors(task).stream().anyMatch(members::contains)) {
                    members.add(task.getName());
                    changed = true;
                }
            }
        }
        // A suspended task does not take part, and does not count among the predecessors of the tasks after it.
        Map<String, List<String>> predecessors = new LinkedHashMap<>();
        for (Task task : tasks) {
            if (members.contains(task.getName()) && !task.getName().equals(root.getName())) {
                predecessors.put(task.getName(), UserTaskSpec.getPredecessors(task).stream()
                        .filter(members::contains)
                        .collect(Collectors.toList()));
            }
        }
        String finalizer = tasks.stream()
                .filter(task -> root.getName().equals(UserTaskSpec.getFinalizeRoot(task)))
                .filter(task -> task.getState() == Constants.TaskState.ACTIVE)
                .map(Task::getName)
                .findFirst().orElse(null);
        if (predecessors.isEmpty() && finalizer == null) {
            return null;
        }
        GraphRun graphRun = new GraphRun(root.getName(), predecessors, finalizer);
        graphRun.states.put(root.getName(), rootState);
        graphRun.queryIds.put(root.getName(), rootQueryId);
        return graphRun;
    }

    private void advance(GraphRun graphRun) {
        boolean changed = true;
        while (changed) {
            changed = false;
            for (Map.Entry<String, List<String>> entry : graphRun.predecessors.entrySet()) {
                String name = entry.getKey();
                if (graphRun.states.containsKey(name)) {
                    continue;
                }
                int succeeded = 0;
                int failed = 0;
                int skipped = 0;
                boolean ready = true;
                for (String predecessor : entry.getValue()) {
                    RunState state = graphRun.states.get(predecessor);
                    if (state == null || !state.isDone()) {
                        ready = false;
                        break;
                    }
                    if (state == RunState.SUCCEEDED) {
                        succeeded++;
                    } else if (state == RunState.FAILED) {
                        failed++;
                    } else {
                        skipped++;
                    }
                }
                if (!ready) {
                    continue;
                }
                changed = true;
                Task task = taskManager.getTask(name);
                if (task == null || task.getState() != Constants.TaskState.ACTIVE) {
                    skip(graphRun, name, "the task was dropped or suspended");
                    continue;
                }
                UserTaskSpec.PredecessorsState condition = UserTaskSpec.getCondition(task);
                if (!condition.test(succeeded, failed, skipped)) {
                    skip(graphRun, name, String.format("SYSTEM$PREDECESSORS_STATE('%s') is false: %d succeeded, "
                            + "%d failed, %d skipped", condition, succeeded, failed, skipped));
                    continue;
                }
                String blocked = UserTaskCondition.blockingReason(task);
                if (blocked != null) {
                    skip(graphRun, name, blocked);
                    continue;
                }
                submit(graphRun, name);
            }
        }

        if (graphRun.isRunning()) {
            return;
        }
        if (graphRun.finalizer != null && !graphRun.states.containsKey(graphRun.finalizer)) {
            Task finalizer = taskManager.getTask(graphRun.finalizer);
            if (finalizer != null && finalizer.getState() == Constants.TaskState.ACTIVE
                    && submit(graphRun, graphRun.finalizer)) {
                return;
            }
            skip(graphRun, graphRun.finalizer, "the finalizer task was dropped or suspended");
        }
        lastGraphRuns.put(graphRun.root, new LinkedHashMap<>(graphRun.states));
        graphRuns.remove(graphRun.root, graphRun);
        graphRun.queryIds.values().forEach(runsByQueryId::remove);
        LOG.info("finish task graph run {} of root task {}: {}", graphRun.id, graphRun.root, graphRun.states);
    }

    /**
     * Runs a failed task of the graph run again if the root's TASK_AUTO_RETRY_ATTEMPTS allows it.
     */
    private boolean retry(GraphRun graphRun, String name, String error) {
        int allowed = UserTaskSpec.getAutoRetryAttempts(taskManager.getTask(graphRun.root));
        int attempt = graphRun.attempts.getOrDefault(name, 0);
        Task task = taskManager.getTask(name);
        if (attempt >= allowed || task == null || task.getState() != Constants.TaskState.ACTIVE) {
            return false;
        }
        graphRun.attempts.put(name, attempt + 1);
        LOG.info("task {} of task graph run {} failed ({}), retry {} of {}", name, graphRun.id, error, attempt + 1,
                allowed);
        return submit(graphRun, name);
    }

    private SubmitResult submitQuietly(String name) {
        try {
            SubmitResult result = taskManager.executeTask(name);
            if (result != null && result.getStatus() == SubmitResult.SubmitStatus.SUBMITTED
                    && result.getQueryId() != null) {
                return result;
            }
        } catch (Exception e) {
            LOG.warn("failed to run task {} again", name, e);
        }
        return null;
    }

    /**
     * Skips a task of the graph run, and records a SKIPPED run of it in the task run history.
     */
    private void skip(GraphRun graphRun, String name, String reason) {
        graphRun.skip(name, reason);
        Task task = taskManager.getTask(name);
        if (task == null) {
            return;
        }
        try {
            TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
            TaskRunStatus status = taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
            status.setState(Constants.TaskRunState.SKIPPED);
            status.setErrorMessage("Skipped by task graph run " + graphRun.id + " of root task " + graphRun.root
                    + ": " + reason);
            status.setFinishTime(System.currentTimeMillis());
            TaskRunHistory history = taskManager.getTaskRunHistory();
            GlobalStateMgr.getCurrentState().getEditLog().logTaskRunCreateStatus(status, wal -> history.addHistory(status));
        } catch (Exception e) {
            LOG.warn("failed to record the skipped run of task {} in task graph run {}", name, graphRun.id, e);
        }
    }

    private boolean submit(GraphRun graphRun, String name) {
        SubmitResult result;
        try {
            result = taskManager.executeTask(name);
        } catch (Exception e) {
            LOG.warn("failed to run task {} of task graph run {}", name, graphRun.id, e);
            result = null;
        }
        if (result == null || result.getStatus() != SubmitResult.SubmitStatus.SUBMITTED || result.getQueryId() == null) {
            graphRun.finish(name, RunState.FAILED, "failed to submit the task run: "
                    + (result == null ? "error" : result.getStatus()));
            return false;
        }
        graphRun.states.put(name, RunState.RUNNING);
        graphRun.queryIds.put(name, result.getQueryId());
        runsByQueryId.put(result.getQueryId(), graphRun);
        LOG.info("task graph run {} of root task {} runs task {}, query id {}", graphRun.id, graphRun.root, name,
                result.getQueryId());
        return true;
    }

    /**
     * Marks the runs of the graph run that are no longer pending or running, and whose finish was not reported,
     * with their archived state, or as failed.
     */
    private void reconcile(GraphRun graphRun) {
        List<String> lost = new ArrayList<>();
        for (Map.Entry<String, RunState> entry : graphRun.states.entrySet()) {
            if (entry.getValue() != RunState.RUNNING) {
                continue;
            }
            String name = entry.getKey();
            String queryId = graphRun.queryIds.get(name);
            if (queryId == null || !isQueued(name, queryId)) {
                lost.add(name);
            }
        }
        for (String name : lost) {
            String queryId = graphRun.queryIds.get(name);
            TaskRunStatus archived = queryId == null ? null : taskManager.getTaskRunHistory().getTask(queryId);
            if (archived != null && archived.getState() != null && archived.getState().isFinishState()) {
                RunState state = toRunState(archived.getState());
                graphRun.finish(name, state == null ? RunState.FAILED : state, archived.getErrorMessage());
            } else {
                graphRun.finish(name, RunState.FAILED, "the task run is no longer pending or running");
            }
            if (queryId != null) {
                runsByQueryId.remove(queryId);
            }
        }
        if (!lost.isEmpty()) {
            advance(graphRun);
        }
    }

    private boolean isQueued(String taskName, String queryId) {
        Task task = taskManager.getTask(taskName);
        if (task == null) {
            return false;
        }
        TaskRunManager taskRunManager = taskManager.getTaskRunManager();
        if (!taskRunManager.tryTaskRunLock()) {
            // cannot tell: keep waiting
            return true;
        }
        try {
            TaskRunScheduler scheduler = taskManager.getTaskRunScheduler();
            TaskRun running = scheduler.getRunningTaskRun(task.getId());
            if (running != null && running.getStatus() != null && queryId.equals(running.getStatus().getQueryId())) {
                return true;
            }
            Set<TaskRun> pending = scheduler.getPendingTaskRunsByTaskId(task.getId());
            return pending != null && pending.stream()
                    .anyMatch(run -> run.getStatus() != null && queryId.equals(run.getStatus().getQueryId()));
        } finally {
            taskRunManager.taskRunUnlock();
        }
    }

    private static final class GraphRun {
        private final String id = UUID.randomUUID().toString();
        private final String root;
        // child task -> its predecessors that take part in this run
        private final Map<String, List<String>> predecessors;
        private final String finalizer;
        private final Map<String, RunState> states = new LinkedHashMap<>();
        private final Map<String, String> queryIds = new HashMap<>();
        private final Map<String, String> returnValues = new HashMap<>();
        private final Map<String, Integer> attempts = new HashMap<>();

        private GraphRun(String root, Map<String, List<String>> predecessors, String finalizer) {
            this.root = root;
            this.predecessors = predecessors;
            this.finalizer = finalizer;
        }

        private void finish(String name, RunState state, String error) {
            states.put(name, state);
            if (state == RunState.FAILED) {
                LOG.info("task {} of task graph run {} of root task {} failed: {}", name, id, root, error);
            }
        }

        private void skip(String name, String reason) {
            states.put(name, RunState.SKIPPED);
            LOG.info("task graph run {} of root task {} skips task {}: {}", id, root, name, reason);
        }

        private boolean isRunning() {
            if (states.containsValue(RunState.RUNNING)) {
                return true;
            }
            return !states.keySet().containsAll(predecessors.keySet());
        }
    }
}
