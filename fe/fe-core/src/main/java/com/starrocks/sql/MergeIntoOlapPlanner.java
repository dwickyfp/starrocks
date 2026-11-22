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

package com.starrocks.sql;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.load.Load;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.MergeIntoStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.type.IntegerType;

import java.util.List;
import java.util.Optional;

/**
 * Plans MERGE INTO a Primary Key table: the query built by the analyzer outputs every column of the table and
 * the load operation {@code __op}, and an OLAP table sink upserts (0) or deletes (1) each row, all in the
 * transaction of the statement.
 */
public class MergeIntoOlapPlanner {

    public ExecPlan plan(MergeIntoStmt stmt, ConnectContext session) {
        OlapTable table = (OlapTable) stmt.getTable();
        QueryRelation query = stmt.getQueryStatement().getQueryRelation();
        List<String> colNames = stmt.getOutputColumnNames();
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, session).transform(query);

        boolean isEnablePipeline = session.getSessionVariable().isEnablePipelineEngine();
        boolean canUsePipeline = isEnablePipeline && DataSink.canTableSinkUsePipeline(table);
        boolean forceDisablePipeline = isEnablePipeline && !canUsePipeline;
        boolean prevIsEnableLocalShuffleAgg = session.getSessionVariable().isEnableLocalShuffleAgg();
        try {
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(false);
            }
            // A table sink assigns scan ranges per driver sequence, which local shuffle agg cannot use.
            session.getSessionVariable().setEnableLocalShuffleAgg(false);

            OptimizerContext optimizerContext = OptimizerFactory.initContext(session, columnRefFactory);
            optimizerContext.setUpdateTableId(table.getId());
            Optimizer optimizer = OptimizerFactory.create(optimizerContext);
            OptExpression optimizedPlan = optimizer.optimize(logicalPlan.getRoot(), new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()));
            ExecPlan execPlan = PlanFragmentBuilder.createPhysicalPlan(optimizedPlan, session,
                    logicalPlan.getOutputColumn(), columnRefFactory, colNames, TResultSinkType.MYSQL_PROTOCAL, false);
            setupSink(execPlan, stmt, table, session, canUsePipeline);
            return execPlan;
        } finally {
            session.getSessionVariable().setEnableLocalShuffleAgg(prevIsEnableLocalShuffleAgg);
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(true);
            }
        }
    }

    private void setupSink(ExecPlan execPlan, MergeIntoStmt stmt, OlapTable table, ConnectContext session,
                           boolean canUsePipeline) {
        DescriptorTable descriptorTable = execPlan.getDescTbl();
        TupleDescriptor tuple = descriptorTable.createTupleDescriptor();
        List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();
        List<Column> columns = table.getBaseSchema();
        Preconditions.checkState(execPlan.getOutputExprs().size() == columns.size() + 1,
                "MERGE INTO outputs %s columns for %s table columns", execPlan.getOutputExprs().size(), columns.size());
        for (Column column : columns) {
            SlotDescriptor slot = descriptorTable.addSlotDescriptor(tuple);
            slot.setIsMaterialized(true);
            slot.setType(column.getType());
            slot.setColumn(column);
            slot.setIsNullable(column.isAllowNull());
            if (column.getType().isVarchar()
                    && IDictManager.getInstance().hasGlobalDict(table.getId(), column.getColumnId())) {
                Optional<ColumnDict> dict = IDictManager.getInstance().getGlobalDict(table.getId(), column.getColumnId());
                dict.ifPresent(columnDict -> globalDicts.add(new Pair<>(slot.getId().asInt(), columnDict)));
            }
        }
        SlotDescriptor op = descriptorTable.addSlotDescriptor(tuple);
        op.setIsMaterialized(true);
        op.setType(IntegerType.TINYINT);
        op.setColumn(new Column(Load.LOAD_OP_COLUMN, IntegerType.TINYINT));
        op.setIsNullable(false);
        tuple.computeMemLayout();

        List<Long> partitionIds = Lists.newArrayList();
        for (Partition partition : table.getPartitions()) {
            partitionIds.add(partition.getId());
        }
        OlapTableSink sink = new OlapTableSink(table, tuple, partitionIds, table.writeQuorum(),
                table.enableReplicatedStorage(), false, table.supportedAutomaticPartition(),
                session.getCurrentComputeResource());
        if (session.getTxnId() != 0) {
            sink.setIsMultiStatementsTxn(true);
        }
        PlanFragment fragment = execPlan.getFragments().get(0);
        fragment.setSink(sink);
        fragment.setLoadGlobalDicts(globalDicts);

        // An OLAP table sink runs on a backend: a compute node cannot run it.
        session.getSessionVariable().setPreferComputeNode(false);
        session.getSessionVariable().setUseComputeNodes(0);
        TableName tableName = TableName.fromTableRef(stmt.getTableRef());
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getDb(session, tableName.getCatalog(), tableName.getDb());
        try {
            sink.init(session.getExecutionId(), stmt.getTxnId(), db.getId(), session.getExecTimeout());
            sink.setEstimatedWriteBytes(DmlWriteEstimates.fromExecPlan(execPlan));
            sink.complete();
        } catch (StarRocksException e) {
            throw new SemanticException(e.getMessage());
        }

        if (!canUsePipeline) {
            fragment.setPipelineDop(1);
            return;
        }
        SessionVariable sv = session.getSessionVariable();
        if (sv.getEnableAdaptiveSinkDop()) {
            fragment.setPipelineDop(sv.getSinkDegreeOfParallelism(session.getCurrentComputeResource().getWarehouseId()));
        } else {
            fragment.setPipelineDop(sv.getParallelExecInstanceNum());
        }
        fragment.setHasOlapTableSink();
        fragment.setForceSetTableSinkDop();
        fragment.setForceAssignScanRangesPerDriverSeq();
        fragment.disableRuntimeAdaptiveDop();
    }
}
