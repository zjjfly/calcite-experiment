package com.siemens.ssi;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcAggregate;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcProject;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.util.ImmutableBeans;

@Slf4j
public class WindowAggregationRule extends RelRule<WindowAggregationRule.Config>
    implements TransformationRule {

  /**
   * Creates a RelRule.
   *
   * @param config
   */
  protected WindowAggregationRule(Config config) {
    super(config);
  }

  @SneakyThrows
  @Override
  public void onMatch(RelOptRuleCall call) {
    log.info("WindowAggregationRule applied");
    LogicalAggregate aggregation = call.rel(0);
    LogicalProject project = call.rel(1);
    LogicalTableFunctionScan tableFunction = call.rel(2);
    RexCall tableFunc = (RexCall) tableFunction.getCall();
    List<RexNode> operands = tableFunc.getOperands();
    RexCall descriptor = (RexCall) operands.get(0);
    RexInputRef timeCol = (RexInputRef) descriptor.getOperands().get(0);
    RexLiteral interval = (RexLiteral) operands.get(operands.size() - 1);
    List<TableScan> tableScan = RelOptExtUtil.findTableScan(aggregation);
    RelDataTypeFactory typeFactory = aggregation.getCluster().getTypeFactory();
    if (tableScan.get(0) instanceof JdbcTableScan) {
      JdbcTableScan scan = (JdbcTableScan) tableScan.get(0);
      SqlDialect dialect = scan.jdbcTable.jdbcSchema.dialect;
      List<RexNode> p = Lists.newArrayList();
      Map<Class<? extends SqlDialect>, SqlWindowStartEnd> sqlWindowStartEndMap = config.sqlWindowStartEnd();
      SqlWindowStartEnd sqlWindowStartEnd =
          sqlWindowStartEndMap.getOrDefault(dialect.getClass(), new ClickHouseSqlWindowStartEnd());
      List<RexNode> projects = project.getProjects();
      int size = scan.getRowType().getFieldList().size();
      for (RexNode rexNode : projects) {
        RexInputRef inputRef = (RexInputRef) rexNode;
        if (inputRef.getIndex() >= size) {
          if (inputRef.getIndex() == size) {
            p.add(sqlWindowStartEnd.windowStart(typeFactory, timeCol, interval));
          }
          if (inputRef.getIndex() == size + 1) {
            p.add(sqlWindowStartEnd.windowEnd(typeFactory, timeCol, interval));
          }
          continue;
        }
        p.add(inputRef);
      }
      JdbcProject jdbcProject =
          new JdbcProject(scan.getCluster(), scan.getTraitSet(), scan, p, project.getRowType());
      //处理汇聚列表
      List<AggregateCall> aggCallList = aggregation.getAggCallList();
      ArrayList<AggregateCall> aggList = Lists.newArrayList();
      for (AggregateCall aggregateCall : aggCallList) {
        if (aggregateCall.getAggregation() instanceof SqlSumEmptyIsZeroAggFunction) {
          aggList.add(
              AggregateCall.create(SqlStdOperatorTable.SUM, aggregateCall.isDistinct(), aggregateCall.isApproximate(),
                  aggregateCall.ignoreNulls(), aggregateCall.getArgList(), aggregateCall.filterArg,
                  aggregateCall.distinctKeys, aggregateCall.collation,
                  aggregateCall.type, aggregateCall.name));
          continue;
        }
        aggList.add(aggregateCall);
      }
      call.transformTo(
          new JdbcAggregate(scan.getCluster(), scan.getTraitSet(), jdbcProject, aggregation.getGroupSet(),
              aggregation.getGroupSets(),
              aggList));
    }
  }

  public interface Config extends RelRule.Config {

    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandSupplier(b0 -> b0.operand(LogicalAggregate.class).oneInput(
            b1 -> b1.operand(LogicalProject.class)
                .oneInput(b2 -> b2.operand(LogicalTableFunctionScan.class).predicate(logicalTableFunctionScan -> {
                  RexNode call = logicalTableFunctionScan.getCall();
                  if (call instanceof RexCall) {
                    RexCall c = (RexCall) call;
                    SqlOperator op = c.op;
                    return op == SqlStdOperatorTable.TUMBLE || op == SqlStdOperatorTable.HOP
                        || op == SqlStdOperatorTable.SESSION;
                  }
                  return false;
                }).anyInputs())))
        .as(Config.class);

    @Override
    default WindowAggregationRule toRule() {
      return new WindowAggregationRule(this);
    }

    Config withSqlWindowStartEnd(Map<Class<? extends SqlDialect>, SqlWindowStartEnd> sqlWindowStartEnd);

    @ImmutableBeans.Property
    Map<Class<? extends SqlDialect>, SqlWindowStartEnd> sqlWindowStartEnd();
  }

  public interface SqlWindowStartEnd {

    RexNode windowStart(RelDataTypeFactory typeFactory, RexNode timeCol, RexLiteral interval);

    RexNode windowEnd(RelDataTypeFactory typeFactory, RexNode timeCol, RexLiteral interval);
  }

}
