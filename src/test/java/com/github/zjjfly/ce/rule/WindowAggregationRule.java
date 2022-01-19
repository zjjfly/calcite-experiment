package com.github.zjjfly.ce.rule;

import com.github.zjjfly.ce.RelOptExtUtil;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcAggregate;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcProject;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBeans;
import org.apache.calcite.util.Util;

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
            Map<Class<? extends SqlDialect>, SqlWindowStartEnd> sqlWindowStartEndMap =
                config.sqlWindowStartEnd();
            SqlWindowStartEnd sqlWindowStartEnd =
                sqlWindowStartEndMap.getOrDefault(dialect.getClass(), new MySqlSqlWindowStartEnd());
            List<RexNode> projects = project.getProjects();
            int size = scan.getRowType().getFieldList().size();
            //替换window_start和window_end的
            projects = Util.transform(projects, rexNode -> {
                RexInputRef inputRef = (RexInputRef) rexNode;
                if (inputRef.getIndex() >= size) {
                    if (inputRef.getIndex() == size) {
                        return sqlWindowStartEnd.windowStart(typeFactory, timeCol, interval);
                    }
                    if (inputRef.getIndex() == size + 1) {
                        return sqlWindowStartEnd.windowEnd(typeFactory, timeCol, interval);
                    }
                }
                return inputRef;
            });
            RelOptCluster cluster = scan.getCluster();
            RelTraitSet jdbcTraitSet = scan.getTraitSet();
            JdbcProject jdbcProject =
                new JdbcProject(cluster, jdbcTraitSet, scan, projects, project.getRowType());
            //处理汇聚列表
            call.transformTo(
                new JdbcAggregate(cluster, jdbcTraitSet, jdbcProject, aggregation.getGroupSet(),
                    aggregation.getGroupSets(),
                    aggregation.getAggCallList()));
        }
    }

    public interface Config extends RelRule.Config {

        Config DEFAULT = EMPTY.as(Config.class)
            .withOperandSupplier(b0 -> b0.operand(LogicalAggregate.class).oneInput(
                b1 -> b1.operand(LogicalProject.class)
                    .oneInput(b2 -> b2.operand(LogicalTableFunctionScan.class)
                        .predicate(logicalTableFunctionScan -> {
                            RexNode call = logicalTableFunctionScan.getCall();
                            if (call instanceof RexCall) {
                                RexCall c = (RexCall) call;
                                SqlOperator op = c.op;
                                return op == SqlStdOperatorTable.TUMBLE
                                    || op == SqlStdOperatorTable.HOP
                                    || op == SqlStdOperatorTable.SESSION;
                            }
                            return false;
                        }).anyInputs())))
            .as(Config.class);

        @Override
        default WindowAggregationRule toRule() {
            return new WindowAggregationRule(this);
        }

        Config withSqlWindowStartEnd(
            Map<Class<? extends SqlDialect>, SqlWindowStartEnd> sqlWindowStartEnd);

        @ImmutableBeans.Property
        Map<Class<? extends SqlDialect>, SqlWindowStartEnd> sqlWindowStartEnd();
    }

    public interface SqlWindowStartEnd {

        default RelDataType getType(RelDataTypeFactory typeFactory) {
            return typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3);
        }

        RexNode windowStart(RelDataTypeFactory typeFactory, RexNode timeCol, RexLiteral interval);

        RexNode windowEnd(RelDataTypeFactory typeFactory, RexNode timeCol, RexLiteral interval);
    }

}
