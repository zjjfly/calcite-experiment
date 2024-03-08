package com.github.zjjfly.ce.rule;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ImmutableBeans;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class TableFunctionScanRule extends RelRule<TableFunctionScanRule.Config>
        implements TransformationRule {

    /**
     * Creates a RelRule.
     *
     * @param config
     */
    protected TableFunctionScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        log.info("TableFunctionScanRule applied");
        LogicalTableFunctionScan rel = call.rel(0);
        RexCall tableFunc = (RexCall) rel.getCall();
        List<RexNode> operands = tableFunc.getOperands();
        RexCall descriptor = (RexCall) operands.get(0);
        RexNode rowTime = descriptor.getOperands().get(0);
        RexLiteral interval = (RexLiteral) operands.get(operands.size() - 1);
        BigDecimal value = (BigDecimal) interval.getValue();
        assert value != null;
        IntervalSqlType intervalSqlType = (IntervalSqlType) interval.getType();
        RelSubset subset = (RelSubset) rel.getInput(0);
        RelOptTable table = new ArrayList<>(RelOptUtil.findTables(subset)).get(0);
        //利用config中的配置做一些逻辑处理
        if (!this.config.matchSchemas().contains(table.getQualifiedName().get(0))) {
            return;
        }
        LogicalTableScan tableScan =
                LogicalTableScan.create(rel.getCluster(), table, new ArrayList<>());
        List<RexNode> projects = new ArrayList<>();
        RelDataType rowType = table.getRowType();
        List<RelDataTypeField> fieldList = rowType.getFieldList();
        for (RelDataTypeField field : fieldList) {
            projects.add(RexInputRef.of(field.getIndex(), rowType));
        }
        RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
//    rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER));
        RelDataType type = rel.getRowType();
        TimeUnitRange timeUnitRange = intervalSqlType.getIntervalQualifier().timeUnitRange;
        //TODO 根据value动态的拼装为CEIL(TIMESTAMPADD(MONTH, -1 * (MONTH(time) % n),time),MONTH)
        projects.add(rexBuilder.makeCall(type.getField("window_start", true, false).getType(),
                SqlStdOperatorTable.FLOOR,
                Lists.newArrayList(rowTime, rexBuilder.makeFlag(
                        timeUnitRange))));
        //TODO 根据value动态的拼装为CEIL(TIMESTAMPADD(MONTH, n - (MONTH(time) % n),time)),MONTH)
        projects.add(rexBuilder.makeCall(type.getField("window_end", true, false).getType(),
                SqlStdOperatorTable.CEIL,
                Lists.newArrayList(rowTime, rexBuilder.makeFlag(
                        timeUnitRange))));
        LogicalProject logicalProject =
                LogicalProject.create(tableScan, tableScan.getHints(), projects, type);
        call.transformTo(logicalProject);
    }

    public interface Config extends RelRule.Config {

        Config DEFAULT = EMPTY.as(Config.class)
                .withOperandSupplier(b0 -> b0.operand(LogicalTableFunctionScan.class)
                        .predicate(logicalTableFunctionScan -> {
                            RexNode call = logicalTableFunctionScan.getCall();
                            if (call instanceof RexCall) {
                                RexCall c = (RexCall) call;
                                SqlOperator op = c.op;
                                if (op == SqlStdOperatorTable.TUMBLE || op == SqlStdOperatorTable.HOP
                                        || op == SqlStdOperatorTable.SESSION) {
                                    List<RexNode> operands = c.getOperands();
                                    RexLiteral op1 = (RexLiteral) operands.get(operands.size() - 1);
                                    if (SqlTypeUtil.isInterval(op1.getType())) {
                                        IntervalSqlType type = (IntervalSqlType) op1.getType();
                                        SqlTypeName sqlTypeName = type.getSqlTypeName();
                                        return sqlTypeName == SqlTypeName.INTERVAL_MONTH
                                                || sqlTypeName == SqlTypeName.INTERVAL_YEAR;
                                    }
                                }
                            }
                            return false;
                        }).anyInputs())
                .as(Config.class);

        @Override
        default TableFunctionScanRule toRule() {
            return new TableFunctionScanRule(this);
        }

        Config withMatchSchemas(List<String> schemas);

        @ImmutableBeans.Property
        List<String> matchSchemas();

    }

}
