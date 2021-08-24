package com.siemens.ssi;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptSamplingParameters;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sample;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

@Slf4j
public class TableSampleRule extends RelRule<TableSampleRule.Config>
    implements TransformationRule {

  /**
   * Creates a RelRule.
   *
   * @param config
   */
  protected TableSampleRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Sample sample = call.rel(0);
    RelOptSamplingParameters samplingParameters = sample.getSamplingParameters();
    if (samplingParameters.isBernoulli()) {
      RelDataTypeFactory typeFactory = sample.getCluster().getTypeFactory();
      RexBuilder rexBuilder = new RexBuilder(typeFactory);
      float percentage = samplingParameters.getSamplingPercentage();
      RelNode filter = RelOptUtil.createFilter(sample.getInput(),
          Lists.newArrayList(rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
              Lists.newArrayList(rexBuilder.makeCall(SqlStdOperatorTable.RAND),
                  rexBuilder.makeLiteral(percentage, typeFactory.createSqlType(
                      SqlTypeName.DOUBLE))))));
      call.transformTo(filter);
    }
  }

  public interface Config extends RelRule.Config {

    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandSupplier(
            b0 -> b0.operand(Sample.class).predicate(sample -> sample.getSamplingParameters().isBernoulli())
                .anyInputs())
        .as(Config.class);

    @Override
    default TableSampleRule toRule() {
      return new TableSampleRule(this);
    }

  }

}
