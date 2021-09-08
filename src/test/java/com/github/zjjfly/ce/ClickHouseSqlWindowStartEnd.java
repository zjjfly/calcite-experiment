package com.github.zjjfly.ce;

import com.google.common.collect.Lists;
import com.github.zjjfly.ce.WindowAggregationRule.SqlWindowStartEnd;
import java.util.List;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

public class ClickHouseSqlWindowStartEnd implements SqlWindowStartEnd {

  @Override
  public RexNode windowStart(RelDataTypeFactory typeFactory, RexNode timeCol, RexLiteral interval) {
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    List<RexNode> op = Lists.newArrayList(timeCol, interval);
    return rexBuilder.makeCall(getType(typeFactory), ClickHouseNativeFunctions.TO_START_OF_INTERVAL, op);
  }

  @Override
  public RexNode windowEnd(RelDataTypeFactory typeFactory, RexNode timeCol, RexLiteral interval) {
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    List<RexNode> op = Lists.newArrayList(rexBuilder.makeCall(ClickHouseNativeFunctions.TO_START_OF_INTERVAL,
        Lists.newArrayList(timeCol, interval)), interval);
    return rexBuilder.makeCall(getType(typeFactory), ClickHouseNativeFunctions.TIMESTAMP_ADD, op);
  }
}
