package com.siemens.ssi;

import com.google.common.collect.Lists;
import com.siemens.ssi.WindowAggregationRule.SqlWindowStartEnd;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 *
 */
public class ClickHouseSqlWindowStartEnd implements SqlWindowStartEnd {

  @Override
  public RexNode windowStart(RelDataTypeFactory typeFactory, RexNode timeCol, RexLiteral interval) {
    RelDataType funcReturnType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3);
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    List<RexNode> op = Lists.newArrayList(timeCol, interval);
    return rexBuilder.makeCall(funcReturnType,
        ClickHouseNativeFunctions.TO_START_OF_INTERVAL, op);
  }

  @Override
  public RexNode windowEnd(RelDataTypeFactory typeFactory, RexNode timeCol, RexLiteral interval) {
    RelDataType funcReturnType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3);
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    List<RexNode> op = Lists.newArrayList(rexBuilder.makeCall(ClickHouseNativeFunctions.TO_START_OF_INTERVAL,
        Lists.newArrayList(timeCol, interval)), interval);
    return rexBuilder.makeCall(funcReturnType, ClickHouseNativeFunctions.TIMESTAMP_ADD, op);
  }
}
