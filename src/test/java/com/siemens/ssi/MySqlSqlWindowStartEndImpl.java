package com.siemens.ssi;

import com.siemens.ssi.WindowAggregationRule.SqlWindowStartEnd;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

public class MySqlSqlWindowStartEndImpl implements SqlWindowStartEnd {

  @Override
  public RexNode windowStart(RelDataTypeFactory typeFactory, RexNode timeCol, RexLiteral interval) {
    RexBuilder rexBuilder = new RexBuilder(typeFactory);

    return null;
  }

  @Override
  public RexNode windowEnd(RelDataTypeFactory typeFactory, RexNode timeCol, RexLiteral interval) {
    return null;
  }
}
