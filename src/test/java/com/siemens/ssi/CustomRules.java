package com.siemens.ssi;

import com.google.common.collect.Lists;
import java.util.Collections;
import org.apache.calcite.sql.dialect.ClickHouseSqlDialect;

/**
 *
 */
public class CustomRules {

  public static TableFunctionScanRule TABLE_FUNCTION_SCAN =
      TableFunctionScanRule.Config.DEFAULT.withMatchSchemas(Lists.newArrayList("ch")).toRule();

  public static WindowAggregationRule WINDOW_AGGREGATION = WindowAggregationRule.Config.DEFAULT.withSqlWindowStartEnd(
      Collections.singletonMap(ClickHouseSqlDialect.class, new ClickHouseSqlWindowStartEnd())).toRule();

}
