package com.github.zjjfly.ce;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.dialect.ClickHouseSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;

public class CustomRules {

    public static TableFunctionScanRule TABLE_FUNCTION_SCAN =
        TableFunctionScanRule.Config.DEFAULT.withMatchSchemas(Lists.newArrayList("ch")).toRule();

    public static WindowAggregationRule WINDOW_AGGREGATION =
        WindowAggregationRule.Config.DEFAULT.withSqlWindowStartEnd(
            ImmutableBiMap.of(ClickHouseSqlDialect.class, new ClickHouseSqlWindowStartEnd(),
                MysqlSqlDialect.class, new MySqlSqlWindowStartEnd())).toRule();

}
