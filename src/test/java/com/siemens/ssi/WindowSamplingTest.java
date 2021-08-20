package com.siemens.ssi;

import com.google.common.collect.Lists;
import com.siemens.ssi.TableFunctionScanRule.Config;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.jupiter.api.Test;

@Slf4j
public class WindowSamplingTest extends CalciteTest {

  @Test
  public void sampling() throws SQLException {
    Hook.CONVERTED.addThread((RelNode relNode) -> {
      log.info("converted rel root:\n" + RelOptUtil.toString(relNode, SqlExplainLevel.ALL_ATTRIBUTES));
    });
    Hook.PLAN_BEFORE_IMPLEMENTATION.addThread((RelRoot relRoot) -> {
      log.info("optimized plan:\n" + RelOptUtil.toString(relRoot.rel, SqlExplainLevel.ALL_ATTRIBUTES));
    });
    Hook.PLANNER.addThread((VolcanoPlanner planner) -> {
      planner.addRule(Config.DEFAULT.withMatchSchemas(Lists.newArrayList("ch")).toRule());
    });
    String sql = "SELECT * FROM (SELECT * from TABLE(\n"
        + " TUMBLE (\n"
        + "    TABLE ch.orders,\n"
        + "    DESCRIPTOR(time_stamp),\n"
        + "    INTERVAL '1' YEAR "
        + " )) "
        + ") TABLESAMPLE SYSTEM(10)";
    executeQuery(sql);
  }

}
