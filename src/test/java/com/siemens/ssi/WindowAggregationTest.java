package com.siemens.ssi;

import com.google.common.collect.Lists;
import com.siemens.ssi.TableFunctionScanRule.Config;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.jupiter.api.Test;

@Slf4j
public class WindowAggregationTest extends CalciteTest {

  @Test
  void every5Min() throws SQLException {
    Hook.PLANNER.addThread((VolcanoPlanner planner) -> {
      planner.addRule(Config.DEFAULT.withMatchSchemas(Lists.newArrayList("ch")).toRule());
    });
    Hook.PLAN_BEFORE_IMPLEMENTATION.addThread((RelRoot relRoot) -> {
      log.info("optimized plan:\n" + RelOptUtil.toString(relRoot.rel, SqlExplainLevel.ALL_ATTRIBUTES));
    });
    String sql = "SELECT sum(price) as price_sum,window_start,window_end FROM TABLE(\n"
        + "  TUMBLE (\n"
        + "    TABLE ch.orders,\n"
        + "    DESCRIPTOR(time_stamp),\n"
        + "    INTERVAL '5' MINUTE ))"
        + "group by window_start,window_end";
    int n = executeQuery(sql);
    assert 5 == n;
  }

  @Test
  void everyDAY() throws SQLException {
    Hook.PLANNER.addThread((VolcanoPlanner planner) -> {
      planner.addRule(Config.DEFAULT.withMatchSchemas(Lists.newArrayList("ch")).toRule());
    });
    Hook.PLAN_BEFORE_IMPLEMENTATION.addThread((RelRoot relRoot) -> {
      log.info("optimized plan:\n" + RelOptUtil.toString(relRoot.rel, SqlExplainLevel.ALL_ATTRIBUTES));
    });
    String sql = "SELECT sum(price) as price_sum,window_start,window_end FROM TABLE(\n"
        + "  TUMBLE (\n"
        + "    TABLE ch.orders,\n"
        + "    DESCRIPTOR(time_stamp),\n"
        + "    INTERVAL '1' DAY ))"
        + "group by window_start,window_end";
    int n = executeQuery(sql);
    assert 4 == n;
  }

  @Test
  void everyMonthWithoutRule() throws SQLException {
    Hook.PLANNER.addThread((VolcanoPlanner planner) -> {
      planner.addRule(Config.DEFAULT.withMatchSchemas(Lists.newArrayList("db")).toRule());
    });
    Hook.PLAN_BEFORE_IMPLEMENTATION.addThread((RelRoot relRoot) -> {
      log.info("optimized plan:\n" + RelOptUtil.toString(relRoot.rel, SqlExplainLevel.ALL_ATTRIBUTES));
    });
    String sql = "SELECT sum(price) as price_sum,window_start,window_end FROM TABLE(\n"
        + "  TUMBLE (\n"
        + "    TABLE ch.orders,\n"
        + "    DESCRIPTOR(time_stamp),\n"
        + "    INTERVAL '1' MONTH ))"
        + "group by window_start,window_end";
    int n = executeQuery(sql);
    assert 6 == n;
  }

  @Test
  void everyMonthWithRule() throws SQLException {
    Hook.PLANNER.addThread((VolcanoPlanner planner) -> {
      planner.addRule(Config.DEFAULT.withMatchSchemas(Lists.newArrayList("ch")).toRule());
    });
    Hook.PLAN_BEFORE_IMPLEMENTATION.addThread((RelRoot relRoot) -> {
      log.info("optimized plan:\n" + RelOptUtil.toString(relRoot.rel, SqlExplainLevel.ALL_ATTRIBUTES));
    });
    String sql = "SELECT sum(price) as price_sum,window_start,window_end FROM TABLE(\n"
        + "  TUMBLE (\n"
        + "    TABLE ch.orders,\n"
        + "    DESCRIPTOR(time_stamp),\n"
        + "    INTERVAL '1' MONTH ))"
        + "group by window_start,window_end";
    int n = executeQuery(sql);
    assert 3 == n;
  }

  @Test
  void everyYear() throws SQLException {
    Hook.PLANNER.addThread((VolcanoPlanner planner) -> {
      planner.addRule(Config.DEFAULT.withMatchSchemas(Lists.newArrayList("ch")).toRule());
    });
    Hook.PLAN_BEFORE_IMPLEMENTATION.addThread((RelRoot relRoot) -> {
      log.info("optimized plan:\n" + RelOptUtil.toString(relRoot.rel, SqlExplainLevel.ALL_ATTRIBUTES));
    });
    String sql = "SELECT sum(price) as price_sum,window_start,window_end FROM TABLE(\n"
        + "  TUMBLE (\n"
        + "    TABLE ch.orders,\n"
        + "    DESCRIPTOR(time_stamp),\n"
        + "    INTERVAL '1' YEAR ))"
        + "group by window_start,window_end";
    int n = executeQuery(sql);
    assert 2 == n;
  }

}
