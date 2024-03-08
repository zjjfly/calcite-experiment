package com.github.zjjfly.ce.rule;

import com.github.zjjfly.ce.CalciteTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Collections;

@Slf4j
public class WindowSamplingTest extends CalciteTest {

    @Test
    public void sampling() throws SQLException {
        Hook.CONVERTED.addThread((RelNode relNode) -> {
            log.info("converted rel root:\n" + RelOptUtil.toString(relNode,
                    SqlExplainLevel.ALL_ATTRIBUTES));
        });
        Hook.PLAN_BEFORE_IMPLEMENTATION.addThread((RelRoot relRoot) -> {
            log.info("optimized plan:\n" + RelOptUtil.toString(relRoot.rel,
                    SqlExplainLevel.ALL_ATTRIBUTES));
        });
        Hook.PLANNER.addThread((VolcanoPlanner planner) -> {
            planner.addRule(TableSampleRule.Config.DEFAULT.toRule());
            planner.addRule(TableFunctionScanRule.Config.DEFAULT.withMatchSchemas(
                    Collections.singletonList("ch")).toRule());
        });
        Hook.JAVA_PLAN.addThread((String code) -> {
            log.info("generated code: \n" + code);
        });
//    String sql = "SELECT * FROM (SELECT * from TABLE(\n"
//        + " TUMBLE (\n"
//        + "    TABLE ch.orders,\n"
//        + "    DESCRIPTOR(time_stamp),\n"
//        + "    INTERVAL '1' MONTH "
//        + " )) "
//        + ") TABLESAMPLE BERNOULLI(40) ";
        String sql =
                "select * over (ORDER BY time_Stamp RANGE INTERVAL '1' HOUR PRECEDING) from ch.orders ";
        executeQuery(sql);
    }

}
