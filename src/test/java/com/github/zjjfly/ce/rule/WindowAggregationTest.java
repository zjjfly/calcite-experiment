package com.github.zjjfly.ce.rule;

import com.github.zjjfly.ce.Benchmark;
import com.github.zjjfly.ce.CalciteTest;
import java.sql.SQLException;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@Slf4j
public class WindowAggregationTest extends CalciteTest {

    @BeforeAll
    @Override
    public void init() throws SQLException {
        super.init();
        Hook.CONVERTED.addThread((RelNode relNode) -> {
            log.info("converted rel root:\n" + RelOptUtil.toString(relNode,
                SqlExplainLevel.ALL_ATTRIBUTES));
        });
        Hook.PLANNER.addThread((VolcanoPlanner planner) -> {
            planner.addRule(CustomRules.WINDOW_AGGREGATION);
        });
        Hook.PLAN_BEFORE_IMPLEMENTATION.addThread((RelRoot relRoot) -> {
            log.info("optimized plan:\n" + RelOptUtil.toString(relRoot.rel,
                SqlExplainLevel.ALL_ATTRIBUTES));
        });
    }

    @Nested
    public class ClickHouseTests {

        @Test
        void every5Min() throws SQLException {
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
                planner.removeRule(CustomRules.WINDOW_AGGREGATION);
            });
            String sql = "SELECT sum(price) as price_sum,window_start,window_end FROM TABLE(\n"
                + "  TUMBLE (\n"
                + "    TABLE ch.orders,\n"
                + "    DESCRIPTOR(time_stamp),\n"
                + "    INTERVAL '1' MONTH ))"
                + "group by window_start,window_end";
            int n = executeQuery(sql);
            assert 6 == n;
            Hook.PLANNER.addThread((VolcanoPlanner planner) -> {
                planner.addRule(CustomRules.WINDOW_AGGREGATION);
            });
        }

        @Test
        void everyMonthWithRule() throws SQLException {
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

    @Nested
    public class MySqlTests {

        @Test
        void every5Min() throws SQLException {
            String sql = "SELECT sum(price) as price_sum,window_start,window_end FROM TABLE(\n"
                + "  TUMBLE (\n"
                + "    TABLE ms.orders,\n"
                + "    DESCRIPTOR(time_stamp),\n"
                + "    INTERVAL '5' MINUTE ))"
                + "group by window_start,window_end";
            int n = executeQuery(sql);
            assert 5 == n;
        }

        @Test
        void everyDAY() throws SQLException {
            String sql = "SELECT sum(price) as price_sum,window_start,window_end FROM TABLE(\n"
                + "  TUMBLE (\n"
                + "    TABLE ms.orders,\n"
                + "    DESCRIPTOR(time_stamp),\n"
                + "    INTERVAL '1' DAY ))"
                + "group by window_start,window_end";
            int n = executeQuery(sql);
            assert 4 == n;
        }

        @Test
        void everyMonthWithoutRule() throws SQLException {
            Hook.PLANNER.addThread((VolcanoPlanner planner) -> {
                planner.removeRule(CustomRules.WINDOW_AGGREGATION);
            });
            String sql = "SELECT sum(price) as price_sum,window_start,window_end FROM TABLE(\n"
                + "  TUMBLE (\n"
                + "    TABLE ms.orders,\n"
                + "    DESCRIPTOR(time_stamp),\n"
                + "    INTERVAL '1' MONTH ))"
                + "group by window_start,window_end";
            int n = executeQuery(sql);
            assert 6 == n;
            Hook.PLANNER.addThread((VolcanoPlanner planner) -> {
                planner.addRule(CustomRules.WINDOW_AGGREGATION);
            });
        }

        @Test
        void everyMonthWithRule() throws SQLException {
            String sql = "SELECT sum(price) as price_sum,window_start,window_end FROM TABLE(\n"
                + "  TUMBLE (\n"
                + "    TABLE ms.orders,\n"
                + "    DESCRIPTOR(time_stamp),\n"
                + "    INTERVAL '1' MONTH ))"
                + "group by window_start,window_end";
            int n = executeQuery(sql);
            assert 3 == n;
        }

        @Test
        void everyYear() throws SQLException {
            String sql = "SELECT sum(price) as price_sum,window_start,window_end FROM TABLE(\n"
                + "  TUMBLE (\n"
                + "    TABLE ms.orders,\n"
                + "    DESCRIPTOR(time_stamp),\n"
                + "    INTERVAL '1' YEAR ))"
                + "group by window_start,window_end";
            int n = executeQuery(sql);
            assert 2 == n;
        }

    }

    @Nested
    public class BenchMarkTests {

        @Test
        @Benchmark
        void withoutRule() throws SQLException {
            Hook.PLANNER.addThread((VolcanoPlanner planner) -> {
                planner.removeRule(CustomRules.WINDOW_AGGREGATION);
            });
            Hook.JAVA_PLAN.addThread((String s) -> {
                log.info("generated code: " + s);
            });
            String sql =
                "SELECT count(0) as hit_count,max(RedirectCount) as max_age,window_start,window_end FROM TABLE(\n"
                    + "  TUMBLE (\n"
                    + "    TABLE ch.hits,\n"
                    + "    DESCRIPTOR(EventTime),\n"
                    + "    INTERVAL '10' MINUTE ))"
                    + "group by window_start,window_end";
            executeQuery(sql);
            Hook.PLANNER.addThread((VolcanoPlanner planner) -> {
                planner.addRule(CustomRules.WINDOW_AGGREGATION);
            });
        }

        @Test
        @Benchmark
        void withRule() throws SQLException {
            String sql =
                "SELECT count(0) as hit_count,max(RedirectCount) as max_age,window_start,window_end FROM TABLE(\n"
                    + "  TUMBLE (\n"
                    + "    TABLE ch.hits,\n"
                    + "    DESCRIPTOR(EventTime),\n"
                    + "    INTERVAL '10' MINUTE ))"
                    + "group by window_start,window_end";
            executeQuery(sql);
        }
    }

}
