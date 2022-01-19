package com.github.zjjfly.ce.common;

import com.github.zjjfly.ce.CalciteTest;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * @author Zi JunJie(junjie.zi@siemens.com)
 */
@Slf4j
public class ModifyTest extends CalciteTest {

    @BeforeAll
    @Override
    public void init() throws SQLException {
        super.init();
        Hook.CONVERTED.addThread((RelNode relNode) -> {
            log.info("converted rel root:\n" + RelOptUtil.toString(relNode,
                SqlExplainLevel.ALL_ATTRIBUTES));
        });
        Hook.PLAN_BEFORE_IMPLEMENTATION.addThread((RelRoot relRoot) -> {
            log.info("optimized plan:\n" + RelOptUtil.toString(relRoot.rel,
                SqlExplainLevel.ALL_ATTRIBUTES));
        });
    }

    @Test
    void sort() throws SQLException {
        executeQuery("select id,price from ms.orders where id >= 1 order by id");
    }

    @Test
    void update() throws SQLException {
        executeUpdate("insert into ms.orders(id,price) values(1,1.0)");
    }
}
