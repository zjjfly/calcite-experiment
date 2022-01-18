package com.github.zjjfly.ce;

import java.sql.SQLException;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Holder;
import org.junit.jupiter.api.Test;

@Slf4j
public class HookTest extends CalciteTest {

    @Test
    public void hook() throws SQLException {
        Hook.PLANNER.addThread((RelOptPlanner optPlanner) -> {
            List<RelOptRule> rules = optPlanner.getRules();
            rules.forEach(relOptRule -> {
                log.info(relOptRule.toString());
            });
        });

        Hook.PROGRAM.add((Holder<Program> holder) -> {
            if (holder == null) {
                throw new IllegalStateException("No program holder");
            }
            Program chain = holder.get();
            if (chain == null) {
                chain = Programs.standard();
            }
            holder.set(Programs.sequence((RelOptPlanner planner, RelNode rel, RelTraitSet
                    requiredOutputTraits, List<RelOptMaterialization> materializations, List<RelOptLattice> lattices) -> rel,
                chain));
        });

        String sql = "select * from ms.users";
        executeQuery(sql);
    }

}
