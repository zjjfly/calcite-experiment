package com.github.zjjfly.ce;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Multimap;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

public class RelOptExtUtil {

    public static List<TableScan> findTableScan(RelNode rel) {
        final Multimap<Class<? extends RelNode>, RelNode> nodes =
            rel.getCluster().getMetadataQuery().getNodeTypes(rel);
        final List<TableScan> usedTables = new ArrayList<>();
        if (nodes == null) {
            return usedTables;
        }
        for (Map.Entry<Class<? extends RelNode>, Collection<RelNode>> e : nodes.asMap()
            .entrySet()) {
            if (TableScan.class.isAssignableFrom(e.getKey())) {
                for (RelNode node : e.getValue()) {
                    TableScan scan = (TableScan) node;
                    usedTables.add(scan);
                }
            }
        }
        return usedTables;
    }

}
