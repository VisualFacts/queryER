package org.imsi.queryEREngine.imsi.er.MetaBlocking;


import java.util.List;
import java.util.Set;

import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.Utilities.MetaBlockingConfiguration.WeightingScheme;


public class EfficientEdgePruning extends EdgePruning {

    public EfficientEdgePruning() {
        super("Efficient Edge Pruning", WeightingScheme.CBS);
        averageWeight = 2;
    }

    public EfficientEdgePruning(Set<Integer> qIds) {
        super("Efficient Edge Pruning", qIds, WeightingScheme.CBS);
        averageWeight = 2;
    }

    @Override
    public void applyProcessing(List<AbstractBlock> blocks) {
        double s = System.currentTimeMillis();
        getStatistics(blocks, qIds);
        System.out.println("Statistics: " + (System.currentTimeMillis() - s)/1000);
    	//initializeEntityIndex(blocks);
        s = System.currentTimeMillis();
        filterComparisons(blocks);
        System.out.println("Filter: " + (System.currentTimeMillis() - s)/1000);
    }
}
