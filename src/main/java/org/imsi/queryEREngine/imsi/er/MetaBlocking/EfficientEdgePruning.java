package org.imsi.queryEREngine.imsi.er.MetaBlocking;


import java.util.List;

import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.Utilities.MetaBlockingConfiguration.WeightingScheme;


public class EfficientEdgePruning extends EdgePruning {

    public EfficientEdgePruning() {
        super("Efficient Edge Pruning", WeightingScheme.CBS);
        averageWeight = 2.86030368;
    }

    @Override
    public void applyProcessing(List<AbstractBlock> blocks) {
    	getStatistics(blocks);
    	//initializeEntityIndex(blocks);
        filterComparisons(blocks);
    }
}
