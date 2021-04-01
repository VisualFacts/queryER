package org.imsi.queryEREngine.imsi.er.EfficiencyLayer.ComparisonRefinement;

import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.BilateralBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.Comparison;
import org.imsi.queryEREngine.imsi.er.DataStructures.DecomposedBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.EntityIndex;
import org.imsi.queryEREngine.imsi.er.EfficiencyLayer.AbstractEfficiencyMethod;
import org.imsi.queryEREngine.imsi.er.Utilities.ComparisonIterator;
import org.imsi.queryEREngine.imsi.er.Utilities.Converter;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author gap2
 */

public class ComparisonPropagation extends AbstractEfficiencyMethod {
    
    private EntityIndex entityIndex;
    
    public ComparisonPropagation() {
        super("Comparisons Propagation");
    }
    
    public ComparisonPropagation(EntityIndex eIndex) {
        super("Comparisons Propagation");
        entityIndex = eIndex;
    }
    
    @Override
    public void applyProcessing(List<AbstractBlock> blocks) {
        if (entityIndex == null) {
            entityIndex = new EntityIndex(blocks);
        }

        final List<AbstractBlock> redundancyFreeBlocks = new ArrayList<AbstractBlock>();
        for (AbstractBlock block : blocks) {
            final List<Integer> entities1 = new ArrayList<Integer>();
            final List<Integer> entities2 = new ArrayList<Integer>();
        
            ComparisonIterator iterator = block.getComparisonIterator();
            while (iterator.hasNext()) {
                Comparison comparison = iterator.next();
                if (!entityIndex.isRepeated(block.getBlockIndex(), comparison)) {
                    entities1.add(comparison.getEntityId1());
                    entities2.add(comparison.getEntityId2());
                }
            }
            
            int[] entityIds1 = Converter.convertListToArray(entities1);
            int[] entityIds2 = Converter.convertListToArray(entities2);
            boolean cleanCleanER = block instanceof BilateralBlock;
            redundancyFreeBlocks.add(new DecomposedBlock(cleanCleanER, entityIds1, entityIds2));
        }
        blocks.clear();
        blocks.addAll(redundancyFreeBlocks);
    }
}