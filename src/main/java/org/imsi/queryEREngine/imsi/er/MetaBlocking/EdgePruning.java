package org.imsi.queryEREngine.imsi.er.MetaBlocking;

import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.BilateralBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.Comparison;
import org.imsi.queryEREngine.imsi.er.Utilities.AbstractMetablocking;
import org.imsi.queryEREngine.imsi.er.Utilities.MetaBlockingConfiguration.WeightingScheme;
import org.imsi.queryEREngine.imsi.er.Utilities.QueryComparisonIterator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EdgePruning extends AbstractMetablocking {

    protected double averageWeight;
    protected Set<Integer> qIds;
    protected int ucomps=0;

    public EdgePruning(WeightingScheme scheme) {
        super("Edge Pruning", scheme);
    }


    protected EdgePruning(String description, WeightingScheme scheme) {
        super(description, scheme);
    }

    protected EdgePruning(String description, Set<Integer> qIds, WeightingScheme scheme) {
        super(description, scheme);
        this.qIds = qIds;
    }

    @Override
    public void applyProcessing(List<AbstractBlock> blocks) {
        getStatistics(blocks, qIds);
        setAverageWeight(blocks);
        filterComparisons(blocks);

    }

    protected void filterComparisons(List<AbstractBlock> blocks) {
        boolean cleanCleanER = blocks.get(0) instanceof BilateralBlock;
        final List<AbstractBlock> newBlocks = new ArrayList<AbstractBlock>();
//        int biggest = (int) blocks.get(blocks.size()/2).getNoOfComparisons()/3;
        for (AbstractBlock block : blocks) {
            final List<Integer> entities1 = new ArrayList<Integer>();
            final List<Integer> entities2 = new ArrayList<Integer>();
//            HashSet<Comparison> uComp = new HashSet<Comparison>(biggest,0.8f);
            HashSet<Comparison> uComp = new HashSet<>();
            //ComparisonIterator iterator = block.getComparisonIterator();
            QueryComparisonIterator iterator = block.getQueryComparisonIterator(qIds);
//            ComparisonIterator iterator = block.getComparisonIterator();

            // if(!iterator.hasComparisons()) continue;
            while (iterator.hasNext()) {
                Comparison comparison = iterator.next();

                if (comparison.getEntityId1() == comparison.getEntityId2()) continue;
                if (comparison.getEntityId1() > comparison.getEntityId2())
                    comparison = new Comparison(false, comparison.getEntityId2(), comparison.getEntityId1());

                if (uComp.contains(comparison)) continue;

                double weight = getWeight(block.getBlockIndex(), comparison);
                uComp.add(comparison);
                if (weight < averageWeight) {
                    continue;
                }
                entities1.add(comparison.getEntityId1());
                entities2.add(comparison.getEntityId2());
            }
            newBlocks.add(getDecomposedBlock(cleanCleanER, entities1, entities2));
        }
        blocks.clear();
        blocks.addAll(newBlocks);
    }

    protected void setAverageWeight(List<AbstractBlock> blocks) {
        averageWeight = 0;
        int limit = 3000;
        double mean = 0.0f;
        int counter = 0;
        for (AbstractBlock block : blocks) {
            QueryComparisonIterator iterator = block.getQueryComparisonIterator(qIds);
            HashSet<Comparison> uComp = new HashSet<>();
            while (iterator.hasNext()) {
                Comparison comparison = iterator.next();
                if (comparison.getEntityId1() == comparison.getEntityId2()) continue;
                if (comparison.getEntityId1() > comparison.getEntityId2())
                    comparison = new Comparison(false, comparison.getEntityId2(), comparison.getEntityId1());

                if (uComp.contains(comparison)) continue;
                double weight = getWeight(block.getBlockIndex(), comparison);
//                System.err.println(weight);
                uComp.add(comparison);
                if (weight < 0) {
                    continue;
                }
                ucomps++;
                if (counter < limit) mean += weight;
                else if (counter == limit) {
                    averageWeight = (mean / limit)+0.5;
                    System.err.println("GA\t" + averageWeight);
                    break;
                }
                counter++;

            }
            if(averageWeight>0) break;

        }

//        System.out.println("Average weight\t:\t" + averageWeight);
////        averageWeight /= (double) ucomps;
//        System.out.println("validComparisons\t:\t" + ucomps);
//        System.out.println("Average weight\t:\t" + averageWeight);
    }
}
