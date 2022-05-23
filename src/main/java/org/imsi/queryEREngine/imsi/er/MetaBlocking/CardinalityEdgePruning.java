/*
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    Copyright (C) 2015 George Antony Papadakis (gpapadis@yahoo.gr)
 */

package org.imsi.queryEREngine.imsi.er.MetaBlocking;


import org.imsi.queryEREngine.imsi.er.Comparators.ComparisonWeightComparator;
import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.BilateralBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.Comparison;
import org.imsi.queryEREngine.imsi.er.DataStructures.EntityIndex;
import org.imsi.queryEREngine.imsi.er.Utilities.*;

import java.util.*;


public class CardinalityEdgePruning extends AbstractMetablocking {

    protected long kThreshold;
    protected double minimumWeight;
    protected Queue<Comparison> topKEdges;
    protected Set<Integer> qIds;

    public CardinalityEdgePruning(WeightingScheme scheme) {
        super("Cardinality Edge Pruning (Top-K Edges)", scheme);
    }

    public CardinalityEdgePruning(String description, WeightingScheme scheme) {
        super(description, scheme);
    }

    public CardinalityEdgePruning( WeightingScheme scheme, Set<Integer> qIds) {
        super("", scheme);
        this.qIds = qIds;
    }


    private void addComparison(Comparison comparison) {
        if (comparison.getUtilityMeasure() < minimumWeight) {
            return;
        }

        topKEdges.add(comparison);
        if (kThreshold < topKEdges.size()) {
            Comparison lastComparison = topKEdges.poll();
            minimumWeight = lastComparison.getUtilityMeasure();
        }
    }

    @Override
    public void applyProcessing(List<AbstractBlock> blocks) {
        getStatistics(blocks, qIds);
        getKThreshold(blocks);
        filterComparisons(blocks);
        gatherComparisons(blocks);
    }

    protected void filterComparisons(List<AbstractBlock> blocks) {
        minimumWeight = Double.MIN_VALUE;
        topKEdges = new PriorityQueue<Comparison>((int) (2 * kThreshold), new ComparisonWeightComparator());
        Set<String> uComparisons = new HashSet<>();
        DumpDirectories dumpDirectories = new DumpDirectories();
        this.entityIndex.setEntityBlocks((int[][]) SerializationUtilities
                .loadSerializedObject(dumpDirectories.getBlockIndexDirPath() + "papers1mEntityBlocks"));
        for (AbstractBlock block : blocks) {
            //ComparisonIterator iterator = block.getComparisonIterator();
            QueryComparisonIterator iterator = block.getQueryComparisonIterator(qIds);

            while (iterator.hasNext()) {
                Comparison comparison = iterator.next();
                int id1 = comparison.getEntityId1();
                int id2 = comparison.getEntityId2();
//                double weight = getWeight(block.getBlockIndex(), comparison);
                if(id1 == id2) continue;
                double[] arr = getWeightIndex(block.getBlockIndex(), comparison);
                double weight = arr[0];
                if((int) arr[1] < block.getBlockIndex()){
                    continue;
                }

                if (weight < 0) {
                    continue;
                }

                comparison.setUtilityMeasure(weight);
                addComparison(comparison);
            }
        }
    }

    private void gatherComparisons(List<AbstractBlock> blocks) {
        boolean cleanCleanER = blocks.get(0) instanceof BilateralBlock;
        blocks.clear();
        blocks.add(getDecomposedBlock(cleanCleanER, topKEdges));
    }

    protected void getKThreshold(List<AbstractBlock> blocks) {
        long blockAssingments = 0;
        for (AbstractBlock block : blocks) {
            blockAssingments += block.getTotalBlockAssignments();
        }
        kThreshold = blockAssingments / 2;
        //kThreshold = (long) (kThreshold * 0.65);
        System.out.println(kThreshold);
    }
}
