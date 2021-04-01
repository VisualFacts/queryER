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

package org.imsi.queryEREngine.imsi.er.EfficiencyLayer.OnTheFlyMethods;

import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.Comparison;
import org.imsi.queryEREngine.imsi.er.EfficiencyLayer.ComparisonRefinement.AbstractDuplicatePropagation;
import org.imsi.queryEREngine.imsi.er.MetaBlocking.WeightingScheme;
import org.imsi.queryEREngine.imsi.er.Utilities.ComparisonIterator;

import java.util.List;

public class WeightedEdgePruning extends org.imsi.queryEREngine.imsi.er.MetaBlocking.WeightedEdgePruning {

    private double totalComparisons;
    private final AbstractDuplicatePropagation duplicatePropagation;

    public WeightedEdgePruning(AbstractDuplicatePropagation adp, WeightingScheme scheme) {
        super("On-the-fly Edge Pruning ("+scheme+")", scheme);
        duplicatePropagation = adp;
    }

    @Override
    protected void filterComparisons(List<AbstractBlock> blocks) {
        totalComparisons = 0;
        duplicatePropagation.resetDuplicates();
        for (AbstractBlock block : blocks) {
            ComparisonIterator iterator = block.getComparisonIterator();
            while (iterator.hasNext()) {
                Comparison comparison = iterator.next();
                double weight = getWeight(block.getBlockIndex(), comparison);
                if (weight < averageWeight) {
                    continue;
                }
                totalComparisons++;
                duplicatePropagation.isSuperfluous(comparison);
            }
        }
    }
    
    public double[] getPerformance() {
        double[] metrics = new double[3];
        metrics[0] = duplicatePropagation.getNoOfDuplicates() / ((double) duplicatePropagation.getExistingDuplicates()); //PC
        metrics[1] = duplicatePropagation.getNoOfDuplicates() / totalComparisons; //PQ
        metrics[2] = totalComparisons;
        return metrics;
    }
}
